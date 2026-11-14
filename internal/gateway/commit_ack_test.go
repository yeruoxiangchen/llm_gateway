package gateway

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
)

// Relay real uncompressed MySQL packets, then drop the first successful COMMIT
// reply after reading it from MariaDB. SQL execution and persistence are real.
func commitAckDropProxy(t *testing.T, target string) (string, <-chan struct{}, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	dropped := make(chan struct{}, 1)
	var armed atomic.Bool
	armed.Store(true)
	var wg sync.WaitGroup
	var mutex sync.Mutex
	connections := map[net.Conn]bool{}
	packet := func(c net.Conn) ([]byte, error) {
		header := make([]byte, 4)
		if _, err := io.ReadFull(c, header); err != nil {
			return nil, err
		}
		size := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
		if size > 1024*1024 {
			return nil, errors.New("test proxy packet too large")
		}
		value := make([]byte, 4+size)
		copy(value, header)
		_, err := io.ReadFull(c, value[4:])
		return value, err
	}
	write := func(c net.Conn, value []byte) error {
		n, err := c.Write(value)
		if err == nil && n != len(value) {
			return io.ErrShortWrite
		}
		return err
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			server, err := net.DialTimeout("tcp", target, 3*time.Second)
			if err != nil {
				_ = client.Close()
				continue
			}
			mutex.Lock()
			connections[client] = true
			connections[server] = true
			mutex.Unlock()
			wg.Add(1)
			go func(client, server net.Conn) {
				defer wg.Done()
				defer func() {
					_ = client.Close()
					_ = server.Close()
					mutex.Lock()
					delete(connections, client)
					delete(connections, server)
					mutex.Unlock()
				}()
				var pending atomic.Bool
				writerDone := make(chan struct{})
				go func() {
					defer close(writerDone)
					defer server.Close()
					for {
						value, err := packet(client)
						if err != nil {
							return
						}
						if len(value) > 5 && value[4] == 3 && strings.EqualFold(strings.TrimSpace(string(value[5:])), "COMMIT") && armed.CompareAndSwap(true, false) {
							pending.Store(true)
						}
						if write(server, value) != nil {
							return
						}
					}
				}()
				for {
					value, err := packet(server)
					if err != nil {
						break
					}
					if pending.Load() && len(value) > 4 && value[4] == 0 {
						dropped <- struct{}{}
						break
					}
					if write(client, value) != nil {
						break
					}
				}
				_ = client.Close()
				_ = server.Close()
				<-writerDone
			}(client, server)
		}
	}()
	closeProxy := func() {
		_ = listener.Close()
		mutex.Lock()
		for c := range connections {
			_ = c.Close()
		}
		mutex.Unlock()
		wg.Wait()
	}
	return listener.Addr().String(), dropped, closeProxy
}

func TestMySQLLostAdmissionCommitAcknowledgement(t *testing.T) {
	dsn := os.Getenv("LLMGW_TEST_DSN")
	if dsn == "" || os.Getenv("LLMGW_TEST_REDIS_ADDRESS") == "" {
		t.Skip("use run.sh test for real MariaDB/Redis integration")
	}
	master := bytes.Repeat([]byte{9}, 32)
	direct, err := OpenStore(Config{DSN: dsn, MasterKey: master, LeaseTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	defer direct.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if err = direct.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	account, err := direct.CreateAccount(ctx, "commit-loss-"+uuid.NewString(), 10000)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		for _, table := range []string{"attempts", "ledger"} {
			_, _ = direct.DB.Exec("DELETE FROM "+table+" WHERE request_id IN (SELECT id FROM requests WHERE account_id=?)", account.ID)
		}
		for _, table := range []string{"requests", "api_keys"} {
			_, _ = direct.DB.Exec("DELETE FROM "+table+" WHERE account_id=?", account.ID)
		}
		_, _ = direct.DB.Exec("DELETE FROM accounts WHERE id=?", account.ID)
	}()
	_, secret, err := direct.CreateKey(ctx, account.ID, "commit-loss", 1000)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = io.WriteString(w, `{"choices":[{"message":{"role":"assistant","content":"unexpected"}}],"usage":{"prompt_tokens":5,"completion_tokens":7,"total_tokens":12}}`)
	}))
	defer model.Close()
	provider, err := direct.CreateProvider(ctx, Provider{Name: "commit-loss", Alias: "commit-loss-" + account.ID, Model: "test", BaseURL: model.URL + "/v1", Enabled: true}, "unused")
	if err != nil {
		t.Fatal(err)
	}
	defer direct.DB.Exec("DELETE FROM providers WHERE id=?", provider.ID)
	parsed, err := mysql.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	address, dropped, closeProxy := commitAckDropProxy(t, parsed.Addr)
	defer closeProxy()
	parsed.Addr = address
	config := Config{DSN: parsed.FormatDSN(), MasterKey: master, LeaseTTL: time.Minute, MaxInflight: 4, MaxDBConnections: 8, MetadataDBConnections: 2, RequestTimeout: 30 * time.Second, StreamIdle: 3 * time.Second,
		CombinedAdmission: true, RequestIDVersion: 7, DataDirectory: t.TempDir(), RedisAddress: os.Getenv("LLMGW_TEST_REDIS_ADDRESS"), RedisPassword: os.Getenv("LLMGW_TEST_REDIS_PASSWORD")}
	proxied, err := OpenStore(config)
	if err != nil {
		t.Fatal(err)
	}
	defer proxied.Close()
	server := NewServer(config, proxied, slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer server.Redis.Close()
	body, _ := json.Marshal(map[string]any{"model": provider.Alias, "messages": []map[string]string{{"role": "user", "content": "hello"}}, "max_tokens": 16})
	request := httptest.NewRequest("POST", "/v1/chat/completions", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+secret)
	request.Header.Set("Idempotency-Key", "commit-loss")
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	select {
	case <-dropped:
	default:
		t.Fatal("real commit acknowledgement was not dropped")
	}
	if calls.Load() != 0 {
		t.Fatalf("model was dispatched after uncertain admission: %d", calls.Load())
	}
	if response.Code != 503 {
		t.Fatalf("unexpected admission response %d", response.Code)
	}
	id := response.Header().Get("X-Request-ID")
	if id == "" {
		t.Fatal("lost admission identity")
	}
	r, err := direct.Request(ctx, id)
	if err != nil || r.State != "released" || r.Usage == nil || r.Usage.Total != 0 {
		t.Fatalf("commit loss stranded request: %+v %v", r, err)
	}
	current, err := direct.Account(ctx, account.ID)
	if err != nil || current.Used != 0 || current.Reserved != 0 {
		t.Fatalf("quota stranded after lost ack: %+v %v", current, err)
	}
	receipt := Receipt{RequestID: id, AccountID: account.ID, Usage: &Usage{}, NotDispatched: true, Outcome: "failed", HTTPStatus: 503}
	if err = direct.Finish(ctx, receipt); err != nil {
		t.Fatal(err)
	}
	var events int
	if err = direct.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM ledger WHERE request_id=? AND event_type='settle'", id).Scan(&events); err != nil || events != 1 {
		t.Fatalf("duplicate release: %d %v", events, err)
	}
	files, err := os.ReadDir(config.DataDirectory + "/spool")
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if len(files) != 0 {
		t.Fatal("unexpected remaining abort receipts")
	}

	t.Run("rolled_back_admission_does_not_strand_abort_receipt", func(t *testing.T) {
		missing := Receipt{RequestID: uuid.NewString(), AccountID: account.ID, Usage: &Usage{}, NotDispatched: true, Outcome: "failed", HTTPStatus: 503}
		if err := direct.Finish(ctx, missing); err != nil {
			t.Fatal(err)
		}
		if _, err := direct.Request(ctx, missing.RequestID); !errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("created a fake request for rollback: %v", err)
		}
		missing.Usage = &Usage{Prompt: 1, Total: 1}
		if err := direct.Finish(ctx, missing); err == nil {
			t.Fatal("nonzero usage accepted as not dispatched")
		}
	})
}
