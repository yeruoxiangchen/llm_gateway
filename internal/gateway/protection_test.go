package gateway

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestMySQLEntryProtection(t *testing.T) {
	dsn := os.Getenv("LLMGW_TEST_DSN")
	if dsn == "" || os.Getenv("LLMGW_TEST_REDIS_ADDRESS") == "" {
		t.Skip("use run.sh test for real MariaDB/Redis integration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	master := bytes.Repeat([]byte{6}, 32)
	direct, err := OpenStore(Config{DSN: dsn, MasterKey: master, LeaseTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	defer direct.Close()
	if err = direct.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	account, err := direct.CreateAccount(ctx, "protection-"+direct.OwnerID, 100000)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		for _, table := range []string{"attempts", "ledger"} {
			_, _ = direct.DB.ExecContext(ctx, "DELETE FROM "+table+" WHERE request_id IN (SELECT id FROM requests WHERE account_id=?)", account.ID)
		}
		for _, table := range []string{"requests", "api_keys"} {
			_, _ = direct.DB.ExecContext(ctx, "DELETE FROM "+table+" WHERE account_id=?", account.ID)
		}
		_, _ = direct.DB.ExecContext(ctx, "DELETE FROM accounts WHERE id=?", account.ID)
	}()
	_, secret, err := direct.CreateKey(ctx, account.ID, "protection", 100000)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	model := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = io.WriteString(w, `{"id":"test","object":"chat.completion","model":"test","choices":[{"index":0,"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}],"usage":{"prompt_tokens":5,"completion_tokens":7,"total_tokens":12}}`)
	}))
	defer model.Close()
	provider, err := direct.CreateProvider(ctx, Provider{Name: "protection", Alias: "protection-" + account.ID, Model: "test", BaseURL: model.URL + "/v1", Enabled: true}, "unused")
	if err != nil {
		t.Fatal(err)
	}
	defer direct.DB.ExecContext(ctx, "DELETE FROM providers WHERE id=?", provider.ID)
	config := Config{DSN: dsn, MasterKey: master, LeaseTTL: time.Minute, MaxInflight: 2, MaxDBConnections: 8, MetadataDBConnections: 2,
		RequestTimeout: 2 * time.Second, DatabaseReadTimeout: 100 * time.Millisecond, AdmissionTimeout: 250 * time.Millisecond,
		StreamIdle: time.Second, CombinedAdmission: true, RequestIDVersion: 7, DataDirectory: t.TempDir(),
		RedisAddress: os.Getenv("LLMGW_TEST_REDIS_ADDRESS"), RedisPassword: os.Getenv("LLMGW_TEST_REDIS_PASSWORD")}
	store, err := OpenStore(config)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	server := NewServer(config, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer server.Redis.Close()
	request := func(requestCtx context.Context, id string) *http.Request {
		body := fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"hello"}],"max_tokens":16}`, provider.Alias)
		r := httptest.NewRequest("POST", "/v1/chat/completions", strings.NewReader(body)).WithContext(requestCtx)
		r.Header.Set("Authorization", "Bearer "+secret)
		r.Header.Set("Idempotency-Key", id)
		return r
	}
	serve := func(r *http.Request) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		server.ServeHTTP(w, r)
		return w
	}
	authCount := func() uint64 {
		var value dto.Metric
		if err := server.stages.WithLabelValues("authenticate").(prometheus.Metric).Write(&value); err != nil {
			t.Fatal(err)
		}
		return value.GetHistogram().GetSampleCount()
	}
	wait := func(condition func() bool) {
		deadline := time.Now().Add(time.Second)
		for !condition() {
			if time.Now().After(deadline) {
				t.Fatal("request did not enter the expected blocked phase")
			}
			time.Sleep(time.Millisecond)
		}
	}
	checkEmpty := func(id string) {
		var count int
		if err := direct.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM requests WHERE account_id=? AND idempotency_key=?", account.ID, id).Scan(&count); err != nil || count != 0 {
			t.Fatalf("rejected request created a reservation: %d %v", count, err)
		}
		if len(server.inflight) != 0 {
			t.Fatal("entry slot was not released")
		}
	}

	t.Run("authentication_pool_wait_is_bounded", func(t *testing.T) {
		store.MetadataDB.SetMaxOpenConns(1)
		held, err := store.MetadataDB.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { held.Close(); store.MetadataDB.SetMaxOpenConns(2); store.MetadataDB.SetMaxIdleConns(2) }()
		before := store.MetadataDB.Stats().WaitCount
		started := time.Now()
		w := serve(request(ctx, "pool-timeout"))
		if w.Code != 503 || !strings.Contains(w.Body.String(), `"code":"authentication_timeout"`) || w.Header().Get("Retry-After") != "1" {
			t.Fatalf("pool exhaustion response: %d %s", w.Code, w.Body.String())
		}
		if time.Since(started) > 700*time.Millisecond || store.MetadataDB.Stats().WaitCount <= before || calls.Load() != 0 {
			t.Fatal("authentication did not leave the real pool wait within its budget")
		}
		checkEmpty("pool-timeout")
	})

	t.Run("write_pool_exhaustion_does_not_block_metadata", func(t *testing.T) {
		if store.DB.Stats().MaxOpenConnections+store.MetadataDB.Stats().MaxOpenConnections != 8 {
			t.Fatal("isolation increased the total database budget")
		}
		var held []*sql.Conn
		defer func() {
			for _, conn := range held {
				_ = conn.Close()
			}
		}()
		for range store.DB.Stats().MaxOpenConnections {
			conn, err := store.DB.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			held = append(held, conn)
		}
		before := authCount()
		metadataWaits := store.MetadataDB.Stats().WaitCount
		w := serve(request(ctx, "write-pool-exhausted"))
		if w.Code != 503 || !strings.Contains(w.Body.String(), `"code":"admission_timeout"`) || authCount() != before+1 {
			t.Fatalf("metadata was starved by writers: %d %s", w.Code, w.Body.String())
		}
		if store.MetadataDB.Stats().WaitCount != metadataWaits || calls.Load() != 0 {
			t.Fatal("read pool queued behind the full write pool")
		}
		checkEmpty("write-pool-exhausted")
	})

	t.Run("overload_precedes_authentication_and_lock_timeout_rolls_back", func(t *testing.T) {
		lock, err := direct.DB.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer lock.Rollback()
		if _, err = lock.ExecContext(ctx, "SELECT id FROM accounts WHERE id=? FOR UPDATE", account.ID); err != nil {
			t.Fatal(err)
		}
		before := authCount()
		responses := make(chan *httptest.ResponseRecorder, 2)
		for _, id := range []string{"locked-a", "locked-b"} {
			go func(id string) { responses <- serve(request(ctx, id)) }(id)
		}
		wait(func() bool { return len(server.inflight) == 2 && authCount() == before+2 })
		started := time.Now()
		w := serve(request(ctx, "over-capacity"))
		if w.Code != 503 || !strings.Contains(w.Body.String(), `"code":"gateway_busy"`) || w.Header().Get("Retry-After") != "1" {
			t.Fatalf("overload response: %d %s", w.Code, w.Body.String())
		}
		if time.Since(started) > 200*time.Millisecond || authCount() != before+2 {
			t.Fatal("excess request reached authentication or waited for SQL")
		}
		for range 2 {
			select {
			case value := <-responses:
				if value.Code != 503 || !strings.Contains(value.Body.String(), `"code":"admission_timeout"`) {
					t.Fatalf("locked admission response: %d %s", value.Code, value.Body.String())
				}
			case <-time.After(time.Second):
				t.Fatal("locked admission exceeded its budget")
			}
		}
		if calls.Load() != 0 {
			t.Fatal("timed-out admission called the model")
		}
		for _, id := range []string{"locked-a", "locked-b", "over-capacity"} {
			checkEmpty(id)
		}
		if err := lock.Rollback(); err != nil {
			t.Fatal(err)
		}
		w = serve(request(ctx, "locked-a"))
		if w.Code != 200 {
			t.Fatalf("recovery request failed: %d %s", w.Code, w.Body.String())
		}
		w = serve(request(ctx, "locked-a"))
		if w.Code != 200 || w.Header().Get("X-Idempotency-Replayed") != "true" || calls.Load() != 1 {
			t.Fatal("retry after recovery repeated model execution")
		}
		current, err := direct.Account(ctx, account.ID)
		if err != nil || current.Used != 12 || current.Reserved != 0 {
			t.Fatalf("recovered account: %+v %v", current, err)
		}
	})

	t.Run("client_cancel_releases_entry_and_transaction_wait", func(t *testing.T) {
		lock, err := direct.DB.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer lock.Rollback()
		if _, err = lock.ExecContext(ctx, "SELECT id FROM accounts WHERE id=? FOR UPDATE", account.ID); err != nil {
			t.Fatal(err)
		}
		before := authCount()
		requestCtx, stop := context.WithCancel(ctx)
		defer stop()
		done := make(chan struct{})
		go func() { serve(request(requestCtx, "cancelled-lock")); close(done) }()
		wait(func() bool { return len(server.inflight) == 1 && authCount() == before+1 })
		stop()
		select {
		case <-done:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("cancelled client retained its entry slot")
		}
		checkEmpty("cancelled-lock")
		if calls.Load() != 1 {
			t.Fatal("cancelled admission called the model")
		}
	})

	t.Run("uuid7_rollout_replays_existing_uuid4_requests", func(t *testing.T) {
		legacyConfig := config
		legacyConfig.RequestIDVersion = 4
		legacy, err := OpenStore(legacyConfig)
		if err != nil {
			t.Fatal(err)
		}
		defer legacy.Close()
		oldServer := NewServer(legacyConfig, legacy, slog.New(slog.NewTextHandler(io.Discard, nil)))
		defer oldServer.Redis.Close()
		before := calls.Load()
		oldResponse := httptest.NewRecorder()
		oldServer.ServeHTTP(oldResponse, request(ctx, "existing-v4"))
		oldID, err := uuid.Parse(oldResponse.Header().Get("X-Request-ID"))
		if oldResponse.Code != 200 || err != nil || oldID.Version() != 4 {
			t.Fatalf("legacy request: %d %s", oldResponse.Code, oldResponse.Body.String())
		}
		w := serve(request(ctx, "existing-v4"))
		if w.Code != 200 || w.Header().Get("X-Request-ID") != oldID.String() || w.Header().Get("X-Idempotency-Replayed") != "true" || calls.Load() != before+1 {
			t.Fatal("new UUID strategy did not preserve legacy idempotency")
		}
		w = serve(request(ctx, "fresh-v7"))
		newID, err := uuid.Parse(w.Header().Get("X-Request-ID"))
		if w.Code != 200 || err != nil || newID.Version() != 7 || calls.Load() != before+2 {
			t.Fatalf("new request: %d %s", w.Code, w.Body.String())
		}
		current, err := direct.Account(ctx, account.ID)
		if err != nil || current.Reserved != 0 {
			t.Fatalf("UUID rollout left a reservation: %+v %v", current, err)
		}
	})

	t.Run("lost_redis_reply_does_not_repeat_model_or_quota", func(t *testing.T) {
		credential, newSecret, err := direct.CreateKey(ctx, account.ID, "lost-redis-reply", 100000)
		if err != nil {
			t.Fatal(err)
		}
		original := server.Redis
		fault, dropped := replyDroppingRedis(t)
		server.Redis = fault
		defer func() { server.Redis = original }()
		beforeCalls := calls.Load()
		beforeAccount, err := direct.Account(ctx, account.ID)
		if err != nil {
			t.Fatal(err)
		}
		r := request(ctx, "redis-reply-loss")
		r.Header.Set("Authorization", "Bearer "+newSecret)
		w := serve(r)
		if w.Code != 200 {
			t.Fatalf("Redis retry failed: %d %s", w.Code, w.Body.String())
		}
		select {
		case <-dropped:
		default:
			t.Fatal("Redis fault did not execute")
		}
		if n, err := fault.HGet(ctx, "llmgw:rate:"+credential.ID, "count").Int(); err != nil || n != 1 {
			t.Fatalf("rate increment repeated: %d %v", n, err)
		}
		r = request(ctx, "redis-reply-loss")
		r.Header.Set("Authorization", "Bearer "+newSecret)
		w = serve(r)
		if w.Code != 200 || w.Header().Get("X-Idempotency-Replayed") != "true" || calls.Load() != beforeCalls+1 {
			t.Fatal("recovered rate operation repeated model execution")
		}
		current, err := direct.Account(ctx, account.ID)
		if err != nil || current.Used != beforeAccount.Used+12 || current.Reserved != 0 {
			t.Fatalf("quota changed twice: %+v %v", current, err)
		}
	})
}
