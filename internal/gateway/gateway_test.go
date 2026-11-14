package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	openai "github.com/sashabaranov/go-openai"
)

func TestCredentialEncryptionAndTampering(t *testing.T) {
	key := bytes.Repeat([]byte{7}, 32)
	first, err := encrypt(key, []byte("provider-secret"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := encrypt(key, []byte("provider-secret"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(first, second) || bytes.Contains(first, []byte("provider-secret")) {
		t.Fatal("encryption did not conceal or randomize the credential")
	}
	plain, err := decrypt(key, first)
	if err != nil || plain != "provider-secret" {
		t.Fatalf("round trip: %q %v", plain, err)
	}
	first[len(first)-1] ^= 1
	if _, err = decrypt(key, first); err == nil {
		t.Fatal("tampered credential accepted")
	}
	if _, err = decrypt(key, []byte{1}); err == nil {
		t.Fatal("short ciphertext accepted")
	}
}

func TestUsageAndCostContract(t *testing.T) {
	cases := []struct {
		name  string
		value Usage
		valid bool
	}{
		{"normal", Usage{5, 7, 12}, true}, {"total omitted", Usage{5, 7, 0}, true},
		{"negative", Usage{-1, 7, 6}, false}, {"disagreement", Usage{5, 7, 13}, false},
		{"overflow guard", Usage{10000001, 1, 10000002}, false}, {"zero", Usage{}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := normalizeUsage(&tc.value); (err == nil) != tc.valid {
				t.Fatalf("usage validation: %v", err)
			}
		})
	}
	if value, err := cost(Usage{Prompt: 5, Completion: 7}, 1000000, 2000000); err != nil || value != 19 {
		t.Fatalf("price calculation: %d %v", value, err)
	}
	if value, _ := cost(Usage{Prompt: 1}, 1, 0); value != 1 {
		t.Fatalf("sub-micro-USD cost must round up, got %d", value)
	}
	if _, err := cost(Usage{}, -1, 0); err == nil {
		t.Fatal("negative rate accepted")
	}
	response := openai.ChatCompletionResponse{Model: "public", Usage: openai.Usage{PromptTokens: 5, CompletionTokens: 7}}
	unknown, err := encodeCompletion(response, nil)
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]json.RawMessage
	if err = json.Unmarshal(unknown, &value); err != nil {
		t.Fatal(err)
	}
	if _, exists := value["usage"]; exists {
		t.Fatal("unknown usage was exposed as a zero-token receipt")
	}
	known, err := encodeCompletion(response, &Usage{Prompt: 5, Completion: 7, Total: 12})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(known, []byte(`"total_tokens":12`)) {
		t.Fatal("wire usage disagrees with the ledger", string(known))
	}
}

func TestRoutingAndCircuitIsolation(t *testing.T) {
	s := NewServer(Config{MaxInflight: 4, RedisAddress: "127.0.0.1:1"}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer s.Redis.Close()
	providers := []Provider{{ID: "a", Priority: 2}, {ID: "b", Priority: 2}, {ID: "c", Priority: 1}}
	if route := s.candidates("alias", providers); route[0].ID != "a" || route[2].ID != "c" {
		t.Fatal(route)
	}
	if route := s.candidates("alias", providers); route[0].ID != "b" || route[2].ID != "c" {
		t.Fatal(route)
	}
	for n := 0; n < 3; n++ {
		done, err := s.circuit(providers[0])
		if err != nil {
			t.Fatal(err)
		}
		done(false)
	}
	if _, err := s.circuit(providers[0]); err == nil {
		t.Fatal("failed provider circuit stayed closed")
	}
	done, err := s.circuit(providers[1])
	if err != nil {
		t.Fatal("healthy provider was affected", err)
	}
	done(true)
}

func TestStreamingCompletionAndTruncation(t *testing.T) {
	for _, complete := range []bool{true, false} {
		t.Run(fmt.Sprint(complete), func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				fmt.Fprint(w, "data: {\"id\":\"x\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello\"}}]}\n\n")
				if complete {
					fmt.Fprint(w, "data: {\"id\":\"x\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: {\"id\":\"x\",\"choices\":[],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":7,\"total_tokens\":12}}\n\ndata: [DONE]\n\n")
				}
			}))
			defer upstream.Close()
			config := openai.DefaultConfig("fixture")
			config.BaseURL = upstream.URL
			s := NewServer(Config{MaxInflight: 1, StreamIdle: time.Second}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
			defer s.Redis.Close()
			writer := httptest.NewRecorder()
			committed, usage, replay, err := s.forwardStream(context.Background(), writer, openai.NewClientWithConfig(config), openai.ChatCompletionRequest{Model: "hidden"}, "public", time.Now())
			if !committed {
				t.Fatal("stream was buffered")
			}
			if complete {
				if err != nil || usage == nil || usage.Total != 12 || !bytes.Contains(replay, []byte("[DONE]")) || !bytes.Contains(replay, []byte(`"model":"public"`)) {
					t.Fatalf("completion contract: %v %v %s", err, usage, replay)
				}
			} else if err == nil || usage != nil || strings.Contains(writer.Body.String(), "[DONE]") {
				t.Fatalf("truncated stream accepted: %v %v", err, usage)
			}
		})
	}
}

// This test uses the project's real, isolated MariaDB; no mocked SQL transaction.
func TestMySQLConcurrentLedgerAndCrashRecovery(t *testing.T) {
	dsn := os.Getenv("LLMGW_TEST_DSN")
	if dsn == "" {
		t.Skip("set LLMGW_TEST_DSN to run the isolated database integration test")
	}
	s, err := OpenStore(Config{DSN: dsn, MasterKey: bytes.Repeat([]byte{1}, 32), LeaseTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	defer s.DB.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	a, err := s.CreateAccount(ctx, "integration-"+s.OwnerID, 10000)
	if err != nil {
		t.Fatal(err)
	}
	key, _, err := s.CreateKey(ctx, a.ID, "integration", 1000)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		for _, table := range []string{"attempts", "ledger"} {
			_, _ = s.DB.Exec("DELETE FROM "+table+" WHERE request_id IN (SELECT id FROM requests WHERE account_id=?)", a.ID)
		}
		_, _ = s.DB.Exec("DELETE FROM requests WHERE account_id=?", a.ID)
		_, _ = s.DB.Exec("DELETE FROM api_keys WHERE account_id=?", a.ID)
		_, _ = s.DB.Exec("DELETE FROM accounts WHERE id=?", a.ID)
		_, _ = s.DB.Exec("DELETE FROM owner_leases WHERE owner_id=?", s.OwnerID)
	}()
	hash := sha256.Sum256([]byte("same request"))
	type result struct {
		request Request
		fresh   bool
		err     error
	}
	results := make(chan result, 16)
	var workers sync.WaitGroup
	for n := 0; n < 16; n++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			r, fresh, err := s.Reserve(ctx, key, "test", "same", hash[:], false, 2000)
			results <- result{r, fresh, err}
		}()
	}
	workers.Wait()
	close(results)
	var record Request
	freshCount := 0
	for value := range results {
		if value.err != nil {
			t.Fatal(value.err)
		}
		if record.ID != "" && record.ID != value.request.ID {
			t.Fatal("duplicate request IDs")
		}
		record = value.request
		if value.fresh {
			freshCount++
		}
	}
	if freshCount != 1 {
		t.Fatalf("fresh reservations: %d", freshCount)
	}
	receipt := Receipt{RequestID: record.ID, AccountID: a.ID, Usage: &Usage{Prompt: 5, Completion: 7, Total: 12}, Outcome: "succeeded", HTTPStatus: 200}
	beatContext, stopBeat := context.WithCancel(ctx)
	defer stopBeat()
	beatDone := make(chan struct{})
	beatErrors := make(chan error, 1)
	go func() {
		defer close(beatDone)
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-beatContext.Done():
				return
			case <-ticker.C:
				if err := s.Heartbeat(ctx); err != nil {
					beatErrors <- err
					return
				}
			}
		}
	}()
	errorsCh := make(chan error, 16)
	for n := 0; n < 16; n++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			copy := receipt
			usage := *receipt.Usage
			copy.Usage = &usage
			errorsCh <- s.Finish(ctx, copy)
		}()
	}
	workers.Wait()
	stopBeat()
	<-beatDone
	select {
	case err := <-beatErrors:
		t.Fatal("heartbeat/settlement conflict", err)
	default:
	}
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	account, err := s.Account(ctx, a.ID)
	if err != nil || account.Used != 12 || account.Reserved != 0 {
		t.Fatalf("double settlement: %+v %v", account, err)
	}
	conflicting := receipt
	conflicting.Usage = &Usage{Prompt: 5, Completion: 8, Total: 13}
	if err = s.Finish(ctx, conflicting); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting receipt accepted: %v", err)
	}
	t.Run("disconnect_after_reservation_commit", func(t *testing.T) {
		disconnected, cancel := context.WithCancel(ctx)
		defer cancel()
		s.Observe = func(_ context.Context, phase string, _ time.Time) {
			if phase == "reserve_commit" {
				cancel()
			}
		}
		r, fresh, err := s.Reserve(disconnected, key, "test", "disconnect-after-commit", hash[:], false, 1000)
		s.Observe = nil
		if err != nil || !fresh || r.ID == "" {
			t.Fatalf("committed reservation lost to caller: fresh=%v id=%q err=%v", fresh, r.ID, err)
		}
		if err = s.Finish(ctx, Receipt{RequestID: r.ID, AccountID: a.ID, Usage: &Usage{}, Outcome: "cancelled", HTTPStatus: 499}); err != nil {
			t.Fatal(err)
		}
	})
	for _, dispatched := range []bool{false, true} {
		r, _, err := s.Reserve(ctx, key, "test", "", hash[:], false, 1000)
		if err != nil {
			t.Fatal(err)
		}
		if dispatched {
			if _, err = s.Dispatch(ctx, r, Provider{ID: "fixture"}); err != nil {
				t.Fatal(err)
			}
		}
		if _, err = s.DB.ExecContext(ctx, "UPDATE requests SET lease_until=UTC_TIMESTAMP(6)-INTERVAL 1 SECOND WHERE id=?", r.ID); err != nil {
			t.Fatal(err)
		}
		if err = s.Heartbeat(ctx); err != nil {
			t.Fatal(err)
		}
		var unchanged bool
		if err = s.DB.QueryRowContext(ctx, "SELECT lease_until<UTC_TIMESTAMP(6) FROM requests WHERE id=?", r.ID).Scan(&unchanged); err != nil {
			t.Fatal(err)
		}
		if !unchanged {
			t.Fatal("heartbeat rewrote request rows and reintroduced the settlement lock inversion")
		}
		if err = s.Recover(ctx); err != nil {
			t.Fatal(err)
		}
		live, err := s.Request(ctx, r.ID)
		if err != nil {
			t.Fatal(err)
		}
		liveState := "reserved"
		if dispatched {
			liveState = "dispatched"
		}
		if live.State != liveState {
			t.Fatalf("live owner was recovered: %s", live.State)
		}
		if _, err = s.DB.ExecContext(ctx, "UPDATE owner_leases SET lease_until=UTC_TIMESTAMP(6)-INTERVAL 1 SECOND WHERE owner_id=?", s.OwnerID); err != nil {
			t.Fatal(err)
		}
		if err = s.Recover(ctx); err != nil {
			t.Fatal(err)
		}
		got, err := s.Request(ctx, r.ID)
		if err != nil {
			t.Fatal(err)
		}
		expected := "released"
		if dispatched {
			expected = "pending_usage"
		}
		if got.State != expected {
			t.Fatalf("crash recovery %v: %s", dispatched, got.State)
		}
		if dispatched {
			if err = s.Finish(ctx, Receipt{RequestID: r.ID, AccountID: a.ID, Usage: &Usage{}, Resolve: true, Reason: "integration refund"}); err != nil {
				t.Fatal(err)
			}
		}
	}
	var used, reserved int64
	if err = s.DB.QueryRowContext(ctx, "SELECT COALESCE(SUM(used_delta),0),COALESCE(SUM(reserved_delta),0) FROM ledger WHERE account_id=?", a.ID).Scan(&used, &reserved); err != nil {
		t.Fatal(err)
	}
	account, _ = s.Account(ctx, a.ID)
	if used != account.Used || reserved != account.Reserved || reserved != 0 {
		t.Fatalf("ledger drift: %d/%d %+v", used, reserved, account)
	}
}
