package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMySQLCombinedAdmission(t *testing.T) {
	dsn := os.Getenv("LLMGW_TEST_DSN")
	if dsn == "" {
		t.Skip("set LLMGW_TEST_DSN for real transaction tests")
	}
	s, err := OpenStore(Config{DSN: dsn, MasterKey: bytes.Repeat([]byte{1}, 32), LeaseTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	defer s.DB.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err = s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	a, err := s.CreateAccount(ctx, "combined-"+s.OwnerID, 100000)
	if err != nil {
		t.Fatal(err)
	}
	key, _, err := s.CreateKey(ctx, a.ID, "combined", 10000)
	if err != nil {
		t.Fatal(err)
	}
	accountIDs := []string{a.ID}
	defer func() {
		for _, accountID := range accountIDs {
			for _, table := range []string{"attempts", "ledger"} {
				_, _ = s.DB.Exec("DELETE FROM "+table+" WHERE request_id IN (SELECT id FROM requests WHERE account_id=?)", accountID)
			}
			for _, table := range []string{"requests", "api_keys"} {
				_, _ = s.DB.Exec("DELETE FROM "+table+" WHERE account_id=?", accountID)
			}
			_, _ = s.DB.Exec("DELETE FROM accounts WHERE id=?", accountID)
		}
		_, _ = s.DB.Exec("DELETE FROM owner_leases WHERE owner_id=?", s.OwnerID)
	}()
	p := Provider{ID: "admit-provider", InputPrice: 1000000, OutputPrice: 2000000}
	hash := sha256.Sum256([]byte("admit"))

	t.Run("one_fresh_dispatch_for_concurrent_duplicates", func(t *testing.T) {
		var selected atomic.Int32
		type result struct {
			r       Request
			fresh   bool
			attempt int64
			err     error
		}
		values := make(chan result, 16)
		var wg sync.WaitGroup
		for range 16 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				r, f, id, e := s.Admit(ctx, key, "test", "same", hash[:], false, 1000, func() (Provider, error) { selected.Add(1); return p, nil })
				values <- result{r, f, id, e}
			}()
		}
		wg.Wait()
		close(values)
		var record Request
		var attempt int64
		fresh := 0
		for v := range values {
			if v.err != nil {
				t.Fatal(v.err)
			}
			if record.ID != "" && record.ID != v.r.ID {
				t.Fatal("duplicate request identity")
			}
			record = v.r
			if v.fresh {
				fresh++
				attempt = v.attempt
			}
		}
		if fresh != 1 || selected.Load() != 1 || attempt == 0 || record.State != "dispatched" {
			t.Fatalf("fresh=%d selections=%d attempt=%d state=%s", fresh, selected.Load(), attempt, record.State)
		}
		var attempts, events int
		if err := s.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM attempts WHERE request_id=?", record.ID).Scan(&attempts); err != nil {
			t.Fatal(err)
		}
		if err := s.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM ledger WHERE request_id=? AND event_type='reserve'", record.ID).Scan(&events); err != nil {
			t.Fatal(err)
		}
		if attempts != 1 || events != 1 {
			t.Fatal("reservation/dispatch not unique")
		}
		stored, e := s.Request(ctx, record.ID)
		if e != nil || stored.ProviderID != p.ID || stored.InputPrice != p.InputPrice || stored.OutputPrice != p.OutputPrice {
			t.Fatalf("dispatch snapshot: %+v %v", stored, e)
		}
		if err := s.Finish(ctx, Receipt{RequestID: record.ID, AccountID: a.ID, AttemptID: attempt, Usage: &Usage{Prompt: 5, Completion: 7, Total: 12}, Outcome: "succeeded", HTTPStatus: 200}); err != nil {
			t.Fatal(err)
		}
		stored, _ = s.Request(ctx, record.ID)
		if stored.Cost != 19 {
			t.Fatalf("snapshot prices not used: %d", stored.Cost)
		}
	})

	t.Run("dispatch_failure_rolls_back_reservation", func(t *testing.T) {
		aborted, stop := context.WithCancel(ctx)
		defer stop()
		_, _, _, err := s.Admit(aborted, key, "test", "rollback", hash[:], false, 1000, func() (Provider, error) { stop(); return p, nil })
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected cancellation: %v", err)
		}
		var count int
		if err := s.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM requests WHERE account_id=? AND idempotency_key='rollback'", a.ID).Scan(&count); err != nil {
			t.Fatal(err)
		}
		account, err := s.Account(ctx, a.ID)
		if err != nil || count != 0 || account.Reserved != 0 || account.Used != 12 {
			t.Fatalf("partial admission: count=%d account=%+v err=%v", count, account, err)
		}
	})

	t.Run("cancel_after_ack_keeps_committed_identity", func(t *testing.T) {
		disconnected, stop := context.WithCancel(ctx)
		defer stop()
		s.Observe = func(_ context.Context, phase string, _ time.Time) {
			if phase == "admit_commit" {
				stop()
			}
		}
		r, fresh, attempt, err := s.Admit(disconnected, key, "test", "ack", hash[:], false, 1000, func() (Provider, error) { return p, nil })
		s.Observe = nil
		if err != nil || !fresh || r.ID == "" || attempt == 0 {
			t.Fatalf("lost acknowledged identity: %+v %d %v", r, attempt, err)
		}
		if err = s.Finish(ctx, Receipt{RequestID: r.ID, AccountID: a.ID, AttemptID: attempt, Usage: &Usage{}, Outcome: "cancelled", HTTPStatus: 499}); err != nil {
			t.Fatal(err)
		}
		var status int
		if err := s.DB.QueryRowContext(ctx, "SELECT status FROM attempts WHERE id=?", attempt).Scan(&status); err != nil || status != 499 {
			t.Fatalf("cancelled attempt not finalized: %d %v", status, err)
		}
	})

	t.Run("crash_after_admission_retains_unknown_usage", func(t *testing.T) {
		r, _, _, err := s.Admit(ctx, key, "test", "crash", hash[:], false, 1000, func() (Provider, error) { return p, nil })
		if err != nil {
			t.Fatal(err)
		}
		if _, err = s.DB.ExecContext(ctx, "UPDATE requests SET lease_until=UTC_TIMESTAMP(6)-INTERVAL 1 SECOND WHERE id=?", r.ID); err != nil {
			t.Fatal(err)
		}
		if err = s.Recover(ctx); err != nil {
			t.Fatal(err)
		}
		stored, err := s.Request(ctx, r.ID)
		account, _ := s.Account(ctx, a.ID)
		if err != nil || stored.State != "pending_usage" || stored.Usage != nil || account.Reserved != 1000 {
			t.Fatalf("unsafe recovery: %+v %+v %v", stored, account, err)
		}
		if err = s.Finish(ctx, Receipt{RequestID: r.ID, AccountID: a.ID, Usage: &Usage{}, Resolve: true, Reason: "controlled test confirms no upstream send"}); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("admission_does_not_lock_another_accounts_active_request", func(t *testing.T) {
		other, err := s.CreateAccount(ctx, "other-"+s.OwnerID, 100000)
		if err != nil {
			t.Fatal(err)
		}
		accountIDs = append(accountIDs, other.ID)
		otherKey, _, err := s.CreateKey(ctx, other.ID, "other", 1000)
		if err != nil {
			t.Fatal(err)
		}
		locked, _, _, err := s.Admit(ctx, key, "test", "locked-unrelated", hash[:], false, 1000, func() (Provider, error) { return p, nil })
		if err != nil {
			t.Fatal(err)
		}
		blocker, err := s.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
		if err != nil {
			t.Fatal(err)
		}
		defer blocker.Rollback()
		var identity string
		if err = blocker.QueryRowContext(ctx, "SELECT id FROM requests WHERE id=? FOR UPDATE", locked.ID).Scan(&identity); err != nil {
			t.Fatal(err)
		}
		bounded, stop := context.WithTimeout(ctx, 15*time.Second)
		defer stop()
		type result struct {
			r       Request
			attempt int64
			err     error
		}
		finished := make(chan result, 1)
		go func() {
			r, _, id, e := s.Admit(bounded, otherKey, "test", "independent-account", hash[:], false, 1000, func() (Provider, error) { return p, nil })
			finished <- result{r, id, e}
		}()
		var got result
		select {
		case got = <-finished:
		case <-bounded.Done():
			_ = blocker.Rollback()
			<-finished
			t.Fatal("independent admission blocked while another account's request was locked")
		}
		if got.err != nil {
			t.Fatal(got.err)
		}
		_ = blocker.Rollback()
		for _, r := range []Request{locked, got.r} {
			if err = s.Finish(ctx, Receipt{RequestID: r.ID, AccountID: r.AccountID, Usage: &Usage{}, Outcome: "cancelled", HTTPStatus: 499}); err != nil {
				t.Fatal(err)
			}
		}
	})

	t.Run("parallel_accounts_complete_without_deadlock", func(t *testing.T) {
		keys := []Key{key}
		for n := 0; n < 3; n++ {
			account, err := s.CreateAccount(ctx, fmt.Sprintf("parallel-%d-%s", n, s.OwnerID), 100000)
			if err != nil {
				t.Fatal(err)
			}
			accountIDs = append(accountIDs, account.ID)
			created, _, err := s.CreateKey(ctx, account.ID, "parallel", 1000)
			if err != nil {
				t.Fatal(err)
			}
			keys = append(keys, created)
		}
		var wg sync.WaitGroup
		failures := make(chan error, 8)
		for worker := 0; worker < 8; worker++ {
			wg.Add(1)
			go func(worker int) {
				defer wg.Done()
				for step := 0; step < 8; step++ {
					r, _, attempt, err := s.Admit(ctx, keys[worker%len(keys)], "test", fmt.Sprintf("parallel-%d-%d", worker, step), hash[:], false, 1000, func() (Provider, error) { return p, nil })
					if err != nil {
						failures <- err
						return
					}
					err = s.Finish(ctx, Receipt{RequestID: r.ID, AccountID: r.AccountID, AttemptID: attempt, Usage: &Usage{}, Outcome: "cancelled", HTTPStatus: 499})
					if err != nil {
						failures <- err
						return
					}
				}
			}(worker)
		}
		wg.Wait()
		close(failures)
		for err := range failures {
			t.Error(err)
		}
	})
}
