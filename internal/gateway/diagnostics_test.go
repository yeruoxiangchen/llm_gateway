package gateway

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestPoolMetricsAndBoundedSlowCommitLogs(t *testing.T) {
	db, err := sql.Open("mysql", "unused@tcp(127.0.0.1:1)/unused")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(16)
	var logs bytes.Buffer
	s := NewServer(Config{MaxInflight: 1, AdminToken: "metrics-admin"}, &Store{DB: db}, slog.New(slog.NewJSONHandler(&logs, nil)))
	defer s.Redis.Close()
	for _, auth := range []bool{false, true} {
		r := httptest.NewRequest("GET", "/metrics", nil)
		if auth {
			r.Header.Set("Authorization", "Bearer metrics-admin")
		}
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		if !auth && w.Code != 401 {
			t.Fatal("pool metrics exposed without authentication")
		}
		if auth && (w.Code != 200 || !strings.Contains(w.Body.String(), `go_sql_max_open_connections{db_name="gateway"} 16`)) {
			t.Fatal("missing database/sql pool statistics")
		}
	}
	ctx := context.WithValue(context.Background(), observationKey{}, &requestObservation{Idempotency: "private-header-do-not-log"})
	observeRequestID(ctx, "request-correlation-id")
	s.observe(ctx, "reserve_commit", time.Now())
	s.observe(ctx, "upstream", time.Now().Add(-time.Second))
	if logs.Len() != 0 {
		t.Fatal("ordinary operations generated slow commit logs")
	}
	for range 10 {
		s.observe(ctx, "reserve_commit", time.Now().Add(-time.Second))
	}
	if bytes.Count(logs.Bytes(), []byte("\n")) != 1 || strings.Contains(logs.String(), "private-header") {
		t.Fatal("slow log exceeded its bound or exposed an idempotency header")
	}
	var event map[string]any
	if err := json.Unmarshal(logs.Bytes(), &event); err != nil || event["request_id"] != "request-correlation-id" || event["started_at"] == nil {
		t.Fatalf("slow event lacks correlation fields: %v", err)
	}
}

func TestDiagnosticsAndCheckpointsAreOptIn(t *testing.T) {
	s := NewServer(Config{MaxInflight: 1}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer s.Redis.Close()
	for _, path := range []string{"/debug/pprof/heap", "/admin/testing/state"} {
		w := httptest.NewRecorder()
		s.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 404 {
			t.Fatalf("default endpoint %s exposed: %d", path, w.Code)
		}
	}
}

func TestDiagnosticAuthenticationAndProfileBounds(t *testing.T) {
	s := NewServer(Config{MaxInflight: 1, Diagnostics: true, AdminToken: "isolated-diagnostics-admin-credential"}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer s.Redis.Close()
	for _, tc := range []struct {
		path   string
		auth   bool
		status int
	}{{"/debug/pprof/heap", false, 401}, {"/debug/pprof/profile?seconds=0", true, 400}, {"/debug/pprof/profile?seconds=21", true, 400}, {"/debug/pprof/goroutine?debug=1", true, 200}} {
		r := httptest.NewRequest("GET", tc.path, nil)
		if tc.auth {
			r.Header.Set("Authorization", "Bearer "+s.Config.AdminToken)
		}
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatalf("%s: %d", tc.path, w.Code)
		}
	}
}

func TestCheckpointAuthorizationReleaseAndCancellation(t *testing.T) {
	s := NewServer(Config{MaxInflight: 1, FaultInjection: true, AdminToken: "isolated-diagnostics-admin-credential"}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer s.Redis.Close()
	arm := func(auth bool) int {
		r := httptest.NewRequest("POST", "/admin/testing/arm", bytes.NewBufferString(`{"point":"before_settle","idempotency_key":"test"}`))
		if auth {
			r.Header.Set("Authorization", "Bearer "+s.Config.AdminToken)
		}
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		return w.Code
	}
	if arm(false) != http.StatusUnauthorized || arm(true) != http.StatusCreated {
		t.Fatal("checkpoint authorization")
	}
	ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), observationKey{}, &requestObservation{Idempotency: "test"}), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.checkpoint(ctx, "before_settle", "request") }()
	deadline := time.Now().Add(time.Second)
	for {
		s.faults.Lock()
		ready := s.faults.gates["before_settle"].Arrived
		s.faults.Unlock()
		if ready {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("checkpoint not reached")
		}
		time.Sleep(time.Millisecond)
	}
	r := httptest.NewRequest("POST", "/admin/testing/release", bytes.NewBufferString(`{"point":"before_settle"}`))
	r.Header.Set("Authorization", "Bearer "+s.Config.AdminToken)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if arm(true) != 201 {
		t.Fatal("rearm failed")
	}
	cancel()
	if err := s.checkpoint(ctx, "before_settle", "request"); err != context.Canceled {
		t.Fatalf("cancelled checkpoint blocked: %v", err)
	}
}
