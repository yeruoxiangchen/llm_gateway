package gateway

import (
	"context"
	"net/http"
	"time"
)

type chatOutcomeKey struct{}

// The handler updates this record synchronously before returning through the
// entry middleware. Counting here includes authentication and early rejection.
type chatOutcome struct {
	started time.Time
	model   string
	result  string
}

func (s *Server) protectionError(w http.ResponseWriter, code, message, id string) {
	s.protectionRejections.WithLabelValues(code).Inc()
	w.Header().Set("Retry-After", "1")
	apiError(w, http.StatusServiceUnavailable, code, message, id)
}

func (s *Server) protectChat(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		outcome := &chatOutcome{started: time.Now(), model: "unknown", result: "denied"}
		defer func() {
			s.requests.WithLabelValues(outcome.model, outcome.result).Inc()
			s.durations.WithLabelValues(outcome.model).Observe(time.Since(outcome.started).Seconds())
		}()
		if bearer(r) == "" {
			apiError(w, http.StatusUnauthorized, "unauthorized", "API key required", "")
			return
		}
		// Refuse excess work before authentication can borrow a DB connection.
		select {
		case s.inflight <- struct{}{}:
			s.active.Inc()
			defer func() { <-s.inflight; s.active.Dec() }()
		default:
			outcome.result = "overloaded"
			s.protectionError(w, "gateway_busy", "Gateway concurrency limit reached", "")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), s.Config.RequestTimeout)
		defer cancel()
		ctx = context.WithValue(ctx, chatOutcomeKey{}, outcome)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
