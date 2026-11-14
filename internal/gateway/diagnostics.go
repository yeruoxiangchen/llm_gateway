package gateway

import (
	"context"
	"net/http"
	"net/http/pprof"
	"runtime"
	"strconv"
	"sync"
	"time"
)

type observationKey struct{}

type requestObservation struct {
	mutex       sync.Mutex
	ID          string
	Idempotency string
	stages      map[string]float64
}

func observation(ctx context.Context) *requestObservation {
	value, _ := ctx.Value(observationKey{}).(*requestObservation)
	return value
}

func observeRequestID(ctx context.Context, id string) {
	if value := observation(ctx); value != nil {
		value.mutex.Lock()
		value.ID = id
		value.mutex.Unlock()
	}
}

func (s *Server) observe(ctx context.Context, phase string, started time.Time) {
	seconds := time.Since(started).Seconds()
	s.stages.WithLabelValues(phase).Observe(seconds)
	if value := observation(ctx); value != nil && value.stages != nil {
		value.mutex.Lock()
		value.stages[phase] += seconds * 1000
		value.mutex.Unlock()
	}
	if seconds < .25 || (phase != "reserve_commit" && phase != "dispatch_commit" && phase != "settle_commit" && phase != "admit_commit") {
		return
	}
	// Bound diagnostic writes during a storage stall. The histogram still records
	// every operation; the log retains at most one event per phase per second.
	now := time.Now()
	s.slowMutex.Lock()
	if s.slowCommits == nil {
		s.slowCommits = map[string]time.Time{}
	}
	if now.Sub(s.slowCommits[phase]) < time.Second {
		s.slowMutex.Unlock()
		return
	}
	s.slowCommits[phase] = now
	s.slowMutex.Unlock()
	id := ""
	if value := observation(ctx); value != nil {
		value.mutex.Lock()
		id = value.ID
		value.mutex.Unlock()
	}
	s.Logger.Warn("slow database commit", "stage", phase, "request_id", id,
		"started_at", started.UTC().Format(time.RFC3339Nano), "elapsed_ms", seconds*1000)
}

func (s *Server) diagnostics() {
	if !s.Config.Diagnostics {
		return
	}
	s.profileGate = make(chan struct{}, 1)
	profiles := map[string]http.HandlerFunc{
		"heap":      pprof.Handler("heap").ServeHTTP,
		"goroutine": pprof.Handler("goroutine").ServeHTTP,
		"block":     pprof.Handler("block").ServeHTTP,
		"mutex":     pprof.Handler("mutex").ServeHTTP,
		"profile":   pprof.Profile,
		"trace":     pprof.Trace,
	}
	for name, handler := range profiles {
		name, handler := name, handler
		s.mux.Handle("GET /debug/pprof/"+name, s.admin(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "no-store")
			if name == "profile" || name == "trace" {
				seconds := 5
				if value := r.URL.Query().Get("seconds"); value != "" {
					var err error
					seconds, err = strconv.Atoi(value)
					if err != nil || seconds < 1 || seconds > 20 {
						apiError(w, 400, "invalid_profile", "Profile duration must be 1..20 seconds", "")
						return
					}
				}
				select {
				case s.profileGate <- struct{}{}:
					defer func() { <-s.profileGate }()
				default:
					apiError(w, 409, "profile_busy", "Another profile is running", "")
					return
				}
				query := r.URL.Query()
				query.Set("seconds", strconv.Itoa(seconds))
				r.URL.RawQuery = query.Encode()
			}
			handler(w, r)
		})))
	}
	// Sampling is enabled only on the diagnostic instances.
	runtime.SetMutexProfileFraction(10)
	runtime.SetBlockProfileRate(10000000)
}

type checkpointGate struct {
	Point       string `json:"point"`
	Idempotency string `json:"idempotency_key"`
	RequestID   string `json:"request_id"`
	Arrived     bool   `json:"arrived"`
	Released    bool   `json:"released"`
	release     chan struct{}
}

type faultController struct {
	sync.Mutex
	gates map[string]*checkpointGate
}

var checkpointNames = map[string]bool{"after_reserve": true, "after_admit": true, "after_dispatch": true, "after_first_event": true, "before_settle": true, "after_settle": true, "after_spool": true}

func (s *Server) faultRoutes() {
	if !s.Config.FaultInjection {
		return
	}
	s.faults = &faultController{gates: map[string]*checkpointGate{}}
	s.mux.Handle("POST /admin/testing/arm", s.admin(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Point       string `json:"point"`
			Idempotency string `json:"idempotency_key"`
		}
		if readJSON(w, r, &body) != nil || !checkpointNames[body.Point] || len(body.Idempotency) < 1 || len(body.Idempotency) > 128 {
			apiError(w, 400, "invalid_checkpoint", "Valid checkpoint and idempotency_key required", "")
			return
		}
		s.faults.Lock()
		defer s.faults.Unlock()
		if old := s.faults.gates[body.Point]; old != nil && !old.Released {
			apiError(w, 409, "checkpoint_armed", "Release the existing checkpoint first", "")
			return
		}
		gate := &checkpointGate{Point: body.Point, Idempotency: body.Idempotency, release: make(chan struct{})}
		s.faults.gates[body.Point] = gate
		writeJSON(w, 201, gate)
	})))
	s.mux.Handle("GET /admin/testing/state", s.admin(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.faults.Lock()
		defer s.faults.Unlock()
		writeJSON(w, 200, s.faults.gates)
	})))
	s.mux.Handle("POST /admin/testing/release", s.admin(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Point string `json:"point"`
		}
		if readJSON(w, r, &body) != nil {
			apiError(w, 400, "invalid_checkpoint", "Point required", "")
			return
		}
		s.faults.Lock()
		defer s.faults.Unlock()
		gate := s.faults.gates[body.Point]
		if gate == nil {
			apiError(w, 404, "checkpoint_missing", "Checkpoint not armed", "")
			return
		}
		if !gate.Released {
			close(gate.release)
			gate.Released = true
		}
		writeJSON(w, 200, gate)
	})))
}

func (s *Server) checkpoint(ctx context.Context, point, requestID string) error {
	if s.faults == nil {
		return nil
	}
	value := observation(ctx)
	if value == nil {
		return nil
	}
	s.faults.Lock()
	gate := s.faults.gates[point]
	if gate == nil || gate.Released || gate.Idempotency != value.Idempotency {
		s.faults.Unlock()
		return nil
	}
	gate.Arrived = true
	gate.RequestID = requestID
	channel := gate.release
	s.faults.Unlock()
	select {
	case <-channel:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
