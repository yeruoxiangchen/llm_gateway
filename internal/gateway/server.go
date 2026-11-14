package gateway

import (
	"context"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"github.com/sony/gobreaker"
)

type Server struct {
	Config               Config
	Store                *Store
	Redis                *redis.Client
	Logger               *slog.Logger
	mux                  *http.ServeMux
	inflight             chan struct{}
	mutex                sync.Mutex
	breakers             map[string]*gobreaker.TwoStepCircuitBreaker
	sequence             map[string]uint64
	requests             *prometheus.CounterVec
	durations            *prometheus.HistogramVec
	firstEvent           *prometheus.HistogramVec
	active               prometheus.Gauge
	storageErrors        prometheus.Counter
	rateRetries          prometheus.Counter
	rateCacheHits        prometheus.Counter
	rateAttempts         *prometheus.CounterVec
	protectionRejections *prometheus.CounterVec
	httpClient           *http.Client
	stages               *prometheus.HistogramVec
	profileGate          chan struct{}
	faults               *faultController
	slowMutex            sync.Mutex
	slowCommits          map[string]time.Time
}

func NewServer(config Config, store *Store, logger *slog.Logger) *Server {
	if config.RequestTimeout == 0 {
		config.RequestTimeout = 120 * time.Second
	}
	if config.DatabaseReadTimeout == 0 {
		config.DatabaseReadTimeout = time.Second
	}
	if config.AdmissionTimeout == 0 {
		config.AdmissionTimeout = 5 * time.Second
	}
	if config.RateLimitTimeout == 0 {
		config.RateLimitTimeout = 3 * time.Second
	}
	registry := prometheus.NewRegistry()
	s := &Server{
		Config: config, Store: store, Logger: logger, mux: http.NewServeMux(),
		Redis: redis.NewClient(&redis.Options{Addr: config.RedisAddress, Password: config.RedisPassword,
			DialTimeout: 2 * time.Second, ReadTimeout: 2 * time.Second, WriteTimeout: 2 * time.Second, PoolSize: 16, MaxRetries: -1, ContextTimeoutEnabled: true}),
		inflight: make(chan struct{}, config.MaxInflight),
		breakers: make(map[string]*gobreaker.TwoStepCircuitBreaker), sequence: make(map[string]uint64),
		requests:             prometheus.NewCounterVec(prometheus.CounterOpts{Name: "llmgw_requests_total", Help: "Gateway chat requests by logical result"}, []string{"model", "result"}),
		durations:            prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "llmgw_request_seconds", Help: "End-to-end chat request duration", Buckets: prometheus.DefBuckets}, []string{"model"}),
		firstEvent:           prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "llmgw_first_event_seconds", Help: "Time until first upstream stream event", Buckets: prometheus.DefBuckets}, []string{"model"}),
		active:               prometheus.NewGauge(prometheus.GaugeOpts{Name: "llmgw_inflight", Help: "Chat handlers from entry through authentication, generation and settlement"}),
		storageErrors:        prometheus.NewCounter(prometheus.CounterOpts{Name: "llmgw_settlement_spool_total", Help: "Receipts persisted to disk after a database error"}),
		rateRetries:          prometheus.NewCounter(prometheus.CounterOpts{Name: "llmgw_rate_limit_retries_total", Help: "Explicit retries of an idempotent rate operation"}),
		rateCacheHits:        prometheus.NewCounter(prometheus.CounterOpts{Name: "llmgw_rate_limit_cache_hits_total", Help: "Rate operations replayed without incrementing the counter"}),
		rateAttempts:         prometheus.NewCounterVec(prometheus.CounterOpts{Name: "llmgw_rate_limit_attempts_total", Help: "Logical Redis script attempts; NOSCRIPT fallback is internal to an attempt"}, []string{"result"}),
		protectionRejections: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "llmgw_protection_rejections_total", Help: "Chat requests rejected by entry or database time budgets"}, []string{"reason"}),
		stages:               prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "llmgw_stage_seconds", Help: "Inclusive request/database stage durations", Buckets: []float64{.0001, .0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2, 5, 10, 30}}, []string{"stage"}),
		// A non-streaming model sends headers after generation. The request's
		// context bounds the whole operation, including any preceding queueing.
		httpClient: &http.Client{Transport: &http.Transport{Proxy: http.ProxyFromEnvironment, MaxIdleConns: 64, MaxIdleConnsPerHost: 32, MaxConnsPerHost: config.MaxInflight, IdleConnTimeout: 90 * time.Second, ResponseHeaderTimeout: config.RequestTimeout},
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
	}
	registry.MustRegister(s.requests, s.durations, s.firstEvent, s.active, s.storageErrors, s.rateRetries, s.rateCacheHits, s.rateAttempts, s.protectionRejections, s.stages, prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	mode := "split"
	if config.CombinedAdmission {
		mode = "combined"
	}
	admissionInfo := prometheus.NewGauge(prometheus.GaugeOpts{Name: "llmgw_admission_info", Help: "Active transaction admission mode", ConstLabels: prometheus.Labels{"mode": mode}})
	admissionInfo.Set(1)
	registry.MustRegister(admissionInfo)
	version := config.RequestIDVersion
	if store != nil {
		version = store.RequestIDVersion
	}
	if version == 0 {
		version = 4
	}
	idInfo := prometheus.NewGauge(prometheus.GaugeOpts{Name: "llmgw_request_id_info", Help: "Request UUID generation version", ConstLabels: prometheus.Labels{"version": strconv.Itoa(version)}})
	idInfo.Set(1)
	registry.MustRegister(idInfo)
	if store != nil {
		store.Observe = s.observe
		registry.MustRegister(collectors.NewDBStatsCollector(store.DB, "gateway"))
		if store.MetadataDB != nil && store.MetadataDB != store.DB {
			registry.MustRegister(collectors.NewDBStatsCollector(store.MetadataDB, "gateway_metadata"))
		}
	}
	s.diagnostics()
	s.faultRoutes()
	s.mux.Handle("GET /metrics", s.admin(promhttp.HandlerFor(registry, promhttp.HandlerOpts{})))
	s.mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, map[string]string{"status": "alive"}) })
	s.mux.HandleFunc("GET /health/ready", s.ready)
	s.mux.Handle("GET /v1/models", s.auth(http.HandlerFunc(s.models)))
	s.mux.Handle("GET /v1/usage", s.auth(http.HandlerFunc(s.usage)))
	s.mux.Handle("GET /v1/requests/{id}", s.auth(http.HandlerFunc(s.request)))
	s.mux.Handle("POST /v1/chat/completions", s.protectChat(s.auth(http.HandlerFunc(s.chat))))
	s.mux.Handle("POST /admin/accounts", s.admin(http.HandlerFunc(s.createAccount)))
	s.mux.Handle("PUT /admin/accounts/{id}/quota", s.admin(http.HandlerFunc(s.setQuota)))
	s.mux.Handle("POST /admin/accounts/{id}/keys", s.admin(http.HandlerFunc(s.createKey)))
	s.mux.Handle("DELETE /admin/keys/{id}", s.admin(http.HandlerFunc(s.revokeKey)))
	s.mux.Handle("POST /admin/providers", s.admin(http.HandlerFunc(s.createProvider)))
	s.mux.Handle("GET /admin/providers", s.admin(http.HandlerFunc(s.providers)))
	s.mux.Handle("PATCH /admin/providers/{id}", s.admin(http.HandlerFunc(s.enableProvider)))
	s.mux.Handle("GET /admin/requests", s.admin(http.HandlerFunc(s.adminRequests)))
	s.mux.Handle("GET /admin/usage", s.admin(http.HandlerFunc(s.usageReport)))
	s.mux.Handle("POST /admin/requests/{id}/resolve", s.admin(http.HandlerFunc(s.resolve)))
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if r.Method == "POST" && r.URL.Path == "/v1/chat/completions" {
		started := time.Now()
		value := &requestObservation{Idempotency: r.Header.Get("Idempotency-Key")}
		if s.Config.RequestTraces {
			value.stages = map[string]float64{}
		}
		r = r.WithContext(context.WithValue(r.Context(), observationKey{}, value))
		if s.Config.RequestTraces {
			defer func() {
				value.mutex.Lock()
				defer value.mutex.Unlock()
				s.Logger.Info("request timing", "request_id", value.ID, "elapsed_ms", float64(time.Since(started).Microseconds())/1000, "stage_ms", value.stages)
			}()
		}
	}
	s.mux.ServeHTTP(w, r)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func apiError(w http.ResponseWriter, status int, code, message, id string) {
	writeJSON(w, status, map[string]any{"error": map[string]any{"type": "gateway_error", "code": code, "message": message, "request_id": id}})
}

func readJSON(w http.ResponseWriter, r *http.Request, target any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 65536)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return errors.New("expected one JSON value")
	}
	return nil
}

func bearer(r *http.Request) string {
	value := r.Header.Get("Authorization")
	if !strings.HasPrefix(value, "Bearer ") {
		return ""
	}
	return strings.TrimPrefix(value, "Bearer ")
}

func (s *Server) admin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if subtle.ConstantTimeCompare([]byte(bearer(r)), []byte(s.Config.AdminToken)) != 1 {
			apiError(w, 401, "unauthorized", "Valid administrator credential required", "")
			return
		}
		next.ServeHTTP(w, r)
	})
}

type contextKey int

const identityKey contextKey = 1

func keyFor(r *http.Request) Key { return r.Context().Value(identityKey).(Key) }

func (s *Server) auth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if bearer(r) == "" {
			apiError(w, 401, "unauthorized", "API key required", "")
			return
		}
		started := time.Now()
		ctx, cancel := context.WithTimeout(r.Context(), s.Config.DatabaseReadTimeout)
		key, err := s.Store.Authenticate(ctx, bearer(r))
		cancel()
		s.observe(r.Context(), "authenticate", started)
		if errors.Is(err, ErrForbidden) {
			apiError(w, 401, "unauthorized", "Invalid API key", "")
			return
		}
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				s.protectionError(w, "authentication_timeout", "Credential lookup exceeded its time budget", "")
				return
			}
			apiError(w, 503, "storage_unavailable", "Credential store unavailable", "")
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), identityKey, key)))
	})
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.Store.DB.PingContext(ctx); err != nil {
		apiError(w, 503, "database_unavailable", "Database is unavailable", "")
		return
	}
	if s.Store.metadataDB() != s.Store.DB {
		if err := s.Store.metadataDB().PingContext(ctx); err != nil {
			apiError(w, 503, "database_unavailable", "Metadata database pool is unavailable", "")
			return
		}
	}
	if err := s.Redis.Ping(ctx).Err(); err != nil {
		apiError(w, 503, "rate_store_unavailable", "Rate limit store is unavailable", "")
		return
	}
	writeJSON(w, 200, map[string]string{"status": "ready"})
}

func (s *Server) storeError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrQuota):
		apiError(w, 402, "quota_exceeded", err.Error(), "")
	case errors.Is(err, ErrConflict):
		apiError(w, 409, "conflict", err.Error(), "")
	case errors.Is(err, ErrForbidden):
		apiError(w, 401, "unauthorized", err.Error(), "")
	case errors.Is(err, sql.ErrNoRows) || errors.Is(err, ErrNotFound):
		apiError(w, 404, "not_found", "Resource not found", "")
	default:
		s.Logger.Error("database operation failed", "error", err)
		apiError(w, 503, "storage_unavailable", "Storage is temporarily unavailable", "")
	}
}

func (s *Server) createAccount(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name  string `json:"name"`
		Limit int64  `json:"quota_tokens"`
	}
	if readJSON(w, r, &body) != nil || len(body.Name) < 1 || len(body.Name) > 120 || body.Limit < 0 || body.Limit > 1000000000000 {
		apiError(w, 400, "invalid_account", "Valid name and token quota required", "")
		return
	}
	a, err := s.Store.CreateAccount(r.Context(), body.Name, body.Limit)
	if err != nil {
		s.storeError(w, err)
		return
	}
	writeJSON(w, 201, a)
}

func (s *Server) setQuota(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Limit int64 `json:"quota_tokens"`
	}
	if readJSON(w, r, &body) != nil || body.Limit < 0 || body.Limit > 1000000000000 {
		apiError(w, 400, "invalid_quota", "Token quota must be between 0 and 10^12", "")
		return
	}
	if err := s.Store.SetQuota(r.Context(), r.PathValue("id"), body.Limit); err != nil {
		s.storeError(w, err)
		return
	}
	a, _ := s.Store.Account(r.Context(), r.PathValue("id"))
	writeJSON(w, 200, a)
}

func (s *Server) createKey(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name string `json:"name"`
		RPM  int    `json:"requests_per_minute"`
	}
	if readJSON(w, r, &body) != nil || len(body.Name) < 1 || len(body.Name) > 120 || body.RPM < 1 || body.RPM > 1000000 {
		apiError(w, 400, "invalid_key", "Key name and requests_per_minute from 1 to 1000000 required", "")
		return
	}
	key, secret, err := s.Store.CreateKey(r.Context(), r.PathValue("id"), body.Name, body.RPM)
	if err != nil {
		s.storeError(w, err)
		return
	}
	writeJSON(w, 201, map[string]any{"key": key, "secret": secret})
}

func (s *Server) revokeKey(w http.ResponseWriter, r *http.Request) {
	if err := s.Store.RevokeKey(r.Context(), r.PathValue("id")); err != nil {
		s.storeError(w, err)
		return
	}
	writeJSON(w, 200, map[string]string{"status": "revoked"})
}

func (s *Server) createProvider(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Provider
		APIKey string `json:"api_key"`
	}
	if err := readJSON(w, r, &body); err != nil {
		apiError(w, 400, "invalid_provider", "Invalid provider JSON", "")
		return
	}
	parsed, err := url.Parse(body.BaseURL)
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || len(body.Name) == 0 || len(body.Name) > 120 || len(body.Alias) == 0 || len(body.Alias) > 100 || len(body.Model) == 0 || len(body.Model) > 160 || len(body.BaseURL) > 500 {
		apiError(w, 400, "invalid_provider", "Valid name, model_alias, upstream_model and HTTP(S) base_url required", "")
		return
	}
	if _, err = cost(Usage{}, body.InputPrice, body.OutputPrice); err != nil {
		apiError(w, 400, "invalid_price", err.Error(), "")
		return
	}
	body.BaseURL = strings.TrimSuffix(body.BaseURL, "/")
	p, err := s.Store.CreateProvider(r.Context(), body.Provider, body.APIKey)
	if err != nil {
		s.storeError(w, err)
		return
	}
	writeJSON(w, 201, p)
}

func (s *Server) providers(w http.ResponseWriter, r *http.Request) {
	providers, err := s.Store.Providers(r.Context(), "")
	if err != nil {
		s.storeError(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"data": providers})
}

func (s *Server) enableProvider(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Enabled *bool `json:"enabled"`
	}
	if readJSON(w, r, &body) != nil || body.Enabled == nil {
		apiError(w, 400, "invalid_provider", "enabled boolean required", "")
		return
	}
	if err := s.Store.EnableProvider(r.Context(), r.PathValue("id"), *body.Enabled); err != nil {
		s.storeError(w, err)
		return
	}
	writeJSON(w, 200, map[string]bool{"enabled": *body.Enabled})
}

func (s *Server) models(w http.ResponseWriter, r *http.Request) {
	providers, err := s.Store.Providers(r.Context(), "")
	if err != nil {
		s.storeError(w, err)
		return
	}
	seen := map[string]bool{}
	data := []map[string]any{}
	for _, p := range providers {
		if p.Enabled && !seen[p.Alias] {
			data = append(data, map[string]any{"id": p.Alias, "object": "model", "owned_by": "gateway"})
			seen[p.Alias] = true
		}
	}
	writeJSON(w, 200, map[string]any{"object": "list", "data": data})
}

func (s *Server) usage(w http.ResponseWriter, r *http.Request) {
	a, err := s.Store.Account(r.Context(), keyFor(r).AccountID)
	if err != nil {
		s.storeError(w, err)
		return
	}
	requests, err := s.Store.Requests(r.Context(), a.ID)
	if err != nil {
		s.storeError(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"account": a, "requests": requests})
}

func (s *Server) request(w http.ResponseWriter, r *http.Request) {
	request, err := s.Store.Request(r.Context(), r.PathValue("id"))
	if err != nil {
		s.storeError(w, err)
		return
	}
	if request.AccountID != keyFor(r).AccountID {
		apiError(w, 404, "not_found", "Resource not found", "")
		return
	}
	writeJSON(w, 200, request)
}

func (s *Server) adminRequests(w http.ResponseWriter, r *http.Request) {
	requests, err := s.Store.Requests(r.Context(), r.URL.Query().Get("account_id"))
	if err != nil {
		s.storeError(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"data": requests})
}

func (s *Server) usageReport(w http.ResponseWriter, r *http.Request) {
	until := time.Now().UTC()
	since := until.Add(-24 * time.Hour)
	query := r.URL.Query()
	if value := query.Get("since"); value != "" {
		parsed, err := time.Parse(time.RFC3339, value)
		if err != nil {
			apiError(w, 400, "invalid_time", "since must use RFC3339", "")
			return
		}
		since = parsed
	}
	if value := query.Get("until"); value != "" {
		parsed, err := time.Parse(time.RFC3339, value)
		if err != nil {
			apiError(w, 400, "invalid_time", "until must use RFC3339", "")
			return
		}
		until = parsed
	}
	group := query.Get("group_by")
	if group == "" {
		group = "model"
	}
	if group != "model" && group != "account" && group != "provider" && group != "day" || !until.After(since) || until.Sub(since) > 31*24*time.Hour {
		apiError(w, 400, "invalid_report", "Use model/account/provider/day grouping and a time range up to 31 days", "")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	groups, err := s.Store.UsageReport(ctx, query.Get("account_id"), group, since, until)
	if err != nil {
		s.storeError(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"since": since, "until": until, "group_by": group, "limit": 1000, "data": groups})
}

func (s *Server) resolve(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Usage  *Usage `json:"usage"`
		Refund bool   `json:"refund"`
		Reason string `json:"reason"`
	}
	if readJSON(w, r, &body) != nil || len(body.Reason) < 3 || len(body.Reason) > 512 || (body.Usage == nil && !body.Refund) || (body.Usage != nil && body.Refund) {
		apiError(w, 400, "invalid_resolution", "Provide usage or refund, and an audit reason", "")
		return
	}
	if body.Refund {
		body.Usage = &Usage{}
	}
	if err := normalizeUsage(body.Usage); err != nil {
		apiError(w, 400, "invalid_usage", err.Error(), "")
		return
	}
	request, err := s.Store.Request(r.Context(), r.PathValue("id"))
	if err != nil {
		s.storeError(w, err)
		return
	}
	if request.State != "pending_usage" {
		apiError(w, 409, "not_pending", "Request has no pending usage to resolve", request.ID)
		return
	}
	receipt := Receipt{RequestID: request.ID, AccountID: request.AccountID, Outcome: request.Outcome, HTTPStatus: request.HTTPStatus,
		Usage: body.Usage, Payload: request.Payload, Error: request.Error, Resolve: true, Reason: body.Reason}
	if err = s.Store.Finish(r.Context(), receipt); err != nil {
		s.storeError(w, err)
		return
	}
	updated, _ := s.Store.Request(r.Context(), request.ID)
	writeJSON(w, 200, updated)
}

func (s *Server) persist(parent context.Context, receipt Receipt) {
	_ = s.checkpoint(parent, "before_settle", receipt.RequestID)
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 3*time.Second)
	err := s.Store.Finish(ctx, receipt)
	cancel()
	if err == nil {
		_ = s.checkpoint(parent, "after_settle", receipt.RequestID)
		return
	}
	s.storageErrors.Inc()
	directory := filepath.Join(s.Config.DataDirectory, "spool")
	spoolStart := time.Now()
	defer s.observe(ctx, "receipt_spool", spoolStart)
	if err = os.MkdirAll(directory, 0700); err == nil {
		var data []byte
		data, err = json.Marshal(receipt)
		if err == nil {
			path := filepath.Join(directory, receipt.RequestID+".json")
			err = writeAtomic(path, data)
		}
	}
	if err != nil {
		s.Logger.Error("settlement receipt could not be persisted", "request_id", receipt.RequestID, "error", err)
		return
	}
	s.Logger.Warn("settlement receipt saved for replay", "request_id", receipt.RequestID)
	_ = s.checkpoint(parent, "after_spool", receipt.RequestID)
}

func writeAtomic(path string, data []byte) error {
	temp := path + ".tmp"
	file, err := os.OpenFile(temp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	if _, err = file.Write(data); err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = os.Rename(temp, path); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func (s *Server) Maintain(ctx context.Context) {
	ticker := time.NewTicker(s.Config.Heartbeat)
	defer ticker.Stop()
	for {
		s.maintainOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Server) maintainOnce(parent context.Context) {
	ctx, cancel := context.WithTimeout(parent, 4*time.Second)
	defer cancel()
	if err := s.Store.Heartbeat(ctx); err != nil {
		s.Logger.Warn("owner heartbeat failed", "error", err)
	}
	directory := filepath.Join(s.Config.DataDirectory, "spool")
	entries, _ := os.ReadDir(directory)
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := filepath.Join(directory, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var receipt Receipt
		if json.Unmarshal(data, &receipt) != nil {
			continue
		}
		if err = s.Store.Finish(ctx, receipt); err == nil {
			_ = os.Remove(path)
		}
	}
	if err := s.Store.Recover(ctx); err != nil {
		s.Logger.Warn("lease recovery deferred", "error", err)
	}
}
