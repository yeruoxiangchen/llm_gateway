package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	openai "github.com/sashabaranov/go-openai"
	"github.com/sony/gobreaker"
)

const responseLimit = 4 * 1024 * 1024

var errNoUpstream = errors.New("no upstream currently available")

type preparedAttempt struct {
	provider Provider
	index    int
	id       int64
	client   *openai.Client
	observer *capture
	done     func(bool)
}

// This only acquires a circuit permit and constructs an SDK client. It performs
// no network I/O and is safe to call while the admission transaction is open.
func (s *Server) prepareAttempt(providers []Provider) (*preparedAttempt, error) {
	for index, provider := range providers {
		done, err := s.circuit(provider)
		if err != nil {
			continue
		}
		client, observer, err := s.client(provider)
		if err != nil {
			done(false)
			continue
		}
		return &preparedAttempt{provider: provider, index: index, client: client, observer: observer, done: done}, nil
	}
	return nil, errNoUpstream
}

type temperatureKey struct{}

// A pointer retains the difference between an omitted temperature and zero.
type chatRequest struct {
	openai.ChatCompletionRequest
	Temperature *float32 `json:"temperature,omitempty"`
}

func canonicalChatRequest(body openai.ChatCompletionRequest, temperature *float32) ([]byte, error) {
	if temperature != nil && *temperature == 0 {
		return json.Marshal(chatRequest{ChatCompletionRequest: body, Temperature: temperature})
	}
	// Keep the existing request identity for default and nonzero sampling.
	return json.Marshal(body)
}

type capture struct {
	mutex sync.Mutex
	data  bytes.Buffer
}

type observedBody struct {
	io.ReadCloser
	capture *capture
}

func (b *observedBody) Read(buffer []byte) (int, error) {
	n, err := b.ReadCloser.Read(buffer)
	b.capture.mutex.Lock()
	defer b.capture.mutex.Unlock()
	if b.capture.data.Len()+n > responseLimit {
		return 0, errors.New("upstream response exceeds 4 MiB")
	}
	_, _ = b.capture.data.Write(buffer[:n])
	return n, err
}

func (c *capture) usagePresent() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	var body map[string]json.RawMessage
	if json.Unmarshal(c.data.Bytes(), &body) != nil {
		return false
	}
	value, ok := body["usage"]
	return ok && !bytes.Equal(value, []byte("null"))
}

type observerDoer struct {
	client  *http.Client
	capture *capture
}

func (d observerDoer) Do(request *http.Request) (*http.Response, error) {
	if temperature, ok := request.Context().Value(temperatureKey{}).(float32); ok {
		// The SDK omits a float32 zero. Restore the explicitly supplied value on
		// its final wire request, for both ordinary and streaming calls.
		raw, err := io.ReadAll(request.Body)
		_ = request.Body.Close()
		if err != nil {
			return nil, err
		}
		var payload map[string]json.RawMessage
		if err = json.Unmarshal(raw, &payload); err != nil {
			return nil, err
		}
		payload["temperature"], err = json.Marshal(temperature)
		if err != nil {
			return nil, err
		}
		encoded, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		request = request.Clone(request.Context())
		request.Body = io.NopCloser(bytes.NewReader(encoded))
		request.ContentLength = int64(len(encoded))
		request.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(encoded)), nil }
	}
	response, err := d.client.Do(request)
	if err == nil {
		response.Body = &observedBody{ReadCloser: response.Body, capture: d.capture}
	}
	return response, err
}

func (s *Server) client(p Provider) (*openai.Client, *capture, error) {
	secret, err := decrypt(s.Config.MasterKey, p.Secret)
	if err != nil {
		return nil, nil, err
	}
	observer := &capture{}
	config := openai.DefaultConfig(secret)
	config.BaseURL = p.BaseURL
	config.HTTPClient = observerDoer{client: s.httpClient, capture: observer}
	return openai.NewClientWithConfig(config), observer, nil
}

func sdkUsage(usage openai.Usage) *Usage {
	return &Usage{Prompt: int64(usage.PromptTokens), Completion: int64(usage.CompletionTokens), Total: int64(usage.TotalTokens)}
}

func encodeCompletion(response openai.ChatCompletionResponse, usage *Usage) ([]byte, error) {
	var wireUsage *openai.Usage
	if usage != nil {
		response.Usage.TotalTokens = int(usage.Total)
		wireUsage = &response.Usage
	}
	// The SDK represents missing usage as a zero-value struct. Preserve the
	// distinction between unknown usage and an explicit zero-token receipt.
	return json.Marshal(struct {
		openai.ChatCompletionResponse
		Usage *openai.Usage `json:"usage,omitempty"`
	}{ChatCompletionResponse: response, Usage: wireUsage})
}

func (s *Server) candidates(alias string, providers []Provider) []Provider {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	sequence := s.sequence[alias]
	s.sequence[alias] = sequence + 1
	result := make([]Provider, 0, len(providers))
	for start := 0; start < len(providers); {
		end := start + 1
		for end < len(providers) && providers[end].Priority == providers[start].Priority {
			end++
		}
		count := end - start
		offset := int(sequence % uint64(count))
		for n := 0; n < count; n++ {
			result = append(result, providers[start+(offset+n)%count])
		}
		start = end
	}
	return result
}

func (s *Server) circuit(p Provider) (func(bool), error) {
	s.mutex.Lock()
	breaker := s.breakers[p.ID]
	if breaker == nil {
		breaker = gobreaker.NewTwoStepCircuitBreaker(gobreaker.Settings{Name: p.ID, MaxRequests: 1, Interval: 30 * time.Second, Timeout: 10 * time.Second,
			ReadyToTrip: func(count gobreaker.Counts) bool { return count.ConsecutiveFailures >= 3 }})
		s.breakers[p.ID] = breaker
	}
	s.mutex.Unlock()
	return breaker.Allow()
}

func (s *Server) chat(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	ctx := r.Context()
	outcome, _ := ctx.Value(chatOutcomeKey{}).(*chatOutcome)
	if outcome != nil {
		start = outcome.started
	}
	metricAlias := "unknown"
	result := "denied"
	defer func() {
		if outcome != nil {
			outcome.model, outcome.result = metricAlias, result
		}
	}()
	var incoming chatRequest
	if err := readJSON(w, r, &incoming); err != nil {
		apiError(w, 400, "invalid_request", "Invalid or unsupported chat request fields", "")
		return
	}
	body := incoming.ChatCompletionRequest
	if incoming.Temperature != nil {
		body.Temperature = *incoming.Temperature
		ctx = context.WithValue(ctx, temperatureKey{}, *incoming.Temperature)
		r = r.WithContext(ctx)
	}
	if body.Model == "" || len(body.Model) > 100 || len(body.Messages) == 0 || len(body.Messages) > 128 || body.N > 1 || body.N < 0 {
		apiError(w, 400, "invalid_request", "Provide model and 1 to 128 messages; n must be 1", "")
		return
	}
	for _, message := range body.Messages {
		for _, part := range message.MultiContent {
			if part.Type != openai.ChatMessagePartTypeText {
				apiError(w, 400, "unsupported_content", "This deployment meters text and tool-call chat requests", "")
				return
			}
		}
	}
	maxOutput := body.MaxTokens
	if body.MaxCompletionTokens > 0 {
		if maxOutput > 0 {
			apiError(w, 400, "invalid_request", "Use one output token limit", "")
			return
		}
		maxOutput = body.MaxCompletionTokens
	}
	if maxOutput == 0 {
		maxOutput = 256
		body.MaxTokens = 256
	}
	if maxOutput < 1 || maxOutput > 4096 || body.MaxCompletionTokens < 0 {
		apiError(w, 400, "invalid_token_limit", "Output token limit must be between 1 and 4096", "")
		return
	}
	idempotency := r.Header.Get("Idempotency-Key")
	if len(idempotency) > 128 {
		apiError(w, 400, "invalid_idempotency_key", "Idempotency-Key is limited to 128 bytes", "")
		return
	}
	key := keyFor(r)
	limited, retry, err := s.limited(r.Context(), key)
	if err != nil {
		s.protectionError(w, "rate_store_unavailable", "Rate limit store unavailable", "")
		return
	}
	if limited {
		w.Header().Set("Retry-After", strconv.Itoa(retry))
		apiError(w, 429, "rate_limit_exceeded", "API key request limit reached", "")
		return
	}
	lookupCtx, lookupCancel := context.WithTimeout(ctx, s.Config.DatabaseReadTimeout)
	providers, err := s.Store.Providers(lookupCtx, body.Model)
	lookupCancel()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			s.protectionError(w, "provider_lookup_timeout", "Channel lookup exceeded its time budget", "")
			return
		}
		s.storeError(w, err)
		return
	}
	if len(providers) == 0 {
		apiError(w, 404, "model_not_found", "No enabled upstream for this model", "")
		return
	}
	metricAlias = body.Model
	canonical, err := canonicalChatRequest(body, incoming.Temperature)
	if err != nil {
		apiError(w, 400, "invalid_request", "Request cannot be encoded", "")
		return
	}
	hash := sha256.Sum256(canonical)
	// This is an admission estimate. The ledger records provider-reported usage.
	reservation := int64(len(canonical) + 1024 + maxOutput)
	var record Request
	var fresh bool
	var prepared *preparedAttempt
	var candidates []Provider
	admissionCtx, admissionCancel := context.WithTimeout(ctx, s.Config.AdmissionTimeout)
	if s.Config.CombinedAdmission {
		var attemptID int64
		record, fresh, attemptID, err = s.Store.Admit(admissionCtx, key, body.Model, idempotency, hash[:], body.Stream, reservation, func() (Provider, error) {
			candidates = s.candidates(body.Model, providers)
			var prepareErr error
			prepared, prepareErr = s.prepareAttempt(candidates)
			if prepareErr != nil {
				return Provider{}, prepareErr
			}
			return prepared.provider, nil
		})
		if prepared != nil {
			prepared.id = attemptID
		}
	} else {
		record, fresh, err = s.Store.Reserve(admissionCtx, key, body.Model, idempotency, hash[:], body.Stream, reservation)
	}
	admissionCancel()
	if err != nil {
		if prepared != nil {
			prepared.done(true)
		}
		var commitError *AdmissionCommitError
		if errors.As(err, &commitError) {
			w.Header().Set("X-Request-ID", commitError.RequestID)
			s.Logger.Warn("admission acknowledgement lost; model not dispatched", "request_id", commitError.RequestID, "error", commitError.Cause)
			s.persist(ctx, Receipt{RequestID: commitError.RequestID, AccountID: commitError.AccountID, AttemptID: commitError.AttemptID,
				Usage: &Usage{}, NotDispatched: true, Outcome: "failed", HTTPStatus: 503, Error: "admission_commit_unacknowledged"})
			apiError(w, 503, "admission_commit_unacknowledged", "Model request was not dispatched; inspect its receipt", commitError.RequestID)
			return
		}
		if errors.Is(err, context.DeadlineExceeded) {
			s.protectionError(w, "admission_timeout", "Quota admission exceeded its time budget", "")
			return
		}
		if errors.Is(err, errNoUpstream) {
			apiError(w, 503, "upstream_unavailable", "No upstream currently available", "")
			return
		}
		s.storeError(w, err)
		return
	}
	w.Header().Set("X-Request-ID", record.ID)
	if value := observation(ctx); value != nil {
		value.mutex.Lock()
		value.ID = record.ID
		value.mutex.Unlock()
	}
	if !fresh {
		if len(record.Payload) > 0 && record.HTTPStatus == 200 {
			w.Header().Set("X-Idempotency-Replayed", "true")
			if record.Stream {
				w.Header().Set("Content-Type", "text/event-stream")
			} else {
				w.Header().Set("Content-Type", "application/json")
			}
			w.WriteHeader(200)
			if err := s.writeResponse(ctx, w, record.Payload); err != nil {
				result = "replay_write_error"
				return
			}
			result = "replayed"
			return
		}
		w.Header().Set("Retry-After", "2")
		apiError(w, 409, "request_exists", "Request already exists; inspect its receipt before retrying", record.ID)
		result = "duplicate"
		return
	}
	point := "after_reserve"
	if prepared != nil {
		point = "after_admit"
	}
	if err = s.checkpoint(ctx, point, record.ID); err != nil {
		var attemptID int64
		if prepared != nil {
			prepared.done(true)
			attemptID = prepared.id
		}
		s.persist(ctx, Receipt{AttemptID: attemptID, RequestID: record.ID, AccountID: record.AccountID, Outcome: "failed", HTTPStatus: 499, Usage: &Usage{}, Error: "client_cancelled"})
		return
	}
	if prepared != nil {
		candidates = candidates[prepared.index:]
	} else {
		candidates = s.candidates(body.Model, providers)
	}
	result = s.forward(ctx, w, body, record, candidates, start, prepared)
}

func (s *Server) forward(ctx context.Context, w http.ResponseWriter, body openai.ChatCompletionRequest, record Request, providers []Provider, start time.Time, prepared *preparedAttempt) string {
	attempts := 0
	status := 503
	code := "upstream_unavailable"
	for _, provider := range providers {
		if attempts >= 2 || ctx.Err() != nil {
			break
		}
		plan := prepared
		prepared = nil
		if plan == nil {
			var prepareErr error
			plan, prepareErr = s.prepareAttempt([]Provider{provider})
			if prepareErr != nil {
				continue
			}
			var dispatchErr error
			plan.id, dispatchErr = s.Store.Dispatch(ctx, record, provider)
			if dispatchErr != nil {
				s.Logger.Error("dispatch transaction failed", "request_id", record.ID, "error", dispatchErr)
				plan.done(true)
				s.persist(ctx, Receipt{RequestID: record.ID, AccountID: record.AccountID, Outcome: "failed", HTTPStatus: 503, Usage: &Usage{}, Error: "dispatch_storage_error"})
				apiError(w, 503, "storage_unavailable", "Unable to commit dispatch", record.ID)
				return "storage_error"
			}
		}
		done, client, observer, attemptID := plan.done, plan.client, plan.observer, plan.id
		attempts++
		if err := s.checkpoint(ctx, "after_dispatch", record.ID); err != nil {
			done(true)
			s.endAttempt(attemptID, 499, "cancelled_before_send")
			break
		}
		upstream := body
		upstream.Model = provider.Model
		if body.Stream {
			upstreamStart := time.Now()
			committed, usage, payload, streamErr := s.forwardStream(ctx, w, client, upstream, body.Model, start)
			s.observe(ctx, "upstream", upstreamStart)
			if streamErr == nil {
				done(true)
				s.persist(ctx, Receipt{AttemptID: attemptID, RequestID: record.ID, AccountID: record.AccountID, Outcome: "succeeded", HTTPStatus: 200, Usage: usage, Payload: payload})
				if usage == nil {
					return "pending_usage"
				}
				return "success"
			}
			status, code = upstreamError(streamErr)
			done(circuitHealthy(streamErr, status))
			s.endAttempt(attemptID, status, code)
			if committed {
				s.persist(ctx, Receipt{RequestID: record.ID, AccountID: record.AccountID, Outcome: "failed", HTTPStatus: 200, Usage: usage, Error: code})
				return "stream_error"
			}
			if !retryable(status) {
				break
			}
			continue
		}
		upstreamStart := time.Now()
		response, err := client.CreateChatCompletion(ctx, upstream)
		s.observe(ctx, "upstream", upstreamStart)
		if err != nil {
			status, code = upstreamError(err)
			done(circuitHealthy(err, status))
			s.endAttempt(attemptID, status, code)
			if !retryable(status) {
				break
			}
			continue
		}
		var usage *Usage
		if observer.usagePresent() {
			usage = sdkUsage(response.Usage)
		}
		if err = normalizeUsage(usage); err != nil {
			done(false)
			status = 502
			code = "invalid_upstream_usage"
			s.endAttempt(attemptID, status, code)
			continue
		}
		response.Model = body.Model
		payload, err := encodeCompletion(response, usage)
		if err != nil {
			done(false)
			status = 502
			code = "invalid_upstream_response"
			s.endAttempt(attemptID, status, code)
			continue
		}
		done(true)
		s.persist(ctx, Receipt{AttemptID: attemptID, RequestID: record.ID, AccountID: record.AccountID, Outcome: "succeeded", HTTPStatus: 200, Usage: usage, Payload: payload})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		if err := s.writeResponse(ctx, w, payload); err != nil {
			return "response_write_error"
		}
		if usage == nil {
			return "pending_usage"
		}
		return "success"
	}
	if ctx.Err() != nil {
		status, code = upstreamError(ctx.Err())
	}
	receipt := Receipt{RequestID: record.ID, AccountID: record.AccountID, Outcome: "failed", HTTPStatus: status, Usage: &Usage{}, Error: code}
	if prepared != nil {
		prepared.done(true)
		receipt.AttemptID = prepared.id
	}
	s.persist(ctx, receipt)
	apiError(w, status, code, "Upstream request did not produce a response", record.ID)
	return "upstream_error"
}

func (s *Server) writeResponse(ctx context.Context, w http.ResponseWriter, payload []byte) error {
	defer s.observe(ctx, "response_write", time.Now())
	controller := http.NewResponseController(w)
	_ = controller.SetWriteDeadline(time.Now().Add(s.Config.StreamIdle))
	defer controller.SetWriteDeadline(time.Time{})
	if _, err := w.Write(payload); err != nil {
		return err
	}
	return controller.Flush()
}

func (s *Server) endAttempt(id int64, status int, code string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Store.EndAttempt(ctx, id, status, code); err != nil {
		s.Logger.Warn("attempt status unavailable", "attempt_id", id)
	}
}

func retryable(status int) bool {
	return status == 429 || status == 502 || status == 503 || status == 504
}

func circuitHealthy(err error, status int) bool {
	return errors.Is(err, context.Canceled) || (status >= 400 && status < 500 && status != 429)
}

func upstreamError(err error) (int, string) {
	if errors.Is(err, context.Canceled) {
		return 499, "client_cancelled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return 504, "upstream_timeout"
	}
	var api *openai.APIError
	if errors.As(err, &api) {
		if api.HTTPStatusCode >= 400 && api.HTTPStatusCode <= 599 {
			return api.HTTPStatusCode, "upstream_http_error"
		}
	}
	return 502, "upstream_transport_error"
}

type streamEvent struct {
	value openai.ChatCompletionStreamResponse
	err   error
}

func (s *Server) forwardStream(parent context.Context, w http.ResponseWriter, client *openai.Client, body openai.ChatCompletionRequest, alias string, start time.Time) (bool, *Usage, []byte, error) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	body.StreamOptions = &openai.StreamOptions{IncludeUsage: true}
	stream, err := client.CreateChatCompletionStream(ctx, body)
	if err != nil {
		return false, nil, nil, err
	}
	events := make(chan streamEvent, 1)
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			value, err := stream.Recv()
			select {
			case events <- streamEvent{value: value, err: err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	defer func() { cancel(); _ = stream.Close(); <-readerDone }()
	timer := time.NewTimer(s.Config.StreamIdle)
	defer timer.Stop()
	committed := false
	finished := false
	var usage *Usage
	var payload bytes.Buffer
	writeEvent := func(data []byte) error {
		writeStart := time.Now()
		defer s.observe(ctx, "stream_write", writeStart)
		if payload.Len()+len(data) > responseLimit {
			return errors.New("stream response too large")
		}
		first := !committed
		if first {
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("Cache-Control", "no-cache")
			w.Header().Set("X-Accel-Buffering", "no")
			w.WriteHeader(200)
			committed = true
			s.firstEvent.WithLabelValues(alias).Observe(time.Since(start).Seconds())
		}
		controller := http.NewResponseController(w)
		_ = controller.SetWriteDeadline(time.Now().Add(s.Config.StreamIdle))
		defer controller.SetWriteDeadline(time.Time{})
		if _, err := w.Write(data); err != nil {
			return err
		}
		if err := controller.Flush(); err != nil {
			return err
		}
		_, _ = payload.Write(data)
		if first {
			if value := observation(ctx); value != nil {
				if err := s.checkpoint(ctx, "after_first_event", value.ID); err != nil {
					return err
				}
			}
		}
		return nil
	}
	streamFailure := func(err error) (bool, *Usage, []byte, error) {
		if committed && parent.Err() == nil {
			controller := http.NewResponseController(w)
			_ = controller.SetWriteDeadline(time.Now().Add(s.Config.StreamIdle))
			defer controller.SetWriteDeadline(time.Time{})
			_, code := upstreamError(err)
			data, _ := json.Marshal(map[string]any{"error": map[string]string{"type": "gateway_error", "code": code, "message": "Upstream stream ended before completion"}})
			_, _ = fmt.Fprintf(w, "data: %s\n\n", data)
			_ = controller.Flush()
		}
		return committed, usage, nil, err
	}
	for {
		select {
		case <-parent.Done():
			return streamFailure(parent.Err())
		case <-timer.C:
			return streamFailure(context.DeadlineExceeded)
		case event := <-events:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(s.Config.StreamIdle)
			if event.err != nil {
				if !errors.Is(event.err, io.EOF) || !finished {
					if errors.Is(event.err, io.EOF) {
						event.err = errors.New("upstream stream truncated")
					}
					return streamFailure(event.err)
				}
				if err = writeEvent([]byte("data: [DONE]\n\n")); err != nil {
					return committed, usage, nil, err
				}
				return committed, usage, payload.Bytes(), nil
			}
			event.value.Model = alias
			for _, choice := range event.value.Choices {
				if choice.FinishReason != "" {
					finished = true
				}
			}
			if event.value.Usage != nil {
				usage = sdkUsage(*event.value.Usage)
				if err = normalizeUsage(usage); err != nil {
					usage = nil
					return streamFailure(err)
				}
				event.value.Usage.TotalTokens = int(usage.Total)
			}
			data, marshalErr := json.Marshal(event.value)
			if marshalErr != nil {
				return streamFailure(marshalErr)
			}
			if err = writeEvent(append(append([]byte("data: "), data...), []byte("\n\n")...)); err != nil {
				return committed, usage, nil, err
			}
		}
	}
}
