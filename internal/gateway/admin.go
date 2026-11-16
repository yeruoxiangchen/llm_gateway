package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

type adminCursor struct {
	Kind    string     `json:"kind"`
	Scope   string     `json:"scope"`
	ID      string     `json:"id"`
	Created *time.Time `json:"created,omitempty"`
}

type adminPage struct {
	Limit       int
	Kind, Scope string
	After       *adminCursor
}

type adminList[T any] struct {
	Data       []T    `json:"data"`
	Limit      int    `json:"limit"`
	HasMore    bool   `json:"has_more"`
	NextCursor string `json:"next_cursor,omitempty"`
}

func finishAdminPage[T any](items []T, page adminPage, position func(T) adminCursor) adminList[T] {
	result := adminList[T]{Data: items, Limit: page.Limit, HasMore: len(items) > page.Limit}
	if result.HasMore {
		result.Data = items[:page.Limit]
		cursor := position(result.Data[len(result.Data)-1])
		cursor.Kind, cursor.Scope = page.Kind, page.Scope
		raw, _ := json.Marshal(cursor)
		result.NextCursor = base64.RawURLEncoding.EncodeToString(raw)
	}
	return result
}

func parseAdminPage(r *http.Request, kind, resource string, allowed ...string) (adminPage, error) {
	page := adminPage{Limit: 100, Kind: kind}
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return page, errors.New("invalid query encoding")
	}
	valid := map[string]bool{"limit": true, "cursor": true}
	for _, name := range allowed {
		valid[name] = true
	}
	for name, values := range query {
		if !valid[name] || len(values) != 1 || len(values[0]) > 2048 {
			return page, errors.New("unknown, repeated or oversized query parameter")
		}
	}
	if value, ok := query["limit"]; ok {
		page.Limit, err = strconv.Atoi(value[0])
		if err != nil || page.Limit < 1 || page.Limit > 200 {
			return page, errors.New("limit must be between 1 and 200")
		}
	}
	rawCursor := query.Get("cursor")
	query.Del("cursor")
	query.Del("limit")
	page.Scope = fmt.Sprintf("%x", sha256.Sum256([]byte(resource+"?"+query.Encode())))
	if rawCursor != "" {
		raw, err := base64.RawURLEncoding.DecodeString(rawCursor)
		if err != nil {
			return page, errors.New("invalid cursor")
		}
		var cursor adminCursor
		if err = json.Unmarshal(raw, &cursor); err != nil || cursor.Kind != kind || cursor.Scope != page.Scope || uuid.Validate(cursor.ID) != nil {
			return page, errors.New("cursor does not match this resource and filters")
		}
		if kind == "keys" {
			if cursor.Created != nil {
				return page, errors.New("invalid key cursor")
			}
		} else if cursor.Created == nil || cursor.Created.IsZero() || cursor.Created.Year() < 1000 || cursor.Created.Year() > 9999 {
			return page, errors.New("invalid timestamp in cursor")
		}
		page.After = &cursor
	}
	return page, nil
}

func adminID(w http.ResponseWriter, r *http.Request) (string, bool) {
	id := r.PathValue("id")
	if uuid.Validate(id) != nil {
		apiError(w, 400, "invalid_id", "A UUID resource ID is required", "")
		return "", false
	}
	return id, true
}

// PATCH distinguishes omitted fields from explicit zero/false values and rejects
// nulls, unknown properties and empty objects instead of silently ignoring them.
func readAdminPatch(w http.ResponseWriter, r *http.Request, target any) error {
	var raw json.RawMessage
	if err := readJSON(w, r, &raw); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || len(fields) == 0 {
		return errors.New("a nonempty JSON object is required")
	}
	for _, value := range fields {
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return errors.New("null patch values are not supported")
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

func (s *Server) adminRoutes() {
	s.mux.Handle("GET /admin/accounts", s.admin(http.HandlerFunc(s.listAccounts)))
	s.mux.Handle("GET /admin/accounts/{id}", s.admin(http.HandlerFunc(s.accountDetail)))
	s.mux.Handle("GET /admin/accounts/{id}/keys", s.admin(http.HandlerFunc(s.listKeys)))
	s.mux.Handle("GET /admin/keys/{id}", s.admin(http.HandlerFunc(s.keyDetail)))
	s.mux.Handle("PATCH /admin/keys/{id}", s.admin(http.HandlerFunc(s.updateKey)))
	s.mux.Handle("GET /admin/providers/{id}", s.admin(http.HandlerFunc(s.providerDetail)))
	s.mux.Handle("GET /admin/requests/{id}", s.admin(http.HandlerFunc(s.adminRequestDetail)))
}

func (s *Server) listAccounts(w http.ResponseWriter, r *http.Request) {
	page, err := parseAdminPage(r, "accounts", "", "q")
	if err != nil || len(r.URL.Query().Get("q")) > 120 {
		apiError(w, 400, "invalid_query", "Use q up to 120 bytes and a valid limit/cursor", "")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	result, err := s.Store.AccountsPage(ctx, page, r.URL.Query().Get("q"))
	if err != nil {
		s.storeError(w, err)
		return
	}
	writeJSON(w, 200, result)
}

func (s *Server) accountDetail(w http.ResponseWriter, r *http.Request) {
	id, ok := adminID(w, r)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	a, err := s.Store.Account(ctx, id)
	if err != nil {
		s.storeError(w, err)
		return
	}
	writeJSON(w, 200, a)
}

func (s *Server) listKeys(w http.ResponseWriter, r *http.Request) {
	id, ok := adminID(w, r)
	if !ok {
		return
	}
	page, err := parseAdminPage(r, "keys", id, "active")
	var active *bool
	if value, exists := r.URL.Query()["active"]; exists {
		if len(value) != 1 || (value[0] != "true" && value[0] != "false") {
			err = errors.New("active must be true or false")
		} else {
			v := value[0] == "true"
			active = &v
		}
	}
	if err != nil {
		apiError(w, 400, "invalid_query", err.Error(), "")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if _, err = s.Store.Account(ctx, id); err != nil {
		s.storeError(w, err)
		return
	}
	result, err := s.Store.KeysPage(ctx, id, active, page)
	if err != nil {
		s.storeError(w, err)
		return
	}
	writeJSON(w, 200, result)
}

func (s *Server) keyDetail(w http.ResponseWriter, r *http.Request) {
	id, ok := adminID(w, r)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	key, err := s.Store.KeyDetail(ctx, id)
	if err != nil {
		s.storeError(w, err)
		return
	}
	writeJSON(w, 200, key)
}

type keyPatch struct {
	Name *string `json:"name"`
	RPM  *int    `json:"requests_per_minute"`
}

func (s *Server) updateKey(w http.ResponseWriter, r *http.Request) {
	id, ok := adminID(w, r)
	if !ok {
		return
	}
	var patch keyPatch
	if readAdminPatch(w, r, &patch) != nil || (patch.Name != nil && (len(strings.TrimSpace(*patch.Name)) == 0 || len(*patch.Name) > 120)) || (patch.RPM != nil && (*patch.RPM < 1 || *patch.RPM > 1000000)) {
		apiError(w, 400, "invalid_key", "Provide a name and/or requests_per_minute from 1 to 1000000", "")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	key, err := s.Store.UpdateKey(ctx, id, patch)
	if err != nil {
		s.storeError(w, err)
		return
	}
	s.Logger.Info("administrator updated key", "key_id", id)
	writeJSON(w, 200, key)
}

type providerPatch struct {
	Name        *string `json:"name"`
	Alias       *string `json:"model_alias"`
	Model       *string `json:"upstream_model"`
	BaseURL     *string `json:"base_url"`
	APIKey      *string `json:"api_key"`
	Priority    *int    `json:"priority"`
	Enabled     *bool   `json:"enabled"`
	InputPrice  *int64  `json:"input_micro_usd_per_million_tokens"`
	OutputPrice *int64  `json:"output_micro_usd_per_million_tokens"`
}

func validProviderURL(value string) bool {
	u, err := url.Parse(value)
	return err == nil && len(value) <= 500 && u.Host != "" && (u.Scheme == "http" || u.Scheme == "https") && u.User == nil && u.RawQuery == "" && u.Fragment == ""
}

func (s *Server) providerDetail(w http.ResponseWriter, r *http.Request) {
	id, ok := adminID(w, r)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	p, err := s.Store.Provider(ctx, id)
	if err != nil {
		s.storeError(w, err)
		return
	}
	writeJSON(w, 200, p)
}

func (s *Server) updateProvider(w http.ResponseWriter, r *http.Request) {
	id, ok := adminID(w, r)
	if !ok {
		return
	}
	var patch providerPatch
	if err := readAdminPatch(w, r, &patch); err != nil {
		apiError(w, 400, "invalid_provider", "Provide valid, non-null provider fields", "")
		return
	}
	for _, field := range []struct {
		value   *string
		maximum int
	}{{patch.Name, 120}, {patch.Alias, 100}, {patch.Model, 160}} {
		if field.value != nil && (strings.TrimSpace(*field.value) == "" || len(*field.value) > field.maximum) {
			apiError(w, 400, "invalid_provider", "Provider name, alias and model must be nonempty and within length limits", "")
			return
		}
	}
	if patch.BaseURL != nil {
		if !validProviderURL(*patch.BaseURL) {
			apiError(w, 400, "invalid_provider", "Valid HTTP(S) base_url required", "")
			return
		}
		*patch.BaseURL = strings.TrimSuffix(*patch.BaseURL, "/")
	}
	if patch.APIKey != nil && len(*patch.APIKey) > 4096 {
		apiError(w, 400, "invalid_provider", "Upstream API key must not exceed 4096 bytes", "")
		return
	}
	for _, price := range []*int64{patch.InputPrice, patch.OutputPrice} {
		if price != nil {
			if _, err := cost(Usage{}, *price, 0); err != nil {
				apiError(w, 400, "invalid_price", err.Error(), "")
				return
			}
		}
	}
	if patch.Priority != nil && (*patch.Priority < -2147483648 || *patch.Priority > 2147483647) {
		apiError(w, 400, "invalid_provider", "Priority must fit a signed 32-bit integer", "")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	p, err := s.Store.UpdateProvider(ctx, id, patch)
	if err != nil {
		s.storeError(w, err)
		return
	}
	s.Logger.Info("administrator updated provider", "provider_id", id, "credential_changed", patch.APIKey != nil)
	writeJSON(w, 200, p)
}

type requestFilter struct {
	Account, Key, Provider, Model, State, Outcome, Error string
	Stream                                               *bool
	HTTPStatus                                           *int
	Since, Until                                         *time.Time
}

func parseRequestFilter(q url.Values) (requestFilter, error) {
	f := requestFilter{Account: q.Get("account_id"), Key: q.Get("key_id"), Provider: q.Get("provider_id"), Model: q.Get("model"), State: q.Get("state"), Outcome: q.Get("outcome"), Error: q.Get("error_code")}
	for _, id := range []string{f.Account, f.Key, f.Provider} {
		if id != "" && uuid.Validate(id) != nil {
			return f, errors.New("account_id, key_id and provider_id must be UUIDs")
		}
	}
	if len(f.Model) > 100 || len(f.Error) > 100 {
		return f, errors.New("model and error_code must not exceed 100 bytes")
	}
	if f.State != "" && !map[string]bool{"reserved": true, "dispatched": true, "pending_usage": true, "settled": true, "released": true}[f.State] {
		return f, errors.New("invalid request state")
	}
	if f.Outcome != "" && !map[string]bool{"succeeded": true, "failed": true, "interrupted": true, "cancelled": true}[f.Outcome] {
		return f, errors.New("invalid request outcome")
	}
	if value, exists := q["stream"]; exists {
		if len(value) != 1 || (value[0] != "true" && value[0] != "false") {
			return f, errors.New("stream must be true or false")
		}
		v := value[0] == "true"
		f.Stream = &v
	}
	if value, exists := q["http_status"]; exists {
		v, err := strconv.Atoi(value[0])
		if err != nil || (v != 0 && (v < 100 || v > 599)) {
			return f, errors.New("http_status must be 0 or between 100 and 599")
		}
		f.HTTPStatus = &v
	}
	for name, target := range map[string]**time.Time{"since": &f.Since, "until": &f.Until} {
		if value, exists := q[name]; exists {
			v, err := time.Parse(time.RFC3339Nano, value[0])
			if err != nil || v.Year() < 1000 || v.Year() > 9999 {
				return f, errors.New("since/until must be RFC3339 timestamps")
			}
			v = v.UTC()
			*target = &v
		}
	}
	if f.Since != nil && f.Until != nil && !f.Until.After(*f.Since) {
		return f, errors.New("until must be after since")
	}
	return f, nil
}

func (s *Server) listAdminRequests(w http.ResponseWriter, r *http.Request) {
	page, err := parseAdminPage(r, "requests", "", "account_id", "key_id", "provider_id", "model", "state", "outcome", "stream", "http_status", "error_code", "since", "until")
	if err != nil {
		apiError(w, 400, "invalid_query", err.Error(), "")
		return
	}
	filter, err := parseRequestFilter(r.URL.Query())
	if err != nil {
		apiError(w, 400, "invalid_query", err.Error(), "")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	result, err := s.Store.RequestsPage(ctx, filter, page)
	if err != nil {
		s.storeError(w, err)
		return
	}
	writeJSON(w, 200, result)
}

func (s *Server) adminRequestDetail(w http.ResponseWriter, r *http.Request) {
	id, ok := adminID(w, r)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	result, err := s.Store.AdminRequestDetail(ctx, id)
	if err != nil {
		s.storeError(w, err)
		return
	}
	writeJSON(w, 200, result)
}
