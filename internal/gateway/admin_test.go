package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestAdminEndpointsRequireAdministrator(t *testing.T) {
	s := NewServer(Config{AdminToken: "management-test-admin", MaxInflight: 1}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer s.Redis.Close()
	id := uuid.NewString()
	for _, route := range []struct{ method, path string }{
		{"GET", "/admin/accounts"}, {"GET", "/admin/accounts/" + id},
		{"GET", "/admin/accounts/" + id + "/keys"}, {"PATCH", "/admin/keys/" + id},
		{"GET", "/admin/keys/" + id},
		{"GET", "/admin/providers/" + id}, {"PATCH", "/admin/providers/" + id},
		{"GET", "/admin/requests"}, {"GET", "/admin/requests/" + id},
	} {
		for _, credential := range []string{"", "ordinary-api-key"} {
			r := httptest.NewRequest(route.method, route.path, strings.NewReader(`{}`))
			r.Header.Set("Authorization", "Bearer "+credential)
			w := httptest.NewRecorder()
			s.ServeHTTP(w, r)
			if w.Code != 401 {
				t.Fatalf("%s %s admitted a non-admin: %d", route.method, route.path, w.Code)
			}
		}
	}
}

// Management reads and writes exercise real MariaDB, not a mocked repository.
func TestMySQLAdminManagement(t *testing.T) {
	dsn := os.Getenv("LLMGW_TEST_DSN")
	if dsn == "" {
		t.Skip("set LLMGW_TEST_DSN for real management API integration tests")
	}
	store, err := OpenStore(Config{DSN: dsn, MasterKey: bytes.Repeat([]byte{9}, 32), LeaseTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := store.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	s := NewServer(Config{AdminToken: "management-test-admin", MaxInflight: 2}, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer s.Redis.Close()
	var accounts, providers []string
	defer func() {
		for _, id := range accounts {
			for _, table := range []string{"attempts", "ledger"} {
				_, _ = store.DB.Exec("DELETE FROM "+table+" WHERE request_id IN (SELECT id FROM requests WHERE account_id=?)", id)
			}
			for _, table := range []string{"requests", "api_keys"} {
				_, _ = store.DB.Exec("DELETE FROM "+table+" WHERE account_id=?", id)
			}
			_, _ = store.DB.Exec("DELETE FROM accounts WHERE id=?", id)
		}
		for _, id := range providers {
			_, _ = store.DB.Exec("DELETE FROM providers WHERE id=?", id)
		}
	}()
	call := func(t *testing.T, method, path, body string, status int) map[string]any {
		t.Helper()
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer management-test-admin")
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		if w.Code != status {
			t.Fatalf("%s %s: status=%d wanted=%d body=%s", method, path, w.Code, status, w.Body.String())
		}
		var result map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	newAccount := func(name string) Account {
		t.Helper()
		a, err := store.CreateAccount(ctx, name, 100000)
		if err != nil {
			t.Fatal(err)
		}
		accounts = append(accounts, a.ID)
		return a
	}
	name := "management-" + store.OwnerID
	a := newAccount(name)
	b := newAccount(name)
	key, secret, err := store.CreateKey(ctx, a.ID, "first", 30)
	if err != nil {
		t.Fatal(err)
	}
	otherKey, _, err := store.CreateKey(ctx, b.ID, "other", 40)
	if err != nil {
		t.Fatal(err)
	}
	p, err := store.CreateProvider(ctx, Provider{Name: name, Alias: "management-model", Model: "native-model", BaseURL: "http://127.0.0.1:9/v1", Priority: 1, Enabled: true, InputPrice: 1000000}, "old-provider-secret")
	if err != nil {
		t.Fatal(err)
	}
	providers = append(providers, p.ID)

	t.Run("account_pagination_and_detail", func(t *testing.T) {
		// Equal timestamps force pagination to use the ID tie-breaker.
		_, err := store.DB.ExecContext(ctx, "UPDATE accounts SET created='2026-11-01 00:00:00' WHERE id IN (?,?)", a.ID, b.ID)
		if err != nil {
			t.Fatal(err)
		}
		path := "/admin/accounts?q=" + url.QueryEscape(name) + "&limit=1"
		first := call(t, "GET", path, "", 200)
		data := first["data"].([]any)
		if len(data) != 1 || first["has_more"] != true {
			t.Fatal(first)
		}
		second := call(t, "GET", path+"&cursor="+url.QueryEscape(first["next_cursor"].(string)), "", 200)
		if second["has_more"] != false || len(second["data"].([]any)) != 1 || data[0].(map[string]any)["id"] == second["data"].([]any)[0].(map[string]any)["id"] {
			t.Fatal("pagination skipped or repeated a tied row", second)
		}
		detail := call(t, "GET", "/admin/accounts/"+a.ID, "", 200)
		if detail["available_tokens"] != float64(100000) {
			t.Fatal(detail)
		}
		call(t, "GET", "/admin/accounts/"+uuid.NewString(), "", 404)
		call(t, "GET", path+"&cursor="+url.QueryEscape(first["next_cursor"].(string))+"&unexpected=x", "", 400)
		call(t, "GET", "/admin/accounts?q=different&cursor="+url.QueryEscape(first["next_cursor"].(string)), "", 400)
	})
	t.Run("key_scope_updates_and_revocation", func(t *testing.T) {
		listed := call(t, "GET", "/admin/accounts/"+a.ID+"/keys", "", 200)
		encoded, _ := json.Marshal(listed)
		if bytes.Contains(encoded, []byte(secret)) || bytes.Contains(encoded, []byte("key_hash")) || len(listed["data"].([]any)) != 1 || listed["data"].([]any)[0].(map[string]any)["id"] == otherKey.ID {
			t.Fatal("key list leaked credentials or another account")
		}
		changed := call(t, "PATCH", "/admin/keys/"+key.ID, `{"name":"renamed","requests_per_minute":7}`, 200)
		if changed["name"] != "renamed" || changed["requests_per_minute"] != float64(7) || changed["active"] != true {
			t.Fatal(changed)
		}
		authenticated, err := store.Authenticate(ctx, secret)
		if err != nil || authenticated.RPM != 7 {
			t.Fatal("new RPM not used by authentication", authenticated, err)
		}
		call(t, "PATCH", "/admin/keys/"+key.ID, `{"requests_per_minute":0}`, 400)
		call(t, "PATCH", "/admin/keys/"+key.ID, `{"active":true}`, 400)
		if call(t, "GET", "/admin/keys/"+key.ID, "", 200)["name"] != "renamed" {
			t.Fatal("invalid key patch changed data")
		}
		call(t, "DELETE", "/admin/keys/"+key.ID, "", 200)
		call(t, "PATCH", "/admin/keys/"+key.ID, `{"name":"still-revoked"}`, 200)
		if _, err := store.Authenticate(ctx, secret); err == nil {
			t.Fatal("editing a revoked key reactivated it")
		}
		inactive := call(t, "GET", "/admin/accounts/"+a.ID+"/keys?active=false", "", 200)
		if len(inactive["data"].([]any)) != 1 {
			t.Fatal(inactive)
		}
		call(t, "GET", "/admin/accounts/"+uuid.NewString()+"/keys", "", 404)
		call(t, "PATCH", "/admin/keys/"+uuid.NewString(), `{"name":"missing"}`, 404)
	})
	t.Run("key_pagination_is_bound_to_account", func(t *testing.T) {
		_, _, err := store.CreateKey(ctx, a.ID, "pagination", 10)
		if err != nil {
			t.Fatal(err)
		}
		first := call(t, "GET", "/admin/accounts/"+a.ID+"/keys?limit=1", "", 200)
		if first["has_more"] != true {
			t.Fatal(first)
		}
		cursor := url.QueryEscape(first["next_cursor"].(string))
		second := call(t, "GET", "/admin/accounts/"+a.ID+"/keys?limit=1&cursor="+cursor, "", 200)
		if second["has_more"] != false || first["data"].([]any)[0].(map[string]any)["id"] == second["data"].([]any)[0].(map[string]any)["id"] {
			t.Fatal("key pagination repeated a row", second)
		}
		call(t, "GET", "/admin/accounts/"+b.ID+"/keys?limit=1&cursor="+cursor, "", 400)
		call(t, "GET", "/admin/accounts/"+a.ID+"/keys?active=1", "", 400)
	})
	t.Run("provider_partial_update_and_secret_rotation", func(t *testing.T) {
		path := "/admin/providers/" + p.ID
		updated := call(t, "PATCH", path, `{"priority":0,"enabled":false,"input_micro_usd_per_million_tokens":0,"api_key":"new-provider-secret"}`, 200)
		raw, _ := json.Marshal(updated)
		if updated["priority"] != float64(0) || updated["enabled"] != false || updated["name"] != name || bytes.Contains(raw, []byte("secret")) || bytes.Contains(raw, []byte("api_key")) {
			t.Fatal("provider patch reset omitted fields or disclosed a secret", updated)
		}
		stored, err := store.Providers(ctx, "")
		if err != nil {
			t.Fatal(err)
		}
		for _, provider := range stored {
			if provider.ID == p.ID {
				plain, err := decrypt(store.MasterKey, provider.Secret)
				if err != nil || plain != "new-provider-secret" {
					t.Fatal("credential rotation failed", err)
				}
			}
		}
		for _, body := range []string{`{}`, `{"name":null}`, `{"priority":null,"enabled":true}`, `{"base_url":"file:///etc/passwd","enabled":true}`, `{"base_url":"http://user:password@localhost"}`, `{"name":"","enabled":true}`, `{"priority":1,"unknown":true}`, `{"output_micro_usd_per_million_tokens":-1}`} {
			call(t, "PATCH", path, body, 400)
		}
		if call(t, "GET", path, "", 200)["enabled"] != false {
			t.Fatal("invalid patch partially applied")
		}
		call(t, "PATCH", path, `{"model_alias":"renamed-model","upstream_model":"new-native","base_url":"http://127.0.0.1:10/v1/","enabled":true,"output_micro_usd_per_million_tokens":2000000}`, 200)
		current := call(t, "GET", path, "", 200)
		if current["model_alias"] != "renamed-model" || current["base_url"] != "http://127.0.0.1:10/v1" {
			t.Fatal(current)
		}
		call(t, "PATCH", path, `{"enabled":true}`, 200) // no-op updates remain successful
		call(t, "PATCH", "/admin/providers/"+uuid.NewString(), `{"enabled":false}`, 404)
	})
	t.Run("request_filters_pagination_and_details", func(t *testing.T) {
		var ids []string
		for i := 0; i < 3; i++ {
			hash := sha256.Sum256([]byte{byte(i)})
			r, _, attempt, err := store.Admit(ctx, otherKey, "filter-model", "admin-query-"+string(rune('a'+i)), hash[:], i == 0, 100, func() (Provider, error) { return p, nil })
			if err != nil {
				t.Fatal(err)
			}
			if err = store.Finish(ctx, Receipt{AttemptID: attempt, RequestID: r.ID, AccountID: r.AccountID, Outcome: "succeeded", HTTPStatus: 200, Usage: &Usage{Prompt: 5, Completion: 7, Total: 12}, Payload: []byte("private-response")}); err != nil {
				t.Fatal(err)
			}
			ids = append(ids, r.ID)
		}
		for _, id := range ids {
			if _, err := store.DB.ExecContext(ctx, "UPDATE requests SET created='2026-11-02 00:00:00' WHERE id=?", id); err != nil {
				t.Fatal(err)
			}
		}
		path := "/admin/requests?account_id=" + b.ID + "&model=filter-model&state=settled&outcome=succeeded&provider_id=" + p.ID + "&limit=2"
		first := call(t, "GET", path, "", 200)
		if len(first["data"].([]any)) != 2 || first["has_more"] != true {
			t.Fatal(first)
		}
		second := call(t, "GET", path+"&cursor="+url.QueryEscape(first["next_cursor"].(string)), "", 200)
		if len(second["data"].([]any)) != 1 || second["has_more"] != false {
			t.Fatal(second)
		}
		seen := map[string]bool{}
		for _, page := range []map[string]any{first, second} {
			for _, item := range page["data"].([]any) {
				id := item.(map[string]any)["id"].(string)
				if seen[id] {
					t.Fatal("repeated request", id)
				}
				seen[id] = true
			}
		}
		stream := call(t, "GET", "/admin/requests?account_id="+b.ID+"&stream=true&http_status=200&since=2026-11-01T00:00:00Z&until=2026-11-03T00:00:00Z", "", 200)
		if len(stream["data"].([]any)) != 1 {
			t.Fatal(stream)
		}
		detail := call(t, "GET", "/admin/requests/"+ids[0], "", 200)
		raw, _ := json.Marshal(detail)
		if len(detail["attempts"].([]any)) != 1 || bytes.Contains(raw, []byte("private-response")) || bytes.Contains(raw, []byte("payload_hash")) {
			t.Fatal("request detail missing attempts or disclosed content")
		}
		call(t, "GET", "/admin/requests/"+uuid.NewString(), "", 404)
		for _, query := range []string{"limit=0", "limit=201", "limit=1&limit=2", "cursor=invalid", "stream=yes", "http_status=99", "state=bogus", "since=tomorrow", "since=2026-11-03T00:00:00Z&until=2026-11-01T00:00:00Z", "account_id=not-a-uuid", "unexpected=1"} {
			call(t, "GET", "/admin/requests?"+query, "", 400)
		}
	})
}
