package gateway

import (
	"context"
	"strings"
	"time"
)

type accountRecord struct {
	Account
	Created time.Time `json:"created"`
}

type keyRecord struct {
	Key
	Active bool `json:"active"`
}

func timePosition(id string, created time.Time) adminCursor {
	created = created.UTC()
	return adminCursor{ID: id, Created: &created}
}

// All dynamic SQL fragments below are fixed server-side column names. Values,
// including cursor positions and LIKE-free name searches, are parameterized.
func (s *Store) AccountsPage(ctx context.Context, page adminPage, name string) (adminList[accountRecord], error) {
	query := "SELECT id,name,quota_tokens,used_tokens,reserved_tokens,created FROM accounts WHERE 1=1"
	args := []any{}
	if name != "" {
		query += " AND LOCATE(?,name)>0"
		args = append(args, name)
	}
	if page.After != nil {
		query += " AND (created<? OR (created=? AND id<?))"
		args = append(args, *page.After.Created, *page.After.Created, page.After.ID)
	}
	query += " ORDER BY created DESC,id DESC LIMIT ?"
	args = append(args, page.Limit+1)
	rows, err := s.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return adminList[accountRecord]{}, err
	}
	defer rows.Close()
	items := []accountRecord{}
	for rows.Next() {
		var a accountRecord
		if err = rows.Scan(&a.ID, &a.Name, &a.Limit, &a.Used, &a.Reserved, &a.Created); err != nil {
			return adminList[accountRecord]{}, err
		}
		a.Available = a.Limit - a.Used - a.Reserved
		items = append(items, a)
	}
	return finishAdminPage(items, page, func(a accountRecord) adminCursor { return timePosition(a.ID, a.Created) }), rows.Err()
}

func (s *Store) KeysPage(ctx context.Context, account string, active *bool, page adminPage) (adminList[keyRecord], error) {
	query := "SELECT id,account_id,name,prefix,rpm,active FROM api_keys WHERE account_id=?"
	args := []any{account}
	if active != nil {
		query += " AND active=?"
		args = append(args, *active)
	}
	if page.After != nil {
		query += " AND id<?"
		args = append(args, page.After.ID)
	}
	query += " ORDER BY id DESC LIMIT ?"
	args = append(args, page.Limit+1)
	rows, err := s.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return adminList[keyRecord]{}, err
	}
	defer rows.Close()
	items := []keyRecord{}
	for rows.Next() {
		var k keyRecord
		if err = rows.Scan(&k.ID, &k.AccountID, &k.Name, &k.Prefix, &k.RPM, &k.Active); err != nil {
			return adminList[keyRecord]{}, err
		}
		items = append(items, k)
	}
	return finishAdminPage(items, page, func(k keyRecord) adminCursor { return adminCursor{ID: k.ID} }), rows.Err()
}

func (s *Store) KeyDetail(ctx context.Context, id string) (keyRecord, error) {
	var k keyRecord
	err := s.DB.QueryRowContext(ctx, "SELECT id,account_id,name,prefix,rpm,active FROM api_keys WHERE id=?", id).Scan(&k.ID, &k.AccountID, &k.Name, &k.Prefix, &k.RPM, &k.Active)
	return k, err
}

func (s *Store) UpdateKey(ctx context.Context, id string, patch keyPatch) (keyRecord, error) {
	fields := []string{}
	args := []any{}
	if patch.Name != nil {
		fields = append(fields, "name=?")
		args = append(args, *patch.Name)
	}
	if patch.RPM != nil {
		fields = append(fields, "rpm=?")
		args = append(args, *patch.RPM)
	}
	args = append(args, id)
	if _, err := s.DB.ExecContext(ctx, "UPDATE api_keys SET "+strings.Join(fields, ",")+" WHERE id=?", args...); err != nil {
		return keyRecord{}, err
	}
	return s.KeyDetail(ctx, id)
}

func (s *Store) Provider(ctx context.Context, id string) (Provider, error) {
	var p Provider
	err := s.DB.QueryRowContext(ctx, "SELECT id,name,model_alias,upstream_model,base_url,secret,priority,enabled,input_price,output_price FROM providers WHERE id=?", id).Scan(&p.ID, &p.Name, &p.Alias, &p.Model, &p.BaseURL, &p.Secret, &p.Priority, &p.Enabled, &p.InputPrice, &p.OutputPrice)
	return p, err
}

func (s *Store) UpdateProvider(ctx context.Context, id string, patch providerPatch) (Provider, error) {
	fields := []string{}
	args := []any{}
	add := func(column string, value any) { fields = append(fields, column+"=?"); args = append(args, value) }
	if patch.Name != nil {
		add("name", *patch.Name)
	}
	if patch.Alias != nil {
		add("model_alias", *patch.Alias)
	}
	if patch.Model != nil {
		add("upstream_model", *patch.Model)
	}
	if patch.BaseURL != nil {
		add("base_url", *patch.BaseURL)
	}
	if patch.Priority != nil {
		add("priority", *patch.Priority)
	}
	if patch.Enabled != nil {
		add("enabled", *patch.Enabled)
	}
	if patch.InputPrice != nil {
		add("input_price", *patch.InputPrice)
	}
	if patch.OutputPrice != nil {
		add("output_price", *patch.OutputPrice)
	}
	if patch.APIKey != nil {
		secret, err := encrypt(s.MasterKey, []byte(*patch.APIKey))
		if err != nil {
			return Provider{}, err
		}
		add("secret", secret)
	}
	args = append(args, id)
	// One UPDATE changes only supplied fields; concurrent patches to different
	// fields cannot reset one another. Existing requests retain their price snapshot.
	if _, err := s.DB.ExecContext(ctx, "UPDATE providers SET "+strings.Join(fields, ",")+" WHERE id=?", args...); err != nil {
		return Provider{}, err
	}
	return s.Provider(ctx, id)
}

// Management queries never load response blobs or hashes used for replay.
var adminRequestColumns = strings.Replace(requestColumns, "response_payload,payload_hash", "NULL,NULL", 1)

func (s *Store) RequestsPage(ctx context.Context, filter requestFilter, page adminPage) (adminList[Request], error) {
	query := "SELECT " + adminRequestColumns + " FROM requests WHERE 1=1"
	args := []any{}
	for _, pair := range []struct{ column, value string }{{"account_id", filter.Account}, {"key_id", filter.Key}, {"provider_id", filter.Provider}, {"model_alias", filter.Model}, {"state", filter.State}, {"outcome", filter.Outcome}, {"error_code", filter.Error}} {
		if pair.value != "" {
			query += " AND " + pair.column + "=?"
			args = append(args, pair.value)
		}
	}
	if filter.Stream != nil {
		query += " AND is_stream=?"
		args = append(args, *filter.Stream)
	}
	if filter.HTTPStatus != nil {
		query += " AND http_status=?"
		args = append(args, *filter.HTTPStatus)
	}
	if filter.Since != nil {
		query += " AND created>=?"
		args = append(args, *filter.Since)
	}
	if filter.Until != nil {
		query += " AND created<?"
		args = append(args, *filter.Until)
	}
	if page.After != nil {
		query += " AND (created<? OR (created=? AND id<?))"
		args = append(args, *page.After.Created, *page.After.Created, page.After.ID)
	}
	query += " ORDER BY created DESC,id DESC LIMIT ?"
	args = append(args, page.Limit+1)
	rows, err := s.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return adminList[Request]{}, err
	}
	defer rows.Close()
	items := []Request{}
	for rows.Next() {
		r, err := scanRequest(rows)
		if err != nil {
			return adminList[Request]{}, err
		}
		items = append(items, r)
	}
	return finishAdminPage(items, page, func(r Request) adminCursor { return timePosition(r.ID, r.Created) }), rows.Err()
}

type attemptRecord struct {
	ID         int64      `json:"id"`
	ProviderID string     `json:"provider_id"`
	HTTPStatus int        `json:"http_status"`
	Error      string     `json:"error,omitempty"`
	Started    time.Time  `json:"started"`
	Ended      *time.Time `json:"ended,omitempty"`
}

type adminRequestRecord struct {
	Request
	Attempts []attemptRecord `json:"attempts"`
}

func (s *Store) AdminRequestDetail(ctx context.Context, id string) (adminRequestRecord, error) {
	r, err := scanRequest(s.DB.QueryRowContext(ctx, "SELECT "+adminRequestColumns+" FROM requests WHERE id=?", id))
	if err != nil {
		return adminRequestRecord{}, err
	}
	result := adminRequestRecord{Request: r, Attempts: []attemptRecord{}}
	rows, err := s.DB.QueryContext(ctx, "SELECT id,provider_id,status,error_code,started,ended FROM attempts WHERE request_id=? ORDER BY id", id)
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var a attemptRecord
		if err = rows.Scan(&a.ID, &a.ProviderID, &a.HTTPStatus, &a.Error, &a.Started, &a.Ended); err != nil {
			return result, err
		}
		result.Attempts = append(result.Attempts, a)
	}
	return result, rows.Err()
}
