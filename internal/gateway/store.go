package gateway

import (
	"bytes"
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
)

//go:embed schema.sql
var schema string

type Store struct {
	DB               *sql.DB
	MetadataDB       *sql.DB
	MasterKey        []byte
	OwnerID          string
	LeaseTTL         time.Duration
	RequestIDVersion int
	Observe          func(context.Context, string, time.Time)
}

func (s *Store) observe(ctx context.Context, phase string, started time.Time) {
	if s.Observe != nil {
		s.Observe(ctx, phase, started)
	}
}

func (s *Store) commit(ctx context.Context, tx *sql.Tx, phase string) error {
	defer s.observe(ctx, phase, time.Now())
	return tx.Commit()
}

func OpenStore(config Config) (*Store, error) {
	db, err := sql.Open("mysql", config.DSN)
	if err != nil {
		return nil, err
	}
	connections := config.MaxDBConnections
	if connections == 0 {
		connections = 32
	}
	if config.MetadataDBConnections < 0 || config.MetadataDBConnections >= connections {
		_ = db.Close()
		return nil, errors.New("metadata pool exceeds database connection budget")
	}
	writeConnections := connections - config.MetadataDBConnections
	db.SetMaxOpenConns(writeConnections)
	db.SetMaxIdleConns(min(8, writeConnections))
	db.SetConnMaxLifetime(3 * time.Minute)
	metadata := db
	if config.MetadataDBConnections > 0 {
		metadata, err = sql.Open("mysql", config.DSN)
		if err != nil {
			_ = db.Close()
			return nil, err
		}
		metadata.SetMaxOpenConns(config.MetadataDBConnections)
		metadata.SetMaxIdleConns(config.MetadataDBConnections)
		metadata.SetConnMaxLifetime(3 * time.Minute)
	}
	version := config.RequestIDVersion
	if version == 0 {
		version = 4
	}
	return &Store{DB: db, MetadataDB: metadata, MasterKey: config.MasterKey, OwnerID: uuid.NewString(), LeaseTTL: config.LeaseTTL, RequestIDVersion: version}, nil
}

func (s *Store) Close() error {
	var metadataErr error
	if s.MetadataDB != nil && s.MetadataDB != s.DB {
		metadataErr = s.MetadataDB.Close()
	}
	return errors.Join(metadataErr, s.DB.Close())
}

func (s *Store) metadataDB() *sql.DB {
	if s.MetadataDB != nil {
		return s.MetadataDB
	}
	return s.DB
}

func (s *Store) metadataConnection(ctx context.Context) (*sql.Conn, error) {
	started := time.Now()
	conn, err := s.metadataDB().Conn(ctx)
	s.observe(ctx, "metadata_acquire", started)
	return conn, err
}

func (s *Store) newRequestID() (string, error) {
	var value uuid.UUID
	var err error
	if s.RequestIDVersion == 7 {
		value, err = uuid.NewV7()
	} else {
		value, err = uuid.NewRandom()
	}
	return value.String(), err
}

func (s *Store) Migrate(ctx context.Context) error {
	for _, statement := range strings.Split(schema, ";") {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		if _, err := s.DB.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	// Preserve byte-exact HTTP idempotency keys on databases initialized by v0.
	var kind string
	if err := s.DB.QueryRowContext(ctx, "SELECT DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='requests' AND COLUMN_NAME='idempotency_key'").Scan(&kind); err != nil {
		return err
	}
	if kind != "varbinary" {
		if _, err := s.DB.ExecContext(ctx, "ALTER TABLE requests MODIFY COLUMN idempotency_key VARBINARY(128) NULL"); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) CreateAccount(ctx context.Context, name string, limit int64) (Account, error) {
	id := uuid.NewString()
	_, err := s.DB.ExecContext(ctx, "INSERT INTO accounts(id,name,quota_tokens) VALUES(?,?,?)", id, name, limit)
	if err != nil {
		return Account{}, err
	}
	return s.Account(ctx, id)
}

func (s *Store) Account(ctx context.Context, id string) (Account, error) {
	var a Account
	err := s.DB.QueryRowContext(ctx, "SELECT id,name,quota_tokens,used_tokens,reserved_tokens FROM accounts WHERE id=?", id).Scan(&a.ID, &a.Name, &a.Limit, &a.Used, &a.Reserved)
	a.Available = a.Limit - a.Used - a.Reserved
	return a, err
}

func (s *Store) SetQuota(ctx context.Context, id string, limit int64) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var committed int64
	if err = tx.QueryRowContext(ctx, "SELECT used_tokens+reserved_tokens FROM accounts WHERE id=? FOR UPDATE", id).Scan(&committed); err != nil {
		return err
	}
	if committed > limit {
		return ErrQuota
	}
	if _, err = tx.ExecContext(ctx, "UPDATE accounts SET quota_tokens=? WHERE id=?", limit, id); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) CreateKey(ctx context.Context, accountID, name string, rpm int) (Key, string, error) {
	if _, err := s.Account(ctx, accountID); err != nil {
		return Key{}, "", err
	}
	raw, err := newSecret("sk-gw-")
	if err != nil {
		return Key{}, "", err
	}
	key := Key{ID: uuid.NewString(), AccountID: accountID, Name: name, Prefix: raw[:14], RPM: rpm}
	_, err = s.DB.ExecContext(ctx, "INSERT INTO api_keys(id,account_id,name,key_hash,prefix,rpm) VALUES(?,?,?,?,?,?)", key.ID, key.AccountID, key.Name, hashKey(raw), key.Prefix, rpm)
	return key, raw, err
}

func (s *Store) Authenticate(ctx context.Context, raw string) (Key, error) {
	var key Key
	conn, err := s.metadataConnection(ctx)
	if err != nil {
		return key, err
	}
	defer conn.Close()
	started := time.Now()
	err = conn.QueryRowContext(ctx, "SELECT id,account_id,name,prefix,rpm FROM api_keys WHERE key_hash=? AND active=TRUE", hashKey(raw)).Scan(&key.ID, &key.AccountID, &key.Name, &key.Prefix, &key.RPM)
	s.observe(ctx, "authenticate_query", started)
	if errors.Is(err, sql.ErrNoRows) {
		return key, ErrForbidden
	}
	return key, err
}

func (s *Store) RevokeKey(ctx context.Context, id string) error {
	result, err := s.DB.ExecContext(ctx, "UPDATE api_keys SET active=FALSE WHERE id=?", id)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) CreateProvider(ctx context.Context, p Provider, secret string) (Provider, error) {
	var err error
	p.ID = uuid.NewString()
	p.Secret, err = encrypt(s.MasterKey, []byte(secret))
	if err != nil {
		return p, err
	}
	_, err = s.DB.ExecContext(ctx, `INSERT INTO providers(id,name,model_alias,upstream_model,base_url,secret,priority,enabled,input_price,output_price)
	 VALUES(?,?,?,?,?,?,?,?,?,?)`, p.ID, p.Name, p.Alias, p.Model, p.BaseURL, p.Secret, p.Priority, p.Enabled, p.InputPrice, p.OutputPrice)
	return p, err
}

func (s *Store) Providers(ctx context.Context, alias string) ([]Provider, error) {
	defer s.observe(ctx, "providers", time.Now())
	conn, err := s.metadataConnection(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	started := time.Now()
	defer s.observe(ctx, "providers_query", started)
	query := "SELECT id,name,model_alias,upstream_model,base_url,secret,priority,enabled,input_price,output_price FROM providers"
	var arguments []any
	if alias != "" {
		query += " WHERE model_alias=? AND enabled=TRUE"
		arguments = append(arguments, alias)
	}
	query += " ORDER BY priority DESC,id"
	rows, err := conn.QueryContext(ctx, query, arguments...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	providers := []Provider{}
	for rows.Next() {
		var p Provider
		if err = rows.Scan(&p.ID, &p.Name, &p.Alias, &p.Model, &p.BaseURL, &p.Secret, &p.Priority, &p.Enabled, &p.InputPrice, &p.OutputPrice); err != nil {
			return nil, err
		}
		providers = append(providers, p)
	}
	return providers, rows.Err()
}

func (s *Store) EnableProvider(ctx context.Context, id string, enabled bool) error {
	result, err := s.DB.ExecContext(ctx, "UPDATE providers SET enabled=? WHERE id=?", enabled, id)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		var found string
		if err = s.DB.QueryRowContext(ctx, "SELECT id FROM providers WHERE id=?", id).Scan(&found); errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}
	return nil
}

const requestColumns = `id,account_id,key_id,model_alias,is_stream,state,outcome,reservation_tokens,
 prompt_tokens,completion_tokens,total_tokens,cost_micro_usd,http_status,error_code,provider_id,
 created,updated,response_payload,payload_hash,input_price,output_price`

type scanner interface{ Scan(...any) error }

func scanRequest(row scanner) (Request, error) {
	var r Request
	var prompt, completion, total sql.NullInt64
	err := row.Scan(&r.ID, &r.AccountID, &r.KeyID, &r.Alias, &r.Stream, &r.State, &r.Outcome, &r.Reserved,
		&prompt, &completion, &total, &r.Cost, &r.HTTPStatus, &r.Error, &r.ProviderID,
		&r.Created, &r.Updated, &r.Payload, &r.Hash, &r.InputPrice, &r.OutputPrice)
	if total.Valid {
		r.Usage = &Usage{Prompt: prompt.Int64, Completion: completion.Int64, Total: total.Int64}
	}
	return r, err
}

func (s *Store) Request(ctx context.Context, id string) (Request, error) {
	return scanRequest(s.DB.QueryRowContext(ctx, "SELECT "+requestColumns+" FROM requests WHERE id=?", id))
}

func (s *Store) Requests(ctx context.Context, account string) ([]Request, error) {
	query := "SELECT " + requestColumns + " FROM requests"
	var args []any
	if account != "" {
		query += " WHERE account_id=?"
		args = append(args, account)
	}
	query += " ORDER BY created DESC LIMIT 100"
	rows, err := s.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []Request{}
	for rows.Next() {
		r, e := scanRequest(rows)
		if e != nil {
			return nil, e
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

func (s *Store) UsageReport(ctx context.Context, account, group string, since, until time.Time) ([]UsageGroup, error) {
	expression, ok := map[string]string{"model": "model_alias", "account": "account_id", "provider": "provider_id", "day": "DATE_FORMAT(created,'%Y-%m-%d')"}[group]
	if !ok {
		return nil, errors.New("invalid report grouping")
	}
	query := "SELECT " + expression + `,COUNT(*),SUM(outcome='succeeded'),SUM(outcome IN ('failed','interrupted')),
	 SUM(state='pending_usage'),COALESCE(SUM(prompt_tokens),0),COALESCE(SUM(completion_tokens),0),
	 COALESCE(SUM(total_tokens),0),SUM(cost_micro_usd) FROM requests WHERE created>=? AND created<?`
	args := []any{since, until}
	if account != "" {
		query += " AND account_id=?"
		args = append(args, account)
	}
	query += " GROUP BY " + expression + " ORDER BY " + expression + " LIMIT 1000"
	rows, err := s.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []UsageGroup{}
	for rows.Next() {
		var group UsageGroup
		if err = rows.Scan(&group.Group, &group.Requests, &group.Succeeded, &group.Failed, &group.Pending, &group.Prompt, &group.Completion, &group.Total, &group.CostMicroUSD); err != nil {
			return nil, err
		}
		result = append(result, group)
	}
	return result, rows.Err()
}

func (s *Store) Reserve(ctx context.Context, key Key, alias, idempotency string, hash []byte, stream bool, tokens int64) (Request, bool, error) {
	r, fresh, _, err := s.reserve(ctx, key, alias, idempotency, hash, stream, tokens, nil)
	return r, fresh, err
}

// Admit commits quota reservation and the first dispatch intent atomically.
// choose runs only for a fresh, funded request and must do no network I/O.
func (s *Store) Admit(ctx context.Context, key Key, alias, idempotency string, hash []byte, stream bool, tokens int64, choose func() (Provider, error)) (Request, bool, int64, error) {
	if choose == nil {
		return Request{}, false, 0, errors.New("admission requires a provider selector")
	}
	return s.reserve(ctx, key, alias, idempotency, hash, stream, tokens, choose)
}

func (s *Store) reserve(ctx context.Context, key Key, alias, idempotency string, hash []byte, stream bool, tokens int64, choose func() (Provider, error)) (Request, bool, int64, error) {
	phase := "reserve"
	if choose != nil {
		phase = "admit"
	}
	defer s.observe(ctx, phase, time.Now())
	begin := time.Now()
	tx, err := s.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	s.observe(ctx, phase+"_begin", begin)
	if err != nil {
		return Request{}, false, 0, err
	}
	defer tx.Rollback()
	var limit, used, reserved int64
	locked := time.Now()
	err = tx.QueryRowContext(ctx, "SELECT quota_tokens,used_tokens,reserved_tokens FROM accounts WHERE id=? FOR UPDATE", key.AccountID).Scan(&limit, &used, &reserved)
	s.observe(ctx, phase+"_account_lock", locked)
	if err != nil {
		return Request{}, false, 0, err
	}
	var active bool
	if err = tx.QueryRowContext(ctx, "SELECT active FROM api_keys WHERE id=? FOR UPDATE", key.ID).Scan(&active); err != nil {
		return Request{}, false, 0, err
	}
	if !active {
		return Request{}, false, 0, ErrForbidden
	}
	if idempotency != "" {
		previous, findErr := scanRequest(tx.QueryRowContext(ctx, "SELECT "+requestColumns+" FROM requests WHERE account_id=? AND idempotency_key=? FOR UPDATE", key.AccountID, idempotency))
		if findErr == nil {
			if !bytes.Equal(previous.Hash, hash) {
				return Request{}, false, 0, ErrConflict
			}
			observeRequestID(ctx, previous.ID)
			return previous, false, 0, s.commit(ctx, tx, phase+"_commit")
		}
		if !errors.Is(findErr, sql.ErrNoRows) {
			return Request{}, false, 0, findErr
		}
	}
	if tokens <= 0 || limit-used-reserved < tokens {
		return Request{}, false, 0, ErrQuota
	}
	id, err := s.newRequestID()
	if err != nil {
		return Request{}, false, 0, err
	}
	observeRequestID(ctx, id)
	created := time.Now().UTC()
	var idem any
	if idempotency != "" {
		idem = idempotency
	}
	initialState := "reserved"
	if choose != nil {
		// The transaction is invisible until its dispatch intent and price
		// snapshot are complete. Avoid inserting one recovery-index key and
		// replacing it with another while retaining all admission locks.
		initialState = "dispatched"
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO requests(id,account_id,key_id,idempotency_key,payload_hash,model_alias,is_stream,state,reservation_tokens,owner_id,lease_until)
	 VALUES(?,?,?,?,?,?,?,?,?,?,?)`, id, key.AccountID, key.ID, idem, hash, alias, stream, initialState, tokens, s.OwnerID, time.Now().UTC().Add(s.LeaseTTL))
	if err != nil {
		return Request{}, false, 0, err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE accounts SET reserved_tokens=reserved_tokens+? WHERE id=?", tokens, key.AccountID); err != nil {
		return Request{}, false, 0, err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO ledger(request_id,account_id,event_type,used_delta,reserved_delta) VALUES(?,?,'reserve',0,?)", id, key.AccountID, tokens); err != nil {
		return Request{}, false, 0, err
	}
	r := Request{ID: id, AccountID: key.AccountID, KeyID: key.ID, Alias: alias, Stream: stream, State: initialState, Reserved: tokens, Hash: bytes.Clone(hash), Created: created, Updated: created}
	var attemptID int64
	if choose != nil {
		provider, selectErr := choose()
		if selectErr != nil {
			return Request{}, false, 0, selectErr
		}
		if attemptID, err = s.initialDispatchTx(ctx, tx, r, provider); err != nil {
			return Request{}, false, 0, err
		}
		r.State, r.ProviderID = "dispatched", provider.ID
		r.InputPrice, r.OutputPrice = provider.InputPrice, provider.OutputPrice
	}
	if err = s.commit(ctx, tx, phase+"_commit"); err != nil {
		return Request{}, false, 0, &AdmissionCommitError{RequestID: r.ID, AccountID: r.AccountID, AttemptID: attemptID, Cause: err}
	}
	// Once commit is acknowledged, retain its identity even when the client
	// disconnects immediately. A cancellable SELECT here can lose that identity
	// and strand the reservation under a still-live owner.
	return r, true, attemptID, nil
}

func (s *Store) Dispatch(ctx context.Context, r Request, p Provider) (int64, error) {
	defer s.observe(ctx, "dispatch", time.Now())
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	id, err := s.dispatchTx(ctx, tx, r, p)
	if err != nil {
		return 0, err
	}
	return id, s.commit(ctx, tx, "dispatch_commit")
}

func (s *Store) dispatchTx(ctx context.Context, tx *sql.Tx, r Request, p Provider) (int64, error) {
	result, err := tx.ExecContext(ctx, `UPDATE requests FORCE INDEX(PRIMARY) SET state='dispatched',provider_id=?,input_price=?,output_price=?,lease_until=?,updated=UTC_TIMESTAMP(6)
	 WHERE id=? AND owner_id=? AND state IN ('reserved','dispatched')`, p.ID, p.InputPrice, p.OutputPrice, time.Now().UTC().Add(s.LeaseTTL), r.ID, s.OwnerID)
	if err != nil {
		return 0, err
	}
	n, _ := result.RowsAffected()
	if n != 1 {
		return 0, ErrConflict
	}
	return insertAttemptTx(ctx, tx, r.ID, p.ID)
}

func (s *Store) initialDispatchTx(ctx context.Context, tx *sql.Tx, r Request, p Provider) (int64, error) {
	// A fresh admission already has its final state/lease index key. Updating
	// only the dispatch snapshot avoids a second recovery-index transition.
	result, err := tx.ExecContext(ctx, `UPDATE requests FORCE INDEX(PRIMARY) SET provider_id=?,input_price=?,output_price=?
	 WHERE id=? AND owner_id=? AND state='dispatched'`, p.ID, p.InputPrice, p.OutputPrice, r.ID, s.OwnerID)
	if err != nil {
		return 0, err
	}
	n, _ := result.RowsAffected()
	if n != 1 {
		return 0, ErrConflict
	}
	return insertAttemptTx(ctx, tx, r.ID, p.ID)
}

func insertAttemptTx(ctx context.Context, tx *sql.Tx, requestID, providerID string) (int64, error) {
	result, err := tx.ExecContext(ctx, "INSERT INTO attempts(request_id,provider_id) VALUES(?,?)", requestID, providerID)
	if err != nil {
		return 0, err
	}
	id, _ := result.LastInsertId()
	return id, nil
}

func (s *Store) EndAttempt(ctx context.Context, id int64, status int, code string) error {
	_, err := s.DB.ExecContext(ctx, "UPDATE attempts SET status=?,error_code=?,ended=UTC_TIMESTAMP(6) WHERE id=?", status, code, id)
	return err
}

func (s *Store) Finish(ctx context.Context, receipt Receipt) error {
	return s.finish(ctx, receipt, false)
}

func (s *Store) finish(ctx context.Context, receipt Receipt, onlyExpired bool) error {
	defer s.observe(ctx, "settle", time.Now())
	if err := normalizeUsage(receipt.Usage); err != nil {
		return err
	}
	if receipt.NotDispatched && (receipt.Usage == nil || *receipt.Usage != (Usage{})) {
		return errors.New("not-dispatched receipt requires explicit zero usage")
	}
	tx, err := s.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var account string
	locked := time.Now()
	if err = tx.QueryRowContext(ctx, "SELECT id FROM accounts WHERE id=? FOR UPDATE", receipt.AccountID).Scan(&account); err != nil {
		return err
	}
	s.observe(ctx, "settle_account_lock", locked)
	r, err := scanRequest(tx.QueryRowContext(ctx, "SELECT "+requestColumns+" FROM requests WHERE id=? AND account_id=? FOR UPDATE", receipt.RequestID, account))
	if errors.Is(err, sql.ErrNoRows) && receipt.NotDispatched {
		// Acquiring the pre-existing account's exclusive lock waits for the
		// original admission to commit or roll back. If its UUID is now absent,
		// it cannot appear later from that admission, so no receipt is stranded.
		return tx.Commit()
	}
	if err != nil {
		return err
	}
	if r.State == "settled" || r.State == "released" {
		if receipt.Usage != nil && r.Usage != nil && *receipt.Usage != *r.Usage {
			return ErrConflict
		}
		return tx.Commit()
	}
	if receipt.Resolve && r.State != "pending_usage" {
		return ErrConflict
	}
	if onlyExpired {
		var lease time.Time
		var owner string
		if err = tx.QueryRowContext(ctx, "SELECT owner_id,lease_until FROM requests WHERE id=?", r.ID).Scan(&owner, &lease); err != nil {
			return err
		}
		var ownerLease time.Time
		if err = tx.QueryRowContext(ctx, "SELECT lease_until FROM owner_leases WHERE owner_id=? FOR UPDATE", owner).Scan(&ownerLease); err == nil {
			lease = ownerLease
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if !lease.Before(time.Now().UTC()) || r.State == "pending_usage" {
			return tx.Commit()
		}
		receipt.Outcome = "interrupted"
		receipt.HTTPStatus = 503
		receipt.Error = "owner_lease_expired"
		if r.State == "reserved" {
			receipt.Usage = &Usage{}
		}
	}
	state := "pending_usage"
	var prompt, completion, total any
	costValue := int64(0)
	if receipt.Usage != nil {
		prompt, completion, total = receipt.Usage.Prompt, receipt.Usage.Completion, receipt.Usage.Total
		state = "settled"
		if receipt.Usage.Total == 0 {
			state = "released"
		}
		costValue, err = cost(*receipt.Usage, r.InputPrice, r.OutputPrice)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, "UPDATE accounts SET reserved_tokens=reserved_tokens-?,used_tokens=used_tokens+? WHERE id=?", r.Reserved, receipt.Usage.Total, account)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, "INSERT INTO ledger(request_id,account_id,event_type,used_delta,reserved_delta,reason) VALUES(?,?,'settle',?,?,?)", r.ID, account, receipt.Usage.Total, -r.Reserved, receipt.Reason)
		if err != nil {
			return err
		}
	}
	_, err = tx.ExecContext(ctx, `UPDATE requests SET state=?,outcome=?,prompt_tokens=?,completion_tokens=?,total_tokens=?,cost_micro_usd=?,http_status=?,error_code=?,response_payload=?,updated=UTC_TIMESTAMP(6) WHERE id=?`,
		state, receipt.Outcome, prompt, completion, total, costValue, receipt.HTTPStatus, receipt.Error, receipt.Payload, r.ID)
	if err != nil {
		return err
	}
	if receipt.AttemptID > 0 {
		// A successful upstream attempt and its usage commit together. This saves
		// a separate durable commit on the common request path.
		if _, err = tx.ExecContext(ctx, "UPDATE attempts SET status=?,error_code=?,ended=UTC_TIMESTAMP(6) WHERE id=? AND request_id=?", receipt.HTTPStatus, receipt.Error, receipt.AttemptID, r.ID); err != nil {
			return err
		}
	}
	return s.commit(ctx, tx, "settle_commit")
}

func (s *Store) Heartbeat(ctx context.Context) error {
	// One independent lease row avoids secondary-index/primary-row lock inversion
	// between batch request heartbeats and request settlement transactions.
	_, err := s.DB.ExecContext(ctx, `INSERT INTO owner_leases(owner_id,lease_until) VALUES(?,?)
	 ON DUPLICATE KEY UPDATE lease_until=VALUES(lease_until)`, s.OwnerID, time.Now().UTC().Add(s.LeaseTTL))
	return err
}

func (s *Store) Recover(ctx context.Context) error {
	rows, err := s.DB.QueryContext(ctx, `SELECT r.id,r.account_id FROM requests r
	 LEFT JOIN owner_leases o ON o.owner_id=r.owner_id
	 WHERE r.state IN ('reserved','dispatched') AND COALESCE(o.lease_until,r.lease_until)<UTC_TIMESTAMP(6) LIMIT 100`)
	if err != nil {
		return err
	}
	type identity struct{ id, account string }
	var pending []identity
	for rows.Next() {
		var p identity
		if err = rows.Scan(&p.id, &p.account); err != nil {
			rows.Close()
			return err
		}
		pending = append(pending, p)
	}
	rowErr := rows.Err()
	rows.Close()
	if rowErr != nil {
		return rowErr
	}
	for _, p := range pending {
		if err = s.finish(ctx, Receipt{RequestID: p.id, AccountID: p.account}, true); err != nil {
			return err
		}
	}
	return nil
}
