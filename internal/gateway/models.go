package gateway

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"time"
)

var (
	ErrQuota     = errors.New("insufficient available token quota")
	ErrConflict  = errors.New("idempotency key already belongs to a different request")
	ErrNotFound  = errors.New("resource not found")
	ErrForbidden = errors.New("credential disabled or invalid")
)

type Account struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Limit     int64  `json:"quota_tokens"`
	Used      int64  `json:"used_tokens"`
	Reserved  int64  `json:"reserved_tokens"`
	Available int64  `json:"available_tokens"`
}

type Key struct {
	ID        string `json:"id"`
	AccountID string `json:"account_id"`
	Name      string `json:"name"`
	Prefix    string `json:"prefix"`
	RPM       int    `json:"requests_per_minute"`
}

type Provider struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Alias       string `json:"model_alias"`
	Model       string `json:"upstream_model"`
	BaseURL     string `json:"base_url"`
	Priority    int    `json:"priority"`
	Enabled     bool   `json:"enabled"`
	InputPrice  int64  `json:"input_micro_usd_per_million_tokens"`
	OutputPrice int64  `json:"output_micro_usd_per_million_tokens"`
	Secret      []byte `json:"-"`
}

type Usage struct {
	Prompt     int64 `json:"prompt_tokens"`
	Completion int64 `json:"completion_tokens"`
	Total      int64 `json:"total_tokens"`
}

type Request struct {
	ID          string    `json:"id"`
	AccountID   string    `json:"account_id"`
	KeyID       string    `json:"key_id"`
	Alias       string    `json:"model"`
	Stream      bool      `json:"stream"`
	State       string    `json:"state"`
	Outcome     string    `json:"outcome"`
	Reserved    int64     `json:"reservation_tokens"`
	Usage       *Usage    `json:"usage,omitempty"`
	Cost        int64     `json:"cost_micro_usd"`
	HTTPStatus  int       `json:"http_status"`
	Error       string    `json:"error,omitempty"`
	ProviderID  string    `json:"provider_id,omitempty"`
	Created     time.Time `json:"created"`
	Updated     time.Time `json:"updated"`
	Payload     []byte    `json:"-"`
	Hash        []byte    `json:"-"`
	InputPrice  int64     `json:"-"`
	OutputPrice int64     `json:"-"`
}

// A receipt survives database outages and is replayed with the same request ID.
type Receipt struct {
	AttemptID     int64  `json:"attempt_id,omitempty"`
	RequestID     string `json:"request_id"`
	AccountID     string `json:"account_id"`
	Outcome       string `json:"outcome"`
	HTTPStatus    int    `json:"http_status"`
	Usage         *Usage `json:"usage,omitempty"`
	Payload       []byte `json:"payload,omitempty"`
	Error         string `json:"error,omitempty"`
	Resolve       bool   `json:"resolve,omitempty"`
	Reason        string `json:"reason,omitempty"`
	NotDispatched bool   `json:"not_dispatched,omitempty"`
}

// AdmissionCommitError retains client-generated identity, not a claim that the
// transaction committed. The caller has not started any model request yet.
type AdmissionCommitError struct {
	RequestID string
	AccountID string
	AttemptID int64
	Cause     error
}

func (e *AdmissionCommitError) Error() string {
	return "admission commit was not acknowledged: " + e.Cause.Error()
}
func (e *AdmissionCommitError) Unwrap() error { return e.Cause }

type UsageGroup struct {
	Group        string `json:"group"`
	Requests     int64  `json:"requests"`
	Succeeded    int64  `json:"succeeded"`
	Failed       int64  `json:"failed"`
	Pending      int64  `json:"pending_usage"`
	Prompt       int64  `json:"prompt_tokens"`
	Completion   int64  `json:"completion_tokens"`
	Total        int64  `json:"total_tokens"`
	CostMicroUSD int64  `json:"cost_micro_usd"`
}

func hashKey(raw string) []byte { sum := sha256.Sum256([]byte(raw)); return sum[:] }

func newSecret(prefix string) (string, error) {
	b := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		return "", err
	}
	return prefix + base64.RawURLEncoding.EncodeToString(b), nil
}

func encrypt(key, value []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return gcm.Seal(nonce, nonce, value, nil), nil
}

func decrypt(key, value []byte) (string, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil || len(value) < gcm.NonceSize() {
		return "", errors.New("invalid encrypted provider credential")
	}
	plain, err := gcm.Open(nil, value[:gcm.NonceSize()], value[gcm.NonceSize():], nil)
	return string(plain), err
}

func normalizeUsage(usage *Usage) error {
	if usage == nil {
		return nil
	}
	if usage.Prompt < 0 || usage.Completion < 0 || usage.Prompt > 10000000 || usage.Completion > 10000000 {
		return errors.New("invalid upstream token usage")
	}
	if usage.Total == 0 {
		usage.Total = usage.Prompt + usage.Completion
	}
	if usage.Total != usage.Prompt+usage.Completion {
		return errors.New("upstream total_tokens disagrees with prompt and completion tokens")
	}
	return nil
}

func cost(usage Usage, input, output int64) (int64, error) {
	if input < 0 || output < 0 || input > 1000000000 || output > 1000000000 {
		return 0, fmt.Errorf("invalid price")
	}
	value := usage.Prompt*input + usage.Completion*output
	return (value + 999999) / 1000000, nil
}
