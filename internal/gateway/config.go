package gateway

import (
	"encoding/base64"
	"errors"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Address               string
	DSN                   string
	RedisAddress          string
	RedisPassword         string
	AdminToken            string
	MasterKey             []byte
	DataDirectory         string
	MaxInflight           int
	RequestTimeout        time.Duration
	DatabaseReadTimeout   time.Duration
	AdmissionTimeout      time.Duration
	RequestIDVersion      int
	StreamIdle            time.Duration
	LeaseTTL              time.Duration
	Heartbeat             time.Duration
	MaxDBConnections      int
	MetadataDBConnections int
	RateLimitTimeout      time.Duration
	Diagnostics           bool
	RequestTraces         bool
	FaultInjection        bool
	CombinedAdmission     bool
}

func FromEnv() (Config, error) {
	key, err := base64.RawURLEncoding.DecodeString(os.Getenv("LLMGW_MASTER_KEY"))
	if err != nil || len(key) != 32 {
		return Config{}, errors.New("LLMGW_MASTER_KEY must contain a base64url encoded 32-byte key")
	}
	c := Config{
		Address:       env("LLMGW_ADDRESS", "127.0.0.1:29480"),
		DSN:           os.Getenv("LLMGW_DSN"),
		RedisAddress:  env("LLMGW_REDIS_ADDRESS", "127.0.0.1:29479"),
		RedisPassword: os.Getenv("LLMGW_REDIS_PASSWORD"),
		AdminToken:    os.Getenv("LLMGW_ADMIN_TOKEN"), MasterKey: key,
		DataDirectory:         env("LLMGW_DATA_ROOT", "/data/zjr/llm_gateway"),
		MaxInflight:           integer("LLMGW_MAX_INFLIGHT", 32),
		RequestTimeout:        time.Duration(integer("LLMGW_REQUEST_TIMEOUT_SECONDS", 120)) * time.Second,
		DatabaseReadTimeout:   time.Duration(integer("LLMGW_DATABASE_READ_TIMEOUT_MS", 1000)) * time.Millisecond,
		AdmissionTimeout:      time.Duration(integer("LLMGW_ADMISSION_TIMEOUT_MS", 5000)) * time.Millisecond,
		RequestIDVersion:      integer("LLMGW_REQUEST_ID_VERSION", 4),
		StreamIdle:            time.Duration(integer("LLMGW_STREAM_IDLE_SECONDS", 20)) * time.Second,
		LeaseTTL:              time.Duration(integer("LLMGW_LEASE_SECONDS", 45)) * time.Second,
		Heartbeat:             time.Duration(integer("LLMGW_HEARTBEAT_SECONDS", 5)) * time.Second,
		MaxDBConnections:      integer("LLMGW_DB_CONNECTIONS", 32),
		MetadataDBConnections: integer("LLMGW_METADATA_DB_CONNECTIONS", 2),
		RateLimitTimeout:      time.Duration(integer("LLMGW_RATE_LIMIT_TIMEOUT_MS", 3000)) * time.Millisecond,
		Diagnostics:           os.Getenv("LLMGW_DIAGNOSTICS") == "true",
		RequestTraces:         os.Getenv("LLMGW_REQUEST_TRACES") == "true",
		FaultInjection:        os.Getenv("LLMGW_FAULT_INJECTION") == "true",
		CombinedAdmission:     env("LLMGW_ADMISSION_MODE", "combined") == "combined",
	}
	if mode := env("LLMGW_ADMISSION_MODE", "combined"); mode != "combined" && mode != "split" {
		return c, errors.New("admission mode must be combined or split")
	}
	if c.MaxDBConnections < 1 || c.MaxDBConnections > 128 {
		return c, errors.New("database connections must be between 1 and 128")
	}
	if c.MetadataDBConnections < 0 || c.MetadataDBConnections >= c.MaxDBConnections {
		return c, errors.New("metadata connections must be nonnegative and smaller than the total database budget")
	}
	if c.RateLimitTimeout <= 0 {
		return c, errors.New("rate limit timeout must be positive")
	}
	if c.DatabaseReadTimeout <= 0 || c.AdmissionTimeout <= 0 {
		return c, errors.New("database read and admission timeouts must be positive")
	}
	if c.RequestIDVersion != 4 && c.RequestIDVersion != 7 {
		return c, errors.New("request ID version must be 4 or 7")
	}
	if c.FaultInjection && c.Address != "127.0.0.1:29483" && c.Address != "127.0.0.1:29484" {
		return c, errors.New("fault injection is restricted to the isolated loopback lab instances")
	}
	if c.DSN == "" || len(c.AdminToken) < 24 || c.MaxInflight < 1 || c.RequestTimeout <= 0 || c.StreamIdle <= 0 || c.Heartbeat <= 0 || c.LeaseTTL <= 2*c.Heartbeat {
		return c, errors.New("invalid database, admin credential, concurrency or timeout configuration")
	}
	return c, nil
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func integer(name string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(name))
	if err != nil {
		return fallback
	}
	return value
}
