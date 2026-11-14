package gateway

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// Consume and discard the first successful Lua reply from the real server.
// Receiving its array prefix proves the script executed; the Redis client sees
// EOF instead. NOSCRIPT is passed through so Script.Run can load normally.
type rateReplyLossConn struct {
	net.Conn
	target      atomic.Bool
	armed       *atomic.Bool
	dropped     chan<- struct{}
	holdReplies bool
}

func (c *rateReplyLossConn) Write(p []byte) (int, error) {
	value := bytes.ToLower(p)
	if bytes.Contains(value, []byte("\r\nevalsha\r\n")) || bytes.Contains(value, []byte("\r\neval\r\n")) {
		c.target.Store(true)
	}
	return c.Conn.Write(p)
}

func (c *rateReplyLossConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 && c.target.Swap(false) && p[0] == '*' {
		if c.holdReplies {
			c.dropped <- struct{}{}
			// The real reply is consumed. Wait for the socket deadline while the
			// caller receives no bytes, just as with a stalled return path.
			for {
				if _, waitErr := c.Conn.Read(p); waitErr != nil {
					return 0, waitErr
				}
			}
		}
		if !c.armed.CompareAndSwap(true, false) {
			return n, err
		}
		_ = c.Conn.Close()
		c.dropped <- struct{}{}
		return 0, io.EOF
	}
	return n, err
}

func replyDroppingRedis(t *testing.T) (*redis.Client, <-chan struct{}) {
	return faultRedis(t, false)
}

func faultRedis(t *testing.T, holdReplies bool) (*redis.Client, <-chan struct{}) {
	t.Helper()
	address, password := os.Getenv("LLMGW_TEST_REDIS_ADDRESS"), os.Getenv("LLMGW_TEST_REDIS_PASSWORD")
	if address == "" {
		t.Skip("use run.sh test for the real Redis service")
	}
	armed := &atomic.Bool{}
	armed.Store(true)
	dropped := make(chan struct{}, 4)
	client := redis.NewClient(&redis.Options{Addr: address, Password: password, Protocol: 2,
		ReadTimeout: 2 * time.Second, WriteTimeout: 2 * time.Second, DialTimeout: 2 * time.Second,
		ContextTimeoutEnabled: true, MaxRetries: -1, PoolSize: 16,
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			return &rateReplyLossConn{Conn: conn, armed: armed, dropped: dropped, holdReplies: holdReplies}, nil
		}})
	t.Cleanup(func() { _ = client.Close() })
	return client, dropped
}

func TestRedisIdempotentRateOperations(t *testing.T) {
	if os.Getenv("LLMGW_TEST_REDIS_ADDRESS") == "" {
		t.Skip("use run.sh test for Redis integration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	server := NewServer(Config{MaxInflight: 1, RedisAddress: os.Getenv("LLMGW_TEST_REDIS_ADDRESS"), RedisPassword: os.Getenv("LLMGW_TEST_REDIS_PASSWORD")}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer server.Redis.Close()
	var keys []string
	defer func() { _ = server.Redis.Del(context.Background(), keys...).Err() }()
	newKey := func() Key {
		key := Key{ID: uuid.NewString(), RPM: 1}
		keys = append(keys, "llmgw:rate:"+key.ID)
		return key
	}
	count := func(key Key) int64 {
		value, err := server.Redis.HGet(ctx, "llmgw:rate:"+key.ID, "count").Int64()
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	t.Run("executed_reply_lost_is_retried_without_double_count", func(t *testing.T) {
		original := server.Redis
		client, dropped := replyDroppingRedis(t)
		server.Redis = client
		defer func() { server.Redis = original }()
		key := newKey()
		limited, _, err := server.limitOperation(ctx, key, uuid.New())
		if err != nil || limited || count(key) != 1 {
			t.Fatalf("lost reply was not recovered once: limited=%v err=%v", limited, err)
		}
		select {
		case <-dropped:
		default:
			t.Fatal("no successful wire response was dropped")
		}
		limited, _, err = server.limited(ctx, key)
		if err != nil || !limited || count(key) != 2 {
			t.Fatal("a separate HTTP operation bypassed RPM")
		}
	})
	t.Run("concurrent_replay_increments_once", func(t *testing.T) {
		key := newKey()
		operation := uuid.New()
		var group sync.WaitGroup
		errors := make(chan error, 16)
		for range 16 {
			group.Add(1)
			go func() {
				defer group.Done()
				limited, _, err := server.limitOperation(ctx, key, operation)
				if limited && err == nil {
					err = io.ErrUnexpectedEOF
				}
				errors <- err
			}()
		}
		group.Wait()
		close(errors)
		for err := range errors {
			if err != nil {
				t.Fatal(err)
			}
		}
		if count(key) != 1 {
			t.Fatal("concurrent retries counted more than once")
		}
	})
	t.Run("two_missing_replies_obey_total_time_budget", func(t *testing.T) {
		original, budget := server.Redis, server.Config.RateLimitTimeout
		client, dropped := faultRedis(t, true)
		server.Redis = client
		server.Config.RateLimitTimeout = 250 * time.Millisecond
		defer func() { server.Redis = original; server.Config.RateLimitTimeout = budget }()
		key := newKey()
		started := time.Now()
		if _, _, err := server.limitOperation(ctx, key, uuid.New()); err == nil {
			t.Fatal("missing replies were accepted")
		}
		elapsed := time.Since(started)
		if elapsed < 200*time.Millisecond || elapsed > 800*time.Millisecond || len(dropped) != 2 {
			t.Fatalf("unbounded or extra retry: %s, replies=%d", elapsed, len(dropped))
		}
		if n, err := original.HGet(ctx, "llmgw:rate:"+key.ID, "count").Int(); err != nil || n != 1 {
			t.Fatalf("timeout repeated the counter mutation: %d %v", n, err)
		}
	})
	t.Run("cached_denial_survives_window_rollover", func(t *testing.T) {
		key := newKey()
		operation := uuid.New()
		now, err := server.Redis.Time(ctx).Result()
		if err != nil {
			t.Fatal(err)
		}
		name := "llmgw:rate:" + key.ID
		oldWindow := now.Unix()/60 - 1
		outcome := "2:" + strconv.FormatInt(oldWindow*60+60, 10) + ":" + strconv.FormatInt(now.Unix()+120, 10)
		if err = server.Redis.HSet(ctx, name, "window", oldWindow, "count", 2, "op:"+operation.String(), outcome).Err(); err != nil {
			t.Fatal(err)
		}
		limited, _, err := server.limited(ctx, key)
		if err != nil || limited || count(key) != 1 {
			t.Fatal("new window did not reset normally")
		}
		limited, retry, err := server.limitOperation(ctx, key, operation)
		if err != nil || !limited || retry < 1 || count(key) != 1 {
			t.Fatal("replay changed its original decision or consumed new-window quota")
		}
	})
	t.Run("legacy_counter_and_expired_operation_cleanup", func(t *testing.T) {
		key := newKey()
		now, err := server.Redis.Time(ctx).Result()
		if err != nil {
			t.Fatal(err)
		}
		name := "llmgw:rate:" + key.ID
		oldField := "op:" + uuid.NewString()
		if err = server.Redis.HSet(ctx, name, "window", now.Unix()/60, "count", 1, oldField, "1:1:1").Err(); err != nil {
			t.Fatal(err)
		}
		limited, _, err := server.limited(ctx, key)
		if err != nil || !limited || count(key) != 2 {
			t.Fatal("legacy RPM state was reset")
		}
		if server.Redis.HExists(ctx, name, oldField).Val() {
			t.Fatal("expired operation was retained")
		}
	})
	t.Run("script_error_does_not_partially_change_counter", func(t *testing.T) {
		key := newKey()
		name := "llmgw:rate:" + key.ID
		if err := server.Redis.Set(ctx, name, "wrong-type", time.Minute).Err(); err != nil {
			t.Fatal(err)
		}
		if _, _, err := server.limited(ctx, key); err == nil {
			t.Fatal("invalid limiter data was accepted")
		}
		if server.Redis.Get(ctx, name).Val() != "wrong-type" {
			t.Fatal("error mutated limiter data")
		}
	})
	t.Run("expired_fields_are_reclaimed_incrementally", func(t *testing.T) {
		key := newKey()
		name := "llmgw:rate:" + key.ID
		fields := make(map[string]interface{})
		for i := 0; i < 600; i++ {
			fields["op:expired-"+strconv.Itoa(i)] = "1:1:1"
		}
		if err := server.Redis.HSet(ctx, name, fields).Err(); err != nil {
			t.Fatal(err)
		}
		for range 200 {
			if _, _, err := server.limited(ctx, key); err != nil {
				t.Fatal(err)
			}
		}
		remaining, err := server.Redis.HKeys(ctx, name).Result()
		if err != nil {
			t.Fatal(err)
		}
		for _, field := range remaining {
			if strings.HasPrefix(field, "op:expired-") {
				t.Fatal("expired operation fields accumulated")
			}
		}
	})
}
