package gateway

import (
	"context"
	"errors"
	"io"
	"net"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// Count, window and operation result change in one HSET. Lua provides isolation,
// not rollback after a later command error. Expired operation fields are pruned
// incrementally; logical replay retention is 120 seconds even on Redis 6.
var limitScript = redis.NewScript(`
redis.replicate_commands()
local t=redis.call('TIME')
local now=tonumber(t[1])
local window=math.floor(now/60)
local ending=(window+1)*60
local operation='op:'..ARGV[1]
local cached=redis.call('HGET',KEYS[1],operation)
if cached then
  local count,until_time,expires=string.match(cached,'^(%d+):(%d+):(%d+)$')
  if not expires then return redis.error_reply('invalid rate operation record') end
  if tonumber(expires)>now then
    return {tonumber(count),math.max(1,tonumber(until_time)-now),1}
  end
end

local previous=redis.call('HGET',KEYS[1],'window')
local count=0
if previous and tonumber(previous)==window then
  local stored=redis.call('HGET',KEYS[1],'count')
  count=tonumber(stored)
  if not count or count<0 or count>=9007199254740990 then
    return redis.error_reply('invalid rate counter')
  end
end

local cursor=redis.call('HGET',KEYS[1],'_gc') or '0'
local scanned=redis.pcall('HSCAN',KEYS[1],cursor,'COUNT',16)
local next_cursor='0'
if not scanned.err then
  next_cursor=scanned[1]
  for i=1,#scanned[2],2 do
    local field=scanned[2][i]
    if string.sub(field,1,3)=='op:' then
      local expiration=string.match(scanned[2][i+1],'^%d+:%d+:(%d+)$')
      if expiration and tonumber(expiration)<=now then
        redis.pcall('HDEL',KEYS[1],field)
      end
    end
  end
end

count=count+1
local count_text=string.format('%.0f',count)
local outcome=count_text..':'..string.format('%.0f',ending)..':'..string.format('%.0f',now+120)
redis.call('HSET',KEYS[1],'window',window,'count',count_text,
  operation,outcome,'_gc',next_cursor)
-- A failure after HSET cannot change the result or make a retry count twice.
redis.pcall('EXPIRE',KEYS[1],180)
return {count,ending-now,0}
`)

func retryableRateError(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, redis.ErrClosed) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) {
		return true
	}
	var failure net.Error
	return errors.As(err, &failure) && (failure.Timeout() || failure.Temporary())
}

func (s *Server) limited(ctx context.Context, key Key) (bool, int, error) {
	operation, err := uuid.NewRandom()
	if err != nil {
		return false, 0, err
	}
	return s.limitOperation(ctx, key, operation)
}

func (s *Server) limitOperation(parent context.Context, key Key, operation uuid.UUID) (bool, int, error) {
	defer s.observe(parent, "rate_limit", time.Now())
	ctx, cancel := context.WithTimeout(parent, s.Config.RateLimitTimeout)
	defer cancel()
	var last error
	for attempt := 0; attempt < 2; attempt++ {
		attemptCtx := ctx
		stop := func() {}
		if attempt == 0 {
			attemptCtx, stop = context.WithTimeout(ctx, s.Config.RateLimitTimeout/2)
		}
		values, err := limitScript.Run(attemptCtx, s.Redis, []string{"llmgw:rate:" + key.ID}, operation.String()).Int64Slice()
		stop()
		if err == nil {
			if len(values) != 3 || values[0] < 1 || values[1] < 1 {
				s.rateAttempts.WithLabelValues("invalid_result").Inc()
				return false, 0, errors.New("invalid rate limiter result")
			}
			s.rateAttempts.WithLabelValues("success").Inc()
			if values[2] == 1 {
				s.rateCacheHits.Inc()
			}
			return values[0] > int64(key.RPM), int(values[1]), nil
		}
		last = err
		canRetry := attempt == 0 && ctx.Err() == nil && retryableRateError(err)
		if !canRetry {
			s.rateAttempts.WithLabelValues("failed").Inc()
			break
		}
		s.rateAttempts.WithLabelValues("retryable_error").Inc()
		pause := time.NewTimer(time.Duration(25+operation[0]%26) * time.Millisecond)
		select {
		case <-ctx.Done():
			pause.Stop()
			return false, 0, ctx.Err()
		case <-pause.C:
		}
		s.rateRetries.Inc()
	}
	s.Logger.Warn("rate operation failed", "error", last, "budget_error", ctx.Err())
	return false, 0, last
}
