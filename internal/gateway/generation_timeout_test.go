package gateway

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	openai "github.com/sashabaranov/go-openai"
)

func TestSlowGenerationResponseHeaders(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		timer := time.NewTimer(15250 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-r.Context().Done():
			return
		case <-timer.C:
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"slow","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":5,"completion_tokens":7,"total_tokens":12}}`)
	}))
	defer upstream.Close()
	key := bytes.Repeat([]byte{9}, 32)
	secret, err := encrypt(key, []byte("unused"))
	if err != nil {
		t.Fatal(err)
	}
	s := NewServer(Config{MasterKey: key, MaxInflight: 1, RequestTimeout: 25 * time.Second}, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer s.Redis.Close()
	defer s.httpClient.CloseIdleConnections()
	client, _, err := s.client(Provider{Secret: secret, BaseURL: upstream.URL})
	if err != nil {
		t.Fatal(err)
	}
	body := openai.ChatCompletionRequest{Model: "test", Messages: []openai.ChatCompletionMessage{{Role: "user", Content: "hello"}}}
	t.Run("generation_longer_than_fifteen_seconds", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		defer cancel()
		response, err := client.CreateChatCompletion(ctx, body)
		if err != nil || len(response.Choices) != 1 || response.Choices[0].Message.Content != "ok" || response.Usage.TotalTokens != 12 {
			t.Fatalf("valid generation inside the request budget failed: %v %+v", err, response)
		}
	})
	t.Run("request_deadline_still_cancels_wait", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		started := time.Now()
		_, err := client.CreateChatCompletion(ctx, body)
		if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > time.Second {
			t.Fatalf("request deadline was not enforced: %v", err)
		}
	})
}
