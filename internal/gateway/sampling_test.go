package gateway

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	openai "github.com/sashabaranov/go-openai"
)

func TestExplicitZeroTemperatureReachesUpstream(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sse"}[stream], func(t *testing.T) {
			var received map[string]json.RawMessage
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
					t.Error(err)
				}
				if stream {
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"}}]}\n\ndata: [DONE]\n\n")
				} else {
					_, _ = io.WriteString(w, `{"choices":[{"index":0,"message":{"role":"assistant","content":"ok"}}]}`)
				}
			}))
			defer upstream.Close()
			config := openai.DefaultConfig("unused")
			config.BaseURL = upstream.URL
			config.HTTPClient = observerDoer{client: upstream.Client(), capture: &capture{}}
			client := openai.NewClientWithConfig(config)
			ctx := context.WithValue(context.Background(), temperatureKey{}, float32(0))
			body := openai.ChatCompletionRequest{Model: "test", Messages: []openai.ChatCompletionMessage{{Role: "user", Content: "hello"}}, Temperature: 0}
			if stream {
				response, err := client.CreateChatCompletionStream(ctx, body)
				if err != nil {
					t.Fatal(err)
				}
				defer response.Close()
				if _, err = response.Recv(); err != nil {
					t.Fatal(err)
				}
			} else if _, err := client.CreateChatCompletion(ctx, body); err != nil {
				t.Fatal(err)
			}
			if string(received["temperature"]) != "0" {
				t.Fatalf("explicit zero temperature lost on upstream wire: %s", received["temperature"])
			}
		})
	}
}

func TestTemperaturePresenceInRequestIdentity(t *testing.T) {
	var omitted, explicit chatRequest
	if err := json.NewDecoder(strings.NewReader(`{"model":"m","messages":[]}`)).Decode(&omitted); err != nil {
		t.Fatal(err)
	}
	if err := json.NewDecoder(strings.NewReader(`{"model":"m","messages":[],"temperature":0}`)).Decode(&explicit); err != nil {
		t.Fatal(err)
	}
	a, _ := canonicalChatRequest(omitted.ChatCompletionRequest, omitted.Temperature)
	b, _ := canonicalChatRequest(explicit.ChatCompletionRequest, explicit.Temperature)
	if string(a) == string(b) || !strings.Contains(string(b), `"temperature":0`) {
		t.Fatal("request identity collapsed default sampling and explicit greedy sampling")
	}
	value := float32(.7)
	legacy := openai.ChatCompletionRequest{Model: "m", Temperature: value}
	before, _ := json.Marshal(legacy)
	after, _ := canonicalChatRequest(legacy, &value)
	if string(before) != string(after) {
		t.Fatal("nonzero temperature changed an existing idempotency identity")
	}
}
