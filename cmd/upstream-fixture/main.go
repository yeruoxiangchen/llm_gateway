// This service is a deterministic protocol/fault fixture, not a model server.
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type state struct {
	mutex     sync.Mutex
	calls     map[string]int
	cancelled map[string]int
	release   chan struct{}
}

func main() {
	s := &state{calls: map[string]int{}, cancelled: map[string]int{}, release: make(chan struct{})}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		write(w, map[string]string{"status": "ready", "kind": "protocol_fixture"})
	})
	mux.HandleFunc("GET /stats", func(w http.ResponseWriter, r *http.Request) {
		s.mutex.Lock()
		defer s.mutex.Unlock()
		write(w, map[string]any{"calls": s.calls, "cancelled": s.cancelled})
	})
	mux.HandleFunc("POST /release", func(w http.ResponseWriter, r *http.Request) {
		s.mutex.Lock()
		close(s.release)
		s.release = make(chan struct{})
		s.mutex.Unlock()
		write(w, map[string]bool{"released": true})
	})
	mux.HandleFunc("POST /{scenario}/v1/chat/completions", s.chat)
	address := os.Getenv("LLMGW_FIXTURE_ADDRESS")
	if address == "" {
		address = "127.0.0.1:29481"
	}
	server := &http.Server{Addr: address, Handler: mux, ReadHeaderTimeout: 3 * time.Second}
	fmt.Println("deterministic upstream fixture", address)
	if err := server.ListenAndServe(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func write(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

func (s *state) chat(w http.ResponseWriter, r *http.Request) {
	scenario := r.PathValue("scenario")
	s.mutex.Lock()
	s.calls[scenario]++
	release := s.release
	s.mutex.Unlock()
	var body struct {
		Model  string `json:"model"`
		Stream bool   `json:"stream"`
		Tools  []any  `json:"tools"`
	}
	if json.NewDecoder(http.MaxBytesReader(w, r.Body, 65536)).Decode(&body) != nil {
		w.WriteHeader(400)
		return
	}
	if scenario == "fail503" || scenario == "fail400" || scenario == "fail429" {
		status := map[string]int{"fail503": 503, "fail400": 400, "fail429": 429}[scenario]
		w.WriteHeader(status)
		write(w, map[string]any{"error": map[string]string{"message": "controlled upstream rejection", "type": "fixture_error"}})
		return
	}
	if scenario == "delay" {
		select {
		case <-time.After(12 * time.Second):
		case <-r.Context().Done():
			s.cancel(scenario)
			return
		}
	}
	if scenario == "slow" {
		select {
		case <-time.After(150 * time.Millisecond):
		case <-r.Context().Done():
			s.cancel(scenario)
			return
		}
	}
	if scenario == "huge" {
		write(w, map[string]any{"choices": []any{}, "padding": strings.Repeat("x", 5*1024*1024)})
		return
	}
	usage := map[string]int{"prompt_tokens": 5, "completion_tokens": 7, "total_tokens": 12}
	if scenario == "overusage" {
		usage = map[string]int{"prompt_tokens": 6000, "completion_tokens": 1, "total_tokens": 6001}
	}
	if scenario == "badusage" {
		usage["total_tokens"] = 999
	}
	if scenario == "omittotal" {
		delete(usage, "total_tokens")
	}
	if !body.Stream {
		message := map[string]any{"role": "assistant", "content": "hello gateway"}
		if scenario == "large" {
			message["content"] = strings.Repeat("x", 3500*1024)
		}
		finish := "stop"
		if len(body.Tools) > 0 {
			message = map[string]any{"role": "assistant", "content": "", "tool_calls": []any{map[string]any{"id": "call_fixture", "type": "function", "function": map[string]string{"name": "echo", "arguments": "{\"value\":\"ok\"}"}}}}
			finish = "tool_calls"
		}
		response := map[string]any{"id": "chatcmpl-fixture", "object": "chat.completion", "created": time.Now().Unix(), "model": body.Model, "choices": []any{map[string]any{"index": 0, "message": message, "finish_reason": finish}}}
		if scenario != "nousage" {
			response["usage"] = usage
		}
		write(w, response)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	flusher := http.NewResponseController(w)
	send := func(delta map[string]any, finish any, includeUsage bool) bool {
		choices := []any{map[string]any{"index": 0, "delta": delta, "finish_reason": finish}}
		if includeUsage {
			choices = []any{}
		}
		response := map[string]any{"id": "chatcmpl-stream-fixture", "object": "chat.completion.chunk", "created": time.Now().Unix(), "model": body.Model, "choices": choices}
		if includeUsage {
			response["usage"] = usage
		}
		data, _ := json.Marshal(response)
		if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
			return false
		}
		return flusher.Flush() == nil
	}
	if !send(map[string]any{"role": "assistant", "content": "hello "}, nil, false) {
		s.cancel(scenario)
		return
	}
	if scenario == "truncate" {
		return
	}
	if scenario == "stall" || scenario == "long" {
		select {
		case <-time.After(30 * time.Second):
		case <-r.Context().Done():
			s.cancel(scenario)
			return
		}
	}
	if scenario == "hold" {
		select {
		case <-release:
		case <-r.Context().Done():
			s.cancel(scenario)
			return
		}
	}
	if scenario == "paced" || scenario == "flood" {
		count := 30
		gap := 20 * time.Millisecond
		content := "frame "
		if scenario == "flood" {
			count = 800
			gap = 0
			content = strings.Repeat("x", 4096)
		}
		for n := 0; n < count; n++ {
			if gap > 0 {
				select {
				case <-time.After(gap):
				case <-r.Context().Done():
					s.cancel(scenario)
					return
				}
			}
			if !send(map[string]any{"content": content}, nil, false) {
				s.cancel(scenario)
				return
			}
		}
	}
	select {
	case <-time.After(40 * time.Millisecond):
	case <-r.Context().Done():
		s.cancel(scenario)
		return
	}
	if !send(map[string]any{"content": "gateway"}, nil, false) {
		s.cancel(scenario)
		return
	}
	if !send(map[string]any{}, "stop", false) {
		s.cancel(scenario)
		return
	}
	if scenario != "nousage" {
		if !send(nil, nil, true) {
			s.cancel(scenario)
			return
		}
	}
	_, _ = fmt.Fprint(w, "data: [DONE]\n\n")
	_ = flusher.Flush()
}

func (s *state) cancel(scenario string) { s.mutex.Lock(); s.cancelled[scenario]++; s.mutex.Unlock() }
