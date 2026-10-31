package web

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
)

// SSEBroker coordinates fan-out streaming of real-time telemetry to connected visualizer clients.
type SSEBroker struct {
	mu      sync.Mutex
	clients map[chan []byte]bool
}

// NewSSEBroker creates an active SSE event broker.
func NewSSEBroker() *SSEBroker {
	return &SSEBroker{
		clients: make(map[chan []byte]bool),
	}
}

// Subscribe registers a new browser connection and returns an event channel.
func (b *SSEBroker) Subscribe() chan []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	ch := make(chan []byte, 64)
	b.clients[ch] = true
	return ch
}

// Unsubscribe removes a disconnected client channel.
func (b *SSEBroker) Unsubscribe(ch chan []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.clients[ch] {
		delete(b.clients, ch)
		close(ch)
	}
}

// Broadcast serializes and pushes an event payload to all active subscribers.
func (b *SSEBroker) Broadcast(eventType string, payload any) {
	b.mu.Lock()
	defer b.mu.Unlock()

	data, err := json.Marshal(payload)
	if err != nil {
		return
	}

	frame := fmt.Sprintf("event: %s\ndata: %s\n\n", eventType, string(data))
	raw := []byte(frame)

	for ch := range b.clients {
		select {
		case ch <- raw:
		default:
			// Non-blocking drop if client is saturated
		}
	}
}

// Handler returns the HTTP handler streaming server-sent events to browser clients.
func (b *SSEBroker) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "SSE streaming unsupported by client", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Access-Control-Allow-Origin", "*")

		ch := b.Subscribe()
		defer b.Unsubscribe(ch)

		// Send initial keep-alive ping
		_, _ = fmt.Fprint(w, ": ping\n\n")
		flusher.Flush()

		ctx := r.Context()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				_, _ = w.Write(msg)
				flusher.Flush()
			}
		}
	}
}
