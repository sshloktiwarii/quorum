package web

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net"
	"net/http"
	"sync"
	"time"

	"systems-suite/quorum/pkg/raft"
	"systems-suite/quorum/pkg/simulation"
	"systems-suite/quorum/pkg/statemachine"
)

//go:embed assets/*
var assetsFS embed.FS

// FindAvailablePort enforces strict port discipline: tests preferred port, falls back to ephemeral dynamic port.
func FindAvailablePort(preferred int) (net.Listener, int, error) {
	// 1. Try preferred port
	l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", preferred))
	if err == nil {
		return l, preferred, nil
	}

	// 2. Fall back to ephemeral dynamic port allocated by kernel
	l, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, 0, fmt.Errorf("port discipline: failed to bind ephemeral port: %w", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	return l, port, nil
}

// Server embeds the real-time visualizer HUD and REST control API.
type Server struct {
	mu         sync.RWMutex
	listener   net.Listener
	httpServer *http.Server
	broker     *SSEBroker
	cluster    *simulation.Cluster
	port       int
	stopCh     chan struct{}
}

// NewServer initializes the HTTP server on an available port.
func NewServer(preferredPort int, cluster *simulation.Cluster) (*Server, error) {
	listener, actualPort, err := FindAvailablePort(preferredPort)
	if err != nil {
		return nil, err
	}

	broker := NewSSEBroker()
	s := &Server{
		listener: listener,
		broker:   broker,
		cluster:  cluster,
		port:     actualPort,
		stopCh:   make(chan struct{}),
	}

	mux := http.NewServeMux()

	// Static assets
	subFS, err := fs.Sub(assetsFS, "assets")
	if err == nil {
		mux.Handle("GET /", http.FileServer(http.FS(subFS)))
	}

	// SSE Telemetry Stream
	mux.HandleFunc("GET /api/stream", broker.Handler())

	// Command Propose Endpoint
	mux.HandleFunc("POST /api/cmd", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		leader, err := s.cluster.GetLeader()
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		cmd, _ := json.Marshal(statemachine.Command{
			Op:    statemachine.OpPut,
			Key:   req.Key,
			Value: req.Value,
		})
		idx, term, err := leader.Propose(cmd)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": true,
			"index":   idx,
			"term":    term,
			"leader":  leader.ID(),
		})
	})

	// Partition Trigger Endpoint
	mux.HandleFunc("POST /api/partition", func(w http.ResponseWriter, r *http.Request) {
		// Default 3-2 partition
		n := len(s.cluster.Nodes)
		if n >= 5 {
			s.cluster.Net.Partitions().Partition(
				[]raft.NodeID{"node-1", "node-2"},
				[]raft.NodeID{"node-3", "node-4", "node-5"},
			)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
	})

	// Heal Cluster Endpoint
	mux.HandleFunc("POST /api/heal", func(w http.ResponseWriter, r *http.Request) {
		s.cluster.Net.Partitions().HealAll()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
	})

	s.httpServer = &http.Server{
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 0, // Infinite for SSE streaming
	}

	return s, nil
}

// Port returns the bound TCP port.
func (s *Server) Port() int {
	return s.port
}

// URL returns the local HTTP URL.
func (s *Server) URL() string {
	return fmt.Sprintf("http://127.0.0.1:%d", s.port)
}

// Start runs the server in background.
func (s *Server) Start() {
	go func() {
		_ = s.httpServer.Serve(s.listener)
	}()
	go s.telemetryBroadcastLoop()
}

// Stop gracefully terminates the server.
func (s *Server) Stop() error {
	close(s.stopCh)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return s.httpServer.Shutdown(ctx)
}

// telemetryBroadcastLoop periodically samples cluster state and broadcasts SSE updates.
func (s *Server) telemetryBroadcastLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.broadcastClusterState()
		}
	}
}

// ClusterTelemetryPayload represents visualizer HUD state.
type ClusterTelemetryPayload struct {
	Timestamp int64                      `json:"timestamp"`
	Nodes     []NodeTelemetry            `json:"nodes"`
	Links     []LinkTelemetry            `json:"links"`
	KVStore   map[string]string          `json:"kv_store"`
}

type NodeTelemetry struct {
	ID          string `json:"id"`
	Role        string `json:"role"`
	Term        uint64 `json:"term"`
	CommitIndex uint64 `json:"commit_index"`
	LastApplied uint64 `json:"last_applied"`
	LastLogTerm uint64 `json:"last_log_term"`
	LastLogIdx  uint64 `json:"last_log_index"`
	LeaderID    string `json:"leader_id"`
}

type LinkTelemetry struct {
	From    string `json:"from"`
	To      string `json:"to"`
	Blocked bool   `json:"blocked"`
}

func (s *Server) broadcastClusterState() {
	if s.cluster == nil {
		return
	}

	payload := ClusterTelemetryPayload{
		Timestamp: time.Now().UnixMilli(),
		Nodes:     make([]NodeTelemetry, 0, len(s.cluster.Nodes)),
		Links:     make([]LinkTelemetry, 0),
		KVStore:   make(map[string]string),
	}

	for _, rn := range s.cluster.Nodes {
		st := rn.SnapshotState()
		payload.Nodes = append(payload.Nodes, NodeTelemetry{
			ID:          string(st.ID),
			Role:        st.RoleName,
			Term:        uint64(st.CurrentTerm),
			CommitIndex: uint64(st.CommitIndex),
			LastApplied: uint64(st.LastApplied),
			LastLogTerm: uint64(st.LastLogTerm),
			LastLogIdx:  uint64(st.LastLogIndex),
			LeaderID:    string(st.LeaderID),
		})
	}

	// Sample partitions
	for _, n1 := range s.cluster.Nodes {
		for _, n2 := range s.cluster.Nodes {
			if n1.ID() != n2.ID() {
				blocked := s.cluster.Net.Partitions().IsBlocked(n1.ID(), n2.ID())
				payload.Links = append(payload.Links, LinkTelemetry{
					From:    string(n1.ID()),
					To:      string(n2.ID()),
					Blocked: blocked,
				})
			}
		}
	}

	// Sample KV store from leader (or first available node)
	for _, store := range s.cluster.Stores {
		payload.KVStore = store.GetAll()
		break
	}

	s.broker.Broadcast("cluster_state", payload)
}
