package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"systems-suite/quorum/pkg/raft"
	"systems-suite/quorum/pkg/statemachine"
	"systems-suite/quorum/pkg/wal"
)

func main() {
	idFlag := flag.String("id", "node-1", "unique identifier for this Raft node")
	peersFlag := flag.String("peers", "", "comma-separated list of peer node IDs (e.g. node-2,node-3)")
	dataDirFlag := flag.String("data-dir", "data", "directory path for WAL and snapshots")
	heartbeatFlag := flag.Duration("heartbeat", 50*time.Millisecond, "heartbeat broadcast interval")
	minElectionFlag := flag.Duration("min-election", 150*time.Millisecond, "minimum randomized election timeout")
	maxElectionFlag := flag.Duration("max-election", 300*time.Millisecond, "maximum randomized election timeout")
	flag.Parse()

	var peers []raft.NodeID
	if *peersFlag != "" {
		for _, p := range strings.Split(*peersFlag, ",") {
			trimmed := strings.TrimSpace(p)
			if trimmed != "" && trimmed != *idFlag {
				peers = append(peers, raft.NodeID(trimmed))
			}
		}
	}

	cfg := &raft.Config{
		ID:                 raft.NodeID(*idFlag),
		Peers:              peers,
		HeartbeatInterval:  *heartbeatFlag,
		MinElectionTimeout: *minElectionFlag,
		MaxElectionTimeout: *maxElectionFlag,
		DataDir:            *dataDirFlag,
		SnapshotThreshold:  5000,
		MaxEntriesPerMsg:   256,
		StorageSync:        true,
	}

	if err := cfg.Validate(); err != nil {
		log.Fatalf("[FATAL] Configuration error: %v", err)
	}

	log.Printf("[QUORUM] Starting node %s with %d peer(s)...", cfg.ID, len(cfg.Peers))

	// Replay and initialize WAL
	w, state, err := wal.RecoverAndOpen(cfg.DataDir, cfg.StorageSync)
	if err != nil {
		log.Fatalf("[FATAL] Failed to initialize WAL: %v", err)
	}
	log.Printf("[QUORUM] WAL recovered %d entries, term %d, votedFor %s", len(state.Entries), state.Term, state.VotedFor)

	// State machine
	store := statemachine.NewKVStore()

	// In standalone mode without network peer addresses specified, node runs self-contained
	rn, err := raft.NewRaftNode(cfg, nil, store, w)
	if err != nil {
		log.Fatalf("[FATAL] Failed to initialize Raft node: %v", err)
	}

	rn.Start()
	log.Printf("[QUORUM] Node %s online and active.", cfg.ID)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	fmt.Printf("\n[QUORUM] Received shutdown signal. Stopping node %s...\n", cfg.ID)
	rn.Stop()
	log.Println("[QUORUM] Clean shutdown complete.")
}
