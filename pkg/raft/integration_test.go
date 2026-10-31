package raft

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"systems-suite/quorum/pkg/statemachine"
	"systems-suite/quorum/pkg/wal"
)

func TestIntegrationLinearizableReadsAndWALPersistence(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "quorum_integ_*")
	if err != nil {
		t.Fatalf("MkdirTemp failed: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Single node cluster for direct ReadIndex and WAL durability check
	cfg := DefaultConfig("node-1", nil)
	cfg.DataDir = filepath.Join(tempDir, "node-1")
	cfg.MinElectionTimeout = 10 * time.Millisecond
	cfg.MaxElectionTimeout = 25 * time.Millisecond
	cfg.HeartbeatInterval = 5 * time.Millisecond

	walWriter, state, err := wal.RecoverAndOpen(cfg.DataDir, true)
	if err != nil {
		t.Fatalf("RecoverAndOpen failed: %v", err)
	}
	if len(state.Entries) != 0 {
		t.Fatalf("expected empty WAL initially, got %d", len(state.Entries))
	}

	sm := statemachine.NewKVStore()
	node, err := NewRaftNode(cfg, nil, sm, walWriter)
	if err != nil {
		t.Fatalf("NewRaftNode failed: %v", err)
	}

	node.Start()

	// Wait for leader election
	time.Sleep(50 * time.Millisecond)
	term, isLeader := node.GetState()
	if !isLeader {
		node.Stop()
		t.Fatalf("expected node to become leader, got term=%d, leader=%t", term, isLeader)
	}

	// Propose 5 commands
	for i := 1; i <= 5; i++ {
		cmdBytes, _ := json.Marshal(statemachine.Command{
			Op:    statemachine.OpPut,
			Key:   "counter",
			Value: string(rune('0' + i)),
		})
		_, _, err := node.Propose(cmdBytes)
		if err != nil {
			node.Stop()
			t.Fatalf("Propose %d failed: %v", i, err)
		}
	}

	// Wait for entries to apply
	time.Sleep(50 * time.Millisecond)

	// Execute Linearizable ReadIndex query
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	readVal, err := node.ReadIndex(ctx, "counter")
	if err != nil {
		node.Stop()
		t.Fatalf("ReadIndex failed: %v", err)
	}
	if readVal != "5" {
		node.Stop()
		t.Fatalf("ReadIndex expected '5', got '%s'", readVal)
	}

	// Graceful stop
	node.Stop()

	// Verify persistence: Reopen WAL and verify entries and term/vote exist
	_, recoveredState, err := wal.RecoverAndOpen(cfg.DataDir, true)
	if err != nil {
		t.Fatalf("RecoverAndOpen after reboot failed: %v", err)
	}

	if recoveredState.Term < 1 {
		t.Fatalf("expected recovered term >= 1, got %d", recoveredState.Term)
	}
	if recoveredState.VotedFor != "node-1" {
		t.Fatalf("expected recovered votedFor 'node-1', got '%s'", recoveredState.VotedFor)
	}
	if len(recoveredState.Entries) < 5 {
		t.Fatalf("expected at least 5 entries in WAL, got %d", len(recoveredState.Entries))
	}
}
