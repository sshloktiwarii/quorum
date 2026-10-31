package raft

import (
	"encoding/json"
	"testing"

	"systems-suite/quorum/pkg/statemachine"
)

func TestLogManagerCompaction(t *testing.T) {
	lm := NewLogManager()
	for i := uint64(1); i <= 10; i++ {
		lm.Append(LogEntry{Index: Index(i), Term: 1, Type: EntryData, Data: []byte("v")})
	}

	if lm.LastIndex() != 10 {
		t.Fatalf("expected LastIndex 10, got %d", lm.LastIndex())
	}

	// Compact up to index 5
	lm.Compact(5, 1)

	if lm.LastIncludedIndex() != 5 {
		t.Fatalf("expected LastIncludedIndex 5, got %d", lm.LastIncludedIndex())
	}
	if lm.LastIndex() != 10 {
		t.Fatalf("expected LastIndex still 10, got %d", lm.LastIndex())
	}

	// Entries <= 5 must no longer be retrievable via GetEntry
	if _, ok := lm.GetEntry(4); ok {
		t.Fatalf("expected entry 4 to be compacted")
	}

	// Entry 6 must be available
	e6, ok := lm.GetEntry(6)
	if !ok || e6.Index != 6 {
		t.Fatalf("expected entry 6 available, got %+v", e6)
	}

	// Slicing across remaining log
	slice := lm.Slice(6, 10)
	if len(slice) != 5 {
		t.Fatalf("expected slice length 5, got %d", len(slice))
	}
}

func TestHandleInstallSnapshot(t *testing.T) {
	cfg := DefaultConfig("node-1", []NodeID{"node-2"})
	sm := statemachine.NewKVStore()

	node, err := NewRaftNode(cfg, &mockTransport{}, sm, nil)
	if err != nil {
		t.Fatalf("NewRaftNode failed: %v", err)
	}

	// Generate snapshot payload
	snapSM := statemachine.NewKVStore()
	cmd, _ := json.Marshal(statemachine.Command{Op: statemachine.OpPut, Key: "balance", Value: "999"})
	snapSM.Apply(cmd, 50)
	snapData, _ := snapSM.Snapshot()

	args := &InstallSnapshotArgs{
		Term:              3,
		LeaderID:          "node-2",
		LastIncludedIndex: 50,
		LastIncludedTerm:  3,
		Offset:            0,
		Data:              snapData,
		Done:              true,
	}

	reply := node.HandleInstallSnapshot(args)
	if reply.Term != 3 {
		t.Fatalf("expected reply term 3, got %d", reply.Term)
	}

	// Verify state restored in follower KVStore
	val, ok := sm.Query("balance")
	if !ok || val != "999" {
		t.Fatalf("expected balance '999', got %s (%t)", val, ok)
	}

	node.mu.RLock()
	defer node.mu.RUnlock()
	if node.commitIndex != 50 || node.lastApplied != 50 {
		t.Fatalf("expected commitIndex and lastApplied = 50, got commit=%d, applied=%d", node.commitIndex, node.lastApplied)
	}
}
