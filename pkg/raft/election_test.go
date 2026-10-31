package raft

import (
	"context"
	"sync"
	"testing"
	"time"

	"systems-suite/quorum/pkg/statemachine"
)

// mockTransport implements in-memory RPC routing for tests.
type mockTransport struct {
	mu           sync.Mutex
	requestVote  func(ctx context.Context, to NodeID, args *RequestVoteArgs) (*RequestVoteReply, error)
	appendEntry  func(ctx context.Context, to NodeID, args *AppendEntriesArgs) (*AppendEntriesReply, error)
	snapHandler  func(ctx context.Context, to NodeID, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}

func (m *mockTransport) SendRequestVote(ctx context.Context, to NodeID, args *RequestVoteArgs) (*RequestVoteReply, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.requestVote != nil {
		return m.requestVote(ctx, to, args)
	}
	return &RequestVoteReply{Term: args.Term, VoteGranted: false}, nil
}

func (m *mockTransport) SendAppendEntries(ctx context.Context, to NodeID, args *AppendEntriesArgs) (*AppendEntriesReply, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.appendEntry != nil {
		return m.appendEntry(ctx, to, args)
	}
	return &AppendEntriesReply{Term: args.Term, Success: true}, nil
}

func (m *mockTransport) SendInstallSnapshot(ctx context.Context, to NodeID, args *InstallSnapshotArgs) (*InstallSnapshotReply, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.snapHandler != nil {
		return m.snapHandler(ctx, to, args)
	}
	return &InstallSnapshotReply{Term: args.Term}, nil
}

func TestSingleNodeElection(t *testing.T) {
	cfg := DefaultConfig("node-1", nil)
	cfg.MinElectionTimeout = 10 * time.Millisecond
	cfg.MaxElectionTimeout = 20 * time.Millisecond
	cfg.HeartbeatInterval = 5 * time.Millisecond

	sm := statemachine.NewKVStore()
	node, err := NewRaftNode(cfg, &mockTransport{}, sm, nil)
	if err != nil {
		t.Fatalf("NewRaftNode failed: %v", err)
	}

	node.Start()
	defer node.Stop()

	// Wait for election timeout to trigger
	time.Sleep(50 * time.Millisecond)

	term, isLeader := node.GetState()
	if !isLeader {
		t.Fatalf("expected single node to become leader, got term %d, isLeader %t", term, isLeader)
	}
	if term < 1 {
		t.Fatalf("expected term >= 1, got %d", term)
	}
}

func TestRequestVoteInvariants(t *testing.T) {
	cfg := DefaultConfig("node-1", []NodeID{"node-2", "node-3"})
	sm := statemachine.NewKVStore()
	node, err := NewRaftNode(cfg, &mockTransport{}, sm, nil)
	if err != nil {
		t.Fatalf("NewRaftNode failed: %v", err)
	}

	node.mu.Lock()
	node.currentTerm = 1
	node.mu.Unlock()

	// 1. Candidate with lower term rejected
	reply1 := node.HandleRequestVote(&RequestVoteArgs{
		Term:         0,
		CandidateID:  "node-2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if reply1.VoteGranted {
		t.Fatalf("expected vote rejected for lower term")
	}

	// 2. Candidate with higher term granted
	reply2 := node.HandleRequestVote(&RequestVoteArgs{
		Term:         2,
		CandidateID:  "node-2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if !reply2.VoteGranted {
		t.Fatalf("expected vote granted for term 2")
	}

	// 3. Second candidate in same term rejected (already voted for node-2)
	reply3 := node.HandleRequestVote(&RequestVoteArgs{
		Term:         2,
		CandidateID:  "node-3",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if reply3.VoteGranted {
		t.Fatalf("expected duplicate vote in same term rejected")
	}

	// 4. Same candidate re-requesting in same term granted (idempotence)
	reply4 := node.HandleRequestVote(&RequestVoteArgs{
		Term:         2,
		CandidateID:  "node-2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if !reply4.VoteGranted {
		t.Fatalf("expected idempotent vote granted for same candidate")
	}

	// 5. Candidate with stale log rejected (§5.4.1)
	node.mu.Lock()
	node.log.Append(LogEntry{Index: 1, Term: 2, Type: EntryData, Data: []byte("val")})
	node.mu.Unlock()

	reply5 := node.HandleRequestVote(&RequestVoteArgs{
		Term:         3,
		CandidateID:  "node-3",
		LastLogIndex: 0, // Stale: receiver has lastLogIndex 1
		LastLogTerm:  1, // Stale: receiver has lastLogTerm 2
	})
	if reply5.VoteGranted {
		t.Fatalf("expected vote rejected for candidate with stale log")
	}
}

func TestCandidateMajorityTransition(t *testing.T) {
	cfg := DefaultConfig("node-1", []NodeID{"node-2", "node-3"})
	cfg.MinElectionTimeout = 100 * time.Millisecond
	cfg.MaxElectionTimeout = 200 * time.Millisecond

	mt := &mockTransport{
		requestVote: func(ctx context.Context, to NodeID, args *RequestVoteArgs) (*RequestVoteReply, error) {
			if to == "node-2" {
				return &RequestVoteReply{Term: args.Term, VoteGranted: true}, nil
			}
			return &RequestVoteReply{Term: args.Term, VoteGranted: false}, nil
		},
	}

	sm := statemachine.NewKVStore()
	node, err := NewRaftNode(cfg, mt, sm, nil)
	if err != nil {
		t.Fatalf("NewRaftNode failed: %v", err)
	}

	node.mu.Lock()
	node.startElection()
	node.mu.Unlock()

	// Wait for goroutines to collect vote
	time.Sleep(30 * time.Millisecond)

	term, isLeader := node.GetState()
	if !isLeader {
		t.Fatalf("expected leader transition after majority vote, got isLeader %t, term %d", isLeader, term)
	}
}
