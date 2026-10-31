package raft

import (
	"encoding/json"
	"testing"
	"time"
)

func TestConfigValidation(t *testing.T) {
	cfg := DefaultConfig("node-1", []NodeID{"node-2", "node-3"})
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected default config to be valid, got: %v", err)
	}

	if cfg.ClusterSize() != 3 {
		t.Fatalf("expected cluster size 3, got: %d", cfg.ClusterSize())
	}
	if cfg.QuorumSize() != 2 {
		t.Fatalf("expected quorum size 2, got: %d", cfg.QuorumSize())
	}

	// Invalid empty ID
	badCfg := *cfg
	badCfg.ID = ""
	if err := badCfg.Validate(); err == nil {
		t.Fatalf("expected error for empty ID")
	}

	// Invalid timeout ordering
	badTimeout := *cfg
	badTimeout.MinElectionTimeout = 300 * time.Millisecond
	badTimeout.MaxElectionTimeout = 150 * time.Millisecond
	if err := badTimeout.Validate(); err == nil {
		t.Fatalf("expected error when MaxElectionTimeout <= MinElectionTimeout")
	}

	// Heartbeat larger than election timeout
	badHB := *cfg
	badHB.HeartbeatInterval = 200 * time.Millisecond
	badHB.MinElectionTimeout = 150 * time.Millisecond
	badHB.MaxElectionTimeout = 300 * time.Millisecond
	if err := badHB.Validate(); err == nil {
		t.Fatalf("expected error when HeartbeatInterval >= MinElectionTimeout")
	}
}

func TestRoleTransitions(t *testing.T) {
	rs := NewRoleState()
	role, leader := rs.Get()
	if role != RoleFollower || leader != "" {
		t.Fatalf("expected initial Follower state, got: %v, %s", role, leader)
	}

	prev := rs.Transition(RoleCandidate, "")
	if prev != RoleFollower {
		t.Fatalf("expected prev RoleFollower, got %v", prev)
	}
	if rs.IsLeader() {
		t.Fatalf("expected candidate not to be leader")
	}

	rs.Transition(RoleLeader, "node-1")
	if !rs.IsLeader() {
		t.Fatalf("expected leader state")
	}
	role, leader = rs.Get()
	if role != RoleLeader || leader != "node-1" {
		t.Fatalf("expected leader node-1, got %v, %s", role, leader)
	}

	if role.String() != "Leader" {
		t.Fatalf("expected role string 'Leader', got: %s", role.String())
	}
}

func TestLogEntryBinaryCodec(t *testing.T) {
	orig := LogEntry{
		Index: Index(42),
		Term:  Term(3),
		Type:  EntryData,
		Data:  []byte("SET foo=bar"),
	}

	encoded, err := orig.EncodeBinary()
	if err != nil {
		t.Fatalf("EncodeBinary failed: %v", err)
	}

	decoded, err := DecodeLogEntry(encoded)
	if err != nil {
		t.Fatalf("DecodeLogEntry failed: %v", err)
	}

	if decoded.Index != orig.Index || decoded.Term != orig.Term || decoded.Type != orig.Type {
		t.Fatalf("mismatch in decoded headers: got %+v, want %+v", decoded, orig)
	}
	if string(decoded.Data) != string(orig.Data) {
		t.Fatalf("mismatch in payload data: got %s, want %s", string(decoded.Data), string(orig.Data))
	}

	// Test deep clone
	cloned := orig.Clone()
	cloned.Data[0] = 'X'
	if orig.Data[0] == 'X' {
		t.Fatalf("Clone did not perform a deep copy of Data")
	}

	// Test truncated bytes error
	if _, err := DecodeLogEntry(encoded[:10]); err == nil {
		t.Fatalf("expected error on truncated bytes")
	}
}

func TestRPCJsonWire(t *testing.T) {
	reqVote := RequestVoteArgs{
		Term:         Term(2),
		CandidateID:  "node-2",
		LastLogIndex: Index(10),
		LastLogTerm:  Term(1),
	}
	bytes, err := json.Marshal(reqVote)
	if err != nil {
		t.Fatalf("json.Marshal failed: %v", err)
	}
	var unmarshaled RequestVoteArgs
	if err := json.Unmarshal(bytes, &unmarshaled); err != nil {
		t.Fatalf("json.Unmarshal failed: %v", err)
	}
	if unmarshaled != reqVote {
		t.Fatalf("unmarshaled mismatch: %+v vs %+v", unmarshaled, reqVote)
	}

	appendReq := AppendEntriesArgs{
		Term:         Term(5),
		LeaderID:     "node-1",
		PrevLogIndex: Index(15),
		PrevLogTerm:  Term(4),
		Entries: []LogEntry{
			{Index: 16, Term: 5, Type: EntryData, Data: []byte("val1")},
		},
		LeaderCommit: Index(15),
	}
	bytes2, err := json.Marshal(appendReq)
	if err != nil {
		t.Fatalf("json.Marshal AppendEntriesArgs failed: %v", err)
	}
	var unmarshaledAppend AppendEntriesArgs
	if err := json.Unmarshal(bytes2, &unmarshaledAppend); err != nil {
		t.Fatalf("json.Unmarshal AppendEntriesArgs failed: %v", err)
	}
	if len(unmarshaledAppend.Entries) != 1 || string(unmarshaledAppend.Entries[0].Data) != "val1" {
		t.Fatalf("AppendEntries unmarshal mismatch: %+v", unmarshaledAppend)
	}
}
