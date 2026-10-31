package raft

import "fmt"

// RequestVoteArgs is invoked by candidates to gather election votes (§5.2, §5.4).
type RequestVoteArgs struct {
	Term         Term   `json:"term"`
	CandidateID  NodeID `json:"candidate_id"`
	LastLogIndex Index  `json:"last_log_index"`
	LastLogTerm  Term   `json:"last_log_term"`
	IsPreVote    bool   `json:"is_prevote,omitempty"`
}

// RequestVoteReply is the candidate's response from a peer.
type RequestVoteReply struct {
	Term        Term `json:"term"`
	VoteGranted bool `json:"vote_granted"`
}

// AppendEntriesArgs is invoked by leader to replicate log entries and keep heartbeat (§5.2, §5.3).
type AppendEntriesArgs struct {
	Term         Term       `json:"term"`
	LeaderID     NodeID     `json:"leader_id"`
	PrevLogIndex Index      `json:"prev_log_index"`
	PrevLogTerm  Term       `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit Index      `json:"leader_commit"`
}

// AppendEntriesReply is the follower response to AppendEntries.
type AppendEntriesReply struct {
	Term          Term  `json:"term"`
	Success       bool  `json:"success"`
	ConflictIndex Index `json:"conflict_index"`
	ConflictTerm  Term  `json:"conflict_term"`
}

// InstallSnapshotArgs is invoked by leader to send chunks of a snapshot to lagging follower (§7).
type InstallSnapshotArgs struct {
	Term              Term   `json:"term"`
	LeaderID          NodeID `json:"leader_id"`
	LastIncludedIndex Index  `json:"last_included_index"`
	LastIncludedTerm  Term   `json:"last_included_term"`
	Offset            uint64 `json:"offset"`
	Data              []byte `json:"data"`
	Done              bool   `json:"done"`
}

// InstallSnapshotReply is follower's response to snapshot chunk installation.
type InstallSnapshotReply struct {
	Term Term `json:"term"`
}

// RPCMessage represents a generic envelope for simulation and network dispatch.
type RPCMessage struct {
	From    NodeID `json:"from"`
	To      NodeID `json:"to"`
	Payload any    `json:"payload"`
}

// String representations for logging and debugging.
func (a RequestVoteArgs) String() string {
	return fmt.Sprintf("RequestVote(Term=%d, Cand=%s, LastIdx=%d, LastTerm=%d)", a.Term, a.CandidateID, a.LastLogIndex, a.LastLogTerm)
}

func (r RequestVoteReply) String() string {
	return fmt.Sprintf("RequestVoteReply(Term=%d, Granted=%t)", r.Term, r.VoteGranted)
}

func (a AppendEntriesArgs) String() string {
	return fmt.Sprintf("AppendEntries(Term=%d, Leader=%s, PrevIdx=%d, PrevTerm=%d, Count=%d, Commit=%d)",
		a.Term, a.LeaderID, a.PrevLogIndex, a.PrevLogTerm, len(a.Entries), a.LeaderCommit)
}

func (r AppendEntriesReply) String() string {
	return fmt.Sprintf("AppendEntriesReply(Term=%d, Success=%t, ConflictIdx=%d, ConflictTerm=%d)",
		r.Term, r.Success, r.ConflictIndex, r.ConflictTerm)
}

func (s InstallSnapshotArgs) String() string {
	return fmt.Sprintf("InstallSnapshot(Term=%d, Leader=%s, LastIdx=%d, LastTerm=%d, Len=%d, Done=%t)",
		s.Term, s.LeaderID, s.LastIncludedIndex, s.LastIncludedTerm, len(s.Data), s.Done)
}
