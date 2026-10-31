package raft

import (
	"fmt"
	"sync"
)

// NodeRole represents the current operational role of a Raft node in the cluster.
type NodeRole int

const (
	// RoleFollower is the passive state receiving heartbeats and log entries.
	RoleFollower NodeRole = iota

	// RoleCandidate is the transient state soliciting votes during an election.
	RoleCandidate

	// RoleLeader is the active consensus coordinator handling proposals and replication.
	RoleLeader
)

// String returns the human-readable identifier of the role.
func (r NodeRole) String() string {
	switch r {
	case RoleFollower:
		return "Follower"
	case RoleCandidate:
		return "Candidate"
	case RoleLeader:
		return "Leader"
	default:
		return fmt.Sprintf("Unknown(%d)", int(r))
	}
}

// NodeState tracks runtime metrics and identity for visualizer HUD and diagnostics.
type NodeState struct {
	ID          NodeID   `json:"id"`
	Role        NodeRole `json:"role"`
	RoleName    string   `json:"role_name"`
	CurrentTerm Term     `json:"current_term"`
	VotedFor    NodeID   `json:"voted_for"`
	CommitIndex Index    `json:"commit_index"`
	LastApplied Index    `json:"last_applied"`
	LastLogTerm Term     `json:"last_log_term"`
	LastLogIndex Index   `json:"last_log_index"`
	LeaderID    NodeID   `json:"leader_id"`
}

// RoleState manages thread-safe volatile role tracking and transitions.
type RoleState struct {
	mu       sync.RWMutex
	role     NodeRole
	leaderID NodeID
}

// NewRoleState initializes the node in the default Follower state.
func NewRoleState() *RoleState {
	return &RoleState{
		role:     RoleFollower,
		leaderID: "",
	}
}

// Get returns the current role and known leader.
func (rs *RoleState) Get() (NodeRole, NodeID) {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.role, rs.leaderID
}

// Transition moves the node to a new role and updates known leader.
func (rs *RoleState) Transition(newRole NodeRole, leaderID NodeID) (prevRole NodeRole) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	prevRole = rs.role
	rs.role = newRole
	rs.leaderID = leaderID
	return prevRole
}

// IsLeader checks if the node is currently the leader.
func (rs *RoleState) IsLeader() bool {
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.role == RoleLeader
}
