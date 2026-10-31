package raft

import (
	"errors"
	"fmt"
)

// NodeID uniquely identifies a node within the Raft consensus cluster.
type NodeID string

// Term represents the logical Raft epoch/term number (strictly monotonic).
type Term uint64

// Index represents the monotonic position of an entry within the Raft log.
type Index uint64

// Standard consensus error definitions.
var (
	ErrNotLeader           = errors.New("raft: node is not the leader")
	ErrStopped             = errors.New("raft: node has been stopped")
	ErrInvalidConfig       = errors.New("raft: invalid cluster configuration")
	ErrLogCompacted        = errors.New("raft: requested log index has been compacted")
	ErrSnapshotDiscard     = errors.New("raft: snapshot is older than current commit index")
	ErrLeaderStepDown      = errors.New("raft: leadership lost during operation")
	ErrProposalDropped     = errors.New("raft: proposal dropped due to term change or shutdown")
	ErrLinearizabilityFail = errors.New("raft: linearizable read quorum confirmation failed")
)

// String representations for formatting and diagnostics.
func (t Term) String() string   { return fmt.Sprintf("Term(%d)", uint64(t)) }
func (i Index) String() string  { return fmt.Sprintf("Index(%d)", uint64(i)) }
func (n NodeID) String() string { return string(n) }
