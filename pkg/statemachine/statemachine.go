package statemachine

import (
	"errors"
	"fmt"
)

// OpType identifies the operation to perform against the deterministic state machine.
type OpType string

const (
	OpPut OpType = "PUT"
	OpGet OpType = "GET"
	OpDel OpType = "DEL"
	OpCAS OpType = "CAS"
)

// Command is the payload replicated across the consensus log to execute against the state machine.
type Command struct {
	Op            OpType `json:"op"`
	Key           string `json:"key"`
	Value         string `json:"value,omitempty"`
	ExpectedValue string `json:"expected_value,omitempty"` // for CAS operations
	ClientID      string `json:"client_id,omitempty"`
	SequenceNum   uint64 `json:"seq_num,omitempty"`
}

// ApplyResult captures the deterministic output of applying a command to the state machine.
type ApplyResult struct {
	Value         string `json:"value,omitempty"`
	PreviousValue string `json:"prev_value,omitempty"`
	Success       bool   `json:"success"`
	Error         string `json:"error,omitempty"`
	AppliedIndex  uint64 `json:"applied_index"`
}

var (
	ErrKeyNotFound   = errors.New("statemachine: key not found")
	ErrCASMismatch   = errors.New("statemachine: compare-and-swap value mismatch")
	ErrUnknownOp     = errors.New("statemachine: unknown command operation")
	ErrInvalidFormat = errors.New("statemachine: invalid command encoding")
)

// StateMachine defines the contract for deterministic state transition systems driven by Raft.
type StateMachine interface {
	// Apply executes a committed command at the specified monotonic index.
	Apply(cmdBytes []byte, index uint64) ApplyResult

	// Query performs a linearizable local state read without state mutation.
	Query(key string) (string, bool)

	// Snapshot serializes the complete in-memory state into a binary snapshot buffer.
	Snapshot() ([]byte, error)

	// Restore replaces in-memory state with the contents of a verified snapshot.
	Restore(snapshot []byte) error

	// LastApplied returns the highest log index applied to the state machine.
	LastApplied() uint64
}

func (c Command) String() string {
	switch c.Op {
	case OpPut:
		return fmt.Sprintf("PUT(%s = %s)", c.Key, c.Value)
	case OpDel:
		return fmt.Sprintf("DEL(%s)", c.Key)
	case OpCAS:
		return fmt.Sprintf("CAS(%s: expected=%s, new=%s)", c.Key, c.ExpectedValue, c.Value)
	case OpGet:
		return fmt.Sprintf("GET(%s)", c.Key)
	default:
		return fmt.Sprintf("CMD(%s, %s)", c.Op, c.Key)
	}
}
