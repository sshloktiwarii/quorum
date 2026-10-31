package linearizability

import (
	"crypto/sha256"
	"fmt"
	"sort"
	"time"
)

// Operation represents an executed client action with invocation and return timestamps.
type Operation struct {
	ID            string        `json:"id"`
	ClientID      string        `json:"client_id"`
	Type          string        `json:"type"` // "PUT", "GET", "CAS", "DEL"
	Key           string        `json:"key"`
	Value         string        `json:"value"`
	ExpectedValue string        `json:"expected_value,omitempty"`
	InvokeAt      time.Duration `json:"invoke_at"`
	ReturnAt      time.Duration `json:"return_at"`
	Result        string        `json:"result"`
	Ok            bool          `json:"ok"`
}

// SequentialModel defines the state and transition rules for a sequential key-value register.
type SequentialModel struct{}

// NewSequentialModel returns a new sequential KV specification model.
func NewSequentialModel() *SequentialModel {
	return &SequentialModel{}
}

// Step applies an operation against the given sequential state and determines whether the output matches.
func (m *SequentialModel) Step(state map[string]string, op Operation) (map[string]string, bool) {
	nextState := cloneState(state)

	switch op.Type {
	case "PUT":
		nextState[op.Key] = op.Value
		// PUT is always valid if it completed
		return nextState, op.Ok

	case "GET":
		expectedVal, exists := state[op.Key]
		if !exists {
			// Key doesn't exist: op must return empty or not ok
			if op.Ok && op.Result != "" {
				return state, false
			}
			return state, true
		}
		// Key exists: op must return matching value
		if !op.Ok || op.Result != expectedVal {
			return state, false
		}
		return state, true

	case "DEL":
		_, exists := state[op.Key]
		if exists {
			delete(nextState, op.Key)
		}
		return nextState, true

	case "CAS":
		currentVal := state[op.Key]
		if currentVal == op.ExpectedValue {
			nextState[op.Key] = op.Value
			// CAS should succeed
			if !op.Ok {
				return state, false
			}
			return nextState, true
		}
		// CAS should fail
		if op.Ok {
			return state, false
		}
		return state, true

	default:
		return state, false
	}
}

func cloneState(s map[string]string) map[string]string {
	c := make(map[string]string, len(s))
	for k, v := range s {
		c[k] = v
	}
	return c
}

// StateHash computes a deterministic digest of the sequential state for memoization.
func StateHash(s map[string]string) string {
	keys := make([]string, 0, len(s))
	for k := range s {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	h := sha256.New()
	for _, k := range keys {
		h.Write([]byte(fmt.Sprintf("%s=%s;", k, s[k])))
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}
