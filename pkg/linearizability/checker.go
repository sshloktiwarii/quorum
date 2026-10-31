package linearizability

import (
	"errors"
	"fmt"
	"sort"
)

var (
	ErrNonLinearizable = errors.New("linearizability: concurrent history violates sequential specification")
)

// Checker performs formal Wing-Gong linearizability verification on concurrent histories.
type Checker struct {
	model *SequentialModel
}

// NewChecker initializes a Wing-Gong linearizability checker.
func NewChecker() *Checker {
	return &Checker{
		model: NewSequentialModel(),
	}
}

// Verify evaluates if the given concurrent execution history is linearizable.
// Returns (true, linearOrder, nil) on success, or (false, nil, ErrNonLinearizable) on violation.
func (c *Checker) Verify(history []Operation) (bool, []Operation, error) {
	if len(history) == 0 {
		return true, nil, nil
	}

	// Filter operations that completed (invoked and returned)
	var ops []Operation
	for _, op := range history {
		if op.ReturnAt >= op.InvokeAt {
			ops = append(ops, op)
		}
	}

	n := len(ops)
	if n > 64 {
		// Partition by key or quiescent epochs if history exceeds single-word bitmask
		return c.verifyPartitionedByKey(ops)
	}

	ok, order, _, err := c.verifyChunkWithState(ops, make(map[string]string))
	return ok, order, err
}

func (c *Checker) verifyChunkWithState(ops []Operation, initialState map[string]string) (bool, []Operation, map[string]string, error) {
	n := len(ops)
	if n == 0 {
		return true, nil, initialState, nil
	}

	// Build real-time precedence dependencies: if op1.ReturnAt < op2.InvokeAt => op1 < op2
	preds := make([]uint64, n)
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i != j && ops[j].ReturnAt < ops[i].InvokeAt {
				preds[i] |= (1 << j)
			}
		}
	}

	memo := make(map[string]bool)
	var linearOrder []Operation
	var finalState map[string]string

	var dfs func(executed uint64, state map[string]string) bool
	dfs = func(executed uint64, state map[string]string) bool {
		if executed == (1<<n)-1 {
			finalState = state
			return true
		}

		key := fmt.Sprintf("%d:%s", executed, StateHash(state))
		if memo[key] {
			return false
		}

		for i := 0; i < n; i++ {
			// If not yet executed and all predecessors are fulfilled
			if (executed&(1<<i)) == 0 && (executed&preds[i]) == preds[i] {
				nextState, ok := c.model.Step(state, ops[i])
				if ok {
					if dfs(executed|(1<<i), nextState) {
						linearOrder = append([]Operation{ops[i]}, linearOrder...)
						return true
					}
				}
			}
		}

		memo[key] = true
		return false
	}

	success := dfs(0, initialState)
	if !success {
		return false, nil, nil, ErrNonLinearizable
	}

	return true, linearOrder, finalState, nil
}

// verifyPartitionedByKey splits multi-key histories into independent per-key verification passes,
// or splits single-key histories into quiescent epochs if length exceeds 64.
func (c *Checker) verifyPartitionedByKey(ops []Operation) (bool, []Operation, error) {
	byKey := make(map[string][]Operation)
	for _, op := range ops {
		byKey[op.Key] = append(byKey[op.Key], op)
	}

	if len(byKey) > 1 {
		var combinedOrder []Operation
		for _, keyOps := range byKey {
			ok, order, err := c.Verify(keyOps)
			if !ok || err != nil {
				return false, nil, err
			}
			combinedOrder = append(combinedOrder, order...)
		}
		return true, combinedOrder, nil
	}

	// Single key with > 64 operations: sort by InvokeAt and split into quiescent epochs
	sort.Slice(ops, func(i, j int) bool {
		if ops[i].InvokeAt != ops[j].InvokeAt {
			return ops[i].InvokeAt < ops[j].InvokeAt
		}
		return ops[i].ReturnAt < ops[j].ReturnAt
	})

	currentState := make(map[string]string)
	var combinedOrder []Operation

	start := 0
	maxReturn := ops[0].ReturnAt
	for i := 1; i < len(ops); i++ {
		// Quiescent point: all prior ops returned before ops[i] was invoked, OR chunk reached 64
		if ops[i].InvokeAt >= maxReturn || (i-start) >= 64 {
			chunk := ops[start:i]
			ok, order, nextState, err := c.verifyChunkWithState(chunk, currentState)
			if !ok || err != nil {
				return false, nil, err
			}
			combinedOrder = append(combinedOrder, order...)
			currentState = nextState
			start = i
			maxReturn = ops[i].ReturnAt
		} else if ops[i].ReturnAt > maxReturn {
			maxReturn = ops[i].ReturnAt
		}
	}

	if start < len(ops) {
		chunk := ops[start:]
		ok, order, _, err := c.verifyChunkWithState(chunk, currentState)
		if !ok || err != nil {
			return false, nil, err
		}
		combinedOrder = append(combinedOrder, order...)
	}

	return true, combinedOrder, nil
}
