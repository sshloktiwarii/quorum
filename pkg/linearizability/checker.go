package linearizability

import (
	"errors"
	"fmt"
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
		// Partition by key if history exceeds single-word bitmask
		return c.verifyPartitionedByKey(ops)
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

	var dfs func(executed uint64, state map[string]string) bool
	dfs = func(executed uint64, state map[string]string) bool {
		if executed == (1<<n)-1 {
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

	initialState := make(map[string]string)
	success := dfs(0, initialState)
	if !success {
		return false, nil, ErrNonLinearizable
	}

	return true, linearOrder, nil
}

// verifyPartitionedByKey splits multi-key histories into independent per-key verification passes.
func (c *Checker) verifyPartitionedByKey(ops []Operation) (bool, []Operation, error) {
	byKey := make(map[string][]Operation)
	for _, op := range ops {
		byKey[op.Key] = append(byKey[op.Key], op)
	}

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
