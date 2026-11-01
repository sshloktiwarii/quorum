package linearizability

import (
	"fmt"
	"testing"
	"time"
)

// TestCheckerSingleKeyExceeding64Ops verifies that single-key histories exceeding
// 64 operations are successfully verified via quiescent epoch chunking.
func TestCheckerSingleKeyExceeding64Ops(t *testing.T) {
	checker := NewChecker()

	// 100 sequential operations on a single key
	ops := make([]Operation, 100)
	for i := 0; i < 100; i++ {
		tBase := time.Duration(i*10) * time.Millisecond
		val := fmt.Sprintf("val-%d", i)
		ops[i] = Operation{
			ID:       fmt.Sprintf("op-%d", i),
			Type:     "PUT",
			Key:      "single_key",
			Value:    val,
			InvokeAt: tBase,
			ReturnAt: tBase + 5*time.Millisecond,
			Ok:       true,
		}
	}

	valid, order, err := checker.Verify(ops)
	if !valid || err != nil {
		t.Fatalf("expected single key >64 ops to pass: %v", err)
	}
	if len(order) != len(ops) {
		t.Fatalf("expected %d linearized operations, got %d", len(ops), len(order))
	}
}

// TestCheckerMultiKeyExceeding64Ops tests partitioned verification across multiple keys.
// When total operations exceed 64, but each individual key has <= 64 operations,
// verifyPartitionedByKey successfully evaluates the history without error.
func TestCheckerMultiKeyExceeding64Ops(t *testing.T) {
	checker := NewChecker()

	// 5 keys, 15 sequential ops each = 75 total ops (> 64)
	var ops []Operation
	opID := 0
	for k := 0; k < 5; k++ {
		keyName := fmt.Sprintf("k%d", k)
		for i := 0; i < 15; i++ {
			tBase := time.Duration(opID*10) * time.Millisecond
			val := fmt.Sprintf("v_%d_%d", k, i)
			ops = append(ops, Operation{
				ID:       fmt.Sprintf("put-%d", opID),
				Type:     "PUT",
				Key:      keyName,
				Value:    val,
				InvokeAt: tBase,
				ReturnAt: tBase + 2*time.Millisecond,
				Ok:       true,
			})
			opID++

			ops = append(ops, Operation{
				ID:       fmt.Sprintf("get-%d", opID),
				Type:     "GET",
				Key:      keyName,
				Result:   val,
				InvokeAt: tBase + 4*time.Millisecond,
				ReturnAt: tBase + 6*time.Millisecond,
				Ok:       true,
			})
			opID++
		}
	}

	valid, order, err := checker.Verify(ops)
	if !valid || err != nil {
		t.Fatalf("expected valid multi-key history: %v", err)
	}
	if len(order) != len(ops) {
		t.Fatalf("expected %d linearized operations, got %d", len(ops), len(order))
	}
}

// TestCheckerStaleReadDetection verifies that Wing-Gong rejects a stale read after an authoritative write has returned.
func TestCheckerStaleReadDetection(t *testing.T) {
	checker := NewChecker()

	// Timeline:
	// 1. PUT key="state" value="initial" [0ms, 10ms]
	// 2. PUT key="state" value="updated" [20ms, 30ms] -> Returned!
	// 3. GET key="state" result="initial" [40ms, 50ms] -> Stale! Must be rejected.
	ops := []Operation{
		{ID: "w1", Type: "PUT", Key: "state", Value: "initial", InvokeAt: 0, ReturnAt: 10 * time.Millisecond, Ok: true},
		{ID: "w2", Type: "PUT", Key: "state", Value: "updated", InvokeAt: 20 * time.Millisecond, ReturnAt: 30 * time.Millisecond, Ok: true},
		{ID: "r1", Type: "GET", Key: "state", Result: "initial", InvokeAt: 40 * time.Millisecond, ReturnAt: 50 * time.Millisecond, Ok: true},
	}

	valid, _, err := checker.Verify(ops)
	if valid || err == nil {
		t.Fatalf("checker accepted non-linearizable stale read!")
	}
}

// TestCheckerCausalInversion verifies that concurrent reads with conflicting sequential orders are rejected.
func TestCheckerCausalInversion(t *testing.T) {
	checker := NewChecker()

	// Timeline:
	// PUT x=1 [0ms, 10ms]
	// PUT x=2 [20ms, 30ms]
	// Read1 [35ms, 45ms] returns 2
	// Read2 [50ms, 60ms] returns 1 -> Causal inversion: x=2 already observed in [35ms, 45ms], so subsequent read cannot see 1
	ops := []Operation{
		{ID: "w1", Type: "PUT", Key: "x", Value: "1", InvokeAt: 0, ReturnAt: 10 * time.Millisecond, Ok: true},
		{ID: "w2", Type: "PUT", Key: "x", Value: "2", InvokeAt: 20 * time.Millisecond, ReturnAt: 30 * time.Millisecond, Ok: true},
		{ID: "r1", Type: "GET", Key: "x", Result: "2", InvokeAt: 35 * time.Millisecond, ReturnAt: 45 * time.Millisecond, Ok: true},
		{ID: "r2", Type: "GET", Key: "x", Result: "1", InvokeAt: 50 * time.Millisecond, ReturnAt: 60 * time.Millisecond, Ok: true},
	}

	valid, _, err := checker.Verify(ops)
	if valid || err == nil {
		t.Fatalf("checker accepted causally inverted read sequence!")
	}
}

// TestCheckerCASAndDELOperations tests CAS and DEL transitions.
func TestCheckerCASAndDELOperations(t *testing.T) {
	checker := NewChecker()

	ops := []Operation{
		{ID: "put-1", Type: "PUT", Key: "lock", Value: "free", InvokeAt: 0, ReturnAt: 10 * time.Millisecond, Ok: true},
		// Successful CAS
		{ID: "cas-1", Type: "CAS", Key: "lock", ExpectedValue: "free", Value: "held_by_A", InvokeAt: 15 * time.Millisecond, ReturnAt: 25 * time.Millisecond, Ok: true},
		// Failing CAS (expected "free", but currently "held_by_A")
		{ID: "cas-2", Type: "CAS", Key: "lock", ExpectedValue: "free", Value: "held_by_B", InvokeAt: 30 * time.Millisecond, ReturnAt: 40 * time.Millisecond, Ok: false},
		// Delete
		{ID: "del-1", Type: "DEL", Key: "lock", InvokeAt: 45 * time.Millisecond, ReturnAt: 55 * time.Millisecond, Ok: true},
		// GET after delete must return empty / not found
		{ID: "get-after-del", Type: "GET", Key: "lock", Result: "", InvokeAt: 60 * time.Millisecond, ReturnAt: 70 * time.Millisecond, Ok: true},
	}

	valid, order, err := checker.Verify(ops)
	if !valid || err != nil {
		t.Fatalf("expected valid CAS and DEL history: %v", err)
	}
	if len(order) != len(ops) {
		t.Fatalf("expected %d linearized operations, got %d", len(ops), len(order))
	}
}
