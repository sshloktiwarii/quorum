package linearizability

import (
	"testing"
	"time"
)

func TestCheckerValidHistory(t *testing.T) {
	checker := NewChecker()

	// Sequential history
	h1 := []Operation{
		{ID: "1", Type: "PUT", Key: "x", Value: "1", InvokeAt: 10 * time.Millisecond, ReturnAt: 20 * time.Millisecond, Ok: true},
		{ID: "2", Type: "GET", Key: "x", Result: "1", InvokeAt: 25 * time.Millisecond, ReturnAt: 35 * time.Millisecond, Ok: true},
		{ID: "3", Type: "PUT", Key: "x", Value: "2", InvokeAt: 40 * time.Millisecond, ReturnAt: 50 * time.Millisecond, Ok: true},
		{ID: "4", Type: "GET", Key: "x", Result: "2", InvokeAt: 55 * time.Millisecond, ReturnAt: 65 * time.Millisecond, Ok: true},
	}

	valid, order, err := checker.Verify(h1)
	if !valid || err != nil {
		t.Fatalf("expected valid sequential history: %v", err)
	}
	if len(order) != 4 {
		t.Fatalf("expected 4 linearized operations, got %d", len(order))
	}

	// Concurrent valid overlapping operations
	h2 := []Operation{
		{ID: "A", Type: "PUT", Key: "y", Value: "foo", InvokeAt: 10 * time.Millisecond, ReturnAt: 50 * time.Millisecond, Ok: true},
		{ID: "B", Type: "GET", Key: "y", Result: "foo", InvokeAt: 20 * time.Millisecond, ReturnAt: 40 * time.Millisecond, Ok: true},
	}
	valid2, _, err2 := checker.Verify(h2)
	if !valid2 || err2 != nil {
		t.Fatalf("expected valid concurrent history: %v", err2)
	}
}

func TestCheckerInvalidHistory(t *testing.T) {
	checker := NewChecker()

	// Invalidation: Op 2 completed before Op 3 started, but Op 3 returned stale pre-Op2 value!
	h := []Operation{
		{ID: "1", Type: "PUT", Key: "z", Value: "100", InvokeAt: 0, ReturnAt: 10 * time.Millisecond, Ok: true},
		{ID: "2", Type: "PUT", Key: "z", Value: "200", InvokeAt: 20 * time.Millisecond, ReturnAt: 30 * time.Millisecond, Ok: true},
		{ID: "3", Type: "GET", Key: "z", Result: "100", InvokeAt: 40 * time.Millisecond, ReturnAt: 50 * time.Millisecond, Ok: true}, // Stale!
	}

	valid, _, err := checker.Verify(h)
	if valid || err == nil {
		t.Fatalf("expected non-linearizable history to be rejected")
	}
}
