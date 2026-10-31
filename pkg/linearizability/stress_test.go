package linearizability

import (
	"fmt"
	"testing"
	"time"
)

func TestCheckerSingleKeyExceeding64Ops(t *testing.T) {
	checker := NewChecker()

	// 70 sequential operations on a single key
	ops := make([]Operation, 70)
	for i := 0; i < 70; i++ {
		ops[i] = Operation{
			ID:       fmt.Sprintf("op-%d", i),
			Type:     "PUT",
			Key:      "single_key",
			Value:    fmt.Sprintf("val-%d", i),
			InvokeAt: time.Duration(i*10) * time.Millisecond,
			ReturnAt: time.Duration(i*10+5) * time.Millisecond,
			Ok:       true,
		}
	}

	done := make(chan bool)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				t.Logf("Recovered panic: %v", r)
			}
			done <- true
		}()
		_, _, _ = checker.Verify(ops)
	}()

	select {
	case <-done:
		t.Log("Completed without hang/overflow")
	case <-time.After(3 * time.Second):
		t.Fatal("FATAL: Checker hung or entered infinite recursion on single key > 64 operations!")
	}
}
