package statemachine

import (
	"encoding/json"
	"testing"
)

func TestKVStoreBasicOperations(t *testing.T) {
	kv := NewKVStore()

	// 1. PUT
	putCmd, _ := json.Marshal(Command{Op: OpPut, Key: "user", Value: "alice"})
	res1 := kv.Apply(putCmd, 1)
	if !res1.Success || res1.Value != "alice" {
		t.Fatalf("PUT failed: %+v", res1)
	}

	// 2. GET
	getCmd, _ := json.Marshal(Command{Op: OpGet, Key: "user"})
	res2 := kv.Apply(getCmd, 2)
	if !res2.Success || res2.Value != "alice" {
		t.Fatalf("GET failed: %+v", res2)
	}

	// Direct Query
	val, exists := kv.Query("user")
	if !exists || val != "alice" {
		t.Fatalf("Query failed: %s, %t", val, exists)
	}

	// 3. CAS Success
	casCmd, _ := json.Marshal(Command{Op: OpCAS, Key: "user", ExpectedValue: "alice", Value: "bob"})
	res3 := kv.Apply(casCmd, 3)
	if !res3.Success || res3.Value != "bob" || res3.PreviousValue != "alice" {
		t.Fatalf("CAS failed: %+v", res3)
	}

	// 4. CAS Failure
	badCasCmd, _ := json.Marshal(Command{Op: OpCAS, Key: "user", ExpectedValue: "wrong", Value: "charlie"})
	res4 := kv.Apply(badCasCmd, 4)
	if res4.Success {
		t.Fatalf("expected CAS failure, got success: %+v", res4)
	}

	// 5. DEL
	delCmd, _ := json.Marshal(Command{Op: OpDel, Key: "user"})
	res5 := kv.Apply(delCmd, 5)
	if !res5.Success || res5.PreviousValue != "bob" {
		t.Fatalf("DEL failed: %+v", res5)
	}

	// Verify deleted
	_, exists = kv.Query("user")
	if exists {
		t.Fatalf("expected key to be deleted")
	}

	if kv.LastApplied() != 5 {
		t.Fatalf("expected LastApplied 5, got %d", kv.LastApplied())
	}
}

func TestKVStoreDeduplication(t *testing.T) {
	kv := NewKVStore()

	cmd := Command{
		Op:          OpPut,
		Key:         "account:101",
		Value:       "1000",
		ClientID:    "client-A",
		SequenceNum: 1,
	}
	raw, _ := json.Marshal(cmd)

	res1 := kv.Apply(raw, 10)
	if !res1.Success || res1.Value != "1000" {
		t.Fatalf("first apply failed: %+v", res1)
	}

	// Re-apply same client sequence: must return identical cached result without mutation
	res2 := kv.Apply(raw, 11)
	if !res2.Success || res2.Value != "1000" {
		t.Fatalf("duplicate apply failed: %+v", res2)
	}
}

func TestKVStoreSnapshotRestore(t *testing.T) {
	kv := NewKVStore()

	// Setup some initial state
	cmd1, _ := json.Marshal(Command{Op: OpPut, Key: "k1", Value: "v1", ClientID: "c1", SequenceNum: 1})
	cmd2, _ := json.Marshal(Command{Op: OpPut, Key: "k2", Value: "v2", ClientID: "c2", SequenceNum: 1})
	kv.Apply(cmd1, 5)
	kv.Apply(cmd2, 6)

	snapBytes, err := kv.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot failed: %v", err)
	}

	// Restore into a fresh store
	kv2 := NewKVStore()
	if err := kv2.Restore(snapBytes); err != nil {
		t.Fatalf("Restore failed: %v", err)
	}

	if kv2.LastApplied() != 6 {
		t.Fatalf("expected LastApplied 6, got %d", kv2.LastApplied())
	}
	v1, ok1 := kv2.Query("k1")
	v2, ok2 := kv2.Query("k2")
	if !ok1 || v1 != "v1" || !ok2 || v2 != "v2" {
		t.Fatalf("restored data mismatch: k1=(%s, %t), k2=(%s, %t)", v1, ok1, v2, ok2)
	}

	// Verify sessions restored (c1 with seq 1 should still be deduplicated)
	dupRes := kv2.Apply(cmd1, 7)
	if !dupRes.Success || dupRes.Value != "v1" {
		t.Fatalf("restored deduplication check failed: %+v", dupRes)
	}
}
