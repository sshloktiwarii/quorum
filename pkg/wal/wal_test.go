package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestWALWriteAndRecovery(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_test_clean")
	if err != nil {
		t.Fatalf("MkdirTemp failed: %v", err)
	}
	defer os.RemoveAll(tempDir)

	w, err := Open(tempDir, true)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write term-vote
	if err := w.WriteTermVote(3, "node-1"); err != nil {
		t.Fatalf("WriteTermVote failed: %v", err)
	}

	// Write entries
	for i := uint64(1); i <= 10; i++ {
		data := []byte(fmt.Sprintf("cmd-%d", i))
		if err := w.WriteEntry(i, 3, byte(RecordTypeEntry), data); err != nil {
			t.Fatalf("WriteEntry %d failed: %v", i, err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Recover
	w2, state, err := RecoverAndOpen(tempDir, true)
	if err != nil {
		t.Fatalf("RecoverAndOpen failed: %v", err)
	}
	defer w2.Close()

	if state.Corrupted {
		t.Fatalf("expected clean log, got corrupted")
	}
	if state.Term != 3 || state.VotedFor != "node-1" {
		t.Fatalf("term/vote mismatch: term=%d, vote=%s", state.Term, state.VotedFor)
	}
	if len(state.Entries) != 10 {
		t.Fatalf("expected 10 entries, got %d", len(state.Entries))
	}
	for i, entry := range state.Entries {
		expectedIndex := uint64(i + 1)
		if entry.Index != expectedIndex {
			t.Fatalf("entry %d index mismatch: %d vs %d", i, entry.Index, expectedIndex)
		}
	}
}

func TestWALTornWriteRecovery(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_test_torn")
	if err != nil {
		t.Fatalf("MkdirTemp failed: %v", err)
	}
	defer os.RemoveAll(tempDir)

	w, err := Open(tempDir, true)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	// Write 5 entries
	for i := uint64(1); i <= 5; i++ {
		_ = w.WriteEntry(i, 1, byte(RecordTypeEntry), []byte("valid-data"))
	}
	_ = w.Close()

	// Append partial/corrupted bytes simulating crash during write
	filePath := filepath.Join(tempDir, WALFileName)
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("OpenFile failed: %v", err)
	}
	// Write torn 7 bytes (incomplete header)
	_, _ = f.Write([]byte("QUOR\x01\x00\x00"))
	_ = f.Close()

	// Recover: should detect torn write and truncate cleanly at 5 entries
	w2, state, err := RecoverAndOpen(tempDir, true)
	if err != nil {
		t.Fatalf("RecoverAndOpen failed: %v", err)
	}
	defer w2.Close()

	if !state.Corrupted {
		t.Fatalf("expected state to flag corruption from torn write")
	}
	if len(state.Entries) != 5 {
		t.Fatalf("expected 5 valid entries recovered, got %d", len(state.Entries))
	}

	// Now append new entry 6 to ensure WAL is active and functional
	if err := w2.WriteEntry(6, 2, byte(RecordTypeEntry), []byte("post-recovery-entry")); err != nil {
		t.Fatalf("WriteEntry after recovery failed: %v", err)
	}
	_ = w2.Close()

	// Reopen and verify entry 6 is cleanly committed
	w3, state3, err := RecoverAndOpen(tempDir, true)
	if err != nil {
		t.Fatalf("Reopen failed: %v", err)
	}
	defer w3.Close()
	if state3.Corrupted {
		t.Fatalf("expected clean state after recovery and append")
	}
	if len(state3.Entries) != 6 {
		t.Fatalf("expected 6 entries, got %d", len(state3.Entries))
	}
}

func TestWALBitrotTruncation(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_test_bitrot")
	if err != nil {
		t.Fatalf("MkdirTemp failed: %v", err)
	}
	defer os.RemoveAll(tempDir)

	w, err := Open(tempDir, true)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	for i := uint64(1); i <= 3; i++ {
		_ = w.WriteEntry(i, 1, byte(RecordTypeEntry), []byte("solid-data"))
	}
	validSize := w.Size()

	// Write 4th entry
	_ = w.WriteEntry(4, 1, byte(RecordTypeEntry), []byte("corrupt-target"))
	_ = w.Close()

	// Corrupt a byte in the 4th entry payload
	filePath := filepath.Join(tempDir, WALFileName)
	raw, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	// Corrupt byte at end
	raw[len(raw)-2] ^= 0xAA
	if err := os.WriteFile(filePath, raw, 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Recover: should detect bitrot via CRC32 and truncate back to validSize (3 entries)
	w2, state, err := RecoverAndOpen(tempDir, true)
	if err != nil {
		t.Fatalf("RecoverAndOpen failed: %v", err)
	}
	defer w2.Close()

	if !state.Corrupted {
		t.Fatalf("expected bitrot corruption detected")
	}
	if state.ValidBytes != validSize {
		t.Fatalf("expected validBytes %d, got %d", validSize, state.ValidBytes)
	}
	if len(state.Entries) != 3 {
		t.Fatalf("expected 3 entries preserved, got %d", len(state.Entries))
	}
}
