package snapshot

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSnapshotSaveAndLoad(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "snap_test_clean")
	if err != nil {
		t.Fatalf("MkdirTemp failed: %v", err)
	}
	defer os.RemoveAll(tempDir)

	payload := []byte(`{"data":{"account:1":"500","account:2":"750"},"last_applied":250}`)
	if err := Save(tempDir, 250, 4, payload); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	meta, loaded, err := Load(tempDir)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if meta.LastIncludedIndex != 250 || meta.LastIncludedTerm != 4 {
		t.Fatalf("metadata mismatch: %+v", meta)
	}
	if string(loaded) != string(payload) {
		t.Fatalf("payload mismatch: %s vs %s", string(loaded), string(payload))
	}
}

func TestSnapshotBitrotChecksum(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "snap_test_bitrot")
	if err != nil {
		t.Fatalf("MkdirTemp failed: %v", err)
	}
	defer os.RemoveAll(tempDir)

	payload := []byte("important state machine checkpoint")
	if err := Save(tempDir, 100, 2, payload); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Corrupt payload byte
	finalPath := filepath.Join(tempDir, SnapshotFileName)
	raw, err := os.ReadFile(finalPath)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	raw[len(raw)-1] ^= 0x55
	if err := os.WriteFile(finalPath, raw, 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	_, _, err = Load(tempDir)
	if err == nil {
		t.Fatalf("expected CRC32 corruption error on bitrot byte flip")
	}
}
