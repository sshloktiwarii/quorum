package wal

import (
	"bytes"
	"testing"
)

func TestFrameHeaderCodec(t *testing.T) {
	orig := FrameHeader{
		Magic:      MagicBytes,
		RecordType: RecordTypeEntry,
		Length:     1024,
		CRC32:      0xDEADBEEF,
	}

	raw := orig.Encode()
	if len(raw) != HeaderSize {
		t.Fatalf("expected HeaderSize %d, got %d", HeaderSize, len(raw))
	}

	decoded, err := DecodeHeader(raw)
	if err != nil {
		t.Fatalf("DecodeHeader failed: %v", err)
	}

	if decoded != orig {
		t.Fatalf("header mismatch: got %+v, want %+v", decoded, orig)
	}

	// Corrupt magic
	corruptMagic := make([]byte, len(raw))
	copy(corruptMagic, raw)
	corruptMagic[0] = 'X'
	if _, err := DecodeHeader(corruptMagic); err == nil {
		t.Fatalf("expected error on corrupt magic bytes")
	}
}

func TestRecordCodecs(t *testing.T) {
	// Entry Record
	entry := EntryRecord{
		Index: 100,
		Term:  5,
		Type:  1,
		Data:  []byte("SET user:1=alice"),
	}
	encEntry, err := entry.Encode()
	if err != nil {
		t.Fatalf("EntryRecord.Encode failed: %v", err)
	}
	decEntry, err := DecodeEntryRecord(encEntry)
	if err != nil {
		t.Fatalf("DecodeEntryRecord failed: %v", err)
	}
	if decEntry.Index != entry.Index || decEntry.Term != entry.Term || decEntry.Type != entry.Type || !bytes.Equal(decEntry.Data, entry.Data) {
		t.Fatalf("EntryRecord mismatch: %+v vs %+v", decEntry, entry)
	}

	// TermVote Record
	tv := TermVoteRecord{
		Term:     10,
		VotedFor: "node-3",
	}
	encTV, err := tv.Encode()
	if err != nil {
		t.Fatalf("TermVoteRecord.Encode failed: %v", err)
	}
	decTV, err := DecodeTermVoteRecord(encTV)
	if err != nil {
		t.Fatalf("DecodeTermVoteRecord failed: %v", err)
	}
	if decTV.Term != tv.Term || decTV.VotedFor != tv.VotedFor {
		t.Fatalf("TermVoteRecord mismatch: %+v vs %+v", decTV, tv)
	}

	// SnapshotMeta Record
	var hash [32]byte
	copy(hash[:], []byte("sha256-checkpoint-dummy-digest-32b"))
	snap := SnapshotMetaRecord{
		LastIndex:      500,
		LastTerm:       12,
		CheckpointHash: hash,
	}
	encSnap, err := snap.Encode()
	if err != nil {
		t.Fatalf("SnapshotMetaRecord.Encode failed: %v", err)
	}
	decSnap, err := DecodeSnapshotMetaRecord(encSnap)
	if err != nil {
		t.Fatalf("DecodeSnapshotMetaRecord failed: %v", err)
	}
	if decSnap != snap {
		t.Fatalf("SnapshotMetaRecord mismatch: %+v vs %+v", decSnap, snap)
	}
}

func TestBuildAndVerifyFrame(t *testing.T) {
	payload := []byte("transaction log payload bytes")
	frame := BuildFrame(RecordTypeEntry, payload)

	hdr, err := DecodeHeader(frame[:HeaderSize])
	if err != nil {
		t.Fatalf("DecodeHeader failed: %v", err)
	}
	if err := VerifyFrameChecksum(hdr, frame[HeaderSize:]); err != nil {
		t.Fatalf("VerifyFrameChecksum failed: %v", err)
	}

	// Test bitrot corruption in payload
	corrupted := make([]byte, len(frame))
	copy(corrupted, frame)
	corrupted[HeaderSize+2] ^= 0xFF

	if err := VerifyFrameChecksum(hdr, corrupted[HeaderSize:]); err == nil {
		t.Fatalf("expected CRC32 failure on bitrot byte flip")
	}
}
