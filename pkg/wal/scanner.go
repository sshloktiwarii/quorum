package wal

import (
	"errors"
	"fmt"
	"io"
	"os"
)

// RecoveredState stores the persistent consensus state extracted during WAL replay.
type RecoveredState struct {
	Entries        []EntryRecord
	Term           uint64
	VotedFor       string
	SnapshotMeta   *SnapshotMetaRecord
	ValidBytes     int64
	Corrupted      bool
	CorruptionErr  error
	CorruptOffset  int64
}

// Scan reads a WAL file sequentially from byte 0, validating frames and checksums.
func Scan(filePath string) (*RecoveredState, error) {
	file, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return &RecoveredState{Entries: make([]EntryRecord, 0)}, nil
		}
		return nil, fmt.Errorf("wal scan: open %s: %w", filePath, err)
	}
	defer file.Close()

	state := &RecoveredState{
		Entries: make([]EntryRecord, 0),
	}

	headerBuf := make([]byte, HeaderSize)
	var currentOffset int64 = 0

	for {
		// Read 13-byte header
		n, err := io.ReadFull(file, headerBuf)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) && n == 0 {
				// Clean EOF at frame boundary
				break
			}
			// Incomplete header: torn write
			state.Corrupted = true
			state.CorruptOffset = currentOffset
			state.CorruptionErr = fmt.Errorf("wal scan: incomplete frame header at offset %d: %w", currentOffset, err)
			break
		}

		hdr, err := DecodeHeader(headerBuf)
		if err != nil {
			state.Corrupted = true
			state.CorruptOffset = currentOffset
			state.CorruptionErr = fmt.Errorf("wal scan: header decode failed at offset %d: %w", currentOffset, err)
			break
		}

		payload := make([]byte, hdr.Length)
		if hdr.Length > 0 {
			if _, err := io.ReadFull(file, payload); err != nil {
				// Torn write: payload truncated
				state.Corrupted = true
				state.CorruptOffset = currentOffset
				state.CorruptionErr = fmt.Errorf("wal scan: truncated payload at offset %d: %w", currentOffset, err)
				break
			}
		}

		if err := VerifyFrameChecksum(hdr, payload); err != nil {
			state.Corrupted = true
			state.CorruptOffset = currentOffset
			state.CorruptionErr = fmt.Errorf("wal scan: CRC32 corruption at offset %d: %w", currentOffset, err)
			break
		}

		// Frame is 100% valid. Process record.
		switch hdr.RecordType {
		case RecordTypeEntry:
			rec, err := DecodeEntryRecord(payload)
			if err != nil {
				state.Corrupted = true
				state.CorruptOffset = currentOffset
				state.CorruptionErr = fmt.Errorf("wal scan: entry record decode failed at offset %d: %w", currentOffset, err)
				return state, nil
			}
			state.Entries = append(state.Entries, rec)

		case RecordTypeTermVote:
			rec, err := DecodeTermVoteRecord(payload)
			if err != nil {
				state.Corrupted = true
				state.CorruptOffset = currentOffset
				state.CorruptionErr = fmt.Errorf("wal scan: term-vote decode failed at offset %d: %w", currentOffset, err)
				return state, nil
			}
			state.Term = rec.Term
			state.VotedFor = rec.VotedFor

		case RecordTypeSnapshotMeta:
			rec, err := DecodeSnapshotMetaRecord(payload)
			if err != nil {
				state.Corrupted = true
				state.CorruptOffset = currentOffset
				state.CorruptionErr = fmt.Errorf("wal scan: snapshot-meta decode failed at offset %d: %w", currentOffset, err)
				return state, nil
			}
			state.SnapshotMeta = &rec
		}

		currentOffset += int64(HeaderSize + hdr.Length)
		state.ValidBytes = currentOffset
	}

	return state, nil
}
