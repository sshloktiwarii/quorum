package wal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// WAL manages an append-only write-ahead log file protected by CRC32 checksums.
type WAL struct {
	mu       sync.Mutex
	dir      string
	filePath string
	file     *os.File
	syncMode bool
	offset   int64
	closed   bool
}

const (
	WALFileName = "quorum.wal"
)

// Open initializes or opens an existing WAL file in the specified directory.
func Open(dir string, syncMode bool) (*WAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("wal: create directory %s: %w", dir, err)
	}

	filePath := filepath.Join(dir, WALFileName)
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("wal: open file %s: %w", filePath, err)
	}

	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("wal: stat file %s: %w", filePath, err)
	}

	w := &WAL{
		dir:      dir,
		filePath: filePath,
		file:     file,
		syncMode: syncMode,
		offset:   info.Size(),
		closed:   false,
	}

	return w, nil
}

// AppendRaw writes a pre-framed binary slice to the end of the log.
func (w *WAL) AppendRaw(frame []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return errors.New("wal: closed")
	}

	n, err := w.file.Write(frame)
	if err != nil {
		return fmt.Errorf("wal: append write failed: %w", err)
	}
	w.offset += int64(n)

	if w.syncMode {
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("wal: fsync failed: %w", err)
		}
	}
	return nil
}

// WriteEntry persists a Raft log entry.
func (w *WAL) WriteEntry(index uint64, term uint64, entryType uint8, data []byte) error {
	rec := EntryRecord{
		Index: index,
		Term:  term,
		Type:  entryType,
		Data:  data,
	}
	payload, err := rec.Encode()
	if err != nil {
		return fmt.Errorf("wal: encode entry: %w", err)
	}
	frame := BuildFrame(RecordTypeEntry, payload)
	return w.AppendRaw(frame)
}

// WriteTermVote persists monotonic term and vote choice before responding to RPCs.
func (w *WAL) WriteTermVote(term uint64, votedFor string) error {
	rec := TermVoteRecord{
		Term:     term,
		VotedFor: votedFor,
	}
	payload, err := rec.Encode()
	if err != nil {
		return fmt.Errorf("wal: encode term-vote: %w", err)
	}
	frame := BuildFrame(RecordTypeTermVote, payload)
	return w.AppendRaw(frame)
}

// WriteSnapshotMeta persists snapshot boundary checkpoints.
func (w *WAL) WriteSnapshotMeta(lastIndex uint64, lastTerm uint64, hash [32]byte) error {
	rec := SnapshotMetaRecord{
		LastIndex:      lastIndex,
		LastTerm:       lastTerm,
		CheckpointHash: hash,
	}
	payload, err := rec.Encode()
	if err != nil {
		return fmt.Errorf("wal: encode snapshot-meta: %w", err)
	}
	frame := BuildFrame(RecordTypeSnapshotMeta, payload)
	return w.AppendRaw(frame)
}

// Sync explicitly flushes in-memory buffers to physical media.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errors.New("wal: closed")
	}
	return w.file.Sync()
}

// Size returns the current physical byte length of the WAL.
func (w *WAL) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.offset
}

// Close safely syncs and closes the WAL file descriptor.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	_ = w.file.Sync()
	return w.file.Close()
}
