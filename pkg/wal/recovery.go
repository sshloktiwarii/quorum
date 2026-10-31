package wal

import (
	"fmt"
	"os"
	"path/filepath"
)

// RecoverAndOpen replays the WAL, truncates any bitrot or torn-write trailing corruption,
// and returns an active WAL ready for append along with the recovered consensus state.
func RecoverAndOpen(dir string, syncMode bool) (*WAL, *RecoveredState, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, nil, fmt.Errorf("wal recovery: mkdir %s: %w", dir, err)
	}

	filePath := filepath.Join(dir, WALFileName)
	state, err := Scan(filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("wal recovery: scan failed: %w", err)
	}

	if state.Corrupted {
		// Truncate cleanly at last valid offset to recover from torn write or bitrot tail
		if err := truncateFile(filePath, state.ValidBytes); err != nil {
			return nil, nil, fmt.Errorf("wal recovery: truncate torn write at offset %d: %w", state.ValidBytes, err)
		}
	}

	wal, err := Open(dir, syncMode)
	if err != nil {
		return nil, nil, fmt.Errorf("wal recovery: open failed: %w", err)
	}

	return wal, state, nil
}

// truncateFile truncates a file to the exact byte offset specified.
func truncateFile(path string, offset int64) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	if err := f.Truncate(offset); err != nil {
		return err
	}
	return f.Sync()
}

// TruncateAt drops all entries after the given byte offset.
func (w *WAL) TruncateAt(offset int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return os.ErrClosed
	}

	if err := w.file.Truncate(offset); err != nil {
		return fmt.Errorf("wal: truncate at %d: %w", offset, err)
	}
	w.offset = offset
	return w.file.Sync()
}
