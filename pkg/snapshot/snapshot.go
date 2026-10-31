package snapshot

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
)

const (
	SnapshotFileName = "snapshot.bin"
	SnapshotTmpName  = "snapshot.bin.tmp"
	SnapshotMagic    = 0x51534E50 // "QSNP"
	HeaderSize       = 24         // Magic(4) + LastIndex(8) + LastTerm(8) + CRC32(4)
)

var (
	ErrCorruptSnapshot = errors.New("snapshot: corrupt header or checksum mismatch")
	ErrNotFound        = errors.New("snapshot: file not found")
)

// Metadata tracks the state machine snapshot boundary.
type Metadata struct {
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	CRC32             uint32
}

// Save writes snapshot data atomically using temp file write + fsync + atomic rename.
func Save(dir string, lastIndex, lastTerm uint64, data []byte) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("snapshot: mkdir %s: %w", dir, err)
	}

	checksum := crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))

	header := make([]byte, HeaderSize)
	binary.BigEndian.PutUint32(header[0:4], SnapshotMagic)
	binary.BigEndian.PutUint64(header[4:12], lastIndex)
	binary.BigEndian.PutUint64(header[12:20], lastTerm)
	binary.BigEndian.PutUint32(header[20:24], checksum)

	tmpPath := filepath.Join(dir, SnapshotTmpName)
	finalPath := filepath.Join(dir, SnapshotFileName)

	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("snapshot: open tmp file: %w", err)
	}

	if _, err := f.Write(header); err != nil {
		_ = f.Close()
		return fmt.Errorf("snapshot: write header: %w", err)
	}
	if len(data) > 0 {
		if _, err := f.Write(data); err != nil {
			_ = f.Close()
			return fmt.Errorf("snapshot: write payload: %w", err)
		}
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("snapshot: fsync: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("snapshot: close: %w", err)
	}

	// Atomic rename ensures readers never see a partial snapshot
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return fmt.Errorf("snapshot: atomic rename: %w", err)
	}

	return nil
}

// Load reads and validates an existing snapshot from disk.
func Load(dir string) (Metadata, []byte, error) {
	var meta Metadata
	finalPath := filepath.Join(dir, SnapshotFileName)

	raw, err := os.ReadFile(finalPath)
	if err != nil {
		if os.IsNotExist(err) {
			return meta, nil, ErrNotFound
		}
		return meta, nil, fmt.Errorf("snapshot: read file: %w", err)
	}

	if len(raw) < HeaderSize {
		return meta, nil, ErrCorruptSnapshot
	}

	magic := binary.BigEndian.Uint32(raw[0:4])
	if magic != SnapshotMagic {
		return meta, nil, fmt.Errorf("%w: invalid magic 0x%X", ErrCorruptSnapshot, magic)
	}

	meta.LastIncludedIndex = binary.BigEndian.Uint64(raw[4:12])
	meta.LastIncludedTerm = binary.BigEndian.Uint64(raw[12:20])
	meta.CRC32 = binary.BigEndian.Uint32(raw[20:24])

	payload := raw[HeaderSize:]
	computedChecksum := crc32.Checksum(payload, crc32.MakeTable(crc32.Castagnoli))
	if computedChecksum != meta.CRC32 {
		return meta, nil, fmt.Errorf("%w: expected 0x%08X, got 0x%08X", ErrCorruptSnapshot, meta.CRC32, computedChecksum)
	}

	data := make([]byte, len(payload))
	copy(data, payload)

	return meta, data, nil
}
