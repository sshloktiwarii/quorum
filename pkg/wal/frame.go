package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

// Magic header identifying a Quorum WAL file (0x51 0x55 0x4F 0x52 = "QUOR")
var MagicBytes = [4]byte{'Q', 'U', 'O', 'R'}

const (
	// HeaderSize: Magic(4B) + RecordType(1B) + Length(4B) + CRC32(4B) = 13 bytes
	HeaderSize = 13

	// MaxPayloadSize: 64MB protection against corrupted length allocations
	MaxPayloadSize = 64 * 1024 * 1024
)

var (
	ErrCorruptFrame = errors.New("wal: invalid magic or frame header")
	ErrLengthExceeded = errors.New("wal: frame payload length exceeds maximum safe bound")
	ErrChecksumMismatch = errors.New("wal: CRC32 checksum mismatch (bitrot or torn write)")
)

// FrameHeader represents the 13-byte record descriptor.
type FrameHeader struct {
	Magic      [4]byte
	RecordType RecordType
	Length     uint32
	CRC32      uint32
}

// EncodeHeader serializes the frame header into a 13-byte buffer.
func (h FrameHeader) Encode() []byte {
	buf := make([]byte, HeaderSize)
	copy(buf[0:4], h.Magic[:])
	buf[4] = byte(h.RecordType)
	binary.BigEndian.PutUint32(buf[5:9], h.Length)
	binary.BigEndian.PutUint32(buf[9:13], h.CRC32)
	return buf
}

// DecodeHeader deserializes and validates magic bytes from a 13-byte buffer.
func DecodeHeader(raw []byte) (FrameHeader, error) {
	var h FrameHeader
	if len(raw) < HeaderSize {
		return h, errors.New("wal: buffer too short for frame header")
	}
	copy(h.Magic[:], raw[0:4])
	if !bytes.Equal(h.Magic[:], MagicBytes[:]) {
		return h, fmt.Errorf("%w: expected %s, got %s", ErrCorruptFrame, string(MagicBytes[:]), string(h.Magic[:]))
	}
	h.RecordType = RecordType(raw[4])
	h.Length = binary.BigEndian.Uint32(raw[5:9])
	if h.Length > MaxPayloadSize {
		return h, fmt.Errorf("%w: length %d", ErrLengthExceeded, h.Length)
	}
	h.CRC32 = binary.BigEndian.Uint32(raw[9:13])
	return h, nil
}
