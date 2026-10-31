package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

// CastagnoliTable is the hardware-accelerated polynomial (0x82F63B78) used in modern filesystems and protocols.
var CastagnoliTable = crc32.MakeTable(crc32.Castagnoli)

// ComputeCRC32 calculates the CRC32-Castagnoli checksum over:
// RecordType (1B) || Length (4B) || Payload (NB)
func ComputeCRC32(recType RecordType, payload []byte) uint32 {
	h := crc32.New(CastagnoliTable)
	var typeLen [5]byte
	typeLen[0] = byte(recType)
	binary.BigEndian.PutUint32(typeLen[1:5], uint32(len(payload)))
	_, _ = h.Write(typeLen[:])
	if len(payload) > 0 {
		_, _ = h.Write(payload)
	}
	return h.Sum32()
}

// VerifyFrameChecksum computes the expected CRC32 and compares against the header.
func VerifyFrameChecksum(hdr FrameHeader, payload []byte) error {
	expected := ComputeCRC32(hdr.RecordType, payload)
	if expected != hdr.CRC32 {
		return fmt.Errorf("%w: record %s len %d: computed 0x%08X != stored 0x%08X",
			ErrChecksumMismatch, hdr.RecordType, hdr.Length, expected, hdr.CRC32)
	}
	return nil
}

// BuildFrame serializes a full record (header + payload) ready for atomic disk append.
func BuildFrame(recType RecordType, payload []byte) []byte {
	checksum := ComputeCRC32(recType, payload)
	hdr := FrameHeader{
		Magic:      MagicBytes,
		RecordType: recType,
		Length:     uint32(len(payload)),
		CRC32:      checksum,
	}
	frame := make([]byte, HeaderSize+len(payload))
	copy(frame[0:HeaderSize], hdr.Encode())
	if len(payload) > 0 {
		copy(frame[HeaderSize:], payload)
	}
	return frame
}
