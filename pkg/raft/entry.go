package raft

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

// EntryType differentiates application data entries from Raft control entries.
type EntryType uint8

const (
	// EntryData carries arbitrary user state machine command bytes.
	EntryData EntryType = iota

	// EntryNoop is an empty entry proposed by a newly elected leader to establish commitIndex in current term (Section 5.4.2).
	EntryNoop

	// EntryConfigChange represents a cluster membership reconfiguration payload.
	EntryConfigChange
)

// String representation of entry type.
func (et EntryType) String() string {
	switch et {
	case EntryData:
		return "Data"
	case EntryNoop:
		return "Noop"
	case EntryConfigChange:
		return "ConfigChange"
	default:
		return fmt.Sprintf("Unknown(%d)", uint8(et))
	}
}

// LogEntry represents an atomic transaction inside the Raft replicated log.
type LogEntry struct {
	Index Index     `json:"index"`
	Term  Term      `json:"term"`
	Type  EntryType `json:"type"`
	Data  []byte    `json:"data"`
}

// Clone creates a deep copy of the LogEntry to eliminate race conditions across goroutines.
func (e LogEntry) Clone() LogEntry {
	var c LogEntry
	c.Index = e.Index
	c.Term = e.Term
	c.Type = e.Type
	if len(e.Data) > 0 {
		c.Data = make([]byte, len(e.Data))
		copy(c.Data, e.Data)
	}
	return c
}

// EncodeBinary serializes the LogEntry into a fixed-layout byte slice.
// Layout: Index(8B) | Term(8B) | Type(1B) | DataLen(4B) | Data(NB)
func (e LogEntry) EncodeBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, uint64(e.Index)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, uint64(e.Term)); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, uint8(e.Type)); err != nil {
		return nil, err
	}
	dataLen := uint32(len(e.Data))
	if err := binary.Write(buf, binary.BigEndian, dataLen); err != nil {
		return nil, err
	}
	if dataLen > 0 {
		if _, err := buf.Write(e.Data); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

// DecodeLogEntry reconstructs a LogEntry from a binary encoded byte slice.
func DecodeLogEntry(raw []byte) (LogEntry, error) {
	var e LogEntry
	if len(raw) < 21 { // 8 + 8 + 1 + 4 = 21 bytes minimum header
		return e, errors.New("entry: insufficient bytes for header")
	}

	r := bytes.NewReader(raw)
	var rawIndex, rawTerm uint64
	var rawType uint8
	var dataLen uint32

	if err := binary.Read(r, binary.BigEndian, &rawIndex); err != nil {
		return e, err
	}
	if err := binary.Read(r, binary.BigEndian, &rawTerm); err != nil {
		return e, err
	}
	if err := binary.Read(r, binary.BigEndian, &rawType); err != nil {
		return e, err
	}
	if err := binary.Read(r, binary.BigEndian, &dataLen); err != nil {
		return e, err
	}

	e.Index = Index(rawIndex)
	e.Term = Term(rawTerm)
	e.Type = EntryType(rawType)

	if uint32(len(raw)-21) < dataLen {
		return e, errors.New("entry: truncated payload data")
	}

	if dataLen > 0 {
		e.Data = make([]byte, dataLen)
		if _, err := r.Read(e.Data); err != nil {
			return e, err
		}
	}
	return e, nil
}
