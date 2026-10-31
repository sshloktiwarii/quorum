package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

// RecordType identifies the category of WAL payload.
type RecordType uint8

const (
	// RecordTypeEntry stores an individual replicated Raft log entry.
	RecordTypeEntry RecordType = 0x01

	// RecordTypeTermVote persists currentTerm and votedFor atomically before responding to RPCs.
	RecordTypeTermVote RecordType = 0x02

	// RecordTypeSnapshotMeta persists snapshot pointers and checkpoint hashes.
	RecordTypeSnapshotMeta RecordType = 0x03
)

func (rt RecordType) String() string {
	switch rt {
	case RecordTypeEntry:
		return "RecordTypeEntry"
	case RecordTypeTermVote:
		return "RecordTypeTermVote"
	case RecordTypeSnapshotMeta:
		return "RecordTypeSnapshotMeta"
	default:
		return fmt.Sprintf("RecordTypeUnknown(0x%02x)", uint8(rt))
	}
}

// EntryRecord payload layout:
// Index (8B) | Term (8B) | Type (1B) | DataLen (4B) | Data (NB)
type EntryRecord struct {
	Index uint64
	Term  uint64
	Type  uint8
	Data  []byte
}

func (r EntryRecord) Encode() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, r.Index); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, r.Term); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, r.Type); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, uint32(len(r.Data))); err != nil {
		return nil, err
	}
	if len(r.Data) > 0 {
		if _, err := buf.Write(r.Data); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func DecodeEntryRecord(raw []byte) (EntryRecord, error) {
	var r EntryRecord
	if len(raw) < 21 {
		return r, errors.New("wal: entry record buffer underflow")
	}
	reader := bytes.NewReader(raw)
	if err := binary.Read(reader, binary.BigEndian, &r.Index); err != nil {
		return r, err
	}
	if err := binary.Read(reader, binary.BigEndian, &r.Term); err != nil {
		return r, err
	}
	if err := binary.Read(reader, binary.BigEndian, &r.Type); err != nil {
		return r, err
	}
	var dataLen uint32
	if err := binary.Read(reader, binary.BigEndian, &dataLen); err != nil {
		return r, err
	}
	if uint32(len(raw)-21) < dataLen {
		return r, errors.New("wal: entry record payload truncated")
	}
	if dataLen > 0 {
		r.Data = make([]byte, dataLen)
		if _, err := reader.Read(r.Data); err != nil {
			return r, err
		}
	}
	return r, nil
}

// TermVoteRecord payload layout:
// Term (8B) | VotedForLen (2B) | VotedFor (NB)
type TermVoteRecord struct {
	Term     uint64
	VotedFor string
}

func (r TermVoteRecord) Encode() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, r.Term); err != nil {
		return nil, err
	}
	votedForBytes := []byte(r.VotedFor)
	if err := binary.Write(buf, binary.BigEndian, uint16(len(votedForBytes))); err != nil {
		return nil, err
	}
	if len(votedForBytes) > 0 {
		if _, err := buf.Write(votedForBytes); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func DecodeTermVoteRecord(raw []byte) (TermVoteRecord, error) {
	var r TermVoteRecord
	if len(raw) < 10 {
		return r, errors.New("wal: term vote record buffer underflow")
	}
	reader := bytes.NewReader(raw)
	if err := binary.Read(reader, binary.BigEndian, &r.Term); err != nil {
		return r, err
	}
	var nameLen uint16
	if err := binary.Read(reader, binary.BigEndian, &nameLen); err != nil {
		return r, err
	}
	if uint16(len(raw)-10) < nameLen {
		return r, errors.New("wal: term vote record candidate string truncated")
	}
	if nameLen > 0 {
		buf := make([]byte, nameLen)
		if _, err := reader.Read(buf); err != nil {
			return r, err
		}
		r.VotedFor = string(buf)
	}
	return r, nil
}

// SnapshotMetaRecord payload layout:
// LastIndex (8B) | LastTerm (8B) | CheckpointHash (32B)
type SnapshotMetaRecord struct {
	LastIndex      uint64
	LastTerm       uint64
	CheckpointHash [32]byte
}

func (r SnapshotMetaRecord) Encode() ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, r.LastIndex); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.BigEndian, r.LastTerm); err != nil {
		return nil, err
	}
	if _, err := buf.Write(r.CheckpointHash[:]); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func DecodeSnapshotMetaRecord(raw []byte) (SnapshotMetaRecord, error) {
	var r SnapshotMetaRecord
	if len(raw) < 48 {
		return r, errors.New("wal: snapshot meta record buffer underflow")
	}
	reader := bytes.NewReader(raw)
	if err := binary.Read(reader, binary.BigEndian, &r.LastIndex); err != nil {
		return r, err
	}
	if err := binary.Read(reader, binary.BigEndian, &r.LastTerm); err != nil {
		return r, err
	}
	if _, err := reader.Read(r.CheckpointHash[:]); err != nil {
		return r, err
	}
	return r, nil
}
