package raft

import (
	"sync"
)

// LogManager maintains the in-memory window of the replicated Raft log with snapshot compaction offset tracking.
type LogManager struct {
	mu                sync.RWMutex
	entries           []LogEntry
	lastIncludedIndex Index
	lastIncludedTerm  Term
}

// NewLogManager creates a new log manager initialized with dummy index 0, term 0.
func NewLogManager() *LogManager {
	return &LogManager{
		entries: []LogEntry{
			{Index: 0, Term: 0, Type: EntryNoop},
		},
		lastIncludedIndex: 0,
		lastIncludedTerm:  0,
	}
}

// LastIndex returns the absolute logical index of the last entry in the log.
func (lm *LogManager) LastIndex() Index {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	return lm.entries[len(lm.entries)-1].Index
}

// LastTerm returns the term of the last entry in the log.
func (lm *LogManager) LastTerm() Term {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	return lm.entries[len(lm.entries)-1].Term
}

// GetTerm returns the term for a specific logical index.
func (lm *LogManager) GetTerm(index Index) (Term, bool) {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	if index == lm.lastIncludedIndex {
		return lm.lastIncludedTerm, true
	}
	if index < lm.lastIncludedIndex {
		return 0, false
	}
	offset := int(index - lm.lastIncludedIndex)
	if offset < 0 || offset >= len(lm.entries) {
		return 0, false
	}
	return lm.entries[offset].Term, true
}

// GetEntry returns a clone of the entry at logical index.
func (lm *LogManager) GetEntry(index Index) (LogEntry, bool) {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	if index <= lm.lastIncludedIndex {
		return LogEntry{}, false
	}
	offset := int(index - lm.lastIncludedIndex)
	if offset < 0 || offset >= len(lm.entries) {
		return LogEntry{}, false
	}
	return lm.entries[offset].Clone(), true
}

// Append adds entries to the log.
func (lm *LogManager) Append(entries ...LogEntry) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	for _, e := range entries {
		lm.entries = append(lm.entries, e.Clone())
	}
}

// TruncateSuffix deletes entries starting from the specified index onwards.
func (lm *LogManager) TruncateSuffix(fromIndex Index) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if fromIndex <= lm.lastIncludedIndex {
		return
	}
	offset := int(fromIndex - lm.lastIncludedIndex)
	if offset < len(lm.entries) {
		lm.entries = lm.entries[:offset]
	}
}

// Slice returns a copy of entries between [from, to] logical indices.
func (lm *LogManager) Slice(from, to Index) []LogEntry {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	if from <= lm.lastIncludedIndex {
		from = lm.lastIncludedIndex + 1
	}
	if from > to || len(lm.entries) == 0 {
		return nil
	}
	fromOffset := int(from - lm.lastIncludedIndex)
	toOffset := int(to - lm.lastIncludedIndex) + 1

	if fromOffset >= len(lm.entries) {
		return nil
	}
	if toOffset > len(lm.entries) {
		toOffset = len(lm.entries)
	}
	if fromOffset >= toOffset {
		return nil
	}

	result := make([]LogEntry, toOffset-fromOffset)
	for i := fromOffset; i < toOffset; i++ {
		result[i-fromOffset] = lm.entries[i].Clone()
	}
	return result
}

// FirstIndexOfTerm scans from lastIncludedIndex upwards to find the first entry matching term.
func (lm *LogManager) FirstIndexOfTerm(term Term) Index {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	for _, e := range lm.entries {
		if e.Term == term {
			return e.Index
		}
	}
	return lm.lastIncludedIndex + 1
}

// LastIndexOfTerm scans backwards to find the last entry with the given term.
func (lm *LogManager) LastIndexOfTerm(term Term) (Index, bool) {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	for i := len(lm.entries) - 1; i >= 0; i-- {
		if lm.entries[i].Term == term {
			return lm.entries[i].Index, true
		}
	}
	return 0, false
}

// Compact discards entries up to and including lastIndex, resetting the log baseline.
func (lm *LogManager) Compact(lastIndex Index, lastTerm Term) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if lastIndex <= lm.lastIncludedIndex {
		return
	}

	offset := int(lastIndex - lm.lastIncludedIndex)
	var remaining []LogEntry
	if offset < len(lm.entries) {
		remaining = lm.entries[offset+1:]
	}

	newEntries := make([]LogEntry, 0, len(remaining)+1)
	// Base dummy entry representing the snapshot point
	newEntries = append(newEntries, LogEntry{
		Index: lastIndex,
		Term:  lastTerm,
		Type:  EntryNoop,
	})
	for _, e := range remaining {
		newEntries = append(newEntries, e.Clone())
	}

	lm.entries = newEntries
	lm.lastIncludedIndex = lastIndex
	lm.lastIncludedTerm = lastTerm
}

func (lm *LogManager) LastIncludedIndex() Index {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	return lm.lastIncludedIndex
}

func (lm *LogManager) LastIncludedTerm() Term {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	return lm.lastIncludedTerm
}
