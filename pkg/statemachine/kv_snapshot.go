package statemachine

import (
	"encoding/json"
	"fmt"
)

// SnapshotData encapsulates the full state of the KVStore for point-in-time recovery.
type SnapshotData struct {
	Data        map[string]string        `json:"data"`
	LastApplied uint64                   `json:"last_applied"`
	Sessions    map[string]clientSession `json:"sessions"`
}

// Snapshot serializes the current KV state and client sessions into a JSON buffer.
func (kv *KVStore) Snapshot() ([]byte, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	snap := SnapshotData{
		Data:        make(map[string]string, len(kv.data)),
		LastApplied: kv.lastApplied,
		Sessions:    make(map[string]clientSession, len(kv.sessions)),
	}

	for k, v := range kv.data {
		snap.Data[k] = v
	}
	for clientID, sess := range kv.sessions {
		snap.Sessions[clientID] = sess
	}

	raw, err := json.Marshal(snap)
	if err != nil {
		return nil, fmt.Errorf("statemachine snapshot: marshal failed: %w", err)
	}
	return raw, nil
}

// Restore resets the in-memory store and replaces all state with the snapshot contents.
func (kv *KVStore) Restore(snapshot []byte) error {
	var snap SnapshotData
	if err := json.Unmarshal(snapshot, &snap); err != nil {
		return fmt.Errorf("statemachine restore: unmarshal failed: %w", err)
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.data = make(map[string]string, len(snap.Data))
	for k, v := range snap.Data {
		kv.data[k] = v
	}

	kv.sessions = make(map[string]clientSession, len(snap.Sessions))
	for clientID, sess := range snap.Sessions {
		kv.sessions[clientID] = sess
	}

	kv.lastApplied = snap.LastApplied
	return nil
}
