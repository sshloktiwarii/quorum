package statemachine

import (
	"encoding/json"
	"sync"
)

// KVStore implements StateMachine as an in-memory transactional key-value store with client session deduplication.
type KVStore struct {
	mu          sync.RWMutex
	data        map[string]string
	lastApplied uint64

	// Exactly-once client session cache
	sessions map[string]clientSession
}

type clientSession struct {
	LastSeq uint64
	Result  ApplyResult
}

// NewKVStore creates an empty initialized KVStore.
func NewKVStore() *KVStore {
	return &KVStore{
		data:     make(map[string]string),
		sessions: make(map[string]clientSession),
	}
}

// Apply executes a command payload monotonically against the store.
func (kv *KVStore) Apply(cmdBytes []byte, index uint64) ApplyResult {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var cmd Command
	if err := json.Unmarshal(cmdBytes, &cmd); err != nil {
		return ApplyResult{
			Success:      false,
			Error:        ErrInvalidFormat.Error(),
			AppliedIndex: index,
		}
	}

	// Exactly-once deduplication check
	if cmd.ClientID != "" && cmd.SequenceNum > 0 {
		if sess, exists := kv.sessions[cmd.ClientID]; exists {
			if cmd.SequenceNum <= sess.LastSeq {
				// Duplicate command: return cached historical result
				res := sess.Result
				res.AppliedIndex = index
				return res
			}
		}
	}

	var res ApplyResult
	res.AppliedIndex = index

	switch cmd.Op {
	case OpPut:
		oldVal, hadOld := kv.data[cmd.Key]
		kv.data[cmd.Key] = cmd.Value
		res.Success = true
		res.Value = cmd.Value
		if hadOld {
			res.PreviousValue = oldVal
		}

	case OpGet:
		val, exists := kv.data[cmd.Key]
		if exists {
			res.Success = true
			res.Value = val
		} else {
			res.Success = false
			res.Error = ErrKeyNotFound.Error()
		}

	case OpDel:
		oldVal, exists := kv.data[cmd.Key]
		if exists {
			delete(kv.data, cmd.Key)
			res.Success = true
			res.PreviousValue = oldVal
		} else {
			res.Success = false
			res.Error = ErrKeyNotFound.Error()
		}

	case OpCAS:
		currentVal, exists := kv.data[cmd.Key]
		if (!exists && cmd.ExpectedValue == "") || (exists && currentVal == cmd.ExpectedValue) {
			kv.data[cmd.Key] = cmd.Value
			res.Success = true
			res.Value = cmd.Value
			res.PreviousValue = currentVal
		} else {
			res.Success = false
			res.Error = ErrCASMismatch.Error()
			res.PreviousValue = currentVal
		}

	default:
		res.Success = false
		res.Error = ErrUnknownOp.Error()
	}

	if index > kv.lastApplied {
		kv.lastApplied = index
	}

	// Cache client session result
	if cmd.ClientID != "" && cmd.SequenceNum > 0 {
		kv.sessions[cmd.ClientID] = clientSession{
			LastSeq: cmd.SequenceNum,
			Result:  res,
		}
	}

	return res
}

// Query performs a direct read without log mutation.
func (kv *KVStore) Query(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	val, exists := kv.data[key]
	return val, exists
}

// LastApplied returns the highest index applied.
func (kv *KVStore) LastApplied() uint64 {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return kv.lastApplied
}

// GetAll returns a snapshot of all active key-value pairs for visualizer telemetry.
func (kv *KVStore) GetAll() map[string]string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	copied := make(map[string]string, len(kv.data))
	for k, v := range kv.data {
		copied[k] = v
	}
	return copied
}
