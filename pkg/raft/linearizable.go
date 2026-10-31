package raft

import (
	"context"
	"sync"
	"time"
)

// ReadIndex executes a linearizable read query without writing a log entry (§6.4).
func (rn *RaftNode) ReadIndex(ctx context.Context, key string) (string, error) {
	rn.mu.Lock()
	if rn.role != RoleLeader {
		leader := rn.leaderID
		rn.mu.Unlock()
		return "", fmtLeaderErr(leader)
	}

	readIndex := rn.commitIndex
	term := rn.currentTerm
	peers := make([]NodeID, len(rn.config.Peers))
	copy(peers, rn.config.Peers)
	rn.mu.Unlock()

	// If single node cluster, immediately satisfied
	if len(peers) == 0 {
		return rn.queryApplied(ctx, readIndex, key)
	}

	// Confirm leadership by broadcasting heartbeat round and gathering majority ack
	quorumReq := rn.config.QuorumSize()
	var acksMu sync.Mutex
	acks := 1 // self
	quorumReached := make(chan struct{}, 1)
	deposed := make(chan struct{}, 1)

	args := &AppendEntriesArgs{
		Term:         term,
		LeaderID:     rn.config.ID,
		PrevLogIndex: readIndex,
		PrevLogTerm:  term,
		Entries:      nil,
		LeaderCommit: readIndex,
	}

	for _, p := range peers {
		peerID := p
		go func() {
			reply, err := rn.transport.SendAppendEntries(ctx, peerID, args)
			if err != nil {
				return
			}
			if reply.Term > term {
				select {
				case deposed <- struct{}{}:
				default:
				}
				return
			}

			acksMu.Lock()
			acks++
			if acks >= quorumReq {
				select {
				case quorumReached <- struct{}{}:
				default:
				}
			}
			acksMu.Unlock()
		}()
	}

	select {
	case <-deposed:
		rn.mu.Lock()
		if rn.currentTerm <= term {
			rn.becomeFollower(term+1, "")
		}
		rn.mu.Unlock()
		return "", ErrLeaderStepDown

	case <-quorumReached:
		// Majority verified this leader is still active in term
		return rn.queryApplied(ctx, readIndex, key)

	case <-ctx.Done():
		return "", ctx.Err()

	case <-time.After(rn.config.HeartbeatInterval * 2):
		return "", ErrLinearizabilityFail
	}
}

// queryApplied waits until lastApplied >= readIndex then executes direct read on state machine.
func (rn *RaftNode) queryApplied(ctx context.Context, readIndex Index, key string) (string, error) {
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()

	for {
		rn.mu.RLock()
		applied := rn.lastApplied
		rn.mu.RUnlock()

		if applied >= readIndex {
			if rn.stateMachine == nil {
				return "", nil
			}
			val, _ := rn.stateMachine.Query(key)
			return val, nil
		}

		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-rn.stopCh:
			return "", ErrStopped
		case <-ticker.C:
		}
	}
}

func fmtLeaderErr(leader NodeID) error {
	if leader == "" {
		return ErrNotLeader
	}
	return ErrNotLeader
}
