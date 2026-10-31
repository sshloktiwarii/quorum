package raft

import (
	"context"

	"systems-suite/quorum/pkg/snapshot"
)

// HandleInstallSnapshot processes incoming snapshot installation requests from cluster leader (§7).
func (rn *RaftNode) HandleInstallSnapshot(args *InstallSnapshotArgs) *InstallSnapshotReply {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	reply := &InstallSnapshotReply{
		Term: rn.currentTerm,
	}

	// Rule 1: Reply immediately if term < currentTerm
	if args.Term < rn.currentTerm {
		return reply
	}

	// Rule 2: If term > currentTerm, step down to follower
	if args.Term > rn.currentTerm {
		rn.becomeFollower(args.Term, args.LeaderID)
		reply.Term = rn.currentTerm
	} else {
		rn.leaderID = args.LeaderID
		rn.resetElectionTimeout()
	}

	// Rule 3: Discard stale snapshot if current commit index is already ahead
	if args.LastIncludedIndex <= rn.commitIndex {
		return reply
	}

	// Rule 4: Restore state machine state from snapshot
	if rn.stateMachine != nil && len(args.Data) > 0 {
		_ = rn.stateMachine.Restore(args.Data)
	}

	// Rule 5: Save snapshot file to disk if data dir is configured
	if rn.config.DataDir != "" {
		_ = snapshot.Save(rn.config.DataDir, uint64(args.LastIncludedIndex), uint64(args.LastIncludedTerm), args.Data)
	}

	// Rule 6: Discard log entries preceding snapshot boundary
	rn.log.Compact(args.LastIncludedIndex, args.LastIncludedTerm)
	rn.commitIndex = args.LastIncludedIndex
	rn.lastApplied = args.LastIncludedIndex

	// Notify application of snapshot installation
	select {
	case rn.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}:
	default:
	}

	return reply
}

// sendInstallSnapshot transmits an atomic snapshot to a lagging peer.
func (rn *RaftNode) sendInstallSnapshot(peer NodeID) {
	rn.mu.Lock()
	if rn.role != RoleLeader || rn.closed {
		rn.mu.Unlock()
		return
	}

	lastIncludedIdx := rn.log.LastIncludedIndex()
	lastIncludedTerm := rn.log.LastIncludedTerm()
	term := rn.currentTerm
	leaderID := rn.config.ID

	var snapData []byte
	var err error
	if rn.stateMachine != nil {
		snapData, err = rn.stateMachine.Snapshot()
		if err != nil {
			rn.mu.Unlock()
			return
		}
	}
	rn.mu.Unlock()

	args := &InstallSnapshotArgs{
		Term:              term,
		LeaderID:          leaderID,
		LastIncludedIndex: lastIncludedIdx,
		LastIncludedTerm:  lastIncludedTerm,
		Offset:            0,
		Data:              snapData,
		Done:              true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), rn.config.HeartbeatInterval*4)
	defer cancel()

	reply, err := rn.transport.SendInstallSnapshot(ctx, peer, args)
	if err != nil {
		return
	}

	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.role != RoleLeader || args.Term != rn.currentTerm {
		return
	}

	if reply.Term > rn.currentTerm {
		rn.becomeFollower(reply.Term, "")
		return
	}

	// Follower caught up to snapshot boundary
	if lastIncludedIdx > rn.matchIndex[peer] {
		rn.matchIndex[peer] = lastIncludedIdx
	}
	rn.nextIndex[peer] = rn.matchIndex[peer] + 1
}
