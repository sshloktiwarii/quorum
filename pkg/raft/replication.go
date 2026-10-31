package raft

import (
	"context"
	"sort"
)

// Propose submits a new command to the leader for replication across the cluster (§5.3).
func (rn *RaftNode) Propose(data []byte) (Index, Term, error) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.role != RoleLeader {
		return 0, 0, ErrNotLeader
	}
	if rn.closed {
		return 0, 0, ErrStopped
	}

	newIndex := rn.log.LastIndex() + 1
	term := rn.currentTerm

	entry := LogEntry{
		Index: newIndex,
		Term:  term,
		Type:  EntryData,
		Data:  data,
	}

	rn.log.Append(entry)
	if rn.wal != nil {
		_ = rn.wal.WriteEntry(uint64(entry.Index), uint64(entry.Term), uint8(entry.Type), entry.Data)
	}

	// Trigger immediate replication
	rn.broadcastAppendEntries()

	return newIndex, term, nil
}

// broadcastHeartbeats sends empty AppendEntries to prevent follower timeouts (§5.2).
func (rn *RaftNode) broadcastHeartbeats() {
	rn.broadcastAppendEntries()
}

// broadcastAppendEntries replicates log entries or sends heartbeats to all peers.
func (rn *RaftNode) broadcastAppendEntries() {
	if rn.transport == nil {
		return
	}

	for _, peer := range rn.config.Peers {
		peerID := peer
		go rn.replicateToPeer(peerID)
	}
}

// replicateToPeer coordinates log entry delivery to a specific follower.
func (rn *RaftNode) replicateToPeer(peer NodeID) {
	rn.mu.Lock()
	if rn.role != RoleLeader || rn.closed {
		rn.mu.Unlock()
		return
	}

	nextIdx := rn.nextIndex[peer]
	if nextIdx == 0 {
		nextIdx = 1
		rn.nextIndex[peer] = 1
	}

	prevLogIndex := nextIdx - 1
	prevLogTerm, ok := rn.log.GetTerm(prevLogIndex)
	if !ok {
		// Log has been compacted behind nextIdx: install snapshot to catch up lagging peer (§7)
		rn.mu.Unlock()
		go rn.sendInstallSnapshot(peer)
		return
	}

	lastLogIndex := rn.log.LastIndex()
	var entries []LogEntry
	if lastLogIndex >= nextIdx {
		toIndex := nextIdx + Index(rn.config.MaxEntriesPerMsg) - 1
		if toIndex > lastLogIndex {
			toIndex = lastLogIndex
		}
		entries = rn.log.Slice(nextIdx, toIndex)
	}

	args := &AppendEntriesArgs{
		Term:         rn.currentTerm,
		LeaderID:     rn.config.ID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rn.commitIndex,
	}
	rn.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), rn.config.HeartbeatInterval*2)
	defer cancel()

	reply, err := rn.transport.SendAppendEntries(ctx, peer, args)
	if err != nil {
		return
	}

	rn.handleAppendEntriesReply(peer, args, reply)
}

// handleAppendEntriesReply processes replication responses on the leader.
func (rn *RaftNode) handleAppendEntriesReply(peer NodeID, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.role != RoleLeader || args.Term != rn.currentTerm || reply.Term < rn.currentTerm {
		return
	}

	// Higher term discovered: step down immediately (§5.1)
	if reply.Term > rn.currentTerm {
		rn.becomeFollower(reply.Term, "")
		return
	}

	if !reply.Success {
		// Fast conflict recovery resolution
		if reply.ConflictTerm > 0 {
			if lastIdx, found := rn.log.LastIndexOfTerm(reply.ConflictTerm); found {
				rn.nextIndex[peer] = lastIdx + 1
			} else {
				rn.nextIndex[peer] = reply.ConflictIndex
			}
		} else if reply.ConflictIndex > 0 {
			rn.nextIndex[peer] = reply.ConflictIndex
		} else {
			if rn.nextIndex[peer] > 1 {
				rn.nextIndex[peer]--
			}
		}

		if rn.nextIndex[peer] < 1 {
			rn.nextIndex[peer] = 1
		}

		// Retry replication immediately with updated nextIndex
		go rn.replicateToPeer(peer)
		return
	}

	// Rule 5: Successful Replication (§5.3)
	matchIdx := args.PrevLogIndex + Index(len(args.Entries))
	if matchIdx > rn.matchIndex[peer] {
		rn.matchIndex[peer] = matchIdx
	}
	rn.nextIndex[peer] = rn.matchIndex[peer] + 1

	// Rule 6: Leader Commit Rule (§5.3, §5.4.2)
	// If there exists an N such that N > commitIndex, a majority of matchIndex[i] >= N,
	// and log[N].term == currentTerm: set commitIndex = N
	matchIndices := make([]Index, 0, len(rn.config.Peers)+1)
	matchIndices = append(matchIndices, rn.log.LastIndex()) // leader's own match index
	for _, p := range rn.config.Peers {
		matchIndices = append(matchIndices, rn.matchIndex[p])
	}
	// Sort ascending: median is at index (len - QuorumSize)
	sort.Slice(matchIndices, func(i, j int) bool {
		return matchIndices[i] < matchIndices[j]
	})

	medianIndex := matchIndices[len(matchIndices)-rn.config.QuorumSize()]
	if medianIndex > rn.commitIndex {
		termAtN, ok := rn.log.GetTerm(medianIndex)
		if ok && termAtN == rn.currentTerm {
			rn.commitIndex = medianIndex
			rn.applyCond.Broadcast()
		}
	}

	// If there are more entries remaining, continue replicating
	if rn.log.LastIndex() >= rn.nextIndex[peer] {
		go rn.replicateToPeer(peer)
	}
}
