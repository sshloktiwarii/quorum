package raft

import (
	"context"
)

// startElection triggers election procedure (§5.2). Must be called with rn.mu locked.
func (rn *RaftNode) startElection() {
	rn.becomeCandidate()

	votesGranted := 1
	term := rn.currentTerm
	lastLogIndex := rn.log.LastIndex()
	lastLogTerm := rn.log.LastTerm()

	// If single node cluster, immediately achieve majority
	if votesGranted >= rn.config.QuorumSize() {
		rn.becomeLeader()
		return
	}

	args := &RequestVoteArgs{
		Term:         term,
		CandidateID:  rn.config.ID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	// Dispatch to peers (implemented in subsequent commit)
	rn.dispatchRequestVotes(args, &votesGranted)
}

func (rn *RaftNode) dispatchRequestVotes(args *RequestVoteArgs, votesGranted *int) {
	if rn.transport == nil {
		return
	}
	for _, peer := range rn.config.Peers {
		peerID := peer
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), rn.config.HeartbeatInterval*2)
			defer cancel()

			reply, err := rn.transport.SendRequestVote(ctx, peerID, args)
			if err != nil {
				return
			}

			rn.handleRequestVoteReply(peerID, args, reply)
		}()
	}
}

func (rn *RaftNode) handleRequestVoteReply(peer NodeID, args *RequestVoteArgs, reply *RequestVoteReply) {
	// Implemented in subsequent commit
}

// becomeLeader converts a candidate to leader upon receiving majority votes. Must be called with rn.mu held.
func (rn *RaftNode) becomeLeader() {
	rn.role = RoleLeader
	rn.leaderID = rn.config.ID

	// Initialize leader volatile state (§5.3)
	lastIdx := rn.log.LastIndex()
	for _, peer := range rn.config.Peers {
		rn.nextIndex[peer] = lastIdx + 1
		rn.matchIndex[peer] = 0
	}

	// Stop election timer on leader
	StopTimer(rn.electionTimer)

	// Append empty Noop entry for term (§5.4.2)
	noop := LogEntry{
		Index: lastIdx + 1,
		Term:  rn.currentTerm,
		Type:  EntryNoop,
	}
	rn.log.Append(noop)
	if rn.wal != nil {
		_ = rn.wal.WriteEntry(uint64(noop.Index), uint64(noop.Term), uint8(noop.Type), nil)
	}

	// Immediately broadcast initial heartbeats
	rn.broadcastHeartbeats()
}
