package raft

import (
	"context"
)

// startElection triggers election procedure (§5.2). Must be called with rn.mu locked.
func (rn *RaftNode) startElection() {
	rn.becomeCandidate()
	rn.votesGranted = make(map[NodeID]bool)

	term := rn.currentTerm
	lastLogIndex := rn.log.LastIndex()
	lastLogTerm := rn.log.LastTerm()

	// If single node cluster (self is majority)
	if 1 >= rn.config.QuorumSize() {
		rn.becomeLeader()
		return
	}

	args := &RequestVoteArgs{
		Term:         term,
		CandidateID:  rn.config.ID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	// Dispatch to peers
	rn.dispatchRequestVotes(args)
}

func (rn *RaftNode) dispatchRequestVotes(args *RequestVoteArgs) {
	if rn.transport == nil {
		return
	}
	for _, peer := range rn.config.Peers {
		peerID := peer
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), rn.config.MaxElectionTimeout*2)
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
	rn.mu.Lock()
	defer rn.mu.Unlock()

	// Stale response or already transitioned
	if rn.role != RoleCandidate || args.Term != rn.currentTerm || reply.Term < rn.currentTerm {
		return
	}

	// Higher term seen: step down immediately (§5.1)
	if reply.Term > rn.currentTerm {
		rn.becomeFollower(reply.Term, "")
		return
	}

	if reply.VoteGranted {
		rn.votesGranted[peer] = true
		totalVotes := 1 + len(rn.votesGranted)
		if totalVotes >= rn.config.QuorumSize() {
			rn.becomeLeader()
		}
	}
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

	if rn.config.QuorumSize() <= 1 {
		rn.commitIndex = noop.Index
		rn.applyCond.Broadcast()
	} else {
		// Immediately broadcast initial heartbeats
		rn.broadcastHeartbeats()
	}
}
