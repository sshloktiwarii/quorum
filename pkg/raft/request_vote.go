package raft

// HandleRequestVote processes incoming vote requests following formal Ongaro-Ousterhout rules (§5.2, §5.4).
func (rn *RaftNode) HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	reply := &RequestVoteReply{
		Term:        rn.currentTerm,
		VoteGranted: false,
	}

	// Rule 1: Reject if term is strictly less than current term
	if args.Term < rn.currentTerm {
		return reply
	}

	// Rule 2: If term is higher, step down to follower
	if args.Term > rn.currentTerm {
		rn.becomeFollower(args.Term, "")
		reply.Term = rn.currentTerm
	}

	// Rule 3: Check if already voted in current term
	canVote := rn.votedFor == "" || rn.votedFor == args.CandidateID

	// Rule 4: Election safety - Candidate log must be at least as up-to-date as receiver log (§5.4.1)
	lastLogTerm := rn.log.LastTerm()
	lastLogIndex := rn.log.LastIndex()
	isUpToDate := false
	if args.LastLogTerm > lastLogTerm {
		isUpToDate = true
	} else if args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex {
		isUpToDate = true
	}

	if canVote && isUpToDate {
		rn.votedFor = args.CandidateID
		if rn.wal != nil {
			_ = rn.wal.WriteTermVote(uint64(rn.currentTerm), string(rn.votedFor))
		}
		rn.resetElectionTimeout()
		reply.VoteGranted = true
	}

	return reply
}
