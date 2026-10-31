package raft

// HandleAppendEntries processes log replication RPCs from cluster leaders (§5.2, §5.3).
func (rn *RaftNode) HandleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	reply := &AppendEntriesReply{
		Term:          rn.currentTerm,
		Success:       false,
		ConflictIndex: 0,
		ConflictTerm:  0,
	}

	// Rule 1: Reply false if term < currentTerm (§5.1)
	if args.Term < rn.currentTerm {
		return reply
	}

	// Rule 2: If RPC term > currentTerm, step down to follower
	if args.Term > rn.currentTerm {
		rn.becomeFollower(args.Term, args.LeaderID)
		reply.Term = rn.currentTerm
	} else if rn.role == RoleCandidate {
		// Valid leader discovered in same term
		rn.becomeFollower(args.Term, args.LeaderID)
	} else {
		rn.leaderID = args.LeaderID
		rn.resetElectionTimeout()
	}

	// Rule 3: Strict Log Matching Property
	// Reply false if log doesn't contain an entry at prevLogIndex matching prevLogTerm (§5.3)
	lastLogIndex := rn.log.LastIndex()
	if args.PrevLogIndex > lastLogIndex {
		// Follower is missing entries
		reply.ConflictIndex = lastLogIndex + 1
		reply.ConflictTerm = 0
		return reply
	}

	termAtPrev, ok := rn.log.GetTerm(args.PrevLogIndex)
	if !ok || termAtPrev != args.PrevLogTerm {
		// Log conflict at prevLogIndex: return ConflictTerm and first index of that term
		reply.ConflictTerm = termAtPrev
		reply.ConflictIndex = rn.log.FirstIndexOfTerm(termAtPrev)
		return reply
	}

	// Suffix truncation and log append will be completed in Commit 27
	reply.Success = true
	return reply
}
