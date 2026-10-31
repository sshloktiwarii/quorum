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

	// Rule 4: Suffix Truncation and Log Append (§5.3)
	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it.
	insertIndex := args.PrevLogIndex + 1
	for i, newEntry := range args.Entries {
		entryIdx := insertIndex + Index(i)
		existingTerm, exists := rn.log.GetTerm(entryIdx)
		if exists {
			if existingTerm != newEntry.Term {
				// Conflict: truncate existing suffix
				rn.log.TruncateSuffix(entryIdx)
				// Append this and all remaining new entries
				for _, e := range args.Entries[i:] {
					rn.log.Append(e)
					if rn.wal != nil {
						_ = rn.wal.WriteEntry(uint64(e.Index), uint64(e.Term), uint8(e.Type), e.Data)
					}
				}
				break
			}
			// Identical entry already in log: continue
		} else {
			// Suffix reached: append remaining entries
			for _, e := range args.Entries[i:] {
				rn.log.Append(e)
				if rn.wal != nil {
					_ = rn.wal.WriteEntry(uint64(e.Index), uint64(e.Term), uint8(e.Type), e.Data)
				}
			}
			break
		}
	}

	// Rule 5: Commit Index Progression (§5.3)
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rn.commitIndex {
		lastNewIndex := rn.log.LastIndex()
		if args.LeaderCommit < lastNewIndex {
			rn.commitIndex = args.LeaderCommit
		} else {
			rn.commitIndex = lastNewIndex
		}
		rn.applyCond.Broadcast()
	}

	reply.Success = true
	return reply
}
