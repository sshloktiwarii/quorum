package raft

import (
	"sync"
	"time"

	"systems-suite/quorum/pkg/statemachine"
	"systems-suite/quorum/pkg/wal"
)

// ApplyMsg is delivered to the client application when log entries are committed.
type ApplyMsg struct {
	CommandValid bool
	Command      []byte
	CommandIndex Index
	CommandTerm  Term

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  Term
	SnapshotIndex Index
}

// RaftNode represents a fully functioning Ongaro-Ousterhout consensus node.
type RaftNode struct {
	mu        sync.RWMutex
	config    *Config
	transport Transport

	// Persistent state
	currentTerm Term
	votedFor    NodeID
	log         *LogManager
	wal         *wal.WAL

	// Volatile state on all servers
	role        NodeRole
	leaderID    NodeID
	commitIndex Index
	lastApplied Index

	// Volatile state on candidates
	votesGranted map[NodeID]bool

	// Volatile state on leaders
	nextIndex  map[NodeID]Index
	matchIndex map[NodeID]Index

	// State Machine & Application
	stateMachine statemachine.StateMachine
	applyCh      chan ApplyMsg
	applyCond    *sync.Cond

	// Timers
	electionTimer  *time.Timer
	heartbeatTimer *time.Ticker

	// Lifecycle
	stopCh chan struct{}
	doneWg sync.WaitGroup
	closed bool
}

// NewRaftNode constructs a RaftNode in the Follower state.
func NewRaftNode(cfg *Config, transport Transport, sm statemachine.StateMachine, w *wal.WAL) (*RaftNode, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	rn := &RaftNode{
		config:         cfg,
		transport:      transport,
		currentTerm:    0,
		votedFor:       "",
		log:            NewLogManager(),
		wal:            w,
		role:           RoleFollower,
		leaderID:       "",
		commitIndex:    0,
		lastApplied:    0,
		votesGranted:   make(map[NodeID]bool),
		nextIndex:      make(map[NodeID]Index),
		matchIndex:     make(map[NodeID]Index),
		stateMachine:   sm,
		applyCh:        make(chan ApplyMsg, 1024),
		electionTimer:  time.NewTimer(RandomElectionDuration(cfg.MinElectionTimeout, cfg.MaxElectionTimeout)),
		heartbeatTimer: time.NewTicker(cfg.HeartbeatInterval),
		stopCh:         make(chan struct{}),
	}
	rn.applyCond = sync.NewCond(&rn.mu)

	return rn, nil
}

// Start launches the background event loop and state machine apply worker.
func (rn *RaftNode) Start() {
	rn.doneWg.Add(2)
	go rn.eventLoop()
	go rn.applyLoop()
}

// Stop gracefully shuts down timers, event loops, and flushes persistent state.
func (rn *RaftNode) Stop() {
	rn.mu.Lock()
	if rn.closed {
		rn.mu.Unlock()
		return
	}
	rn.closed = true
	close(rn.stopCh)
	StopTimer(rn.electionTimer)
	rn.heartbeatTimer.Stop()
	rn.applyCond.Broadcast()
	rn.mu.Unlock()

	rn.doneWg.Wait()

	rn.mu.Lock()
	if rn.wal != nil {
		_ = rn.wal.Close()
	}
	rn.mu.Unlock()
}

// resetElectionTimeout locks and arms the election timer with a randomized duration.
func (rn *RaftNode) resetElectionTimeout() {
	dur := RandomElectionDuration(rn.config.MinElectionTimeout, rn.config.MaxElectionTimeout)
	ResetTimer(rn.electionTimer, dur)
}

// becomeFollower transitions to follower in term T, resetting votedFor if term increased.
func (rn *RaftNode) becomeFollower(term Term, leader NodeID) {
	if term > rn.currentTerm {
		rn.currentTerm = term
		rn.votedFor = ""
		if rn.wal != nil {
			_ = rn.wal.WriteTermVote(uint64(rn.currentTerm), string(rn.votedFor))
		}
	}
	rn.role = RoleFollower
	rn.leaderID = leader
	rn.resetElectionTimeout()
}

// becomeCandidate increments term, votes for self, and transitions role to Candidate (§5.2).
func (rn *RaftNode) becomeCandidate() {
	rn.role = RoleCandidate
	rn.currentTerm++
	rn.votedFor = rn.config.ID
	rn.leaderID = ""
	if rn.wal != nil {
		_ = rn.wal.WriteTermVote(uint64(rn.currentTerm), string(rn.votedFor))
	}
	rn.resetElectionTimeout()
}

// handleElectionTimeout is invoked when electionTimer fires.
func (rn *RaftNode) handleElectionTimeout() {
	rn.mu.Lock()
	defer rn.mu.Unlock()

	if rn.role == RoleLeader {
		return
	}

	// Follower or Candidate timed out without hearing from a valid leader: transition to candidate and start election
	rn.startElection()
}

// eventLoop monitors election timeouts and heartbeat ticks.
func (rn *RaftNode) eventLoop() {
	defer rn.doneWg.Done()

	for {
		select {
		case <-rn.stopCh:
			return

		case <-rn.electionTimer.C:
			rn.handleElectionTimeout()

		case <-rn.heartbeatTimer.C:
			rn.mu.Lock()
			if rn.role == RoleLeader {
				rn.broadcastHeartbeats()
			}
			rn.mu.Unlock()
		}
	}
}



// applyLoop stub (will be expanded in Stage 5)
func (rn *RaftNode) applyLoop() {
	defer rn.doneWg.Done()
	for {
		rn.mu.Lock()
		for rn.commitIndex <= rn.lastApplied && !rn.closed {
			rn.applyCond.Wait()
		}
		if rn.closed {
			rn.mu.Unlock()
			return
		}
		rn.mu.Unlock()
	}
}

// GetState returns current term, role, and whether node is leader.
func (rn *RaftNode) GetState() (Term, bool) {
	rn.mu.RLock()
	defer rn.mu.RUnlock()
	return rn.currentTerm, rn.role == RoleLeader
}

// SnapshotState captures complete snapshot of node state for diagnostics and HUD.
func (rn *RaftNode) SnapshotState() NodeState {
	rn.mu.RLock()
	defer rn.mu.RUnlock()

	return NodeState{
		ID:           rn.config.ID,
		Role:         rn.role,
		RoleName:     rn.role.String(),
		CurrentTerm:  rn.currentTerm,
		VotedFor:     rn.votedFor,
		CommitIndex:  rn.commitIndex,
		LastApplied:  rn.lastApplied,
		LastLogTerm:  rn.log.LastTerm(),
		LastLogIndex: rn.log.LastIndex(),
		LeaderID:     rn.leaderID,
	}
}

func (rn *RaftNode) ID() NodeID {
	return rn.config.ID
}
