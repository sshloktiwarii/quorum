package simulation

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	"systems-suite/quorum/pkg/raft"
)

var (
	ErrNetworkDrop     = errors.New("sim net: packet dropped by fault injector")
	ErrNodeUnreachable = errors.New("sim net: destination partitioned or unreachable")
)

// NetworkConfig configures physical link simulation parameters.
type NetworkConfig struct {
	MinLatency time.Duration
	MaxLatency time.Duration
	DropRate   float64
	DupRate    float64
	Seed       int64
}

// Endpoint represents a node's local receiver callbacks.
type Endpoint struct {
	NodeID          raft.NodeID
	OnRequestVote   func(args *raft.RequestVoteArgs) *raft.RequestVoteReply
	OnAppendEntries func(args *raft.AppendEntriesArgs) *raft.AppendEntriesReply
	OnSnapshot      func(args *raft.InstallSnapshotArgs) *raft.InstallSnapshotReply
}

// SimulatedNetwork models a multi-node packet-switched mesh on the virtual clock.
type SimulatedNetwork struct {
	mu        sync.Mutex
	sched     *Scheduler
	config    NetworkConfig
	rng       *rand.Rand
	endpoints map[raft.NodeID]*Endpoint
}

// NewSimulatedNetwork creates an initialized mesh driven by the given virtual clock scheduler.
func NewSimulatedNetwork(sched *Scheduler, cfg NetworkConfig) *SimulatedNetwork {
	if cfg.Seed == 0 {
		cfg.Seed = 42
	}
	if cfg.MinLatency <= 0 {
		cfg.MinLatency = 2 * time.Millisecond
	}
	if cfg.MaxLatency < cfg.MinLatency {
		cfg.MaxLatency = cfg.MinLatency + 5*time.Millisecond
	}

	return &SimulatedNetwork{
		sched:     sched,
		config:    cfg,
		rng:       rand.New(rand.NewSource(cfg.Seed)),
		endpoints: make(map[raft.NodeID]*Endpoint),
	}
}

// RegisterEndpoint registers a node's handlers with the network.
func (net *SimulatedNetwork) RegisterEndpoint(ep *Endpoint) {
	net.mu.Lock()
	defer net.mu.Unlock()
	net.endpoints[ep.NodeID] = ep
}

// computeLatency returns a deterministic latency with jitter. Must be called with net.mu held.
func (net *SimulatedNetwork) computeLatency() time.Duration {
	if net.config.MaxLatency <= net.config.MinLatency {
		return net.config.MinLatency
	}
	delta := net.config.MaxLatency - net.config.MinLatency
	jitter := time.Duration(net.rng.Int63n(int64(delta)))
	return net.config.MinLatency + jitter
}

// TransportFor returns a raft.Transport adapter bound to a specific source node.
func (net *SimulatedNetwork) TransportFor(src raft.NodeID) raft.Transport {
	return &nodeTransport{
		net: net,
		src: src,
	}
}

type nodeTransport struct {
	net *SimulatedNetwork
	src raft.NodeID
}

func (nt *nodeTransport) SendRequestVote(ctx context.Context, to raft.NodeID, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	return nt.net.dispatchRequestVote(ctx, nt.src, to, args)
}

func (nt *nodeTransport) SendAppendEntries(ctx context.Context, to raft.NodeID, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	return nt.net.dispatchAppendEntries(ctx, nt.src, to, args)
}

func (nt *nodeTransport) SendInstallSnapshot(ctx context.Context, to raft.NodeID, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	return nt.net.dispatchSnapshot(ctx, nt.src, to, args)
}

func (net *SimulatedNetwork) dispatchRequestVote(ctx context.Context, from, to raft.NodeID, args *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	net.mu.Lock()
	ep, exists := net.endpoints[to]
	if !exists {
		net.mu.Unlock()
		return nil, ErrNodeUnreachable
	}
	lat := net.computeLatency()
	net.mu.Unlock()

	resCh := make(chan *raft.RequestVoteReply, 1)

	net.sched.Schedule(lat, EventDeliverRPC, from, to, func() {
		reply := ep.OnRequestVote(args)
		// Schedule reply return trip
		net.mu.Lock()
		returnLat := net.computeLatency()
		net.mu.Unlock()

		net.sched.Schedule(returnLat, EventDeliverRPC, to, from, func() {
			resCh <- reply
		})
	})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-resCh:
		return r, nil
	}
}

func (net *SimulatedNetwork) dispatchAppendEntries(ctx context.Context, from, to raft.NodeID, args *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	net.mu.Lock()
	ep, exists := net.endpoints[to]
	if !exists {
		net.mu.Unlock()
		return nil, ErrNodeUnreachable
	}
	lat := net.computeLatency()
	net.mu.Unlock()

	resCh := make(chan *raft.AppendEntriesReply, 1)

	net.sched.Schedule(lat, EventDeliverRPC, from, to, func() {
		reply := ep.OnAppendEntries(args)
		net.mu.Lock()
		returnLat := net.computeLatency()
		net.mu.Unlock()

		net.sched.Schedule(returnLat, EventDeliverRPC, to, from, func() {
			resCh <- reply
		})
	})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-resCh:
		return r, nil
	}
}

func (net *SimulatedNetwork) dispatchSnapshot(ctx context.Context, from, to raft.NodeID, args *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	net.mu.Lock()
	ep, exists := net.endpoints[to]
	if !exists {
		net.mu.Unlock()
		return nil, ErrNodeUnreachable
	}
	lat := net.computeLatency()
	net.mu.Unlock()

	resCh := make(chan *raft.InstallSnapshotReply, 1)

	net.sched.Schedule(lat, EventDeliverRPC, from, to, func() {
		reply := ep.OnSnapshot(args)
		net.mu.Lock()
		returnLat := net.computeLatency()
		net.mu.Unlock()

		net.sched.Schedule(returnLat, EventDeliverRPC, to, from, func() {
			resCh <- reply
		})
	})

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-resCh:
		return r, nil
	}
}
