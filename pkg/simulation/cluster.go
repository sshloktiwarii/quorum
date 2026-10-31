package simulation

import (
	"errors"
	"fmt"
	"os"
	"time"

	"systems-suite/quorum/pkg/raft"
	"systems-suite/quorum/pkg/statemachine"
	"systems-suite/quorum/pkg/wal"
)

// Cluster coordinates an N-node Raft ensemble wired to a deterministic virtual network.
type Cluster struct {
	Sched     *Scheduler
	Net       *SimulatedNetwork
	Nodes     map[raft.NodeID]*raft.RaftNode
	Stores    map[raft.NodeID]*statemachine.KVStore
	WALs      map[raft.NodeID]*wal.WAL
	tempDir   string
	nodeNames []raft.NodeID
}

// NewCluster spins up a fully configured simulated cluster of size `count`.
func NewCluster(count int, netCfg NetworkConfig) (*Cluster, error) {
	tempDir, err := os.MkdirTemp("", "quorum_sim_cluster_*")
	if err != nil {
		return nil, fmt.Errorf("sim cluster: mkdir temp: %w", err)
	}

	sched := NewScheduler()
	simNet := NewSimulatedNetwork(sched, netCfg)

	nodeNames := make([]raft.NodeID, count)
	for i := 0; i < count; i++ {
		nodeNames[i] = raft.NodeID(fmt.Sprintf("node-%d", i+1))
	}

	c := &Cluster{
		Sched:     sched,
		Net:       simNet,
		Nodes:     make(map[raft.NodeID]*raft.RaftNode),
		Stores:    make(map[raft.NodeID]*statemachine.KVStore),
		WALs:      make(map[raft.NodeID]*wal.WAL),
		tempDir:   tempDir,
		nodeNames: nodeNames,
	}

	for _, nodeID := range nodeNames {
		peers := make([]raft.NodeID, 0, count-1)
		for _, peer := range nodeNames {
			if peer != nodeID {
				peers = append(peers, peer)
			}
		}

		cfg := raft.DefaultConfig(nodeID, peers)
		cfg.MinElectionTimeout = 50 * time.Millisecond
		cfg.MaxElectionTimeout = 100 * time.Millisecond
		cfg.HeartbeatInterval = 20 * time.Millisecond
		cfg.DataDir = fmt.Sprintf("%s/%s", tempDir, string(nodeID))

		w, err := wal.Open(cfg.DataDir, false)
		if err != nil {
			_ = c.Stop()
			return nil, err
		}
		c.WALs[nodeID] = w

		store := statemachine.NewKVStore()
		c.Stores[nodeID] = store

		transport := simNet.TransportFor(nodeID)
		rn, err := raft.NewRaftNode(cfg, transport, store, w)
		if err != nil {
			_ = c.Stop()
			return nil, err
		}
		c.Nodes[nodeID] = rn

		ep := &Endpoint{
			NodeID:          nodeID,
			OnRequestVote:   rn.HandleRequestVote,
			OnAppendEntries: rn.HandleAppendEntries,
			OnSnapshot:      rn.HandleInstallSnapshot,
		}
		simNet.RegisterEndpoint(ep)
	}

	for _, rn := range c.Nodes {
		rn.Start()
	}

	return c, nil
}

// GetLeader returns the currently elected leader, if any.
func (c *Cluster) GetLeader() (*raft.RaftNode, error) {
	var leader *raft.RaftNode
	for _, rn := range c.Nodes {
		_, isLeader := rn.GetState()
		if isLeader {
			if leader != nil {
				return nil, errors.New("multiple leaders detected in cluster")
			}
			leader = rn
		}
	}
	if leader == nil {
		return nil, errors.New("no leader elected")
	}
	return leader, nil
}

// Stop safely shuts down all nodes and cleans up temporary test files.
func (c *Cluster) Stop() error {
	for _, rn := range c.Nodes {
		rn.Stop()
	}
	if c.tempDir != "" {
		_ = os.RemoveAll(c.tempDir)
	}
	return nil
}
