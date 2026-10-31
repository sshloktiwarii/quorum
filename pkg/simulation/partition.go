package simulation

import (
	"sync"

	"systems-suite/quorum/pkg/raft"
)

// PartitionMatrix maintains directed link reachability between nodes in the cluster.
type PartitionMatrix struct {
	mu      sync.RWMutex
	blocked map[raft.NodeID]map[raft.NodeID]bool
}

// NewPartitionMatrix initializes a fully connected reachability matrix.
func NewPartitionMatrix() *PartitionMatrix {
	return &PartitionMatrix{
		blocked: make(map[raft.NodeID]map[raft.NodeID]bool),
	}
}

// CutLink severs directed communication from -> to.
func (pm *PartitionMatrix) CutLink(from, to raft.NodeID) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if pm.blocked[from] == nil {
		pm.blocked[from] = make(map[raft.NodeID]bool)
	}
	pm.blocked[from][to] = true
}

// CutBidirectional severs communication in both directions between n1 and n2.
func (pm *PartitionMatrix) CutBidirectional(n1, n2 raft.NodeID) {
	pm.CutLink(n1, n2)
	pm.CutLink(n2, n1)
}

// HealLink restores directed communication from -> to.
func (pm *PartitionMatrix) HealLink(from, to raft.NodeID) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if pm.blocked[from] != nil {
		delete(pm.blocked[from], to)
	}
}

// HealBidirectional restores communication in both directions between n1 and n2.
func (pm *PartitionMatrix) HealBidirectional(n1, n2 raft.NodeID) {
	pm.HealLink(n1, n2)
	pm.HealLink(n2, n1)
}

// Partition partitions the cluster into disjoint groups. Nodes within the same group can
// communicate; communication between different groups is blocked.
func (pm *PartitionMatrix) Partition(groups ...[]raft.NodeID) {
	pm.HealAll()

	// Build map: node -> group index
	nodeGroup := make(map[raft.NodeID]int)
	for gIdx, group := range groups {
		for _, node := range group {
			nodeGroup[node] = gIdx
		}
	}

	pm.mu.Lock()
	defer pm.mu.Unlock()

	for n1, g1 := range nodeGroup {
		for n2, g2 := range nodeGroup {
			if g1 != g2 {
				if pm.blocked[n1] == nil {
					pm.blocked[n1] = make(map[raft.NodeID]bool)
				}
				pm.blocked[n1][n2] = true
			}
		}
	}
}

// IsBlocked checks if packets from `from` destined to `to` are severed.
func (pm *PartitionMatrix) IsBlocked(from, to raft.NodeID) bool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	if pm.blocked[from] == nil {
		return false
	}
	return pm.blocked[from][to]
}

// HealAll restores full connectivity across all nodes.
func (pm *PartitionMatrix) HealAll() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.blocked = make(map[raft.NodeID]map[raft.NodeID]bool)
}
