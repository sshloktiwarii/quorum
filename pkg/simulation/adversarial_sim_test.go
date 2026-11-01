package simulation

import (
	"encoding/json"
	"testing"
	"time"

	"systems-suite/quorum/pkg/linearizability"
	"systems-suite/quorum/pkg/raft"
	"systems-suite/quorum/pkg/statemachine"
)

// TestJepsenLeaderIsolationAndRejoin tests that an isolated leader steps down,
// a new leader is elected by the majority, and upon rejoin the old leader reconciles state.
func TestJepsenLeaderIsolationAndRejoin(t *testing.T) {
	cfg := NetworkConfig{
		MinLatency: 1 * time.Millisecond,
		MaxLatency: 3 * time.Millisecond,
		DropRate:   0,
		DupRate:    0,
		Seed:       54321,
	}

	cluster, err := NewCluster(5, cfg)
	if err != nil {
		t.Fatalf("NewCluster failed: %v", err)
	}
	defer cluster.Stop()

	// Wait for initial leader
	var initialLeader *raft.RaftNode
	for i := 0; i < 25; i++ {
		cluster.Sched.RunUntil(cluster.Sched.Now() + 40*time.Millisecond)
		time.Sleep(20 * time.Millisecond)
		if l, err := cluster.GetLeader(); err == nil && l != nil {
			initialLeader = l
			break
		}
	}
	if initialLeader == nil {
		t.Fatalf("failed to elect initial leader")
	}

	history := make([]linearizability.Operation, 0)
	startTime := cluster.Sched.Now()

	// 1. Propose key on initial leader
	cmd1, _ := json.Marshal(statemachine.Command{Op: statemachine.OpPut, Key: "cluster_epoch", Value: "epoch_1"})
	inv1 := cluster.Sched.Now()
	_, _, err = initialLeader.Propose(cmd1)
	if err != nil {
		t.Fatalf("propose 1 failed: %v", err)
	}

	for i := 0; i < 10; i++ {
		cluster.Sched.RunUntil(cluster.Sched.Now() + 30*time.Millisecond)
		time.Sleep(10 * time.Millisecond)
	}
	ret1 := cluster.Sched.Now()
	history = append(history, linearizability.Operation{
		ID:       "op-epoch-1",
		Type:     "PUT",
		Key:      "cluster_epoch",
		Value:    "epoch_1",
		InvokeAt: inv1 - startTime,
		ReturnAt: ret1 - startTime,
		Ok:       true,
	})

	// 2. Isolate initial leader completely (1 vs 4 partition)
	isolatedNode := initialLeader.ID()
	majorityGroup := make([]raft.NodeID, 0, 4)
	for _, id := range cluster.nodeNames {
		if id != isolatedNode {
			majorityGroup = append(majorityGroup, id)
		}
	}

	cluster.Net.Partitions().Partition([]raft.NodeID{isolatedNode}, majorityGroup)

	// Step simulation so majority elects new leader
	var newLeader *raft.RaftNode
	for i := 0; i < 40; i++ {
		cluster.Sched.RunUntil(cluster.Sched.Now() + 40*time.Millisecond)
		time.Sleep(20 * time.Millisecond)
		for _, id := range majorityGroup {
			node := cluster.Nodes[id]
			_, isL := node.GetState()
			if isL {
				newLeader = node
				break
			}
		}
		if newLeader != nil {
			break
		}
	}
	if newLeader == nil {
		t.Fatalf("majority failed to elect new leader after isolation")
	}

	// 3. Propose key on new leader
	cmd2, _ := json.Marshal(statemachine.Command{Op: statemachine.OpPut, Key: "cluster_epoch", Value: "epoch_2"})
	inv2 := cluster.Sched.Now()
	_, _, err = newLeader.Propose(cmd2)
	if err != nil {
		t.Fatalf("propose 2 on new leader failed: %v", err)
	}

	for i := 0; i < 15; i++ {
		cluster.Sched.RunUntil(cluster.Sched.Now() + 30*time.Millisecond)
		time.Sleep(10 * time.Millisecond)
	}
	ret2 := cluster.Sched.Now()
	history = append(history, linearizability.Operation{
		ID:       "op-epoch-2",
		Type:     "PUT",
		Key:      "cluster_epoch",
		Value:    "epoch_2",
		InvokeAt: inv2 - startTime,
		ReturnAt: ret2 - startTime,
		Ok:       true,
	})

	// 4. Heal partition and let isolated node rejoin
	cluster.Net.Partitions().HealAll()

	for i := 0; i < 20; i++ {
		cluster.Sched.RunUntil(cluster.Sched.Now() + 40*time.Millisecond)
		time.Sleep(15 * time.Millisecond)
	}

	// 5. Verify convergence across all 5 nodes
	for _, id := range cluster.nodeNames {
		val, ok := cluster.Stores[id].Query("cluster_epoch")
		if !ok || val != "epoch_2" {
			t.Fatalf("node %s store mismatch after heal: got %s (ok=%t), expected 'epoch_2'", id, val, ok)
		}
	}

	// 6. Verify linearizability
	checker := linearizability.NewChecker()
	ok, order, err := checker.Verify(history)
	if !ok || err != nil || len(order) != 2 {
		t.Fatalf("linearizability verification failed: ok=%t, err=%v, order_len=%d", ok, err, len(order))
	}
}
