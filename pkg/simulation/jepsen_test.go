package simulation

import (
	"encoding/json"
	"testing"
	"time"

	"systems-suite/quorum/pkg/linearizability"
	"systems-suite/quorum/pkg/raft"
	"systems-suite/quorum/pkg/statemachine"
)

func TestJepsenSplitBrainAndHeal(t *testing.T) {
	cfg := NetworkConfig{
		MinLatency: 1 * time.Millisecond,
		MaxLatency: 3 * time.Millisecond,
		DropRate:   0,
		DupRate:    0,
		Seed:       9999,
	}

	// 1. Initialize 5-node cluster
	cluster, err := NewCluster(5, cfg)
	if err != nil {
		t.Fatalf("NewCluster failed: %v", err)
	}
	defer cluster.Stop()

	// 2. Wait for leader election
	var initialLeader *raft.RaftNode
	for i := 0; i < 20; i++ {
		cluster.Sched.RunUntil(cluster.Sched.Now() + 50*time.Millisecond)
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

	// 3. Propose and commit initial value
	initCmd, _ := json.Marshal(statemachine.Command{Op: statemachine.OpPut, Key: "account", Value: "init_100"})
	inv1 := cluster.Sched.Now()
	_, _, err = initialLeader.Propose(initCmd)
	if err != nil {
		t.Fatalf("initial Propose failed: %v", err)
	}

	for i := 0; i < 10; i++ {
		cluster.Sched.RunUntil(cluster.Sched.Now() + 30*time.Millisecond)
		time.Sleep(10 * time.Millisecond)
	}
	ret1 := cluster.Sched.Now()
	history = append(history, linearizability.Operation{
		ID:       "op-init",
		Type:     "PUT",
		Key:      "account",
		Value:    "init_100",
		InvokeAt: inv1 - startTime,
		ReturnAt: ret1 - startTime,
		Ok:       true,
	})

	// 4. Inject 3-2 network partition:
	// Minority: {initialLeader.ID(), peerX}
	// Majority: remaining 3 nodes
	minorityNodes := []raft.NodeID{initialLeader.ID()}
	majorityNodes := make([]raft.NodeID, 0, 3)

	for _, id := range cluster.nodeNames {
		if id != initialLeader.ID() {
			if len(minorityNodes) < 2 {
				minorityNodes = append(minorityNodes, id)
			} else {
				majorityNodes = append(majorityNodes, id)
			}
		}
	}

	cluster.Net.Partitions().Partition(minorityNodes, majorityNodes)

	// 5. Submit write to minority leader: CANNOT achieve majority consensus
	minorityCmd, _ := json.Marshal(statemachine.Command{Op: statemachine.OpPut, Key: "account", Value: "minority_stale"})
	_, _, err = initialLeader.Propose(minorityCmd)
	if err != nil {
		t.Fatalf("minority propose error: %v", err)
	}

	// Step simulation for election in majority group
	for i := 0; i < 20; i++ {
		cluster.Sched.RunUntil(cluster.Sched.Now() + 40*time.Millisecond)
		time.Sleep(15 * time.Millisecond)
	}

	// 6. Find newly elected majority leader
	var majorityLeader *raft.RaftNode
	for _, id := range majorityNodes {
		node := cluster.Nodes[id]
		_, isL := node.GetState()
		if isL {
			majorityLeader = node
			break
		}
	}
	if majorityLeader == nil {
		t.Fatalf("majority partition failed to elect a new leader")
	}

	// 7. Submit write to majority leader: MUST succeed and commit
	majorityCmd, _ := json.Marshal(statemachine.Command{Op: statemachine.OpPut, Key: "account", Value: "majority_authoritative"})
	inv2 := cluster.Sched.Now()
	_, _, err = majorityLeader.Propose(majorityCmd)
	if err != nil {
		t.Fatalf("majority propose failed: %v", err)
	}

	for i := 0; i < 15; i++ {
		cluster.Sched.RunUntil(cluster.Sched.Now() + 30*time.Millisecond)
		time.Sleep(10 * time.Millisecond)
	}
	ret2 := cluster.Sched.Now()
	history = append(history, linearizability.Operation{
		ID:       "op-majority",
		Type:     "PUT",
		Key:      "account",
		Value:    "majority_authoritative",
		InvokeAt: inv2 - startTime,
		ReturnAt: ret2 - startTime,
		Ok:       true,
	})

	// 8. Heal partition
	cluster.Net.Partitions().HealAll()

	// Step simulation allowing cluster to converge
	for i := 0; i < 20; i++ {
		cluster.Sched.RunUntil(cluster.Sched.Now() + 40*time.Millisecond)
		time.Sleep(15 * time.Millisecond)
	}

	// 9. Verify consensus safety across ALL 5 nodes:
	// All stores must contain "majority_authoritative", and minority write must have been truncated!
	for _, id := range cluster.nodeNames {
		store := cluster.Stores[id]
		val, ok := store.Query("account")
		if !ok || val != "majority_authoritative" {
			t.Fatalf("consensus safety violation on node %s: expected 'majority_authoritative', got '%s' (exists=%t)",
				id, val, ok)
		}
	}

	inv3 := cluster.Sched.Now()
	val, ok := cluster.Stores[cluster.nodeNames[0]].Query("account")
	ret3 := cluster.Sched.Now()
	history = append(history, linearizability.Operation{
		ID:       "op-final-get",
		Type:     "GET",
		Key:      "account",
		Result:   val,
		InvokeAt: inv3 - startTime,
		ReturnAt: ret3 - startTime,
		Ok:       ok,
	})

	// 10. Formal Wing-Gong linearizability verification
	checker := linearizability.NewChecker()
	isLinearizable, order, err := checker.Verify(history)
	if !isLinearizable || err != nil {
		t.Fatalf("Wing-Gong linearizability verification failed: %v", err)
	}
	if len(order) != 3 {
		t.Fatalf("expected 3 linearized operations, got %d", len(order))
	}
}
