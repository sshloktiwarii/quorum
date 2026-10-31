package simulation

import (
	"encoding/json"
	"testing"
	"time"

	"systems-suite/quorum/pkg/statemachine"
)

func TestClusterLeaderElectionAndReplication(t *testing.T) {
	cfg := NetworkConfig{
		MinLatency: 1 * time.Millisecond,
		MaxLatency: 3 * time.Millisecond,
		DropRate:   0,
		DupRate:    0,
		Seed:       12345,
	}

	cluster, err := NewCluster(3, cfg)
	if err != nil {
		t.Fatalf("NewCluster failed: %v", err)
	}
	defer cluster.Stop()

	// Advance virtual time / wall-clock time until a leader is chosen
	var leaderFound bool
	for attempt := 0; attempt < 20; attempt++ {
		cluster.Sched.RunUntil(cluster.Sched.Now() + 50*time.Millisecond)
		time.Sleep(20 * time.Millisecond)

		if leader, err := cluster.GetLeader(); err == nil && leader != nil {
			leaderFound = true
			break
		}
	}

	if !leaderFound {
		t.Fatalf("failed to elect a leader in cluster")
	}

	leader, err := cluster.GetLeader()
	if err != nil || leader == nil {
		t.Fatalf("GetLeader failed: %v", err)
	}

	// Propose a PUT command
	cmd, _ := json.Marshal(statemachine.Command{
		Op:    statemachine.OpPut,
		Key:   "cluster_test_key",
		Value: "consensus_val_100",
	})

	idx, term, err := leader.Propose(cmd)
	if err != nil {
		t.Fatalf("Propose failed: %v", err)
	}
	if idx < 1 || term < 1 {
		t.Fatalf("invalid proposal metadata: idx=%d, term=%d", idx, term)
	}

	// Run scheduler to propagate AppendEntries
	for attempt := 0; attempt < 15; attempt++ {
		cluster.Sched.RunUntil(cluster.Sched.Now() + 50*time.Millisecond)
		time.Sleep(20 * time.Millisecond)
	}

	// Verify all stores have the key
	for nodeID, store := range cluster.Stores {
		val, ok := store.Query("cluster_test_key")
		if !ok || val != "consensus_val_100" {
			t.Fatalf("node %s store mismatch: got (%s, %t)", nodeID, val, ok)
		}
	}
}
