package web

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"systems-suite/quorum/pkg/simulation"
)

func TestWebServerEndpointsAndPortDiscipline(t *testing.T) {
	// 1. Test Port Discipline
	l, port, err := FindAvailablePort(8095)
	if err != nil {
		t.Fatalf("FindAvailablePort failed: %v", err)
	}
	_ = l.Close() // Release so server can bind

	// 2. Initialize simulated cluster
	netCfg := simulation.NetworkConfig{
		MinLatency: 1 * time.Millisecond,
		MaxLatency: 3 * time.Millisecond,
	}
	cluster, err := simulation.NewCluster(3, netCfg)
	if err != nil {
		t.Fatalf("NewCluster failed: %v", err)
	}
	defer cluster.Stop()

	// 3. Initialize Server
	srv, err := NewServer(port, cluster)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}
	srv.Start()
	defer srv.Stop()

	client := &http.Client{Timeout: 2 * time.Second}

	// 4. Test GET / index asset
	resp, err := client.Get(srv.URL() + "/")
	if err != nil {
		t.Fatalf("GET / failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for index, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// 5. Test POST /api/partition
	partResp, err := client.Post(srv.URL()+"/api/partition", "application/json", nil)
	if err != nil {
		t.Fatalf("POST /api/partition failed: %v", err)
	}
	if partResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for partition, got %d", partResp.StatusCode)
	}
	_ = partResp.Body.Close()

	// 6. Test POST /api/heal
	healResp, err := client.Post(srv.URL()+"/api/heal", "application/json", nil)
	if err != nil {
		t.Fatalf("POST /api/heal failed: %v", err)
	}
	if healResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for heal, got %d", healResp.StatusCode)
	}
	_ = healResp.Body.Close()

	// Wait for leader
	for i := 0; i < 15; i++ {
		cluster.Sched.RunUntil(cluster.Sched.Now() + 40*time.Millisecond)
		time.Sleep(15 * time.Millisecond)
		if l, err := cluster.GetLeader(); err == nil && l != nil {
			break
		}
	}

	// 7. Test POST /api/cmd if leader available
	if leader, err := cluster.GetLeader(); err == nil && leader != nil {
		cmdBody, _ := json.Marshal(map[string]string{"key": "test_hud", "value": "val_1"})
		cmdResp, err := client.Post(srv.URL()+"/api/cmd", "application/json", bytes.NewReader(cmdBody))
		if err != nil {
			t.Fatalf("POST /api/cmd failed: %v", err)
		}
		if cmdResp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 OK for cmd, got %d", cmdResp.StatusCode)
		}
		_ = cmdResp.Body.Close()
	}
}
