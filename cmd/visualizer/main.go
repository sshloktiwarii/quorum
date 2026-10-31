package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"systems-suite/quorum/pkg/simulation"
	"systems-suite/quorum/pkg/web"
)

func main() {
	portFlag := flag.Int("port", 8090, "preferred HTTP port for visualizer HUD")
	nodesFlag := flag.Int("nodes", 5, "number of cluster nodes to simulate")
	flag.Parse()

	log.Printf("[QUORUM] Initializing %d-node simulated Raft cluster...", *nodesFlag)
	netCfg := simulation.NetworkConfig{
		MinLatency: 2 * time.Millisecond,
		MaxLatency: 8 * time.Millisecond,
		DropRate:   0.0,
		DupRate:    0.0,
		Seed:       time.Now().UnixNano(),
	}

	cluster, err := simulation.NewCluster(*nodesFlag, netCfg)
	if err != nil {
		log.Fatalf("[FATAL] Failed to initialize simulation cluster: %v", err)
	}
	defer cluster.Stop()

	// Virtual clock advancement worker
	stopSim := make(chan struct{})
	go func() {
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopSim:
				return
			case <-ticker.C:
				cluster.Sched.RunUntil(cluster.Sched.Now() + 30*time.Millisecond)
			}
		}
	}()

	// Initialize HTTP Visualizer Server with Port Discipline
	srv, err := web.NewServer(*portFlag, cluster)
	if err != nil {
		log.Fatalf("[FATAL] Port discipline error: %v", err)
	}

	srv.Start()
	log.Printf("[QUORUM] Real-time visualizer HUD listening at: %s", srv.URL())
	fmt.Printf("\n>>> QUORUM CLUSTER VISUALIZER HUD: %s <<<\n\n", srv.URL())

	// Wait for SIGINT / SIGTERM for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	log.Println("\n[QUORUM] Gracefully shutting down visualizer and consensus engine...")
	close(stopSim)
	_ = srv.Stop()
	log.Println("[QUORUM] Shutdown complete.")
}
