```
 ██████╗ ██╗   ██╗ ██████╗ ██████╗ ██╗   ██╗███╗   ███╗
██╔═══██╗██║   ██║██╔═══██╗██╔══██╗██║   ██║████╗ ████║
██║   ██║██║   ██║██║   ██║██████╔╝██║   ██║██╔████╔██║
██║▄▄ ██║██║   ██║██║   ██║██╔══██╗██║   ██║██║╚██╔╝██║
╚██████╔╝╚██████╔╝╚██████╔╝██║  ██║╚██████╔╝██║ ╚═╝ ██║
 ╚══▀▀═╝  ╚═════╝  ╚═════╝ ╚═╝  ╚═╝ ╚═════╝ ╚═╝     ╚═╝
```

# QUORUM // Distributed Raft Consensus Engine

Industrial-grade, pure Go 1.22+ implementation of the formal Ongaro-Ousterhout Raft distributed consensus specification, featuring bitrot-resistant write-ahead logging (WAL), snapshot compaction, deterministic discrete-event simulation, Wing-Gong linearizability verification, and an embedded real-time Swiss brutalist SSE topology visualizer HUD.

---

## Quick Navigation

- [Detailed Architecture Specification](ARCHITECTURE.md)
- [Verification & Benchmarks](#verification--test-suite)
- [Visualizer Quickstart](#real-time-sse-visualizer-hud)

---

## Architectural Highlights

- **Formal Ongaro-Ousterhout Raft Core (`pkg/raft/`)**:
  - State machine roles (`RoleFollower`, `RoleCandidate`, `RoleLeader`) with term monotonicity.
  - Randomized anti-split election timers ($150\text{ms} - 300\text{ms}$) via `crypto/rand`.
  - ConflictIndex/ConflictTerm hints for fast $O(1)$ round-trip log reconciliation.
  - Section 5.4.2 commit invariant & Section 6.4 `ReadIndex` linearizable queries without disk I/O.

- **Bitrot-Resistant Framed WAL (`pkg/wal/`)**:
  - 13-Byte Header: `Magic [4B] ("QUOR") || Type [1B] || Length [4B] || CRC32-Castagnoli [4B]`.
  - Automatic torn-write detection and bitrot truncation.

- **Snapshotting & Compaction (`pkg/snapshot/`)**:
  - Atomic temporary renaming (`snapshot.bin.tmp` $\to$ `snapshot.bin`) with log prefix trimming.
  - Streamed `InstallSnapshot` RPC for catching up partitioned peers.

- **Deterministic Discrete-Event Simulation (`pkg/simulation/`)**:
  - Virtual clock min-heap scheduler simulating thousands of network interactions without sleeping.
  - Dynamic partition matrix verifying consensus safety under 3-2 split-brain and asymmetric network cuts.

- **Wing-Gong Linearizability Checker (`pkg/linearizability/`)**:
  - Formal DFS memoized checker verifying sequential consistency across concurrent operations.

- **Swiss Brutalist SSE Visualizer (`pkg/web/` & `cmd/visualizer/`)**:
  - Real-time SVG topology HUD served via Go `embed.FS` with strict port discipline.

---

## Verification & Test Suite

All tests execute cleanly under Go's race detector:

```bash
# Run all unit, integration, and chaos test suites with thread sanitizer
go test -v -race ./...
```

Run the interactive cluster visualizer:

```bash
# Launch 5-node cluster with real-time SSE topology HUD
go run ./cmd/visualizer -nodes 5 -port 8090
```
