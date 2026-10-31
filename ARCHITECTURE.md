# Quorum: Distributed Raft Consensus Engine

`systems-suite/quorum` is an industrial-grade, pure Go 1.22+ implementation of the formal Ongaro-Ousterhout Raft consensus specification, featuring bitrot-resistant write-ahead logging (WAL), snapshot compaction, deterministic discrete-event simulation, Wing-Gong linearizability verification, and an embedded real-time Swiss brutalist SSE topology visualizer HUD.

---

## 1. Architectural Highlights

### Formal Ongaro-Ousterhout Raft Core (`pkg/raft/`)
- **Role State Machine**: Strict transitions between `RoleFollower`, `RoleCandidate`, and `RoleLeader`.
- **Term Monotonicity**: Any higher incoming term forces immediate step-down to Follower and updates persistent state before responding.
- **Randomized Election Timers**: Anti-split drift protection with randomized timers $150\text{ms} - 300\text{ms}$ using `crypto/rand`.
- **Log Matching & Fast Conflict Recovery Hints**:
  - Follower responds with `ConflictIndex` and `ConflictTerm` allowing leader to bypass round-trips and jump directly to conflicting term boundaries.
  - Suffix truncation eliminates uncommitted speculative branches upon leader divergence.
- **Section 5.4.2 Commit Invariant**: Leaders only commit entries from their current term directly by replica counting; prior-term entries are indirectly committed.
- **Linearizable Reads (`ReadIndex`)**: Section 6.4 ReadIndex protocol verifies quorum leadership via heartbeat acknowledgments before serving reads from the local state machine without generating log writes.

### Bitrot-Resistant Framed WAL (`pkg/wal/`)
- **Binary Frame Layout**:
  - 13-Byte Header: `Magic [4B] ("QUOR") || Type [1B] || Length [4B] || CRC32 [4B]`
  - Hardware-accelerated CRC32-Castagnoli polynomial (`0x82F63B78`) protecting against bitrot and media degradation.
- **Torn-Write Recovery**: Startup scanner detects incomplete headers, truncated payloads, or checksum errors, automatically truncating cleanly to the last verified transaction offset.

### Snapshotting & Compaction (`pkg/snapshot/`)
- **Atomic Renaming**: Writes state machine snapshots to `snapshot.bin.tmp`, fsyncs, and atomically renames to `snapshot.bin`.
- **Compaction**: Discards compacted log suffixes while retaining baseline `lastIncludedIndex` and `lastIncludedTerm`.
- **InstallSnapshot RPC**: Streams point-in-time snapshots to lagging peers whose logs have been compacted.

### Deterministic Discrete-Event Simulation (`pkg/simulation/`)
- **Virtual Clock & Min-Heap Scheduler**: Priority queue executes thousands of distributed interactions without wall-clock sleep.
- **Fault-Injecting Network Mesh**: Dynamic latency jitter, packet drops, packet duplications, and asymmetric network cuts.
- **Dynamic Partition Matrix**: Seamless 3-2 split-brain injection and partition healing.

### Wing-Gong Linearizability Safety Checker (`pkg/linearizability/`)
- Depth-first search (DFS) with memoization verifies whether concurrent client execution histories ($Invoke \rightarrow Return$) satisfy sequential register specifications.

### Real-Time SSE Visualizer HUD (`pkg/web/` & `cmd/visualizer/`)
- **Swiss Brutalist Aesthetics**: Obsidian (`#07090e`), electric cobalt (`#2563eb`), emerald leader status (`#10b981`), hazard amber (`#f59e0b`), and crimson severed links (`#ef4444`).
- **Interactive SVG Topology**: Pentagon layout with glowing node ripples and link health status.
- **Port Discipline**: Probes preferred port (8090) and dynamically falls back to an ephemeral kernel-assigned port (`127.0.0.1:0`), handling SIGINT/SIGTERM gracefully without zombie processes.

---

## 2. Verification & Test Suite

All tests pass under Go's thread sanitizer without data races:
```bash
go test -v -race ./...
```

Key verified scenarios:
1. `TestJepsenSplitBrainAndHeal`: 5-node cluster subjected to 3-2 network partition; minority writes fail to commit; majority elects new leader and commits; partition heals; uncommitted minority writes are truncated; all 5 nodes converge; Wing-Gong algorithm verifies linearizability.
2. `TestWALTornWriteRecovery` & `TestWALBitrotTruncation`: Bitrot flips and power-loss torn writes are automatically detected and truncated back to the last valid frame.
3. `TestIntegrationLinearizableReadsAndWALPersistence`: Validates ReadIndex linearizable reads and post-reboot state recovery from disk.
