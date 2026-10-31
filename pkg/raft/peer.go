package raft

import (
	"context"
)

// Transport defines RPC transmission across cluster nodes.
type Transport interface {
	SendRequestVote(ctx context.Context, to NodeID, args *RequestVoteArgs) (*RequestVoteReply, error)
	SendAppendEntries(ctx context.Context, to NodeID, args *AppendEntriesArgs) (*AppendEntriesReply, error)
	SendInstallSnapshot(ctx context.Context, to NodeID, args *InstallSnapshotArgs) (*InstallSnapshotReply, error)
}
