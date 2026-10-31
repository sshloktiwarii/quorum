package raft

import (
	"fmt"
	"time"
)

// Config encapsulates operational parameters for a Raft node.
type Config struct {
	ID                 NodeID
	Peers              []NodeID
	HeartbeatInterval  time.Duration
	MinElectionTimeout time.Duration
	MaxElectionTimeout time.Duration
	DataDir            string
	SnapshotThreshold  uint64
	MaxEntriesPerMsg   uint64
	StorageSync        bool
}

// DefaultConfig initializes recommended production defaults following Ongaro-Ousterhout.
func DefaultConfig(id NodeID, peers []NodeID) *Config {
	return &Config{
		ID:                 id,
		Peers:              peers,
		HeartbeatInterval:  50 * time.Millisecond,
		MinElectionTimeout: 150 * time.Millisecond,
		MaxElectionTimeout: 300 * time.Millisecond,
		DataDir:            "",
		SnapshotThreshold:  5000,
		MaxEntriesPerMsg:   256,
		StorageSync:        true,
	}
}

// Validate verifies config bounds and invariants.
func (c *Config) Validate() error {
	if c.ID == "" {
		return fmt.Errorf("%w: ID cannot be empty", ErrInvalidConfig)
	}
	if c.MinElectionTimeout <= 0 {
		return fmt.Errorf("%w: MinElectionTimeout must be positive", ErrInvalidConfig)
	}
	if c.MaxElectionTimeout <= c.MinElectionTimeout {
		return fmt.Errorf("%w: MaxElectionTimeout must exceed MinElectionTimeout", ErrInvalidConfig)
	}
	if c.HeartbeatInterval <= 0 || c.HeartbeatInterval >= c.MinElectionTimeout {
		return fmt.Errorf("%w: HeartbeatInterval must be positive and less than MinElectionTimeout", ErrInvalidConfig)
	}
	if c.SnapshotThreshold == 0 {
		c.SnapshotThreshold = 5000
	}
	if c.MaxEntriesPerMsg == 0 {
		c.MaxEntriesPerMsg = 256
	}
	return nil
}

// ClusterSize returns total members in cluster (peers + self).
func (c *Config) ClusterSize() int {
	return len(c.Peers) + 1
}

// QuorumSize returns minimum votes/acknowledgments required for majority consensus.
func (c *Config) QuorumSize() int {
	return (c.ClusterSize() / 2) + 1
}
