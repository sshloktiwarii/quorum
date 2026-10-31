package raft

import (
	"crypto/rand"
	"math/big"
	"time"
)

// RandomElectionDuration returns a random duration in [min, max] using crypto/rand to prevent election synchronization.
func RandomElectionDuration(min, max time.Duration) time.Duration {
	if max <= min {
		return min
	}
	delta := max - min
	nBig, err := rand.Int(rand.Reader, big.NewInt(int64(delta)))
	if err != nil {
		// Fallback to min
		return min
	}
	return min + time.Duration(nBig.Int64())
}

// ResetTimer safely resets a time.Timer, draining its channel if already expired or pending.
func ResetTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}

// StopTimer safely stops a timer and drains its channel.
func StopTimer(t *time.Timer) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
}
