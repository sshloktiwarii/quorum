package simulation

import (
	"container/heap"
	"fmt"
	"sync"
	"time"

	"systems-suite/quorum/pkg/raft"
)

// EventType categorizes scheduled discrete events.
type EventType int

const (
	EventDeliverRPC EventType = iota
	EventElectionTimeout
	EventHeartbeatTick
	EventInjectPartition
	EventHealPartition
	EventClientOp
)

func (et EventType) String() string {
	switch et {
	case EventDeliverRPC:
		return "DeliverRPC"
	case EventElectionTimeout:
		return "ElectionTimeout"
	case EventHeartbeatTick:
		return "HeartbeatTick"
	case EventInjectPartition:
		return "InjectPartition"
	case EventHealPartition:
		return "HealPartition"
	case EventClientOp:
		return "ClientOp"
	default:
		return fmt.Sprintf("Event(%d)", int(et))
	}
}

// Event represents a discrete action scheduled at a deterministic virtual clock timestamp.
type Event struct {
	Timestamp time.Duration
	Seq       uint64
	Type      EventType
	From      raft.NodeID
	To        raft.NodeID
	Action    func()
	index     int // internal heap index
}

// eventHeap implements container/heap.Interface for Event min-heap.
type eventHeap []*Event

func (h eventHeap) Len() int { return len(h) }
func (h eventHeap) Less(i, j int) bool {
	if h[i].Timestamp == h[j].Timestamp {
		return h[i].Seq < h[j].Seq // Deterministic tie-breaker
	}
	return h[i].Timestamp < h[j].Timestamp
}
func (h eventHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *eventHeap) Push(x any) {
	n := len(*h)
	item := x.(*Event)
	item.index = n
	*h = append(*h, item)
}
func (h *eventHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*h = old[0 : n-1]
	return item
}

// Scheduler drives a deterministic virtual clock simulation without wall-clock sleep.
type Scheduler struct {
	mu     sync.Mutex
	pq     eventHeap
	now    time.Duration
	seqGen uint64
}

// NewScheduler initializes an empty discrete-event scheduler at virtual time 0.
func NewScheduler() *Scheduler {
	s := &Scheduler{
		pq:  make(eventHeap, 0, 1024),
		now: 0,
	}
	heap.Init(&s.pq)
	return s
}

// Now returns current virtual time.
func (s *Scheduler) Now() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.now
}

// Schedule inserts an action to execute at (Now() + delay).
func (s *Scheduler) Schedule(delay time.Duration, eventType EventType, from, to raft.NodeID, action func()) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.seqGen++
	evt := &Event{
		Timestamp: s.now + delay,
		Seq:       s.seqGen,
		Type:      eventType,
		From:      from,
		To:        to,
		Action:    action,
	}
	heap.Push(&s.pq, evt)
}

// Step pops and executes the next earliest event. Returns false if event queue is empty.
func (s *Scheduler) Step() bool {
	s.mu.Lock()
	if s.pq.Len() == 0 {
		s.mu.Unlock()
		return false
	}
	evt := heap.Pop(&s.pq).(*Event)
	s.now = evt.Timestamp
	action := evt.Action
	s.mu.Unlock()

	if action != nil {
		action()
	}
	return true
}

// RunUntil processes events until virtual time reaches or exceeds target, or queue becomes empty.
func (s *Scheduler) RunUntil(target time.Duration) {
	for {
		s.mu.Lock()
		if s.pq.Len() == 0 || s.pq[0].Timestamp > target {
			s.mu.Unlock()
			break
		}
		s.mu.Unlock()

		if !s.Step() {
			break
		}
	}
	s.mu.Lock()
	if s.now < target {
		s.now = target
	}
	s.mu.Unlock()
}
