package raft

import (
	"fmt"
	"sync"
	"time"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type LogEntry struct {
	Index int64  `json:"index"`
	Term  int64  `json:"term"`
	Key   string `json:"key"`
	Value string `json:"value"`
	Op    string `json:"op"`
}

type Node struct {
	mu sync.Mutex

	CurrentTerm int64
	VotedFor    string
	Log         []LogEntry

	commitIndex int64
	lastApplied int64

	nextIndex  map[string]int64
	matchIndex map[string]int64

	ID    string
	Peers []string
	State State

	applyCh     chan LogEntry
	rpcCh       chan RPC
	heartbeatCh chan struct{}
	voteCh      chan bool
}

type RPC struct {
	From    string
	Term    int64
	Type    string
	Payload interface{}
}

type RequestVoteRequest struct {
	Term        int64
	CandidateID string
}

type RequestVoteResponse struct {
	Term        int64
	VoteGranted bool
}

type AppendEntriesRequest struct {
	Term         int64
	LeaderID     string
	PrevLogIndex int64
	PrevLogTerm  int64
	Entries      []LogEntry
	LeaderCommit int64
}

type AppendEntriesResponse struct {
	Term    int64
	Success bool
}

func NewNode(id string, peers []string) *Node {
	return &Node{
		ID:          id,
		Peers:       peers,
		State:       Follower,
		applyCh:     make(chan LogEntry, 100),
		rpcCh:       make(chan RPC, 100),
		heartbeatCh: make(chan struct{}, 10),
		voteCh:      make(chan bool, len(peers)),
		Log:         make([]LogEntry, 0),
		nextIndex:   make(map[string]int64),
		matchIndex:  make(map[string]int64),
	}
}

func (n *Node) Run() {
	go n.applyLoop()
	for {
		switch n.State {
		case Follower:
			n.runFollower()
		case Candidate:
			n.runCandidate()
		case Leader:
			n.runLeader()
		}
	}
}

func (n *Node) applyLoop() {
	for {
		n.mu.Lock()
		if n.lastApplied < n.commitIndex {
			n.lastApplied++
			entry := n.Log[n.lastApplied-1]
			n.mu.Unlock()
			n.applyCh <- entry
		} else {
			n.mu.Unlock()
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (n *Node) runFollower() {
	timeout := randomTimeout(150*time.Millisecond, 300*time.Millisecond)
	select {
	case <-n.heartbeatCh:
	case <-time.After(timeout):
		fmt.Printf("[%s] Election timeout, becoming candidate\n", n.ID)
		n.becomeCandidate()
	}
}

func (n *Node) becomeCandidate() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.State = Candidate
	n.CurrentTerm++
	n.VotedFor = n.ID
	fmt.Printf("[%s] Became candidate for term %d\n", n.ID, n.CurrentTerm)
}

func (n *Node) runCandidate() {
	n.mu.Lock()
	n.becomeCandidate()
	votes := 1
	n.mu.Unlock()

	// Simulate sending RequestVote to peers
	// For now, auto-win if single node or majority simulated
	for _, peer := range n.Peers {
		if peer == n.ID {
			continue
		}
		// In real impl, send RPC and wait for response
		// Simulated: always grant vote for demo
		votes++
	}

	if votes > len(n.Peers)/2 {
		n.becomeLeader()
	} else {
		time.Sleep(100 * time.Millisecond)
	}
}

func (n *Node) becomeLeader() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.State = Leader
	fmt.Printf("[%s] Became leader for term %d\n", n.ID, n.CurrentTerm)
	for _, peer := range n.Peers {
		n.nextIndex[peer] = int64(len(n.Log)) + 1
		n.matchIndex[peer] = 0
	}
}

func (n *Node) runLeader() {
	n.broadcastHeartbeat()
	time.Sleep(50 * time.Millisecond)
}

func (n *Node) broadcastHeartbeat() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, peer := range n.Peers {
		if peer == n.ID {
			continue
		}
		fmt.Printf("[%s] Sending heartbeat to %s (term %d)\n", n.ID, peer, n.CurrentTerm)
	}
}

func (n *Node) RequestVote(req RequestVoteRequest) RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.CurrentTerm {
		return RequestVoteResponse{Term: n.CurrentTerm, VoteGranted: false}
	}
	if req.Term > n.CurrentTerm {
		n.CurrentTerm = req.Term
		n.State = Follower
		n.VotedFor = ""
	}
	if n.VotedFor == "" || n.VotedFor == req.CandidateID {
		n.VotedFor = req.CandidateID
		return RequestVoteResponse{Term: n.CurrentTerm, VoteGranted: true}
	}
	return RequestVoteResponse{Term: n.CurrentTerm, VoteGranted: false}
}

func (n *Node) AppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.CurrentTerm {
		return AppendEntriesResponse{Term: n.CurrentTerm, Success: false}
	}

	select {
	case n.heartbeatCh <- struct{}{}:
	default:
	}

	if req.Term > n.CurrentTerm {
		n.CurrentTerm = req.Term
		n.State = Follower
		n.VotedFor = ""
	}

	// Append entries logic
	if req.PrevLogIndex > 0 {
		if len(n.Log) < int(req.PrevLogIndex) {
			return AppendEntriesResponse{Term: n.CurrentTerm, Success: false}
		}
		if n.Log[req.PrevLogIndex-1].Term != req.PrevLogTerm {
			return AppendEntriesResponse{Term: n.CurrentTerm, Success: false}
		}
	}

	// Append new entries
	for i, entry := range req.Entries {
		idx := int(req.PrevLogIndex) + i
		if idx < len(n.Log) {
			if n.Log[idx].Term != entry.Term {
				n.Log = n.Log[:idx]
				n.Log = append(n.Log, entry)
			}
		} else {
			n.Log = append(n.Log, entry)
		}
	}

	// Update commit index
	if req.LeaderCommit > n.commitIndex {
		n.commitIndex = min(req.LeaderCommit, int64(len(n.Log)))
	}

	return AppendEntriesResponse{Term: n.CurrentTerm, Success: true}
}

func (n *Node) Submit(entry LogEntry) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.State != Leader {
		return fmt.Errorf("not leader")
	}

	entry.Term = n.CurrentTerm
	entry.Index = int64(len(n.Log)) + 1
	n.Log = append(n.Log, entry)

	// Trigger replication
	n.broadcastHeartbeat()
	return nil
}

func (n *Node) GetApplyCh() <-chan LogEntry {
	return n.applyCh
}

func randomTimeout(min, max time.Duration) time.Duration {
	return min + time.Duration(time.Now().UnixNano())%(max-min)
}

func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
