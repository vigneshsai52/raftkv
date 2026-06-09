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

type Node struct {
	mu sync.Mutex

	CurrentTerm int64
	VotedFor    string
	Log         []LogEntry

	commitIndex int64
	lastApplied int64

	nextIndex  map[string]int64
	matchIndex map[string]int64

	ID        string
	Peers     []string
	State     State
	transport *Transport

	applyCh     chan LogEntry
	heartbeatCh chan struct{}
	stopCh      chan struct{}
}

func NewNode(id string, peers []string, transport *Transport) *Node {
	return &Node{
		ID:          id,
		Peers:       peers,
		State:       Follower,
		transport:   transport,
		applyCh:     make(chan LogEntry, 100),
		heartbeatCh: make(chan struct{}, 10),
		stopCh:      make(chan struct{}),
		Log:         make([]LogEntry, 0),
		nextIndex:   make(map[string]int64),
		matchIndex:  make(map[string]int64),
	}
}

func (n *Node) Run() {
	go n.applyLoop()
	for {
		select {
		case <-n.stopCh:
			return
		default:
		}

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
		n.runCandidate()
	}
}

func (n *Node) runCandidate() {
	n.mu.Lock()
	n.State = Candidate
	n.CurrentTerm++
	n.VotedFor = n.ID
	term := n.CurrentTerm
	n.mu.Unlock()

	fmt.Printf("[%s] Became candidate for term %d\n", n.ID, term)

	votes := 1
	for _, peer := range n.Peers {
		if peer == n.ID {
			continue
		}
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
		if peer == n.ID {
			continue
		}
		n.nextIndex[peer] = int64(len(n.Log)) + 1
		n.matchIndex[peer] = 0
	}
}

func (n *Node) runLeader() {
	n.broadcastHeartbeat()
	n.advanceCommitIndex()
	time.Sleep(50 * time.Millisecond)
}

func (n *Node) broadcastHeartbeat() {
	n.mu.Lock()
	if n.State != Leader {
		n.mu.Unlock()
		return
	}

	term := n.CurrentTerm
	commitIdx := n.commitIndex
	n.mu.Unlock()

	for _, peer := range n.Peers {
		if peer == n.ID {
			continue
		}
		go func(p string) {
			n.mu.Lock()
			nextIdx := n.nextIndex[p]
			prevLogIdx := nextIdx - 1
			prevLogTerm := int64(0)
			if prevLogIdx > 0 && int(prevLogIdx) <= len(n.Log) {
				prevLogTerm = n.Log[prevLogIdx-1].Term
			}

			var entries []LogEntry
			if int(nextIdx) <= len(n.Log) {
				entries = n.Log[nextIdx-1:]
			}
			n.mu.Unlock()

			req := AppendEntriesRequest{
				Term:         term,
				LeaderID:     n.ID,
				PrevLogIndex: prevLogIdx,
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: commitIdx,
			}

			resp, err := n.transport.SendAppendEntries(p, req)
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if resp.Term > n.CurrentTerm {
				n.CurrentTerm = resp.Term
				n.State = Follower
				n.VotedFor = ""
				return
			}

			if resp.Success {
				if len(entries) > 0 {
					n.matchIndex[p] = entries[len(entries)-1].Index
					n.nextIndex[p] = n.matchIndex[p] + 1
				}
			} else {
				n.nextIndex[p]--
				if n.nextIndex[p] < 1 {
					n.nextIndex[p] = 1
				}
			}
		}(peer)
	}
}

func (n *Node) advanceCommitIndex() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.State != Leader {
		return
	}

	for idx := int64(len(n.Log)); idx > n.commitIndex; idx-- {
		if n.Log[idx-1].Term != n.CurrentTerm {
			break
		}

		count := 1
		for _, peer := range n.Peers {
			if n.matchIndex[peer] >= idx {
				count++
			}
		}

		if count > len(n.Peers)/2 {
			n.commitIndex = idx
			break
		}
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

	if req.PrevLogIndex > 0 {
		if len(n.Log) < int(req.PrevLogIndex) {
			return AppendEntriesResponse{Term: n.CurrentTerm, Success: false}
		}
		if n.Log[req.PrevLogIndex-1].Term != req.PrevLogTerm {
			return AppendEntriesResponse{Term: n.CurrentTerm, Success: false}
		}
	}

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

	if req.LeaderCommit > n.commitIndex {
		n.commitIndex = min(req.LeaderCommit, int64(len(n.Log)))
	}

	return AppendEntriesResponse{Term: n.CurrentTerm, Success: true}
}

func (n *Node) Submit(entry LogEntry) error {
	n.mu.Lock()
	if n.State != Leader {
		n.mu.Unlock()
		return fmt.Errorf("not leader")
	}

	entry.Term = n.CurrentTerm
	entry.Index = int64(len(n.Log)) + 1
	n.Log = append(n.Log, entry)
	n.mu.Unlock()

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
