package raft

import (
	"fmt"
	"time"
)

func (n *Node) Run() {
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
	n.State = Candidate
	n.CurrentTerm++
	n.VotedFor = n.ID
	fmt.Printf("[%s] Became candidate for term %d\n", n.ID, n.CurrentTerm)
}

func (n *Node) runCandidate() {
	n.becomeCandidate()
	votes := 1
	for _, peer := range n.Peers {
		if peer == n.ID {
			continue
		}
		votes++
	}
	if votes > len(n.Peers)/2 {
		n.becomeLeader()
	}
}

func (n *Node) becomeLeader() {
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
	for _, peer := range n.Peers {
		if peer == n.ID {
			continue
		}
		fmt.Printf("[%s] Sending heartbeat to %s\n", n.ID, peer)
	}
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
	Term     int64
	LeaderID string
	Entries  []LogEntry
}

type AppendEntriesResponse struct {
	Term    int64
	Success bool
}

func (n *Node) RequestVote(req RequestVoteRequest) RequestVoteResponse {
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
	return AppendEntriesResponse{Term: n.CurrentTerm, Success: true}
}
