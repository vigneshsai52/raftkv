package raft

import "time"

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
	CurrentTerm int64
	VotedFor    string
	Log         []LogEntry
	commitIndex int64
	lastApplied int64
	nextIndex   map[string]int64
	matchIndex  map[string]int64
	ID          string
	Peers       []string
	State       State
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

func NewNode(id string, peers []string) *Node {
	return &Node{
		ID:          id,
		Peers:       peers,
		State:       Follower,
		rpcCh:       make(chan RPC, 100),
		heartbeatCh: make(chan struct{}, 10),
		voteCh:      make(chan bool, len(peers)),
		Log:         make([]LogEntry, 0),
		nextIndex:   make(map[string]int64),
		matchIndex:  make(map[string]int64),
	}
}

func randomTimeout(min, max time.Duration) time.Duration {
	return min + time.Duration(time.Now().UnixNano())%(max-min)
}
