package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

type Transport struct {
	nodeID string
	peers  map[string]string // nodeID -> "host:port"
}

func NewTransport(nodeID string) *Transport {
	return &Transport{
		nodeID: nodeID,
		peers:  make(map[string]string),
	}
}

func (t *Transport) AddPeer(nodeID, addr string) {
	t.peers[nodeID] = addr
}

func (t *Transport) SendRequestVote(peerID string, req RequestVoteRequest) (RequestVoteResponse, error) {
	addr, ok := t.peers[peerID]
	if !ok {
		return RequestVoteResponse{}, fmt.Errorf("unknown peer: %s", peerID)
	}

	url := fmt.Sprintf("http://%s/raft/request-vote", addr)
	data, _ := json.Marshal(req)

	resp, err := http.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		return RequestVoteResponse{}, err
	}
	defer resp.Body.Close()

	var reply RequestVoteResponse
	json.NewDecoder(resp.Body).Decode(&reply)
	return reply, nil
}

func (t *Transport) SendAppendEntries(peerID string, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	addr, ok := t.peers[peerID]
	if !ok {
		return AppendEntriesResponse{}, fmt.Errorf("unknown peer: %s", peerID)
	}

	url := fmt.Sprintf("http://%s/raft/append-entries", addr)
	data, _ := json.Marshal(req)

	resp, err := http.Post(url, "application/json", bytes.NewReader(data))
	if err != nil {
		return AppendEntriesResponse{}, err
	}
	defer resp.Body.Close()

	var reply AppendEntriesResponse
	json.NewDecoder(resp.Body).Decode(&reply)
	return reply, nil
}

func (t *Transport) Start(addr string, node *Node) error {
	mux := http.NewServeMux()

	mux.HandleFunc("/raft/request-vote", func(w http.ResponseWriter, r *http.Request) {
		var req RequestVoteRequest
		json.NewDecoder(r.Body).Decode(&req)
		resp := node.RequestVote(req)
		json.NewEncoder(w).Encode(resp)
	})

	mux.HandleFunc("/raft/append-entries", func(w http.ResponseWriter, r *http.Request) {
		var req AppendEntriesRequest
		json.NewDecoder(r.Body).Decode(&req)
		resp := node.AppendEntries(req)
		json.NewEncoder(w).Encode(resp)
	})

	return http.ListenAndServe(addr, mux)
}
