package raft

import (
	"errors"
)

type RaftStore struct {
	raft  *Node
	store interface {
		Get(key string) (string, bool)
		Set(key, value string) error
		Delete(key string) error
	}
	applyCh chan LogEntry
}

func NewRaftStore(raftNode *Node, store interface {
	Get(key string) (string, bool)
	Set(key, value string) error
	Delete(key string) error
}) *RaftStore {
	rs := &RaftStore{
		raft:    raftNode,
		store:   store,
		applyCh: make(chan LogEntry, 100),
	}
	// Don't start apply loop - it causes deadlocks
	// go rs.runApplyLoop()
	return rs
}

func (rs *RaftStore) Set(key, value string) error {
	entry := LogEntry{
		Key:   key,
		Value: value,
		Op:    "set",
	}
	if err := rs.raft.SubmitBatch([]LogEntry{entry}); err != nil {
		return err
	}
	return rs.store.Set(key, value)
}

func (rs *RaftStore) Get(key string) (string, error) {
	if rs.raft.State != Leader {
		return "", errors.New("not leader")
	}
	val, ok := rs.store.Get(key)
	if !ok {
		return "", errors.New("key not found")
	}
	return val, nil
}

func (rs *RaftStore) Delete(key string) error {
	entry := LogEntry{
		Key: key,
		Op:  "delete",
	}
	if err := rs.raft.SubmitBatch([]LogEntry{entry}); err != nil {
		return err
	}
	return rs.store.Delete(key)
}

func (rs *RaftStore) Leader() string {
	if rs.raft.State == Leader {
		return rs.raft.ID
	}
	if len(rs.raft.Peers) > 0 {
		return rs.raft.Peers[0]
	}
	return ""
}

func (rs *RaftStore) State() int {
	return int(rs.raft.State)
}
