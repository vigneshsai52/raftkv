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
	go rs.runApplyLoop()
	return rs
}

func (rs *RaftStore) Set(key, value string) error {
	// Only leader can write
	if rs.raft.State != Leader {
		return errors.New("not leader")
	}
	entry := LogEntry{
		Key:   key,
		Value: value,
		Op:    "set",
	}
	if err := rs.raft.Submit(entry); err != nil {
		return err
	}
	return rs.store.Set(key, value)
}

func (rs *RaftStore) Get(key string) (string, error) {
	// Allow reads on ANY node (eventual consistency)
	val, ok := rs.store.Get(key)
	if !ok {
		return "", errors.New("key not found")
	}
	return val, nil
}

func (rs *RaftStore) Delete(key string) error {
	// Only leader can delete
	if rs.raft.State != Leader {
		return errors.New("not leader")
	}
	entry := LogEntry{
		Key: key,
		Op:  "delete",
	}
	if err := rs.raft.Submit(entry); err != nil {
		return err
	}
	return rs.store.Delete(key)
}

func (rs *RaftStore) runApplyLoop() {
	for entry := range rs.raft.GetApplyCh() {
		switch entry.Op {
		case "set":
			rs.store.Set(entry.Key, entry.Value)
		case "delete":
			rs.store.Delete(entry.Key)
		}
	}
}

func (rs *RaftStore) Leader() string {
	if rs.raft.State == Leader {
		return rs.raft.ID
	}
	// Find leader from peers
	for _, peer := range rs.raft.Peers {
		if peer != rs.raft.ID {
			return peer
		}
	}
	return ""
}

func (rs *RaftStore) State() int {
	return int(rs.raft.State)
}
