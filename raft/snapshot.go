package raft

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type Snapshot struct {
	LastIndex int64
	LastTerm  int64
	Data      []byte
}

func (n *Node) CreateSnapshot(dataDir string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if len(n.Log) == 0 {
		return nil
	}

	snap := Snapshot{
		LastIndex: n.commitIndex,
		LastTerm:  n.CurrentTerm,
	}

	// Serialize log entries up to commit index
	entries := make([]LogEntry, n.commitIndex)
	copy(entries, n.Log[:n.commitIndex])
	entriesData, err := json.Marshal(entries)
	if err != nil {
		return err
	}
	snap.Data = entriesData

	// Save to file
	snapPath := filepath.Join(dataDir, "snapshot.dat")
	file, err := os.Create(snapPath)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(snap); err != nil {
		return err
	}

	// Truncate log
	n.Log = n.Log[n.commitIndex:]
	fmt.Printf("[%s] Created snapshot at index %d\n", n.ID, snap.LastIndex)

	return nil
}

func (n *Node) InstallSnapshot(dataDir string) error {
	snapPath := filepath.Join(dataDir, "snapshot.dat")
	file, err := os.Open(snapPath)
	if err != nil {
		return err
	}
	defer file.Close()

	var snap Snapshot
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&snap); err != nil {
		return err
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	var entries []LogEntry
	if err := json.Unmarshal(snap.Data, &entries); err != nil {
		return err
	}

	n.Log = entries
	n.commitIndex = snap.LastIndex
	n.lastApplied = snap.LastIndex

	fmt.Printf("[%s] Installed snapshot at index %d\n", n.ID, snap.LastIndex)
	return nil
}

func (n *Node) MaybeSnapshot(dataDir string) {
	n.mu.Lock()
	logLen := len(n.Log)
	n.mu.Unlock()

	if logLen > 10000 {
		n.CreateSnapshot(dataDir)
	}
}
