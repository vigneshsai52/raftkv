package store

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
)

type LogEntry struct {
	Index int64  `json:"index"`
	Term  int64  `json:"term"`
	Key   string `json:"key"`
	Value string `json:"value"`
	Op    string `json:"op"` // "set" or "delete"
}

type WAL struct {
	mu        sync.Mutex
	file      *os.File
	entries   []LogEntry
	nextIndex int64
}

func OpenWAL(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	wal := &WAL{
		file:      file,
		entries:   make([]LogEntry, 0),
		nextIndex: 1,
	}

	// Replay existing entries
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var entry LogEntry
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			continue
		}
		wal.entries = append(wal.entries, entry)
		wal.nextIndex = entry.Index + 1
	}

	return wal, nil
}

func (w *WAL) Append(entry LogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	entry.Index = w.nextIndex
	w.nextIndex++

	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	if _, err := w.file.Write(data); err != nil {
		return err
	}
	if _, err := w.file.WriteString("\n"); err != nil {
		return err
	}

	w.entries = append(w.entries, entry)
	return w.file.Sync() // fsync for durability
}

func (w *WAL) Replay(apply func(LogEntry)) error {
	for _, entry := range w.entries {
		apply(entry)
	}
	return nil
}

func (w *WAL) Close() error {
	return w.file.Close()
}
