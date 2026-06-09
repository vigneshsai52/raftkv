package store

import "sync"

type Store struct {
	mu   sync.RWMutex
	data map[string]string
	wal  *WAL
}

func New() *Store {
	return &Store{data: make(map[string]string)}
}

func NewStoreWithWAL(path string) (*Store, error) {
	wal, err := OpenWAL(path)
	if err != nil {
		return nil, err
	}

	s := &Store{
		data: make(map[string]string),
		wal:  wal,
	}

	// Replay WAL on startup
	wal.Replay(func(e LogEntry) {
		s.apply(e)
	})

	return s, nil
}

func (s *Store) apply(e LogEntry) {
	if e.Op == "set" {
		s.data[e.Key] = e.Value
	} else {
		delete(s.data, e.Key)
	}
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.data[key]
	return val, ok
}

func (s *Store) Set(key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry := LogEntry{
		Key:   key,
		Value: value,
		Op:    "set",
	}

	if err := s.wal.Append(entry); err != nil {
		return err
	}

	s.data[key] = value
	return nil
}

func (s *Store) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry := LogEntry{
		Key: key,
		Op:  "delete",
	}

	if err := s.wal.Append(entry); err != nil {
		return err
	}

	delete(s.data, key)
	return nil
}

func (s *Store) Close() error {
	if s.wal != nil {
		return s.wal.Close()
	}
	return nil
}
