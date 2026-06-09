package server

import (
	"encoding/json"
	"net/http"
)

func (s *Server) StartMetrics(addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", s.handleMetrics)
	return http.ListenAndServe(addr, mux)
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	// Simple metrics response
	metrics := map[string]interface{}{
		"raft_state":    s.raftStore.State(),
		"kv_keys_total": 0, // Would need to track this
		"kv_get_total":  0,
		"kv_set_total":  0,
	}
	json.NewEncoder(w).Encode(metrics)
}
