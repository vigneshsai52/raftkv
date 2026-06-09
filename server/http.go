package server

import (
	"encoding/json"
	"net/http"
)

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if val, ok := s.store.Get(key); ok {
		json.NewEncoder(w).Encode(map[string]string{"value": val})
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

func (s *Server) handleSet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	var req struct{ Value string }
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if err := s.store.Set(key, req.Value); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if err := s.store.Delete(key); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
