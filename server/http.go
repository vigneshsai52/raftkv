package server

import (
	"encoding/json"
	"net/http"
)

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	val, err := s.raftStore.Get(key)
	if err != nil {
		if err.Error() == "not leader" {
			leader := s.raftStore.Leader()
			http.Redirect(w, r, "http://"+leader+"/kv/"+key, http.StatusTemporaryRedirect)
			return
		}
		w.WriteHeader(http.StatusNotFound)
		return
	}
	json.NewEncoder(w).Encode(map[string]string{"value": val})
}

func (s *Server) handleSet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	var req struct{ Value string }
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if err := s.raftStore.Set(key, req.Value); err != nil {
		if err.Error() == "not leader" {
			leader := s.raftStore.Leader()
			http.Redirect(w, r, "http://"+leader+"/kv/"+key, http.StatusTemporaryRedirect)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if err := s.raftStore.Delete(key); err != nil {
		if err.Error() == "not leader" {
			leader := s.raftStore.Leader()
			http.Redirect(w, r, "http://"+leader+"/kv/"+key, http.StatusTemporaryRedirect)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
