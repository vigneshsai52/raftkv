package server

import (
	"net/http"

	"github.com/vigneshsai52/raftkv/store"
)

type Server struct {
	store *store.Store
}

func New(s *store.Store) *Server {
	return &Server{store: s}
}
func (s *Server) Start(addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /kv/{key}", s.handleGet)
	mux.HandleFunc("POST /kv/{key}", s.handleSet)
	mux.HandleFunc("DELETE /kv/{key}", s.handleDelete)
	return http.ListenAndServe(addr, mux)
}
