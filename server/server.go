package server

import (
	"net/http"
)

type Server struct {
	raftStore interface {
		Get(key string) (string, error)
		Set(key, value string) error
		Delete(key string) error
		Leader() string
		State() int
	}
}

func New(raftStore interface {
	Get(key string) (string, error)
	Set(key, value string) error
	Delete(key string) error
	Leader() string
	State() int
}) *Server {
	return &Server{raftStore: raftStore}
}

func (s *Server) Start(addr string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /kv/{key}", s.handleGet)
	mux.HandleFunc("POST /kv/{key}", s.handleSet)
	mux.HandleFunc("DELETE /kv/{key}", s.handleDelete)
	return http.ListenAndServe(addr, mux)
}
