package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/vigneshsai52/raftkv/server"
	"github.com/vigneshsai52/raftkv/store"
)

func main() {
	var (
		httpAddr = flag.String("http-addr", ":8080", "HTTP server address")
		dataDir  = flag.String("data-dir", "./data", "Directory for persistent data")
	)
	flag.Parse()

	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	walPath := filepath.Join(*dataDir, "wal.log")
	s, err := store.NewStoreWithWAL(walPath)
	if err != nil {
		log.Fatalf("Failed to open WAL: %v", err)
	}
	defer s.Close()

	fmt.Printf("RaftKV starting on %s...\n", *httpAddr)

	srv := server.New(s)
	log.Fatal(srv.Start(*httpAddr))
}
