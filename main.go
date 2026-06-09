package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/vigneshsai52/raftkv/raft"
	"github.com/vigneshsai52/raftkv/server"
	"github.com/vigneshsai52/raftkv/store"
)

func main() {
	var (
		httpAddr = flag.String("http-addr", ":8080", "HTTP server address")
		dataDir  = flag.String("data-dir", "./data", "Directory for persistent data")
		nodeID   = flag.String("node-id", "node1", "Unique node identifier")
		peers    = flag.String("peers", "node1", "Comma-separated list of peers")
	)
	flag.Parse()

	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	peerList := []string{*nodeID}
	if *peers != "" && *peers != *nodeID {
		peerList = append(peerList, *peers)
	}

	raftNode := raft.NewNode(*nodeID, peerList)
	go raftNode.Run()

	walPath := filepath.Join(*dataDir, "wal.log")
	s, err := store.NewStoreWithWAL(walPath)
	if err != nil {
		log.Fatalf("Failed to open WAL: %v", err)
	}
	defer s.Close()

	raftStore := raft.NewRaftStore(raftNode, s)

	fmt.Printf("RaftKV starting on %s (node: %s)...\n", *httpAddr, *nodeID)

	srv := server.New(raftStore)
	log.Fatal(srv.Start(*httpAddr))
}
