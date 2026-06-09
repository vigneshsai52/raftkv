package main

import (
	"flag"
	"fmt"
	"log"
	"os"
)

func main() {
	var (
		httpAddr = flag.String("http-addr", ":8080", "HTTP server address")
		dataDir  = flag.String("data-dir", "./data", "Directory for persistent data")
		raftAddr = flag.String("raft-addr", ":12000", "Raft consensus address")
		nodeID   = flag.String("node-id", "node1", "Unique node identifier")
		peers    = flag.String("peers", "", "Comma-separated list of peer addresses")
	)
	flag.Parse()

	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	fmt.Printf("RaftKV starting...\n")
	fmt.Printf("  Node ID:   %s\n", *nodeID)
	fmt.Printf("  HTTP:      %s\n", *httpAddr)
	fmt.Printf("  Raft:      %s\n", *raftAddr)
	fmt.Printf("  Data Dir:  %s\n", *dataDir)
	fmt.Printf("  Peers:     %s\n", *peers)

	// TODO: Initialize store, raft, and HTTP server
	select {}
}
