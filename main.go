package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/vigneshsai52/raftkv/raft"
	"github.com/vigneshsai52/raftkv/server"
	"github.com/vigneshsai52/raftkv/store"
)

func main() {
	var (
		httpAddr = flag.String("http-addr", ":8080", "HTTP server address")
		raftAddr = flag.String("raft-addr", ":12000", "Raft RPC address")
		dataDir  = flag.String("data-dir", "./data", "Directory for persistent data")
		nodeID   = flag.String("node-id", "node1", "Unique node identifier")
		peersStr = flag.String("peers", "", "Comma-separated: node2:host:port,node3:host:port")
	)
	flag.Parse()

	if err := os.MkdirAll(*dataDir, 0755); err != nil {
		log.Fatalf("Failed to create data directory: %v", err)
	}

	// Parse peers
	peers := []string{*nodeID}
	peerAddrs := make(map[string]string)

	if *peersStr != "" {
		for _, p := range strings.Split(*peersStr, ",") {
			parts := strings.Split(p, ":")
			if len(parts) == 3 {
				id := parts[0]
				addr := parts[1] + ":" + parts[2]
				peers = append(peers, id)
				peerAddrs[id] = addr
			}
		}
	}

	// Create transport
	transport := raft.NewTransport(*nodeID)
	for id, addr := range peerAddrs {
		transport.AddPeer(id, addr)
	}

	// Create Raft node
	raftNode := raft.NewNode(*nodeID, peers, transport)
	go raftNode.Run()

	// Start Raft RPC server
	go func() {
		log.Printf("Starting Raft RPC on %s", *raftAddr)
		if err := transport.Start(*raftAddr, raftNode); err != nil {
			log.Fatalf("Raft RPC failed: %v", err)
		}
	}()

	// Wait for Raft to initialize
	time.Sleep(100 * time.Millisecond)

	// Create store
	walPath := filepath.Join(*dataDir, "wal.log")
	s, err := store.NewStoreWithWAL(walPath)
	if err != nil {
		log.Fatalf("Failed to open WAL: %v", err)
	}
	defer s.Close()

	// Create Raft-aware store
	raftStore := raft.NewRaftStore(raftNode, s)

	fmt.Printf("RaftKV starting on %s (node: %s, raft: %s)...\n", *httpAddr, *nodeID, *raftAddr)

	// Start HTTP server
	srv := server.New(raftStore)
	log.Fatal(srv.Start(*httpAddr))
}
