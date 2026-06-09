package main

import (
	"flag"
	"fmt"
	"log"
	"os"

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

	fmt.Printf("RaftKV starting on %s...\n", *httpAddr)

	s := store.New()
	srv := server.New(s)
	log.Fatal(srv.Start(*httpAddr))
}
