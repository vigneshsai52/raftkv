package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"sync"
	"time"
)

func main() {
	var (
		addr    = flag.String("addr", "http://localhost:8080", "Server address")
		workers = flag.Int("workers", 10, "Number of concurrent workers")
		ops     = flag.Int("ops", 10000, "Total operations")
		mode    = flag.String("mode", "mixed", "Benchmark mode: write, read, or mixed")
	)
	flag.Parse()

	var wg sync.WaitGroup
	start := time.Now()

	opsPerWorker := *ops / *workers
	writeErrors := 0
	readErrors := 0
	var mu sync.Mutex

	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			client := &http.Client{Timeout: 5 * time.Second}

			for j := 0; j < opsPerWorker; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				val := fmt.Sprintf("value-%d-%d", id, j)

				// WRITE (POST) - only if mode is write or mixed
				if *mode == "write" || *mode == "mixed" {
					body, _ := json.Marshal(map[string]string{"value": val})
					resp, err := client.Post(*addr+"/kv/"+key, "application/json", bytes.NewReader(body))
					if err != nil || resp.StatusCode != 201 {
						mu.Lock()
						writeErrors++
						mu.Unlock()
					}
					if resp != nil {
						resp.Body.Close()
					}
				}

				// READ (GET) - only if mode is read or mixed
				if *mode == "read" || *mode == "mixed" {
					resp, err := client.Get(*addr + "/kv/" + key)
					if err != nil || resp.StatusCode != 200 {
						mu.Lock()
						readErrors++
						mu.Unlock()
					}
					if resp != nil {
						resp.Body.Close()
					}
				}
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("Benchmark Results:\n")
	fmt.Printf("  Server:       %s\n", *addr)
	fmt.Printf("  Mode:         %s\n", *mode)
	fmt.Printf("  Workers:      %d\n", *workers)
	fmt.Printf("  Total ops:    %d\n", *ops)
	fmt.Printf("  Write errors: %d\n", writeErrors)
	fmt.Printf("  Read errors:  %d\n", readErrors)
	fmt.Printf("  Duration:     %v\n", elapsed)
	fmt.Printf("  Ops/sec:      %.2f\n", float64(*ops)/elapsed.Seconds())
	fmt.Printf("  Latency avg:  %.2f ms\n", float64(elapsed.Milliseconds())/float64(*ops))
}
