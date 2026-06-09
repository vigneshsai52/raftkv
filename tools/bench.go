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
	)
	flag.Parse()

	var wg sync.WaitGroup
	start := time.Now()

	opsPerWorker := *ops / *workers
	errors := 0
	var mu sync.Mutex

	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			client := &http.Client{Timeout: 5 * time.Second}

			for j := 0; j < opsPerWorker; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				val := fmt.Sprintf("value-%d-%d", id, j)

				// SET
				body, _ := json.Marshal(map[string]string{"value": val})
				resp, err := client.Post(*addr+"/kv/"+key, "application/json", bytes.NewReader(body))
				if err != nil || resp.StatusCode != 201 {
					mu.Lock()
					errors++
					mu.Unlock()
					continue
				}

				// GET
				resp, err = client.Get(*addr + "/kv/" + key)
				if err != nil || resp.StatusCode != 200 {
					mu.Lock()
					errors++
					mu.Unlock()
				}
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("Benchmark Results:\n")
	fmt.Printf("  Total ops:    %d\n", *ops)
	fmt.Printf("  Errors:       %d\n", errors)
	fmt.Printf("  Duration:     %v\n", elapsed)
	fmt.Printf("  Ops/sec:      %.2f\n", float64(*ops)/elapsed.Seconds())
	fmt.Printf("  Latency avg:  %.2f ms\n", float64(elapsed.Milliseconds())/float64(*ops))
}
