package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type TestMetrics struct {
	TotalSent      uint64
	ThrottledLocal uint64
	Accepted       uint64
	Rejected       uint64
	Timeouts       uint64
}

func main() {
	client := NewResilientRPCClient(2.0, 0.10) // K = 2.0, Max 10% Retries
	targetURL := "http://localhost:8080/api/v1/process"

	log.Println("=== Starting Cascading Failure Load Generator ===")
	log.Println("Simulating: Baseline 50 QPS -> Spike 250 QPS -> Recovery")

	metrics := &TestMetrics{}

	// Step 1: Send Baseline Traffic (50 QPS for 5 Seconds)
	runTrafficPhase("Phase 1: Baseline (50 QPS)", client, targetURL, 50, 5*time.Second, metrics)

	// Step 2: Inject Overload Spike (250 QPS for 5 Seconds)
	runTrafficPhase("Phase 2: Overload Spike (250 QPS)", client, targetURL, 250, 5*time.Second, metrics)

	// Step 3: Cool down back to Baseline (50 QPS for 5 Seconds)
	runTrafficPhase("Phase 3: Cool Down (50 QPS)", client, targetURL, 50, 5*time.Second, metrics)

	printSummaryReport(metrics)
}

func runTrafficPhase(phaseName string, client *ResilientRPCClient, url string, qps int, duration time.Duration, m *TestMetrics) {
	fmt.Printf("\n--- Running %s ---\n", phaseName)
	ticker := time.NewTicker(time.Second / time.Duration(qps))
	defer ticker.Stop()

	stopTime := time.Now().Add(duration)
	var wg sync.WaitGroup

	reqID := 0
	for time.Now().Before(stopTime) {
		<-ticker.C
		reqID++
		wg.Add(1)

		go func(id int) {
			defer wg.Done()
			atomic.AddUint64(&m.TotalSent, 1)

			crit := Critical
			if id%3 == 0 {
				crit = Sheddable
			}

			req := WorkRequest{
				ID:          fmt.Sprintf("req-%d", id),
				Criticality: crit,
				Payload:     "benchmark-data",
				CreatedAt:   time.Now(),
			}

			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			resp, err := client.ExecuteRPC(ctx, url, req)
			if err != nil {
				if err.Error() == "RPC_DROPPED_BY_CLIENT_THROTTLER" {
					atomic.AddUint64(&m.ThrottledLocal, 1)
				} else {
					atomic.AddUint64(&m.Rejected, 1)
				}
				return
			}

			if resp.Status == "ACCEPTED" {
				atomic.AddUint64(&m.Accepted, 1)
			} else {
				atomic.AddUint64(&m.Rejected, 1)
			}
		}(reqID)
	}
	wg.Wait()
}

func printSummaryReport(m *TestMetrics) {
	fmt.Println("\n==============================================")
	fmt.Println("       CASCADING FAILURE LAB METRICS          ")
	fmt.Println("==============================================")
	fmt.Printf("Total Requests Sent:        %d\n", m.TotalSent)
	fmt.Printf("Client Locally Throttled:  %d (%.2f%%)\n", m.ThrottledLocal, float64(m.ThrottledLocal)/float64(m.TotalSent)*100)
	fmt.Printf("Backend Accepted:          %d (%.2f%%)\n", m.Accepted, float64(m.Accepted)/float64(m.TotalSent)*100)
	fmt.Printf("Backend Rejections/Timeouts: %d (%.2f%%)\n", m.Rejected, float64(m.Rejected)/float64(m.TotalSent)*100)
	fmt.Println("==============================================")
}
