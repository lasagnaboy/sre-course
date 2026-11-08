package main


import (
	"bytes"
	"context"
	"flag"
	"fmt"


	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)


type ChaosConfig struct {
	TargetURL   string
	Mode        string
	Concurrency int
	Duration    time.Duration
	RateLimit   int
}


type ExperimentStats struct {
	TotalRequests uint64
	SuccessCount  uint64
	ErrorCount    uint64
	LatencySumMs  uint64
}


func main() {
	target := flag.String("target", "http://localhost:8080", "Target base URL of the frontend service")
	mode := flag.String("mode", "latency_spike", "Chaos mode: latency_spike | error_burst | connection_leak | bimodal_timeout")
	concurrency := flag.Int("concurrency", 10, "Number of concurrent worker goroutines")
	duration := flag.Duration("duration", 30*time.Second, "Duration of the chaos test run")
	rate := flag.Int("rate", 50, "Target overall QPS")
	flag.Parse()


	config := ChaosConfig{
		TargetURL:   *target,
		Mode:        *mode,
		Concurrency: *concurrency,
		Duration:    *duration,
		RateLimit:   *rate,
	}


	fmt.Println("===============================================================")
	fmt.Println("       GOOGLE SRE IMAGESERVER CHAOS INJECTION ENGINE           ")
	fmt.Println("===============================================================")
	fmt.Printf("Target Service : %s\n", config.TargetURL)
	fmt.Printf("Chaos Mode     : %s\n", config.Mode)
	fmt.Printf("Concurrency    : %d goroutines\n", config.Concurrency)
	fmt.Printf("Duration       : %s\n", config.Duration)
	fmt.Println("---------------------------------------------------------------")


	stats := &ExperimentStats{}
	ctx, cancel := context.WithTimeout(context.Background(), config.Duration)
	defer cancel()


	var wg sync.WaitGroup
	ticker := time.NewTicker(time.Second / time.Duration(config.RateLimit))
	defer ticker.Stop()


	startTime := time.Now()


	for i := 0; i < config.Concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			client := &http.Client{
				Timeout: 12 * time.Second,
			}


			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					executeChaosRequest(ctx, client, config, workerID, stats)
				}
			}
		}(i)
	}


	wg.Wait()
	totalTime := time.Since(startTime)


	fmt.Println("---------------------------------------------------------------")
	fmt.Println("CHAOS EXPERIMENT COMPLETED")
	fmt.Printf("Total Elapsed Time : %.2fs\n", totalTime.Seconds())
	fmt.Printf("Total Requests     : %d\n", stats.TotalRequests)
	fmt.Printf("Successful (2xx)   : %d\n", stats.SuccessCount)
	fmt.Printf("Failed (5xx/Err)   : %d\n", stats.ErrorCount)
	if stats.TotalRequests > 0 {
		avgLatency := float64(stats.LatencySumMs) / float64(stats.TotalRequests)
		fmt.Printf("Average Latency    : %.2f ms\n", avgLatency)
		errRate := (float64(stats.ErrorCount) / float64(stats.TotalRequests)) * 100.0
		fmt.Printf("Effective Error Rate: %.2f%%\n", errRate)
	}
	fmt.Println("===============================================================")
}


func executeChaosRequest(
	ctx context.Context,
	client *http.Client,
	config ChaosConfig,
	workerID int,
	stats *ExperimentStats,
) {
	atomic.AddUint64(&stats.TotalRequests, 1)
	reqStart := time.Now()


	var endpoint string
	var req *http.Request
	var err error


	// Select endpoint and inject headers based on Chaos Mode
	switch config.Mode {
	case "latency_spike":
		// Target Download service with injected latency header
		endpoint = fmt.Sprintf("%s/api/download?image_id=img_1024", config.TargetURL)
		req, err = http.NewRequestWithContext(ctx, "GET", endpoint, nil)
		if rand.Float32() < 0.30 { // 30% of requests trigger latency injection
			req.Header.Set("X-Chaos-Simulate-Delay", "2500ms")
		}


	case "error_burst":
		// Target Upload service with fault injection header
		endpoint = fmt.Sprintf("%s/api/upload", config.TargetURL)
		payload := []byte(`{"image_name":"test.png", "data":"base64_stream..."}`)
		req, err = http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewBuffer(payload))
		req.Header.Set("Content-Type", "application/json")
		if rand.Float32() < 0.40 { // 40% error injection
			req.Header.Set("X-Chaos-Inject-Error", "500_INTERNAL")
		}


	case "connection_leak":
		// Target Search service with heavy query and leak header
		endpoint = fmt.Sprintf("%s/api/search?tag=landscape&depth=deep", config.TargetURL)
		req, err = http.NewRequestWithContext(ctx, "GET", endpoint, nil)
		req.Header.Set("X-Chaos-Leak-Connection", "true")


	case "bimodal_timeout":
		// Target Download service with 5% catastrophic 10-second hang
		endpoint = fmt.Sprintf("%s/api/download?image_id=img_9999", config.TargetURL)
		req, err = http.NewRequestWithContext(ctx, "GET", endpoint, nil)
		if rand.Float32() < 0.05 { // 5% bimodal hang
			req.Header.Set("X-Chaos-Simulate-Hang", "10000ms")
		}


	default:
		endpoint = fmt.Sprintf("%s/api/search?tag=general", config.TargetURL)
		req, err = http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	}


	if err != nil {
		atomic.AddUint64(&stats.ErrorCount, 1)
		return
	}


	// Attach trace header to simulate frontend client trace
	req.Header.Set("X-Trace-ID", fmt.Sprintf("chaos-trace-%d-%d", time.Now().UnixNano(), workerID))


	resp, err := client.Do(req)
	elapsed := time.Since(reqStart).Milliseconds()
	atomic.AddUint64(&stats.LatencySumMs, uint64(elapsed))


	if err != nil || resp == nil || resp.StatusCode >= 500 {
		atomic.AddUint64(&stats.ErrorCount, 1)
	} else {
		atomic.AddUint64(&stats.SuccessCount, 1)
	}


	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
}
