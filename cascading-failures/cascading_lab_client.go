package main

import (
        "bytes"
        "context"
        "encoding/json"
        "fmt"
        "io"
        "log"
        "math"
        "math/rand"
        "net/http"
        "sync"
        "sync/atomic"
        "time"
)

// AdaptiveThrottler implements Google SRE's Adaptive Client-Side Throttling formula:
// P_drop = max(0, (requests - K * accepts) / (requests + 1))
type AdaptiveThrottler struct {
        mu          sync.Mutex
        kMultiplier float64
        requests    uint64
        accepts     uint64
        dropped     uint64
}

func NewAdaptiveThrottler(k float64) *AdaptiveThrottler {
        return &AdaptiveThrottler{
                kMultiplier: k, // Recommended default K = 2.0
        }
}

// ShouldThrottle returns true if the request should be dropped locally
func (at *AdaptiveThrottler) ShouldThrottle() bool {
        at.mu.Lock()
        defer at.mu.Unlock()

        atomic.AddUint64(&at.requests, 1)

        reqs := float64(atomic.LoadUint64(&at.requests))
        accs := float64(atomic.LoadUint64(&at.accepts))

        dropProb := math.Max(0, (reqs-at.kMultiplier*accs)/(reqs+1.0))

        if rand.Float64() < dropProb {
                atomic.AddUint64(&at.dropped, 1)
                return true
        }
        return false
}

func (at *AdaptiveThrottler) RecordAccept() {
        atomic.AddUint64(&at.accepts, 1)
}

// ResilientRPCClient executes requests with retries, jitter, and throttling
type ResilientRPCClient struct {
        httpClient    *http.Client
        throttler     *AdaptiveThrottler
        totalRetries  uint64
        totalRequests uint64
        maxRetryRatio float64 // e.g. 0.10 (10% max retries)
        enabled       bool    // Flag to enable/disable resilience for testing
}

func NewResilientRPCClient(kMultiplier float64, maxRetryRatio float64) *ResilientRPCClient {
        return &ResilientRPCClient{
                httpClient: &http.Client{
                        Timeout: 2 * time.Second, // TODO: paramaterize this in a FLAG
                },
                throttler:     NewAdaptiveThrottler(kMultiplier),
                maxRetryRatio: maxRetryRatio,
                enabled:       true,
        }
}
func (c *ResilientRPCClient) ExecuteRPC(parentCtx context.Context, targetURL string, reqPayload WorkRequest) (*WorkResponse, error) {
        atomic.AddUint64(&c.totalRequests, 1)

        // 1. Check Adaptive Client-Side Throttling
        if c.enabled && c.throttler.ShouldThrottle() {
                return nil, fmt.Errorf("RPC_DROPPED_BY_CLIENT_THROTTLER")
        }

        maxAttempts := 3
        if !c.enabled {
                maxAttempts = 10 // Fragile mode: unbounded retries
        }

        var lastErr error
        for attempt := 0; attempt < maxAttempts; attempt++ {
                // Check per-client retry budget (Max 10% of total traffic can be retries)
                if attempt > 0 && c.enabled {
                        currentRetries := atomic.LoadUint64(&c.totalRetries)
                        totalReqs := atomic.LoadUint64(&c.totalRequests)
                        if float64(currentRetries)/float64(totalReqs) > c.maxRetryRatio {
                                return nil, fmt.Errorf("RETRY_BUDGET_EXCEEDED (Retries > 10%%)")
                        }
                        atomic.AddUint64(&c.totalRetries, 1)

                        // Exponential Backoff with Jitter: T = 2^attempt * 20ms + rand(0, 20ms)
                        backoff := time.Duration(math.Pow(2, float64(attempt))*20)*time.Millisecond + time.Duration(rand.Intn(20))*time.Millisecond
                        time.Sleep(backoff)
                }

                // Propagate Context Deadline
                ctx, cancel := context.WithTimeout(parentCtx, 500*time.Millisecond)
                bodyBytes, _ := json.Marshal(reqPayload)

                httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewBuffer(bodyBytes))
                if err != nil {
                        cancel()
                        lastErr = err
                        continue
                }
                httpReq.Header.Set("Content-Type", "application/json")

                resp, err := c.httpClient.Do(httpReq)
                if err != nil {
                        cancel()
                        lastErr = err
                        continue
                }

                // Check explicit Do-Not-Retry header from backend
                doNotRetry := resp.Header.Get("X-SRE-Do-Not-Retry") == "true"

                body, _ := io.ReadAll(resp.Body)
                resp.Body.Close()
                cancel()

                if resp.StatusCode == http.StatusOK {
                        c.throttler.RecordAccept()
                        var workResp WorkResponse
                        json.Unmarshal(body, &workResp)
                        return &workResp, nil
                }

                if resp.StatusCode == http.StatusServiceUnavailable && doNotRetry && c.enabled {
                        return nil, fmt.Errorf("BACKEND_OVERLOADED_DO_NOT_RETRY")
                }

                lastErr = fmt.Errorf("HTTP_%d: %s", resp.StatusCode, string(body))
        }

        return nil, fmt.Errorf("MAX_RETRIES_EXCEEDED: %v", lastErr)
}

func main() {
        log.Println("[Client] Resilient RPC Client initialized.")
}
