package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// Criticality Levels according to Google SRE Overload Standards
const (
	CriticalPlus = "CRITICAL_PLUS"
	Critical     = "CRITICAL"
	Sheddable    = "SHEDDABLE"
)

// RPC Request and Response structs
type WorkRequest struct {
	ID          string    `json:"id"`
	Criticality string    `json:"criticality"`
	Payload     string    `json:"payload"`
	CreatedAt   time.Time `json:"created_at"`
}

type WorkResponse struct {
	ID         string `json:"id"`
	Status     string `json:"status"`
	WorkerID   int    `json:"worker_id"`
	DurationMS int64  `json:"duration_ms"`
	Message    string `json:"message"`
}

// Server Configuration & State Metrics
type BackendServer struct {
	mu             sync.RWMutex
	workerCount    int
	maxQueueDepth  int
	workQueue      chan WorkRequest
	activeWorkers  int64
	queuedRequests int64
	totalReceived  uint64
	totalProcessed uint64
	totalShed      uint64
	totalExpired   uint64
	fragileMode    bool // Milestone 1 fragile mode flag
}

func NewBackendServer(workers int, queueDepth int) *BackendServer {
	return &BackendServer{
		workerCount:   workers,
		maxQueueDepth: queueDepth,
		workQueue:     make(chan WorkRequest, queueDepth),
		fragileMode:   false,
	}
}

// StartWorkerPool initializes goroutines to process incoming requests
func (s *BackendServer) StartWorkerPool(ctx context.Context) {
	for i := 1; i <= s.workerCount; i++ {
		go func(workerID int) {
			for {
				select {
				case <-ctx.Done():
					return
				case req, ok := <-s.workQueue:
					if !ok {
						return
					}
					atomic.AddInt64(&s.queuedRequests, -1)
					s.processWork(ctx, workerID, req)
				}
			}
		}(i)
	}
}

// processWork simulates database or heavy compute operations
func (s *BackendServer) processWork(ctx context.Context, workerID int, req WorkRequest) {
	atomic.AddInt64(&s.activeWorkers, 1)
	defer atomic.AddInt64(&s.activeWorkers, -1)

	// Check deadline before starting work (Avoid Doing Uncredited Work)
	if err := ctx.Err(); err != nil && !s.fragileMode {
		atomic.AddUint64(&s.totalExpired, 1)
		log.Printf("[Server] [Worker %d] Context expired before processing Request %s. Dropping.", workerID, req.ID)
		return
	}

	start := time.Now()

	// Simulate IO/DB Latency (50ms base + 20ms jitter)
	processingTime := time.Duration(50+rand.Intn(20)) * time.Millisecond
	
	select {
	case <-time.After(processingTime):
		// Work completed
		atomic.AddUint64(&s.totalProcessed, 1)
		duration := time.Since(start).Milliseconds()
		log.Printf("[Server] [Worker %d] Completed Request %s (%s) in %dms", workerID, req.ID, req.Criticality, duration)
	case <-ctx.Done():
		// Client cancelled or deadline hit mid-processing
		if !s.fragileMode {
			atomic.AddUint64(&s.totalExpired, 1)
			log.Printf("[Server] [Worker %d] Context cancelled during execution for Request %s", workerID, req.ID)
			return
		}
		// Fragile mode: ignores context cancellation and continues burning CPU!
		time.Sleep(processingTime)
		atomic.AddUint64(&s.totalProcessed, 1)
	}
}

// HTTP Handler with Criticality Shedding and Queue Management
func (s *BackendServer) handleProcess(w http.ResponseWriter, r *http.Request) {
	atomic.AddUint64(&s.totalReceived, 1)

	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	var req WorkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	// 1. Check Queue Overflow
	currentQueue := atomic.LoadInt64(&s.queuedRequests)
	activeWorkers := atomic.LoadInt64(&s.activeWorkers)
	utilization := float64(activeWorkers) / float64(s.workerCount)

	// Milestone 3: Criticality Load Shedding
	// Rejection condition: Utilization > 85% and criticality is SHEDDABLE
	if !s.fragileMode && utilization >= 0.85 && req.Criticality == Sheddable {
		atomic.AddUint64(&s.totalShed, 1)
		w.Header().Set("X-SRE-Do-Not-Retry", "true")
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(WorkResponse{
			ID:      req.ID,
			Status:  "REJECTED_SHEDDABLE",
			Message: "Server overloaded: Sheddable load rejected",
		})
		return
	}

	// 2. Queue Full Rejection
	if currentQueue >= int64(s.maxQueueDepth) {
		atomic.AddUint64(&s.totalShed, 1)
		w.Header().Set("X-SRE-Do-Not-Retry", "true")
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(WorkResponse{
			ID:      req.ID,
			Status:  "REJECTED_QUEUE_FULL",
			Message: "Server overloaded: Queue depth exceeded",
		})
		return
	}

	// Enqueue Work
	atomic.AddInt64(&s.queuedRequests, 1)
	s.workQueue <- req

	// Wait for processing or context timeout
	select {
	case <-ctx.Done():
		if !s.fragileMode {
			w.WriteHeader(http.StatusGatewayTimeout)
			json.NewEncoder(w).Encode(WorkResponse{
				ID:      req.ID,
				Status:  "TIMEOUT",
				Message: "Client deadline exceeded while queued",
			})
			return
		}
	default:
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(WorkResponse{
			ID:      req.ID,
			Status:  "ACCEPTED",
			Message: "Request enqueued successfully",
		})
	}
}

// Mode Toggle Handler for Chaos Demonstrations
func (s *BackendServer) handleToggleFragile(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.fragileMode = !s.fragileMode
	current := s.fragileMode
	s.mu.Unlock()

	mode := "RESILIENT"
	if current {
		mode = "FRAGILE"
	}
	fmt.Fprintf(w, "Switched mode to: %s", mode)
	log.Printf("[Server] Mode updated to %s", mode)
}

func main() {
	server := NewBackendServer(10, 50) // 10 Workers, Max Queue Depth 50
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server.StartWorkerPool(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/process", server.handleProcess)
	mux.HandleFunc("/admin/toggle-fragile", server.handleToggleFragile)

	httpServer := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	go func() {
		log.Println("[Server] Backend listening on :8080...")
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	// Graceful shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	log.Println("[Server] Shutting down gracefully...")
	httpServer.Shutdown(ctx)
}
