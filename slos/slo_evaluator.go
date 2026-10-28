package main

import (
	"fmt"
	"math"
	"time"
)

type EndpointMetrics struct {
	Endpoint      string
	TotalRequests int
	GoodRequests  int
	P95LatencyMs  float64
}

type MinuteWindow struct {
	WindowID  int
	Timestamp time.Time
	Metrics   map[string]EndpointMetrics
}

type EndpointThreshold struct {
	MinAvailability float64
	MaxP95LatencyMs float64
}

type SLOConfig struct {
	TargetCompositeSLO float64
	EndpointRules      map[string]EndpointThreshold
}

type WindowEvaluationResult struct {
	WindowID       int
	IsGoodWindow   bool
	FailedEndpoint string
	FailureReason  string
}

type OperationalStatus string

const (
	StatusOK          OperationalStatus = "STATUS_OK: Normal Operations & Releases Allowed"
	StatusWarning     OperationalStatus = "STATUS_WARNING: High Error Budget Burn Rate"
	StatusFreezeDeploys OperationalStatus = "FREEZE_RELEASES: Error Budget Exhausted! Feature Freeze Engaged"
)

type OverallSLOResult struct {
	TotalWindows      int
	GoodWindows       int
	BadWindows        int
	AchievedSLO       float64
	AllowedBadWindows int
	RemainingBudget   int
	PolicyStatus      OperationalStatus
	FailuresByReason  map[string]int
}

func EvaluateMinuteWindow(window MinuteWindow, config SLOConfig) WindowEvaluationResult {
	for ep, rule := range config.EndpointRules {
		metrics, exists := window.Metrics[ep]
		if !exists || metrics.TotalRequests == 0 {
			return WindowEvaluationResult{
				WindowID:       window.WindowID,
				IsGoodWindow:   false,
				FailedEndpoint: ep,
				FailureReason:  "Omission / Missing Telemetry",
			}
		}

		avail := float64(metrics.GoodRequests) / float64(metrics.TotalRequests)
		if avail < rule.MinAvailability {
			return WindowEvaluationResult{
				WindowID:       window.WindowID,
				IsGoodWindow:   false,
				FailedEndpoint: ep,
				FailureReason:  fmt.Sprintf("Availability Low (%.2f%% < %.2f%%)", avail*100, rule.MinAvailability*100),
			}
		}

		if metrics.P95LatencyMs > rule.MaxP95LatencyMs {
			return WindowEvaluationResult{
				WindowID:       window.WindowID,
				IsGoodWindow:   false,
				FailedEndpoint: ep,
				FailureReason:  fmt.Sprintf("Latency High (%.1fms > %.1fms)", metrics.P95LatencyMs, rule.MaxP95LatencyMs),
			}
		}
	}

	return WindowEvaluationResult{
		WindowID:     window.WindowID,
		IsGoodWindow: true,
	}
}

func EvaluateCompositeSLO(windows []MinuteWindow, config SLOConfig) OverallSLOResult {
	totalWindows := len(windows)
	goodWindows := 0
	badWindows := 0
	failuresByReason := make(map[string]int)

	for _, window := range windows {
		res := EvaluateMinuteWindow(window, config)
		if res.IsGoodWindow {
			goodWindows++
		} else {
			badWindows++
			key := fmt.Sprintf("%s (%s)", res.FailedEndpoint, res.FailureReason)
			failuresByReason[key]++
		}
	}

	achievedSLO := 0.0
	if totalWindows > 0 {
		achievedSLO = float64(goodWindows) / float64(totalWindows)
	}

	allowedBad := int(math.Floor(float64(totalWindows) * (1.0 - config.TargetCompositeSLO)))
	remainingBudget := allowedBad - badWindows

	var status OperationalStatus
	if remainingBudget < 0 {
		status = StatusFreezeDeploys
	} else if remainingBudget <= int(float64(allowedBad)*0.20) {
		status = StatusWarning
	} else {
		status = StatusOK
	}

	return OverallSLOResult{
		TotalWindows:      totalWindows,
		GoodWindows:       goodWindows,
		BadWindows:        badWindows,
		AchievedSLO:       achievedSLO,
		AllowedBadWindows: allowedBad,
		RemainingBudget:   remainingBudget,
		PolicyStatus:      status,
		FailuresByReason:  failuresByReason,
	}
}

func main() {
	config := SLOConfig{
		TargetCompositeSLO: 0.990, // 99% composite SLO
		EndpointRules: map[string]EndpointThreshold{
			"/api/createAccount": {MinAvailability: 0.990, MaxP95LatencyMs: 500.0},
			"/api/authenticate":  {MinAvailability: 0.990, MaxP95LatencyMs: 200.0},
			"/api/syncData":      {MinAvailability: 0.999, MaxP95LatencyMs: 100.0},
		},
	}

	// Generate 1000 simulated 1-minute windows
	windows := make([]MinuteWindow, 1000)
	startTime := time.Now().Add(-1000 * time.Minute)

	for i := 0; i < 1000; i++ {
		wTime := startTime.Add(time.Duration(i) * time.Minute)
		metrics := make(map[string]EndpointMetrics)

		// Base Healthy Telemetry
		metrics["/api/createAccount"] = EndpointMetrics{"/api/createAccount", 100, 100, 250.0}
		metrics["/api/authenticate"] = EndpointMetrics{"/api/authenticate", 1000, 998, 120.0}
		metrics["/api/syncData"] = EndpointMetrics{"/api/syncData", 100000, 99950, 45.0}

		// Inject Faults:
		if i >= 100 && i < 250 { // 150 windows with latency fault on /api/syncData
			m := metrics["/api/syncData"]
			m.P95LatencyMs = 150.0 // Exceeds 100ms threshold
			metrics["/api/syncData"] = m
		}

		if i >= 500 && i < 550 { // 50 windows with availability fault on /api/createAccount
			m := metrics["/api/createAccount"]
			m.GoodRequests = 80 // 80% availability < 99% target
			metrics["/api/createAccount"] = m
		}

		windows[i] = MinuteWindow{
			WindowID:  i + 1,
			Timestamp: wTime,
			Metrics:   metrics,
		}
	}

	result := EvaluateCompositeSLO(windows, config)

	fmt.Println("===============================================================")
	fmt.Println("       GAME! SRE SLO & ERROR BUDGET EVALUATION REPORT     ")
	fmt.Println("===============================================================")
	fmt.Printf("Target Composite SLO    : %.2f%%\n", config.TargetCompositeSLO*100)
	fmt.Printf("Total Windows Evaluated : %d\n", result.TotalWindows)
	fmt.Printf("Good Windows (Passed)   : %d\n", result.GoodWindows)
	fmt.Printf("Bad Windows (Failed)    : %d\n", result.BadWindows)
	fmt.Println()
	fmt.Printf("Achieved Composite SLO  : %.2f%%\n", result.AchievedSLO*100)
	fmt.Printf("Allowed Bad Windows     : %d\n", result.AllowedBadWindows)
	fmt.Printf("Remaining Error Budget  : %d Bad Minutes\n", result.RemainingBudget)
	fmt.Println()
	fmt.Println("---------------------------------------------------------------")
	fmt.Println("FAILURE BREAKDOWN BY ENDPOINT & REASON:\n")
	for reason, count := range result.FailuresByReason {
		fmt.Printf("  - %s: %d Bad Minutes\n", reason, count)
	}
	fmt.Println()
	fmt.Println("---------------------------------------------------------------")
	fmt.Println("OPERATIONAL POLICY DECISION:")
	fmt.Printf("  >>> %s <<<\n", result.PolicyStatus)
	fmt.Println("===============================================================")
}
Reply ready.
