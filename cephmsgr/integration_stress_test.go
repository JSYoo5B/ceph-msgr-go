package cephmsgr

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/internal/session"
)

func TestCephStressIntegration(t *testing.T) {
	value := os.Getenv("CEPH_MSGR_STRESS_DURATION")
	if value == "" || os.Getenv("CEPH_MSGR_CONTROL_DIR") == "" {
		t.Skip("set CEPH_MSGR_STRESS_DURATION for the disposable fixture")
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration < 45*time.Second {
		t.Fatal("stress duration must be at least 45s")
	}
	baseline := runtime.NumGoroutine()
	c, err := Dial(context.Background(), integrationOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	var calls, faults, uncertain atomic.Int64
	var causeMu sync.Mutex
	unknownCauses := make(map[string]int64)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			call, prefix := c.MonCommand, "status"
			if i%2 != 0 {
				call, prefix = c.MgrCommand, "pg stat"
			}
			encoded, _ := json.Marshal(map[string]string{"prefix": prefix, "format": "json"})
			for ctx.Err() == nil {
				callCtx, stop := context.WithTimeout(ctx, 5*time.Second)
				result, err := call(callCtx, Command{JSON: encoded})
				stop()
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					var unknown *OutcomeUnknownError
					var connection *net.OpError
					cause := ""
					switch {
					case errors.Is(err, ErrManagerChanged):
						cause = "MGR-changed"
					case errors.Is(err, session.ErrRetired):
						cause = "MON-retired"
					case errors.Is(err, ErrKeepaliveTimeout):
						cause = "keepalive-timeout"
					case errors.Is(err, context.DeadlineExceeded):
						cause = "deadline"
					case errors.As(err, &connection), errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, net.ErrClosed):
						cause = "transport"
					}
					// Unknown execution does not make a protocol, crypto or
					// authentication failure acceptable during this workload.
					if cause == "" {
						t.Errorf("unexpected %s error under load: %v", prefix, err)
						cancel()
						return
					}
					if errors.As(err, &unknown) {
						uncertain.Add(1)
						causeMu.Lock()
						unknownCauses[cause]++
						causeMu.Unlock()
					}
				} else if !json.Valid(result.Data) {
					t.Errorf("invalid %s output", prefix)
					cancel()
					return
				} else {
					calls.Add(1)
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(20 * time.Millisecond):
				}
			}
		}(i)
	}
	previous := c.snapshotAuth()
	renewals := 0
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	started := time.Now()
	sampledMaxSessions, sampledMaxWorkers := 0, 0
	sampleResources := func() (int, int) {
		c.mu.Lock()
		active := len(c.sessions)
		c.mu.Unlock()
		workers := runtime.NumGoroutine()
		if active > sampledMaxSessions {
			sampledMaxSessions = active
		}
		if workers > sampledMaxWorkers {
			sampledMaxWorkers = workers
		}
		return active, workers
	}
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
		case <-ticker.C:
			current := c.snapshotAuth()
			for service, old := range previous.Tickets {
				if current.Tickets[service].Expires.After(old.Expires) {
					renewals++
					previous = current
					break
				}
			}
			c.mu.Lock()
			mgrName := c.mgrMap.Name
			c.mu.Unlock()
			active, _ := sampleResources()
			if active > 4 {
				t.Errorf("sessions accumulate across recovery: %d", active)
				cancel()
			}
			// Repeated MGR takeover occurs while reads and ticket renewals run.
			if time.Since(started) >= time.Duration(faults.Load()+1)*30*time.Second {
				cmd, _ := json.Marshal(map[string]string{"prefix": "mgr fail", "who": mgrName})
				if _, err := c.MonCommand(ctx, Command{JSON: cmd}); err != nil && ctx.Err() == nil {
					t.Errorf("MGR fault injection: %v", err)
					cancel()
				}
				faults.Add(1)
				active, workers := sampleResources()
				t.Logf("elapsed=%s calls=%d renewals=%d MGR-faults=%d unknown=%d sessions=%d goroutines=%d", time.Since(started).Round(time.Second), calls.Load(), renewals, faults.Load(), uncertain.Load(), active, workers)
			}
		}
	}
	wg.Wait()
	c.Close()
	if renewals < 3 || calls.Load() < 100 || faults.Load() < 1 {
		t.Errorf("insufficient exercise: calls=%d renewals=%d faults=%d", calls.Load(), renewals, faults.Load())
	}
	if after := runtime.NumGoroutine(); after > baseline+4 {
		t.Errorf("workers remain after Close: before=%d after=%d", baseline, after)
	}
	t.Logf("calls=%d ticket-renewals=%d MGR-faults=%d unknown-outcomes=%d sampled-max-sessions=%d sampled-max-goroutines=%d unknown-causes=%v", calls.Load(), renewals, faults.Load(), uncertain.Load(), sampledMaxSessions, sampledMaxWorkers, unknownCauses)
}
