package cephmsgr

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func fixtureFDCount() int {
	// Development-only Linux fixture metric; no product path uses /proc.
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return -1
	}
	return len(entries)
}

func TestCephRepeatedShutdownUnderTrafficIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_CONTROL_DIR") == "" {
		t.Skip("repeated shutdown requires the disposable fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	baselineWorkers, baselineFDs := runtime.NumGoroutine(), fixtureFDCount()
	runtime.GC()
	var initial runtime.MemStats
	runtime.ReadMemStats(&initial)
	for cycle := 0; cycle < 12; cycle++ {
		c, err := Dial(ctx, integrationOptions(t))
		if err != nil {
			t.Fatalf("bootstrap during shutdown cycle %d: %v", cycle+1, err)
		}
		var completed atomic.Int64
		ready := make(chan struct{})
		var readyOnce sync.Once
		var workers sync.WaitGroup
		for i := 0; i < 16; i++ {
			workers.Add(1)
			go func(i int) {
				defer workers.Done()
				call, prefix := c.MonCommand, "status"
				if i%2 != 0 {
					call, prefix = c.MgrCommand, "pg stat"
				}
				encoded, _ := json.Marshal(map[string]string{"prefix": prefix, "format": "json"})
				for c.ctx.Err() == nil && ctx.Err() == nil {
					callCtx, stop := context.WithTimeout(ctx, 2*time.Second)
					result, err := call(callCtx, Command{JSON: encoded})
					stop()
					if err != nil {
						var network *net.OpError
						closing := c.ctx.Err() != nil
						if !errors.Is(err, ErrClosed) && !(closing && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, net.ErrClosed) || errors.As(err, &network))) {
							t.Errorf("shutdown request: %v", err)
						}
						return
					}
					if !json.Valid(result.Data) {
						t.Error("invalid response while closing client")
						return
					}
					if completed.Add(1) >= 32 {
						readyOnce.Do(func() { close(ready) })
					}
				}
			}(i)
		}
		select {
		case <-ready:
		case <-ctx.Done():
			c.Close()
			t.Fatal("traffic did not start", ctx.Err())
		}
		closed := make(chan struct{})
		go func() { c.Close(); close(closed) }()
		select {
		case <-closed:
		case <-ctx.Done():
			t.Fatal("Close blocked under traffic", ctx.Err())
		}
		workers.Wait()
		c.Close()
		if after := fixtureFDCount(); baselineFDs >= 0 && after > baselineFDs+2 {
			t.Fatalf("file descriptors accumulate after cycle %d: before=%d after=%d", cycle+1, baselineFDs, after)
		}
	}
	runtime.GC()
	var final runtime.MemStats
	runtime.ReadMemStats(&final)
	if after := runtime.NumGoroutine(); after > baselineWorkers+4 {
		t.Errorf("workers remain after repeated Close: before=%d after=%d", baselineWorkers, after)
	}
	if final.HeapAlloc > initial.HeapAlloc+8<<20 {
		t.Errorf("heap retained across client shutdowns: before=%d after=%d", initial.HeapAlloc, final.HeapAlloc)
	}
	t.Logf("shutdown-cycles=12 baseline-fds=%d final-fds=%d heap-before=%d heap-after=%d", baselineFDs, fixtureFDCount(), initial.HeapAlloc, final.HeapAlloc)
}
