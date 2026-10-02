package cephmsgr

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
	"io"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCephRepeatedMonitorRecoveryIntegration(t *testing.T) {
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if control == "" {
		t.Skip("MON restart requires the disposable fixture")
	}
	baseline := runtime.NumGoroutine()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	c, err := Dial(ctx, integrationOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var calls, unknown atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				callCtx, stop := context.WithTimeout(ctx, 5*time.Second)
				result, err := c.MonCommand(callCtx, Command{JSON: []byte(`{"prefix":"status","format":"json"}`)})
				stop()
				var uncertain *OutcomeUnknownError
				if errors.As(err, &uncertain) {
					unknown.Add(1)
				} else if err == nil {
					if !json.Valid(result.Data) {
						t.Error("invalid MON output during recovery")
						cancel()
						return
					}
					calls.Add(1)
				} else if ctx.Err() == nil {
					var connection *net.OpError
					if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, net.ErrClosed) && !errors.As(err, &connection) {
						t.Errorf("unexpected MON recovery error: %v", err)
						cancel()
						return
					}
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(20 * time.Millisecond):
				}
			}
		}()
	}
	for i := 0; i < 8; i++ {
		c.mu.Lock()
		old := c.mon
		if old == nil {
			c.mu.Unlock()
			t.Error("no current MON during recovery")
			break
		}
		endpoint := old.RemoteAddr().String()
		c.mu.Unlock()
		_, port, err := net.SplitHostPort(endpoint)
		name := map[string]string{"33300": "a", "33301": "b", "33302": "c"}[port]
		if err != nil || name == "" {
			t.Errorf("unexpected fixture MON endpoint: %s", endpoint)
			break
		}
		if err := testcluster.RestartMonitor(ctx, control, name); err != nil {
			t.Errorf("restart MON %s: %v", name, err)
			break
		}
		select {
		case <-old.Done():
		case <-ctx.Done():
			t.Error("restarted MON did not close its previous session")
		}
		result, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"quorum_status","format":"json"}`)})
		if err != nil || !json.Valid(result.Data) {
			t.Errorf("MON recovery %d failed: %v", i+1, err)
			break
		}
		if _, err := c.MgrCommand(ctx, Command{JSON: []byte(`{"prefix":"pg stat","format":"json"}`)}); err != nil {
			t.Errorf("MGR after MON recovery %d: %v", i+1, err)
			break
		}
		t.Logf("MON restart=%d name=%s completed-calls=%d unknown=%d", i+1, name, calls.Load(), unknown.Load())
	}
	cancel()
	wg.Wait()
	c.Close()
	if calls.Load() == 0 {
		t.Error("no concurrent MON requests completed")
	}
	if after := runtime.NumGoroutine(); after > baseline+4 {
		t.Errorf("workers remain after repeated MON recovery: before=%d after=%d", baseline, after)
	}
}
