package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// These are observation intervals, not simultaneous cluster snapshots. MgrMap
// availability advertises a manager; it does not prove digest delivery or commit.
type pruneMgrState struct {
	Epoch      uint64 `json:"epoch"`
	ActiveName string `json:"active_name"`
	ActiveGID  uint64 `json:"active_gid"`
	Available  *bool  `json:"available"`
}
type pruneObservation struct {
	Kind     string                         `json:"kind"`
	Started  time.Time                      `json:"started"`
	Ended    time.Time                      `json:"ended"`
	Deadline time.Time                      `json:"deadline"`
	Error    string                         `json:"error,omitempty"`
	Canceled bool                           `json:"canceled,omitempty"`
	Timeout  bool                           `json:"timeout"`
	Manager  *pruneMgrState                 `json:"manager,omitempty"`
	Trim     *copyPurgedTrimState           `json:"trim,omitempty"`
	Resume   *testcluster.DaemonLeaseRecord `json:"resume,omitempty"`
}
type pruneTimeline struct {
	Deadline     time.Time          `json:"deadline"`
	Observations []pruneObservation `json:"observations"`
	FirstFailure *pruneObservation  `json:"first_failure,omitempty"`
	Verdict      string             `json:"verdict"`
	Cleanup      *poolReceipt       `json:"pool_cleanup,omitempty"`
	mu           sync.Mutex
}

func (p *pruneTimeline) record(o pruneObservation) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Observations = append(p.Observations, o)
	if o.Error != "" && p.FirstFailure == nil {
		p.FirstFailure = &o
	}
}
func pruneRead(ctx context.Context, kind string, bound time.Duration, read func(context.Context, *pruneObservation) error) pruneObservation {
	ctx, cancel := context.WithTimeout(ctx, bound)
	defer cancel()
	o := pruneObservation{Kind: kind, Started: time.Now().UTC()}
	o.Deadline, _ = ctx.Deadline()
	err := read(ctx, &o)
	o.Ended = time.Now().UTC()
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		o.Error = err.Error()
		o.Manager = nil
		o.Canceled = errors.Is(err, context.Canceled)
	}
	o.Timeout = ctx.Err() == context.DeadlineExceeded
	return o
}

// The sampler is independent of the PG oracle, so a slow or failed PG read
// cannot suppress MgrMap observations. stop joins it before receipt serialization.
func (p *pruneTimeline) sampleManagers(ctx context.Context, bound, cadence time.Duration, read func(context.Context, *pruneObservation) error) func() {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for ctx.Err() == nil {
			o := pruneRead(ctx, "mgr-map", bound, read)
			// A caller stopping the sampler is not a new diagnostic failure.
			if ctx.Err() == context.Canceled && o.Canceled {
				return
			}
			p.record(o)
			timer := time.NewTimer(cadence)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
	}()
	return func() { cancel(); <-done }
}
func readPruneManager(ctx context.Context, c *cephmsgr.Client, o *pruneObservation) error {
	raw, err := c.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"mgr dump","format":"json"}`)})
	if err != nil {
		return err
	}
	var state pruneMgrState
	if err := json.Unmarshal(raw.Data, &state); err != nil {
		return err
	}
	if state.Epoch == 0 || state.Available == nil || (*state.Available && (state.ActiveName == "" || state.ActiveGID == 0)) {
		return fmt.Errorf("mgr dump lacks epoch or advertised availability")
	}
	o.Manager = &state
	return nil
}

// A delayed/missing reply must not outlive its per-read or enclosing deadline.
// An explicit unavailable map is data, whereas a timeout has no map at all.
func TestPruneTimelineBoundsAndRetainsFirstFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	p := &pruneTimeline{}
	unavailable := false
	p.record(pruneRead(ctx, "mgr-map", time.Second, func(ctx context.Context, o *pruneObservation) error {
		o.Manager = &pruneMgrState{Epoch: 7, Available: &unavailable}
		return nil
	}))
	blocked := func(ctx context.Context, o *pruneObservation) error { <-ctx.Done(); return ctx.Err() }
	p.record(pruneRead(ctx, "mgr-map", 5*time.Millisecond, blocked))
	p.record(pruneRead(ctx, "pruned", time.Second, func(ctx context.Context, o *pruneObservation) error {
		<-ctx.Done()
		o.Manager = &pruneMgrState{Epoch: 8, Available: &unavailable}
		return nil // a late successful callback is still past the read deadline.
	}))
	if len(p.Observations) != 3 || p.Observations[0].Error != "" || p.Observations[0].Manager == nil || *p.Observations[0].Manager.Available || p.Observations[1].Manager != nil || !p.Observations[1].Timeout || !p.Observations[2].Timeout || p.Observations[2].Manager != nil || p.FirstFailure.Kind != "mgr-map" {
		t.Fatalf("lost explicit unavailable, timeout or first failure: %+v", p)
	}
	parentDeadline, _ := ctx.Deadline()
	if p.Observations[2].Deadline.After(parentDeadline) {
		t.Fatal("read exceeded enclosing deadline")
	}
	if _, err := json.Marshal(p); err != nil {
		t.Fatal(err)
	}
}
func TestPruneManagerSamplerRunsDuringBlockedTrim(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	p := &pruneTimeline{}
	observed := make(chan struct{}, 8)
	stop := p.sampleManagers(ctx, 20*time.Millisecond, time.Millisecond, func(ctx context.Context, o *pruneObservation) error {
		select {
		case observed <- struct{}{}:
		default:
		}
		return nil
	})
	trim := pruneRead(ctx, "pruned", 100*time.Millisecond, func(ctx context.Context, o *pruneObservation) error {
		for i := 0; i < 3; i++ {
			select {
			case <-observed:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	stop()
	stop() // cleanup may join an already stopped sampler.
	if trim.Error != "" || len(p.Observations) < 2 {
		t.Fatalf("MGR sampling blocked behind trim: %+v", p)
	}
	for _, o := range p.Observations {
		if o.Error != "" {
			t.Fatal("normal stop became a failure", o)
		}
	}
}
