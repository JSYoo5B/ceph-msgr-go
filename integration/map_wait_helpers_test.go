package integration_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

func nativeMonitorMapWait(t *testing.T, ctx context.Context, c *cephmsgr.Client, control, label string) cephmsgr.MonitorState {
	t.Helper()
	for {
		if err := testcluster.ControlDaemon(ctx, control, "verify", "monitor-map", label); err != nil {
			t.Fatal("fresh native MonMap", err)
		}
		data, err := os.ReadFile(filepath.Join(control, "monitor-oracle.json"))
		var oracle struct {
			Epoch   uint32                   `json:"epoch"`
			FSID    string                   `json:"fsid"`
			Members []cephmsgr.MonitorMember `json:"mons"`
		}
		if err != nil || json.Unmarshal(data, &oracle) != nil || oracle.Epoch == 0 {
			t.Fatal("native monitor metadata", err)
		}
		got, err := c.WaitMonMap(ctx, oracle.Epoch-1)
		if err != nil {
			t.Fatal("wait for native monitor epoch", err)
		}
		if got.MapEpoch != oracle.Epoch {
			continue
		} // Repeat only the independent read.
		if c.Snapshot().FSID != oracle.FSID || !reflect.DeepEqual(got.Members, oracle.Members) {
			t.Fatal("waited MON map differs from native map", got, oracle)
		}
		t.Logf("%s: waited MON epoch %d members match native MonMap", label, got.MapEpoch)
		return got
	}
}

// A Done observation establishes that the public wait reached its blocking
// select, rather than merely that its goroutine was scheduled.
type mapWaitObservedContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *mapWaitObservedContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}
