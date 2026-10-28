package cephmsgr

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/session"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// Native holders outlive their control requests. The second registration is
// created only after the first watch's actual MON has stopped, so receiving it
// on that same watch cannot be satisfied by a buffered pre-fault snapshot.
func TestCephMonServiceMapLifecycleIntegration(t *testing.T) {
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if control == "" {
		t.Skip("ServiceMap lifecycle requires the disposable fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	c, err := Dial(ctx, integrationOptions(t))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	initial := c.Snapshot()
	watch, err := c.WatchServiceMap(ctx, ServiceMapOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer watch.Close()

	tokenA := serviceLifecycleToken(t)
	tokenB := serviceLifecycleToken(t)
	for _, token := range []string{tokenA, tokenB} {
		t.Cleanup(func() {
			cleanup, stop := context.WithTimeout(context.Background(), 90*time.Second)
			defer stop()
			r, err := observeNativeServiceHolder(cleanup, control, token, "cleanup")
			if err != nil {
				t.Error("native holder cleanup", err)
				return
			}
			if r.Holder != nil && r.Holder.Alive && !r.Killed {
				t.Error("native holder survived cleanup")
			}
			t.Logf("holder cleanup state=%s killed=%t restored=%v", r.State, r.Killed, r.Restored != nil && *r.Restored)
		})
	}
	startA, readyA := startServiceLifecycleHolder(t, ctx, control, tokenA)
	nativeA := waitServiceHolder(t, ctx, control, tokenA, "registered A", func(r nativeServiceHolderReceipt) bool {
		return serviceHolderPresent(r, readyA) && r.Holder != nil && r.Holder.Alive
	})
	first := waitServiceLifecycleMap(t, ctx, watch, func(m ServiceMap) bool {
		return serviceLifecyclePresent(m, nativeA, readyA)
	})
	c.mu.Lock()
	old := watch.source
	c.mu.Unlock()
	if old == nil || old.Err() != nil {
		t.Fatal("watch has no live registered MON source")
	}
	_, role, err := testcluster.SplitPeer(old.RemoteAddr().String())
	name := strings.TrimPrefix(role, "mon.")
	if err != nil || !slices.Contains([]string{"a", "b", "c"}, name) {
		t.Fatal("watch source is not an owned MON", role, err)
	}
	stopped := false
	defer func() {
		if !stopped {
			return
		}
		restore, stop := context.WithTimeout(context.Background(), 180*time.Second)
		defer stop()
		if err := testcluster.ControlDaemon(restore, control, "start", "mon", name); err != nil {
			t.Error("restore stopped watch MON", err)
			return
		}
		if err := serviceLifecycleQuorum(restore, control, initial); err != nil {
			t.Error("restore full native quorum", err)
		}
	}()
	// Ownership precedes the request: an uncertain stop still needs restoration.
	stopped = true
	if err := testcluster.ControlDaemon(ctx, control, "stop", "mon", name); err != nil {
		t.Fatal("stop watch's MON", err)
	}
	select {
	case <-old.Done():
	case <-ctx.Done():
		t.Fatal("stopped MON connection did not end", ctx.Err())
	}
	if err := waitServiceLifecycleSource(ctx, watch, old); err != nil {
		t.Fatal(err)
	}
	// The native holder must remain alive through the real stop/recovery.
	heldA, err := observeNativeServiceHolder(ctx, control, tokenA, "observe")
	if err != nil || !serviceHolderPresent(heldA, readyA) || heldA.Holder == nil || !heldA.Holder.Alive {
		t.Fatal("A was not held across connected-MON recovery", err)
	}
	startB, readyB := startServiceLifecycleHolder(t, ctx, control, tokenB)
	nativeB := waitServiceHolder(t, ctx, control, tokenB, "post-recovery B", func(r nativeServiceHolderReceipt) bool {
		return serviceHolderPresent(r, readyB) && r.Holder != nil && r.Holder.Alive
	})
	recovered := waitServiceLifecycleMap(t, ctx, watch, func(m ServiceMap) bool {
		return m.Epoch > first.Epoch && serviceLifecyclePresent(m, heldA, readyA) && serviceLifecyclePresent(m, nativeB, readyB)
	})
	if c.Snapshot().GlobalID != initial.GlobalID || c.Snapshot().FSID != initial.FSID {
		t.Fatal("client identity changed across watch recovery")
	}
	t.Logf("same watch recovered after orderly mon.%s stop: Go epochs=%d->%d native epochs=%d/%d holders GID=%d/%d", name, first.Epoch, recovered.Epoch, *heldA.View.Epoch, *nativeB.View.Epoch, readyA.GID, readyB.GID)

	releasedAt := time.Now()
	for _, token := range []string{tokenA, tokenB} {
		r, err := observeNativeServiceHolder(ctx, control, token, "release")
		if err != nil || r.State != "released" || r.Exited == nil || r.Exited.Reason != "released" || !r.Exited.ShutdownReturned {
			t.Fatal("native holder release did not return a released shutdown", err)
		}
	}
	absenceCtx, stopAbsence := context.WithTimeout(ctx, 120*time.Second)
	defer stopAbsence()
	var nativeAbsent [2]nativeServiceHolderReceipt
	for i, token := range []string{tokenA, tokenB} {
		var since time.Time
		nativeAbsent[i] = waitServiceHolder(t, absenceCtx, control, token, "stable native removal", func(r nativeServiceHolderReceipt) bool {
			if !serviceHolderAbsent(r) {
				since = time.Time{}
				return false
			}
			if since.IsZero() {
				since = time.Now()
			}
			return time.Since(since) >= 2*time.Second
		})
	}
	removed := waitServiceLifecycleMap(t, absenceCtx, watch, func(m ServiceMap) bool {
		return serviceLifecycleAbsent(m, recovered.Epoch, startA.Daemon, startB.Daemon)
	})
	for _, token := range []string{tokenA, tokenB} {
		r, err := observeNativeServiceHolder(ctx, control, token, "cleanup")
		if err != nil || !serviceHolderRestored(r) || r.Holder == nil || r.Holder.Alive {
			t.Fatal("released holder baseline was not restored without a kill", err)
		}
	}
	if err := testcluster.ControlDaemon(ctx, control, "start", "mon", name); err != nil {
		t.Fatal("restart watch MON", err)
	}
	if err := serviceLifecycleQuorum(ctx, control, initial); err != nil {
		t.Fatal(err)
	}
	stopped = false
	t.Logf("bounded removal: Go epoch=%d native epochs=%d/%d elapsed=%s; holders absent, no kill, full native quorum restored", removed.Epoch, *nativeAbsent[0].View.Epoch, *nativeAbsent[1].View.Epoch, time.Since(releasedAt))
}

func serviceLifecycleToken(t *testing.T) string {
	t.Helper()
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%x", token)
}

func startServiceLifecycleHolder(t *testing.T, ctx context.Context, control, token string) (nativeServiceHolderReceipt, nativeServiceHolderReady) {
	t.Helper()
	r, err := observeNativeServiceHolder(ctx, control, token, "start")
	if err != nil {
		t.Fatal("start native holder", err)
	}
	ready, err := serviceHolderReady(r)
	if err != nil {
		t.Fatal(err)
	}
	return r, ready
}

func serviceLifecyclePresent(m ServiceMap, r nativeServiceHolderReceipt, ready nativeServiceHolderReady) bool {
	if !serviceHolderPresent(r, ready) || m.Epoch == 0 || len(m.Raw) == 0 {
		return false
	}
	d, found := m.Services[r.Service].Daemons[r.Daemon]
	return found && d.GID == ready.GID && d.StartEpoch > 0 && d.StartEpoch <= m.Epoch &&
		d.Metadata["ceph_msgr_role"] == "holder" && d.Metadata["ceph_msgr_daemon"] == r.Daemon &&
		(m.Epoch != *r.View.Epoch || r.View.Entry.StartEpoch != nil && d.StartEpoch == *r.View.Entry.StartEpoch)
}

func serviceLifecycleAbsent(m ServiceMap, previous uint32, daemons ...string) bool {
	if m.Epoch <= previous || len(m.Raw) == 0 {
		return false
	}
	for _, name := range daemons {
		if _, found := m.Services["ceph-msgr"].Daemons[name]; found {
			return false
		}
	}
	return true
}

func waitServiceLifecycleMap(t *testing.T, ctx context.Context, watch *ServiceMapStream, accept func(ServiceMap) bool) ServiceMap {
	t.Helper()
	for {
		m, err := watch.Next(ctx)
		if err != nil {
			t.Fatal("ServiceMap lifecycle wait", err)
		}
		if accept(m) {
			return m
		}
	}
}

func waitServiceLifecycleSource(ctx context.Context, watch *ServiceMapStream, old *session.Session) error {
	c := watch.client
	for {
		c.mu.Lock()
		current := c.serviceMapWatch == watch && watch.source != nil && watch.source != old && c.mon == watch.source && c.monReady && watch.terminal == nil
		changed := c.changed
		c.mu.Unlock()
		if current {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("same watch did not register on a different admitted MON: %w", ctx.Err())
		case <-changed:
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// The fixture's existing native barrier verifies the authenticated monmap and
// full owned quorum at one unchanged election epoch over two separated reads.
func serviceLifecycleQuorum(ctx context.Context, control string, initial State) error {
	quorum := make([]string, len(initial.Monitor.Members))
	for i, m := range initial.Monitor.Members {
		quorum[i] = m.Name
	}
	slices.Sort(quorum)
	type expectation struct {
		FSID    string          `json:"fsid"`
		Epoch   uint32          `json:"epoch"`
		Members []MonitorMember `json:"members"`
		Quorum  []string        `json:"quorum"`
		Label   string          `json:"label"`
	}
	expected := expectation{initial.FSID, initial.Monitor.MapEpoch, initial.Monitor.Members, quorum, "restarted"}
	input, err := json.Marshal(expected)
	if err != nil {
		return err
	}
	if err := testcluster.RequestControl(ctx, control, "wait-monitor-election", "monitor-election-settled", string(input)); err != nil {
		return err
	}
	data, err := os.ReadFile(filepath.Join(control, "monitor-election-settled.json"))
	var receipt struct {
		SchemaVersion int             `json:"schema_version"`
		Expected      json.RawMessage `json:"expected"`
		First, Second struct {
			FSID          string          `json:"fsid"`
			MapEpoch      uint32          `json:"map_epoch"`
			Members       []MonitorMember `json:"members"`
			Quorum        []string        `json:"quorum"`
			ElectionEpoch uint64          `json:"election_epoch"`
			ObservedAt    time.Time       `json:"observed_at"`
		}
	}
	if err != nil || len(data) > 16<<10 || json.Unmarshal(data, &receipt) != nil || receipt.SchemaVersion != 1 {
		return errors.New("missing bounded native quorum receipt")
	}
	var observedExpected expectation
	if json.Unmarshal(receipt.Expected, &observedExpected) != nil || !reflect.DeepEqual(observedExpected, expected) {
		return errors.New("native quorum receipt expectation differs")
	}
	for _, r := range []int{0, 1} {
		observation := receipt.First
		if r == 1 {
			observation = receipt.Second
		}
		if observation.FSID != initial.FSID || observation.MapEpoch != initial.Monitor.MapEpoch || !reflect.DeepEqual(observation.Members, initial.Monitor.Members) || !slices.Equal(observation.Quorum, quorum) || observation.ElectionEpoch == 0 || observation.ObservedAt.IsZero() {
			return errors.New("native quorum observation differs from owned topology")
		}
	}
	if receipt.First.ElectionEpoch != receipt.Second.ElectionEpoch || receipt.Second.ObservedAt.Sub(receipt.First.ObservedAt) < 2*time.Second {
		return errors.New("native quorum was not stable over two separated observations")
	}
	return nil
}
