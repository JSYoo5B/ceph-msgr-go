package cephmsgr

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// The lifecycle cell's Go watch can only deliver what the MON holds. The MON's
// ServiceMap advances only when the active MGR reports a dirty pending map
// (Ceph v20.2.4: DaemonServer marks it dirty on a daemon's registration or
// removal, src/mgr/DaemonServer.cc:609-618 and 705-706, and sends it with
// MMonMgrReport; MgrStatMonitor::prepare_report commits it, src/mon/MgrStatMonitor.cc).
// `service dump` is a MGR command, so a native holder present there proves
// only the MGR's map. A stall of the MGR's own MON session (for example a
// stock MGR whose CephX ticket renewal fails after the leader MON stops)
// therefore looks like a Go watch that missed an epoch unless the MON is read
// independently. These consumer helpers are test-only.

// serviceLifecycleStall is what was independently observed when a lifecycle
// map did not arrive: the last epoch the Go watch delivered, the last map a
// fresh MON session delivered and whether it holds the wanted state, and the
// MGR-answered `service dump` epoch.
type serviceLifecycleStall struct {
	Watch    uint32
	MON      uint32
	MONHolds bool
	MONErr   error
	MGR      uint32
	MGRErr   error
}

// String classifies the stall. Only a fresh MON map that holds the wanted
// state blames the Go watch; a MON behind the MGR names the stalled stock
// MGR->MON report; anything unread or contradictory stays unclassified.
func (s serviceLifecycleStall) String() string {
	observed := fmt.Sprintf("Go watch last epoch %d, fresh MON session epoch %d, MGR service dump epoch %d", s.Watch, s.MON, s.MGR)
	switch {
	case s.MONErr != nil || s.MGRErr != nil || s.MON == 0 || s.MGR == 0:
		return fmt.Sprintf("unclassified (%s; MON read: %v; MGR read: %v)", observed, s.MONErr, s.MGRErr)
	case s.MONHolds:
		return fmt.Sprintf("Go watch missed an epoch: a fresh MON session holds the wanted map (%s)", observed)
	case s.MON < s.MGR:
		return fmt.Sprintf("MON ServiceMap not advancing: the MON is behind the MGR, stock MGR->MON report stalled (%s)", observed)
	default:
		return fmt.Sprintf("neither the MON nor the MGR holds the wanted state (%s)", observed)
	}
}

// serviceLifecycleMonMap reads the MON's ServiceMap on a fresh client session,
// independent of the watch under test, until a map satisfies accept or bound
// ends. It returns the last map read and whether it satisfied accept.
func serviceLifecycleMonMap(t *testing.T, ctx context.Context, bound time.Duration, accept func(ServiceMap) bool) (ServiceMap, bool, error) {
	t.Helper()
	read, cancel := context.WithTimeout(ctx, bound)
	defer cancel()
	c, err := Dial(read, integrationOptions(t))
	if err != nil {
		return ServiceMap{}, false, err
	}
	defer c.Close()
	watch, err := c.WatchServiceMap(read, ServiceMapOptions{})
	if err != nil {
		return ServiceMap{}, false, err
	}
	defer watch.Close()
	var last ServiceMap
	for {
		m, err := watch.Next(read)
		if err != nil {
			if last.Epoch != 0 && errors.Is(err, context.DeadlineExceeded) {
				return last, false, nil
			}
			return last, false, err
		}
		last = m
		if accept(m) {
			return m, true, nil
		}
	}
}

// serviceLifecycleObserve gathers a stall: a fresh MON read bounded by bound
// and the MGR-answered epoch of the holder token, on a context of its own so
// that it also runs after the cell's context ended.
func serviceLifecycleObserve(t *testing.T, control, token string, watch uint32, bound time.Duration, accept func(ServiceMap) bool) serviceLifecycleStall {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), bound+30*time.Second)
	defer cancel()
	s := serviceLifecycleStall{Watch: watch}
	var m ServiceMap
	m, s.MONHolds, s.MONErr = serviceLifecycleMonMap(t, ctx, bound, accept)
	s.MON = m.Epoch
	r, err := observeNativeServiceHolder(ctx, control, token, "observe")
	switch {
	case err != nil:
		s.MGRErr = err
	case r.View == nil || r.View.Epoch == nil:
		s.MGRErr = errors.New("service dump view without an epoch")
	default:
		s.MGR = *r.View.Epoch
	}
	return s
}

// serviceLifecycleWatch is the lifecycle cell's wait on its own watch. A
// failed wait is reported with the independent observation of
// serviceLifecycleObserve, so a MON that never advanced is not reported as
// a watch fault.
func serviceLifecycleWatch(t *testing.T, ctx context.Context, watch *ServiceMapStream, control, token string, accept func(ServiceMap) bool) ServiceMap {
	t.Helper()
	var last uint32
	for {
		m, err := watch.Next(ctx)
		if err != nil {
			stall := serviceLifecycleObserve(t, control, token, last, 15*time.Second, accept)
			t.Fatalf("ServiceMap lifecycle watch: %v; %s", err, stall)
		}
		last = m.Epoch
		if accept(m) {
			return m
		}
	}
}

func TestServiceLifecycleStallClassification(t *testing.T) {
	for _, c := range []struct {
		name  string
		stall serviceLifecycleStall
		want  string
	}{
		{"watch missed", serviceLifecycleStall{Watch: 7, MON: 9, MONHolds: true, MGR: 9}, "Go watch missed an epoch"},
		{"watch missed with the MGR ahead", serviceLifecycleStall{Watch: 7, MON: 9, MONHolds: true, MGR: 10}, "Go watch missed an epoch"},
		{"MON behind the MGR", serviceLifecycleStall{Watch: 7, MON: 7, MGR: 9}, "MON ServiceMap not advancing"},
		{"MON and MGR agree without the state", serviceLifecycleStall{Watch: 7, MON: 9, MGR: 9}, "neither the MON nor the MGR"},
		{"MON ahead of a stale MGR read", serviceLifecycleStall{Watch: 7, MON: 10, MGR: 9}, "neither the MON nor the MGR"},
		{"MON unread", serviceLifecycleStall{Watch: 7, MONErr: errors.New("dial"), MGR: 9}, "unclassified"},
		{"MON empty", serviceLifecycleStall{Watch: 7, MGR: 9}, "unclassified"},
		{"MGR unread", serviceLifecycleStall{Watch: 7, MON: 7, MGRErr: errors.New("oracle")}, "unclassified"},
		{"MGR unread although the MON holds it", serviceLifecycleStall{Watch: 7, MON: 9, MONHolds: true, MGRErr: errors.New("oracle")}, "unclassified"},
	} {
		got := c.stall.String()
		if !strings.HasPrefix(got, c.want) {
			t.Errorf("%s: %q, want prefix %q", c.name, got, c.want)
		}
		if !strings.Contains(got, fmt.Sprintf("Go watch last epoch %d", c.stall.Watch)) {
			t.Errorf("%s: %q omits the watch epoch", c.name, got)
		}
	}
}
