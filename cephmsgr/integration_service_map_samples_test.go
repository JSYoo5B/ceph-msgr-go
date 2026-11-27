package cephmsgr

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// What a failed ServiceMap read of the lifecycle cell is reported with. The
// MON's ServiceMap advances when the active MGR reports a dirty pending map
// (Ceph v20.2.4: DaemonServer marks it dirty on a daemon's registration or
// removal, src/mgr/DaemonServer.cc:609-618 and 705-706, and
// MgrStatMonitor::prepare_report commits it, src/mon/MgrStatMonitor.cc).
// `service dump` is answered by the MGR, but from its ClusterState copy
// (DaemonServer.cc:1682-1687), which only Mgr::handle_service_map sets on a
// MServiceMap received from a MON (src/mgr/Mgr.cc:567-572,
// src/mgr/ClusterState.cc:62-65). Its epoch is therefore a map some MON
// already published, not the MGR's unreported pending map.
//
// The observation is taken after the watch's read ended, on a fresh client
// session that may use another MON, and the MGR is sampled at yet another
// time. None of the three samples overlaps the original read, so the report
// states what each one saw and when, and names no cause: a map the fresh
// session reads later may have been published after the watch's deadline,
// and a fresh session behind the MGR's copy says nothing about the MGR's
// report to the MON. These consumer helpers are test-only.

// serviceLifecycleSamples is a failed read of the cell's watch and the later
// independent samples.
type serviceLifecycleSamples struct {
	// Watch is the last epoch the stream delivered in any phase of the cell;
	// Ended is when the failed read returned.
	Watch uint32
	Ended time.Time
	// MON is the last map epoch a fresh session read between MONStart and
	// MONEnd, MONHolds whether that map holds the wanted state, MONErr a
	// failed dial, subscription or read.
	MON              uint32
	MONHolds         bool
	MONStart, MONEnd time.Time
	MONErr           error
	// MGR is the epoch of the MGR's received copy (`service dump`), sampled
	// at MGRAt.
	MGR    uint32
	MGRAt  time.Time
	MGRErr error
}

// String reports the samples without a causal classification.
func (s serviceLifecycleSamples) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "watch last delivered epoch %d, its read ended %s; ", s.Watch, s.Ended.UTC().Format(time.RFC3339Nano))
	switch {
	case s.MONErr != nil:
		fmt.Fprintf(&b, "later fresh MON session unread (%v)", s.MONErr)
	case s.MON == 0:
		b.WriteString("later fresh MON session delivered no map")
	default:
		state := "without the wanted state"
		if s.MONHolds {
			state = "holding the wanted state"
		}
		fmt.Fprintf(&b, "later fresh MON session read epoch %d %s between %s and %s", s.MON, state,
			s.MONStart.UTC().Format(time.RFC3339Nano), s.MONEnd.UTC().Format(time.RFC3339Nano))
	}
	b.WriteString("; ")
	if s.MGRErr != nil {
		fmt.Fprintf(&b, "MGR's received ServiceMap unread (%v)", s.MGRErr)
	} else {
		fmt.Fprintf(&b, "MGR's received ServiceMap epoch %d at %s", s.MGR, s.MGRAt.UTC().Format(time.RFC3339Nano))
	}
	b.WriteString("; no sample overlaps the failed read, so no cause is classified")
	return b.String()
}

// serviceLifecycleMonMap reads the MON's ServiceMap on a fresh client session,
// independent of the watch under test, until a map satisfies accept or bound
// ends. It returns the last map read and whether it satisfied accept; err is
// a failed dial, subscription or read, or the bound with no map at all.
func serviceLifecycleMonMap(t *testing.T, ctx context.Context, bound time.Duration, accept func(ServiceMap) bool) (ServiceMap, bool, error) {
	t.Helper()
	read, cancel := context.WithTimeout(ctx, bound)
	defer cancel()
	c, err := Dial(read, integrationOptions(t))
	if err != nil {
		return ServiceMap{}, false, fmt.Errorf("fresh dial: %w", err)
	}
	defer c.Close()
	watch, err := c.WatchServiceMap(read, ServiceMapOptions{})
	if err != nil {
		return ServiceMap{}, false, fmt.Errorf("fresh subscription: %w", err)
	}
	defer watch.Close()
	var last ServiceMap
	for {
		m, err := watch.Next(read)
		if err != nil {
			if last.Epoch != 0 && errors.Is(err, context.DeadlineExceeded) {
				return last, false, nil
			}
			return last, false, fmt.Errorf("fresh read: %w", err)
		}
		last = m
		if accept(m) {
			return m, true, nil
		}
	}
}

// serviceLifecycleObserve takes the later samples: a fresh MON read bounded by
// bound and the MGR's received epoch through the holder token, on a context
// of its own so that it also runs after the cell's context ended.
func serviceLifecycleObserve(t *testing.T, control, token string, s serviceLifecycleSamples, bound time.Duration, accept func(ServiceMap) bool) serviceLifecycleSamples {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), bound+30*time.Second)
	defer cancel()
	var m ServiceMap
	s.MONStart = time.Now()
	m, s.MONHolds, s.MONErr = serviceLifecycleMonMap(t, ctx, bound, accept)
	s.MONEnd, s.MON = time.Now(), m.Epoch
	r, err := observeNativeServiceHolder(ctx, control, token, "observe")
	s.MGRAt = time.Now()
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

// serviceLifecycleCursor is the last epoch the cell's one stream delivered,
// kept across its phases.
type serviceLifecycleCursor struct{ last uint32 }

// read returns the first map next delivers that satisfies accept. A failed
// read returns the samples with the cursor's epoch and the end time, for the
// caller's later samples.
func (c *serviceLifecycleCursor) read(next func() (ServiceMap, error), accept func(ServiceMap) bool) (ServiceMap, *serviceLifecycleSamples, error) {
	for {
		m, err := next()
		if err != nil {
			return ServiceMap{}, &serviceLifecycleSamples{Watch: c.last, Ended: time.Now()}, err
		}
		c.last = m.Epoch
		if accept(m) {
			return m, nil, nil
		}
	}
}

// serviceLifecycleWatch is the lifecycle cell's read of its own watch. A
// failed read is reported with the later samples of serviceLifecycleObserve.
func serviceLifecycleWatch(t *testing.T, ctx context.Context, watch *ServiceMapStream, cursor *serviceLifecycleCursor, control, token string, accept func(ServiceMap) bool) ServiceMap {
	t.Helper()
	m, samples, err := cursor.read(func() (ServiceMap, error) { return watch.Next(ctx) }, accept)
	if err != nil {
		t.Fatalf("ServiceMap lifecycle watch: %v; %s", err, serviceLifecycleObserve(t, control, token, *samples, 15*time.Second, accept))
	}
	return m
}

func TestServiceLifecycleSamplesReport(t *testing.T) {
	ended := time.Date(2026, 11, 26, 22, 16, 3, 0, time.UTC)
	base := serviceLifecycleSamples{Watch: 7, Ended: ended, MON: 9, MONStart: ended.Add(time.Second), MONEnd: ended.Add(2 * time.Second), MGR: 9, MGRAt: ended.Add(3 * time.Second)}
	for _, c := range []struct {
		name  string
		edit  func(*serviceLifecycleSamples)
		want  []string
		never []string
	}{
		{"map read after the deadline", func(s *serviceLifecycleSamples) { s.MONHolds = true },
			[]string{"later fresh MON session read epoch 9 holding the wanted state between 2026-11-26T22:16:04Z", "its read ended 2026-11-26T22:16:03Z"}, nil},
		{"older fresh session than the MGR's MON-fed copy", func(s *serviceLifecycleSamples) { s.MON = 7 },
			[]string{"read epoch 7 without the wanted state", "MGR's received ServiceMap epoch 9"}, nil},
		{"failed fresh dial", func(s *serviceLifecycleSamples) { s.MON, s.MONErr = 0, errors.New("fresh dial: refused") },
			[]string{"later fresh MON session unread (fresh dial: refused)"}, []string{"read epoch"}},
		{"unread fresh session", func(s *serviceLifecycleSamples) { s.MON = 0 },
			[]string{"later fresh MON session delivered no map"}, []string{"read epoch"}},
		{"unread MGR", func(s *serviceLifecycleSamples) { s.MGRErr = errors.New("oracle") },
			[]string{"MGR's received ServiceMap unread (oracle)"}, []string{"MGR's received ServiceMap epoch"}},
	} {
		s := base
		c.edit(&s)
		got := s.String()
		// No report names a cause from samples taken after the read.
		for _, cause := range []string{"missed", "stall", "behind", "not advancing"} {
			c.never = append(c.never, cause)
		}
		for _, want := range append(c.want, "watch last delivered epoch 7", "no cause is classified") {
			if !strings.Contains(got, want) {
				t.Errorf("%s: %q lacks %q", c.name, got, want)
			}
		}
		for _, never := range c.never {
			if strings.Contains(got, never) {
				t.Errorf("%s: %q contains %q", c.name, got, never)
			}
		}
	}
}

func TestServiceLifecycleCursorAcrossPhases(t *testing.T) {
	failed := errors.New("context deadline exceeded")
	for _, c := range []struct {
		name string
		maps []uint32
		// accepted is the epoch each successful phase reads up to; the next
		// phase then fails.
		accepted []uint32
		watch    uint32
	}{
		{"no new map in recovery", []uint32{5, 7}, []uint32{7}, 7},
		{"no new map in removal", []uint32{7, 9}, []uint32{7, 9}, 9},
		{"unaccepted map before the failure", []uint32{7, 8}, []uint32{7}, 8},
	} {
		maps := c.maps
		next := func() (ServiceMap, error) {
			if len(maps) == 0 {
				return ServiceMap{}, failed
			}
			m := ServiceMap{Epoch: maps[0]}
			maps = maps[1:]
			return m, nil
		}
		var cursor serviceLifecycleCursor
		for _, epoch := range c.accepted {
			m, stall, err := cursor.read(next, func(m ServiceMap) bool { return m.Epoch >= epoch })
			if err != nil || stall != nil || m.Epoch != epoch {
				t.Fatalf("%s: phase up to %d: %+v %+v %v", c.name, epoch, m, stall, err)
			}
		}
		_, stall, err := cursor.read(next, func(m ServiceMap) bool { return m.Epoch >= 100 })
		if !errors.Is(err, failed) || stall == nil || stall.Watch != c.watch || stall.Ended.IsZero() {
			t.Errorf("%s: failed phase %+v err %v; want the cursor at %d", c.name, stall, err, c.watch)
		}
	}
}
