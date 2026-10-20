package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

const (
	// osdCleanBudget bounds how long the controller waits for every owned OSD
	// to be up and in and every PG to be exactly active+clean.
	osdCleanBudget = 30 * time.Second
	// osdCleanPollTimeout bounds one `ceph status` invocation separately from
	// the wait, so a stuck CLI/Docker exec is not reported as unclean state.
	osdCleanPollTimeout  = 10 * time.Second
	osdCleanPollInterval = 500 * time.Millisecond
)

// osdCleanStatus is the part of `ceph status --format json` that decides OSD
// fixture readiness, plus the map epoch and health check names kept as
// diagnostics only.
type osdCleanStatus struct {
	Health struct {
		Checks map[string]json.RawMessage `json:"checks"`
	} `json:"health"`
	MgrMap struct {
		Available bool `json:"available"`
	} `json:"mgrmap"`
	OSDMap struct {
		Epoch     uint32 `json:"epoch"`
		NumOSDs   int    `json:"num_osds"`
		NumUpOSDs int    `json:"num_up_osds"`
		NumInOSDs int    `json:"num_in_osds"`
	} `json:"osdmap"`
	PGMap struct {
		NumPGs     int `json:"num_pgs"`
		PGsByState []struct {
			StateName string `json:"state_name"`
			Count     int    `json:"count"`
		} `json:"pgs_by_state"`
	} `json:"pgmap"`
}

// clean reports whether the status satisfies the readiness predicate and, if
// not, which condition failed. The predicate is exact: PG states with extra
// flags (for example active+clean+scrubbing) do not count.
func (s osdCleanStatus) clean(expected int) (bool, string) {
	switch {
	case !s.MgrMap.Available:
		return false, "no available manager"
	case s.OSDMap.NumOSDs != expected || s.OSDMap.NumUpOSDs != expected || s.OSDMap.NumInOSDs != expected:
		return false, fmt.Sprintf("osds total/up/in %d/%d/%d, want %d", s.OSDMap.NumOSDs, s.OSDMap.NumUpOSDs, s.OSDMap.NumInOSDs, expected)
	case s.PGMap.NumPGs == 0:
		return false, "no PGs"
	}
	clean := 0
	for _, state := range s.PGMap.PGsByState {
		if state.StateName != "active+clean" {
			return false, "PGs " + s.pgSummary()
		}
		clean += state.Count
	}
	if clean != s.PGMap.NumPGs {
		return false, fmt.Sprintf("PG states cover %d of %d PGs", clean, s.PGMap.NumPGs)
	}
	return true, ""
}

func (s osdCleanStatus) pgSummary() string {
	parts := make([]string, 0, len(s.PGMap.PGsByState))
	for _, state := range s.PGMap.PGsByState {
		parts = append(parts, fmt.Sprintf("%dx%s", state.Count, state.StateName))
	}
	return strings.Join(parts, ",")
}

func (s osdCleanStatus) summary(expected int) string {
	checks := make([]string, 0, len(s.Health.Checks))
	for name := range s.Health.Checks {
		checks = append(checks, name)
	}
	slices.Sort(checks)
	return fmt.Sprintf("osdmap epoch %d, osds total/up/in %d/%d/%d (want %d), mgr available %t, pgs %d [%s], health checks [%s]",
		s.OSDMap.Epoch, s.OSDMap.NumOSDs, s.OSDMap.NumUpOSDs, s.OSDMap.NumInOSDs, expected, s.MgrMap.Available,
		s.PGMap.NumPGs, s.pgSummary(), strings.Join(checks, ","))
}

// osdCleanTimeline keeps the bounded per-poll facts of one readiness wait.
type osdCleanTimeline struct {
	polls       int
	statusErrs  int
	lastErr     error
	lastErrAt   time.Duration
	last        *osdCleanStatus
	lastAt      time.Duration
	lastReason  string
	slowestPoll time.Duration
}

// waitOSDClean polls until every owned OSD is up and in and every PG is
// exactly active+clean. The budget covers daemon-state waiting; each status
// invocation has its own deadline and an invocation failure is reported
// separately from an unclean observation, with the last observed state.
func waitOSDClean(ctx context.Context, budget, pollTimeout, interval time.Duration, expected int,
	status func(context.Context) (osdCleanStatus, error)) error {
	start := time.Now()
	deadline, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	var tl osdCleanTimeline
	for {
		pollCtx, pollCancel := context.WithTimeout(deadline, pollTimeout)
		began := time.Now()
		s, err := status(pollCtx)
		pollCancel()
		now := time.Now()
		tl.polls++
		tl.slowestPoll = max(tl.slowestPoll, now.Sub(began))
		if err != nil {
			tl.statusErrs++
			tl.lastErr, tl.lastErrAt = err, now.Sub(start)
		} else {
			ok, reason := s.clean(expected)
			if ok {
				return nil
			}
			tl.last, tl.lastAt, tl.lastReason = &s, now.Sub(start), reason
		}
		timer := time.NewTimer(interval)
		select {
		case <-deadline.Done():
			timer.Stop()
			return tl.failure(ctx, budget, start, expected)
		case <-timer.C:
		}
	}
}

func (tl osdCleanTimeline) failure(parent context.Context, budget time.Duration, start time.Time, expected int) error {
	elapsed := time.Since(start).Round(time.Millisecond)
	var cause error
	if parent.Err() != nil {
		cause = fmt.Errorf("canceled after %s: %w", elapsed, parent.Err())
	} else {
		cause = fmt.Errorf("%w after %s", context.DeadlineExceeded, elapsed)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "restore native OSD fixture readiness: %v (budget %s, %d polls, slowest %s, %d status invocation failures)",
		cause, budget, tl.polls, tl.slowestPoll.Round(time.Millisecond), tl.statusErrs)
	switch {
	case tl.last != nil:
		fmt.Fprintf(&b, "; last unclean observation at +%s: %s: %s", tl.lastAt.Round(time.Millisecond), tl.lastReason, tl.last.summary(expected))
	case tl.lastErr != nil:
		b.WriteString("; no status observation succeeded")
	}
	if tl.lastErr != nil {
		fmt.Fprintf(&b, "; last status invocation error at +%s: %v", tl.lastErrAt.Round(time.Millisecond), tl.lastErr)
	}
	return errors.New(b.String() + "; the wait ended before the controller observed clean state")
}

// osdCleanStatus reads the native status through the owned cluster.
func (c *fixtureCluster) osdCleanStatus(ctx context.Context) (osdCleanStatus, error) {
	var s osdCleanStatus
	data, err := c.cluster.Ceph(ctx, "status", "--format", "json")
	if err != nil {
		return s, err
	}
	if err := json.Unmarshal(data, &s); err != nil {
		return s, fmt.Errorf("decode ceph status: %w", err)
	}
	return s, nil
}
