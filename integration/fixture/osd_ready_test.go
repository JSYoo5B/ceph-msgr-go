package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func cleanStatus(pgs ...string) osdCleanStatus {
	var s osdCleanStatus
	s.MgrMap.Available = true
	s.OSDMap.Epoch, s.OSDMap.NumOSDs, s.OSDMap.NumUpOSDs, s.OSDMap.NumInOSDs = 42, 3, 3, 3
	s.PGMap.NumPGs = len(pgs)
	for _, state := range pgs {
		s.PGMap.PGsByState = append(s.PGMap.PGsByState, struct {
			StateName string `json:"state_name"`
			Count     int    `json:"count"`
		}{state, 1})
	}
	return s
}

func TestOSDCleanPredicateIsExact(t *testing.T) {
	if ok, why := cleanStatus("active+clean", "active+clean").clean(3); !ok {
		t.Fatal("rejected a clean cluster", why)
	}
	for _, pgs := range [][]string{
		{"active+clean", "active+recovery_toofull+degraded"},
		{"active+clean+scrubbing"},
		{},
	} {
		if ok, _ := cleanStatus(pgs...).clean(3); ok {
			t.Fatal("accepted an unclean state", pgs)
		}
	}
	down := cleanStatus("active+clean")
	down.OSDMap.NumUpOSDs = 2
	if ok, why := down.clean(3); ok || !strings.Contains(why, "2") {
		t.Fatal("accepted a down OSD", why)
	}
	noMgr := cleanStatus("active+clean")
	noMgr.MgrMap.Available = false
	if ok, _ := noMgr.clean(3); ok {
		t.Fatal("accepted a missing manager")
	}
}

func TestWaitOSDCleanAcceptsDelayedRecovery(t *testing.T) {
	polls := 0
	tl, err := waitOSDClean(context.Background(), 5*time.Second, time.Second, time.Millisecond, 3, func(context.Context) (osdCleanStatus, error) {
		polls++
		if polls < 4 {
			return cleanStatus("active+clean", "active+recovery_toofull+degraded"), nil
		}
		return cleanStatus("active+clean", "active+clean"), nil
	})
	if err != nil || polls != 4 {
		t.Fatal("delayed clean recovery was not accepted", polls, err)
	}
	if len(tl.observations) != 4 || !strings.Contains(tl.observations[0].Summary, "recovery_toofull") || strings.Contains(tl.observations[3].Summary, "recovery_toofull") {
		t.Fatal("native recovery transition was not retained", tl.observations)
	}
	if tl.toofullPolls != 3 || tl.firstToofullAt > tl.lastToofullAt {
		t.Fatal("toofull timeline was not retained across recovery", tl)
	}
}

func TestWaitOSDCleanReportsLastUncleanObservation(t *testing.T) {
	var fullness osdFullnessStatus
	if err := json.Unmarshal([]byte(`{"epoch":43,"nearfull_ratio":0.85,"backfillfull_ratio":0.9,"full_ratio":0.95,"osds":[{"osd":1,"up":1,"in":1,"state":["exists","up","backfillfull"]}]}`), &fullness); err != nil {
		t.Fatal(err)
	}
	_, err := waitOSDClean(context.Background(), 50*time.Millisecond, time.Second, time.Millisecond, 3, func(context.Context) (osdCleanStatus, error) {
		s := cleanStatus("active+clean", "active+recovery_toofull+degraded")
		s.Health.Checks = map[string]json.RawMessage{"PG_RECOVERY_FULL": nil, "OSD_BACKFILLFULL": nil}
		s.Fullness = &fullness
		return s, nil
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("permanently unclean state succeeded")
	}
	for _, want := range []string{"restore native OSD fixture readiness", "deadline exceeded", "budget 50ms", "osdmap epoch 42", "1xactive+recovery_toofull+degraded", "OSD_BACKFILLFULL,PG_RECOVERY_FULL", "0 status invocation failures", "before the controller observed clean state", "toofull PG states in", "first at +", "last at +", "dump epoch 43", "ratios nearfull/backfillfull/full 0.85/0.9/0.95", "osd.1 up=1 in=1 [exists,up,backfillfull]"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("failure lacks %q: %v", want, err)
		}
	}
}

func TestOSDCleanTimelineDoesNotAttributeAStaleSnapshotToALaterPoll(t *testing.T) {
	var tl osdCleanTimeline
	s := cleanStatus("active+recovery_toofull")
	s.Fullness = &osdFullnessStatus{Epoch: 43}
	tl.observeToofull(s, time.Second)
	tl.observeToofull(cleanStatus("active+degraded"), 2*time.Second)
	if tl.toofullPolls != 1 || tl.lastToofullAt != time.Second || tl.lastToofullFullness != s.Fullness {
		t.Fatal("unrelated state changed the toofull snapshot", tl)
	}
	tl.observeToofull(cleanStatus("active+backfill_toofull"), 3*time.Second)
	if tl.toofullPolls != 2 || tl.firstToofullAt != time.Second || tl.lastToofullAt != 3*time.Second {
		t.Fatal("toofull observations lost their count or timing", tl)
	}
	err := tl.failure(context.Background(), time.Second, time.Now(), 3)
	if !strings.Contains(err.Error(), "fullness snapshot unavailable at the last toofull observation") || strings.Contains(err.Error(), "dump epoch 43") {
		t.Fatal("old fullness state was attributed to the latest toofull poll", err)
	}
}

func TestWaitOSDCleanSeparatesInvocationFailures(t *testing.T) {
	_, err := waitOSDClean(context.Background(), 50*time.Millisecond, 10*time.Millisecond, time.Millisecond, 3, func(ctx context.Context) (osdCleanStatus, error) {
		<-ctx.Done()
		return osdCleanStatus{}, errors.New("exec create: " + ctx.Err().Error())
	})
	if err == nil || !strings.Contains(err.Error(), "no status observation succeeded") || !strings.Contains(err.Error(), "last status invocation error") || strings.Contains(err.Error(), "last unclean observation") {
		t.Fatal("invocation deadline was reported as daemon state", err)
	}
}

func TestWaitOSDCleanReportsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := waitOSDClean(ctx, time.Second, time.Second, time.Millisecond, 3, func(context.Context) (osdCleanStatus, error) {
		t.Fatal("canceled wait started a native invocation")
		return osdCleanStatus{}, nil
	})
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "canceled") {
		t.Fatal("cancellation was reported as a state timeout", err)
	}
}

func TestWaitOSDCleanRejectsLateCleanObservation(t *testing.T) {
	for _, cancelParent := range []bool{false, true} {
		t.Run(map[bool]string{false: "deadline", true: "cancellation"}[cancelParent], func(t *testing.T) {
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, err := waitOSDClean(parent, 10*time.Millisecond, time.Second, time.Millisecond, 3, func(ctx context.Context) (osdCleanStatus, error) {
				if cancelParent {
					cancel()
				} else {
					<-ctx.Done()
				}
				// Model an invocation returning buffered output after cancellation.
				return cleanStatus("active+clean"), nil
			})
			want := context.DeadlineExceeded
			if cancelParent {
				want = context.Canceled
			}
			if !errors.Is(err, want) {
				t.Fatal("late observation established readiness", err)
			}
		})
	}
}

func TestWaitOSDCleanDoesNotAcceptAnExpiredInvocation(t *testing.T) {
	polls := 0
	tl, err := waitOSDClean(context.Background(), time.Second, time.Millisecond, time.Millisecond, 3, func(ctx context.Context) (osdCleanStatus, error) {
		polls++
		if polls == 1 {
			<-ctx.Done()
		}
		return cleanStatus("active+clean"), nil
	})
	if err != nil || polls != 2 || tl.statusErrs != 1 || !tl.observations[0].InvocationFailed {
		t.Fatal("expired native invocation established readiness", polls, tl, err)
	}
}
