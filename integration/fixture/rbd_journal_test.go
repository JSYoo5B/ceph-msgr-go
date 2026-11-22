package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestRBDJournalPlanIsTokenBound(t *testing.T) {
	token := strings.Repeat("a1", 16)
	plan := rbdJournalPlanFor(token)
	if !plan.owned(token) || plan.owned(strings.Repeat("b2", 16)) || plan.Pool != rbdBasePlanFor(token).Pool || plan.BaseImage != rbdBasePlanFor(token).Image ||
		plan.Image != "ceph-msgr-journal-"+token || plan.OracleClient != "ceph-msgr-oracle-a1a1a1a1" || plan.GoClient != "ceph-msgr-mirror-a1a1a1a1" || plan.Features != 69 {
		t.Fatal("owned RBD journal plan names changed", plan)
	}
	if !reflect.DeepEqual(plan, rbdJournalPlanFor(token)) || reflect.DeepEqual(plan, rbdJournalPlanFor(strings.Repeat("b2", 16))) {
		t.Fatal("journal plan is not deterministic and token-bound")
	}
	// Writes stay inside one object and below librbd's 16 KiB journal
	// payload; discards cover whole objects (64 KiB discard granularity).
	objectSize := uint64(1) << plan.Order
	for _, e := range slices.Concat(plan.SetupEvents, plan.WriteEvents, plan.ConcurrentOwnerEvents, plan.ConcurrentEvents, plan.TakeoverEvents) {
		inside := e.Length > 0 && e.Offset%4096 == 0 && e.Offset+e.Length <= plan.Size && e.Offset/objectSize == (e.Offset+e.Length-1)/objectSize
		if !inside || e.Kind == "write" && e.Length > 8<<10 || e.Kind == "discard" && (e.Offset%objectSize != 0 || e.Length != objectSize) || e.Kind != "write" && e.Kind != "discard" {
			t.Fatal("plan event is not a bounded write or a whole-object discard", e)
		}
	}
	for _, e := range slices.Concat(plan.GoWrites, plan.GoConcurrentWrites) {
		if e.Length == 0 || e.Length > 8<<10 || e.Offset+e.Length > plan.Size || e.Offset/objectSize != (e.Offset+e.Length-1)/objectSize {
			t.Fatal("Go journal write is not bounded", e)
		}
	}
	for name, change := range map[string]func(*rbdJournalPlan){
		"no journaling":  func(p *rbdJournalPlan) { p.Features = 5 },
		"foreign image":  func(p *rbdJournalPlan) { p.Image = p.BaseImage },
		"foreign client": func(p *rbdJournalPlan) { p.GoClient = "" },
		"no Go writes":   func(p *rbdJournalPlan) { p.GoWrites = nil },
		"no concurrency": func(p *rbdJournalPlan) { p.ConcurrentEvents = nil },
		"no Go appender": func(p *rbdJournalPlan) { p.GoConcurrentWrites = nil },
		"discard in session": func(p *rbdJournalPlan) {
			p.ConcurrentEvents = []rbdJournalEvent{{"discard", 0, 64 << 10}}
		},
		"no takeover write":   func(p *rbdJournalPlan) { p.TakeoverEvents = nil },
		"discard in takeover": func(p *rbdJournalPlan) { p.TakeoverEvents = []rbdJournalEvent{{"discard", 0, 64 << 10}} },
	} {
		changed := rbdJournalPlanFor(token)
		change(&changed)
		if changed.owned(token) {
			t.Fatal("changed journal plan admitted", name)
		}
	}
	for _, field := range []string{"'ceph-msgr-journal-' + token", "'ceph-msgr-oracle-' + token[:8]", "'ceph-msgr-mirror-' + token[:8]", "plan.get('features') != 69"} {
		if !strings.Contains(rbdJournalNativePython, field) {
			t.Fatal("native journal driver no longer checks", field)
		}
	}
	// The concurrent session gives Go one entry per splay offset 0 to 2, and
	// one native aio write overlaps a Go extent so that the image shows the
	// order of the two owners.
	if len(plan.GoConcurrentWrites) != 3 || !slices.ContainsFunc(plan.ConcurrentEvents, func(e rbdJournalEvent) bool {
		return slices.ContainsFunc(plan.GoConcurrentWrites, func(g rbdBaseExtent) bool { return e.Offset < g.Offset+g.Length && g.Offset < e.Offset+e.Length })
	}) {
		t.Fatal("concurrent plan lacks an overlap between the two owners", plan)
	}
	for _, field := range []string{"image.aio_write(", "wait_proceed(3)", "'rbd-journal-proceed-%d'", "'rbd-journal-state.json'"} {
		if !strings.Contains(rbdJournalNativePython, field) {
			t.Fatal("native journal driver concurrent session lacks", field)
		}
	}
	// The takeover write covers the first half of the first Go extent, so
	// the image shows that the replay of that entry came first.
	g, w := plan.GoWrites[0], plan.TakeoverEvents[0]
	if len(plan.TakeoverEvents) != 1 || w.Offset != g.Offset || w.Length == 0 || w.Length >= g.Length {
		t.Fatal("takeover write does not cover the start of the first Go extent only", plan.TakeoverEvents)
	}
	// A user flush would journal an AioFlush event the test does not plan.
	if strings.Contains(rbdJournalNativePython, ".flush()") {
		t.Fatal("native journal driver flushes")
	}
	for _, driver := range []string{rbdLockNativePython, rbdWriteNativePython} {
		if !strings.Contains(driver, "'ceph-msgr-journal-' + token") {
			t.Fatal("the lock and write setups must admit the journaling image in the owned pool")
		}
	}
}

func TestRBDJournalRequestIsClosedAndOrdered(t *testing.T) {
	token := strings.Repeat("a1", 16)
	for _, actions := range [][]string{rbdJournalActions, rbdJournalLostAppendActions, rbdJournalPrimaryChangeActions, rbdJournalOwnerLossActions, rbdJournalForcedBreakActions} {
		for next := range actions {
			for i, action := range actions {
				id, got, err := parseRBDJournalRequest(actions, []string{"7", token, action}, token, 6, next)
				if (err == nil) != (i == next) || err == nil && (id != 7 || got != action) {
					t.Fatal("RBD journal action order differs", next, action, err)
				}
			}
		}
		if _, _, err := parseRBDJournalRequest(actions, []string{"99", token, "replay"}, token, 1, len(actions)); err == nil {
			t.Fatal("action after the closed sequence admitted")
		}
	}
	for _, fields := range [][]string{
		{"7", token, "reset"}, {"7", token, "client-disconnect"}, {"7", token}, {"7", token, "setup", "extra"},
		{"6", token, "setup"}, {"0", token, "setup"}, {"07", token, "setup"}, {"7", strings.Repeat("b2", 16), "setup"}, {"7", token, "write"},
	} {
		if _, _, err := parseRBDJournalRequest(rbdJournalActions, fields, token, 6, 0); err == nil {
			t.Fatal("unscoped RBD journal request admitted", fields)
		}
	}
	// Each sequence refuses the other's actions at every position.
	for next := range 8 {
		for _, action := range []string{"observe-locked", "checkpoint", "observe-recorded"} {
			if _, _, err := parseRBDJournalRequest(rbdJournalActions, []string{"7", token, action}, token, 6, next); err == nil {
				t.Fatal("lost-append action admitted in the ordinary sequence", action, next)
			}
		}
		for _, action := range []string{"observe-registered", "write", "observe-committed", "inspect", "concurrent"} {
			if _, _, err := parseRBDJournalRequest(rbdJournalLostAppendActions, []string{"7", token, action}, token, 6, next); err == nil {
				t.Fatal("ordinary action admitted in the lost-append sequence", action, next)
			}
		}
	}
	if !rbdJournalReadOnly("observe-registered") || !rbdJournalReadOnly("observe-committed") || rbdJournalReadOnly("inspect") || rbdJournalReadOnly("replay") ||
		!rbdJournalReadOnly("observe-locked") || !rbdJournalReadOnly("observe-recorded") || rbdJournalReadOnly("checkpoint") ||
		!rbdJournalInspected("checkpoint") || rbdJournalInspected("observe-locked") || rbdJournalInspected("observe-recorded") {
		t.Fatal("journal action classification differs")
	}
}

// The native tests that declare the RBD journal and lock capabilities
// (their //scenario:row declarations).
const (
	rbdJournalLostAppendSelector    = "^TestCephRBDJournalLostAppendReplyIntegration$"
	rbdJournalPrimaryChangeSelector = "^TestCephRBDJournalPrimaryChangeIntegration$"
	rbdLockOwnerLossSelector        = "^TestCephRBDLockOwnerLossIntegration$"
	rbdLockOwnerLossPendingSelector = "^TestCephRBDLockOwnerLossPendingIntegration$"
	rbdJournalOwnerLossSelector     = "^TestCephRBDJournalOwnerLossIntegration$"
	rbdJournalForcedBreakSelector   = "^TestCephRBDJournalForcedBreakIntegration$"
)

// rbdSelectionProfile is the profile of the RBD catalog row that selects
// selector, with its declared capabilities and topology.
func rbdSelectionProfile(t *testing.T, selector string) profile {
	t.Helper()
	for _, row := range ciRepositoryCatalog(t) {
		if row.Params["test_run"] == selector && row.Params["rbd"] == "1" {
			p, err := ciProfile(row)
			if err != nil {
				t.Fatal(row.ID, err)
			}
			return p
		}
	}
	t.Fatal("no RBD row selects", selector)
	return profile{}
}

// The lost-append-reply sequence is admitted only under its exact selector,
// which the RBD fixture accepts as its one further exclusive selection.
func TestRBDJournalLostAppendSelectorIsExclusive(t *testing.T) {
	base := map[string]string{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_RBD": "1"}
	load := func(selector string) (profile, error) {
		return loadProfile(func(key string) string {
			switch key {
			case "CEPH_MSGR_TEST_RUN":
				return selector
			case "CEPH_MSGR_TEST_CAPABILITIES":
				return catalogCapabilities(t, selector)
			}
			return base[key]
		})
	}
	broad := "^TestCephRBD(Base|Write|ExclusiveLock|Clone|Journal)Integration$"
	for selector, want := range map[string]string{"": broad, broad: broad, rbdJournalLostAppendSelector: rbdJournalLostAppendSelector} {
		p, err := load(selector)
		if err != nil || p.testRun != want {
			t.Fatal("RBD selector admission differs", selector, p.testRun, err)
		}
		actions := rbdJournalActionsFor(p)
		if (selector == rbdJournalLostAppendSelector) != slices.Equal(actions, rbdJournalLostAppendActions) || selector != rbdJournalLostAppendSelector && !slices.Equal(actions, rbdJournalActions) {
			t.Fatal("journal action sequence differs for", selector, actions)
		}
	}
	for _, selector := range []string{"^TestCephRBDJournalLostAppendReplyIntegration", "TestCephRBDJournalLostAppendReplyIntegration", "^TestCephRBD.*Integration$",
		"^TestCephRBD(Journal|JournalLostAppendReply)Integration$"} {
		if _, err := load(selector); err == nil {
			t.Fatal("unanchored or widened lost-append selector admitted", selector)
		}
	}
	// Without the RBD opt-in the selector matches a test no fixture serves.
	delete(base, "CEPH_MSGR_TEST_RBD")
	if p, err := load(rbdJournalLostAppendSelector); err == nil && p.rbdBase == "1" {
		t.Fatal("lost-append selector enabled the RBD fixture by itself")
	}
	for _, action := range rbdJournalLostAppendActions {
		if !strings.Contains(rbdJournalNativePython, "'"+action+"'") {
			t.Fatal("native journal driver does not admit", action)
		}
	}
	if rbdJournalTarget(rbdJournalPlan{Image: "i"}, "checkpoint") != "i:journal-inspect" || rbdJournalTarget(rbdJournalPlan{Image: "i"}, "observe-locked") != "i" {
		t.Fatal("lost-append action targets differ")
	}
}

// The primary-change sequence is the lost-append sequence with change-primary
// and observe-resubmitted before the replay. It is admitted only under its
// exact selector and only on three BlueStore OSDs, a topology no other RBD
// selection is admitted on.
func TestRBDJournalPrimaryChangeSelectorIsExclusive(t *testing.T) {
	base := map[string]string{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_RBD": "1", "CEPH_MSGR_TEST_OSD_REPLICAS": "3", "CEPH_MSGR_TEST_OSD_STORE": "bluestore"}
	load := func(selector string) (profile, error) {
		return loadProfile(func(key string) string {
			switch key {
			case "CEPH_MSGR_TEST_RUN":
				return selector
			case "CEPH_MSGR_TEST_CAPABILITIES":
				return catalogCapabilities(t, selector)
			}
			return base[key]
		})
	}
	p, err := load(rbdJournalPrimaryChangeSelector)
	if err != nil || p.testRun != rbdJournalPrimaryChangeSelector || p.rbdReplicas() != "3" || p.osdTopology().poolSize != 3 || p.osdTopology().poolMinSize != 2 ||
		!slices.Equal(rbdJournalActionsFor(p), rbdJournalPrimaryChangeActions) {
		t.Fatal("primary-change selector admission differs", p.testRun, err)
	}
	// Three OSDs admit no other RBD selection, not even the default one.
	for _, selector := range []string{"", "^TestCephRBD(Base|Write|ExclusiveLock|Clone|Journal)Integration$", rbdJournalLostAppendSelector, rbdLockOwnerLossSelector,
		rbdLockOwnerLossPendingSelector, rbdJournalOwnerLossSelector, rbdJournalForcedBreakSelector, "^TestCephRBDJournalPrimaryChangeIntegration",
		"^TestCephRBDJournal(LostAppendReply|PrimaryChange)Integration$"} {
		if _, err := load(selector); err == nil {
			t.Fatal("another RBD selection admitted on three OSDs:", selector)
		}
	}
	// The selector needs exactly that topology.
	for key, value := range map[string]string{"CEPH_MSGR_TEST_OSD_REPLICAS": "1", "CEPH_MSGR_TEST_OSD_STORE": "memstore", "CEPH_MSGR_TEST_OSD_PROFILE": "replicated-pg-v8"} {
		previous, had := base[key]
		base[key] = value
		if _, err := load(rbdJournalPrimaryChangeSelector); err == nil {
			t.Fatal("primary-change selector admitted with", key, value)
		}
		if had {
			base[key] = previous
		} else {
			delete(base, key)
		}
	}
	if rbdSelectionProfile(t, rbdJournalLostAppendSelector).rbdReplicas() != "1" || (profile{}).rbdReplicas() != "1" {
		t.Fatal("another RBD selection asks for three OSDs")
	}
	if !slices.Equal(rbdJournalActionsFor(rbdSelectionProfile(t, rbdJournalLostAppendSelector)), rbdJournalLostAppendActions) || !slices.Equal(rbdLockActionsFor(rbdSelectionProfile(t, rbdJournalPrimaryChangeSelector)), rbdLockActions) {
		t.Fatal("the primary-change selector changed another sequence")
	}
	want := slices.Insert(slices.Clone(rbdJournalLostAppendActions), len(rbdJournalLostAppendActions)-1, "change-primary", "observe-resubmitted")
	if !slices.Equal(rbdJournalPrimaryChangeActions, want) {
		t.Fatal("primary-change sequence differs", rbdJournalPrimaryChangeActions)
	}
	token := strings.Repeat("a1", 16)
	for next := 0; next <= len(rbdJournalLostAppendActions); next++ {
		for _, action := range []string{"change-primary", "observe-resubmitted"} {
			if _, _, err := parseRBDJournalRequest(rbdJournalLostAppendActions, []string{"7", token, action}, token, 6, next); err == nil {
				t.Fatal("primary-change action admitted in the lost-append sequence", action, next)
			}
			if _, _, err := parseRBDJournalRequest(rbdJournalActions, []string{"7", token, action}, token, 6, next); err == nil {
				t.Fatal("primary-change action admitted in the ordinary sequence", action, next)
			}
		}
	}
	for _, action := range rbdJournalPrimaryChangeActions {
		if !strings.Contains(rbdJournalNativePython, "'"+action+"'") {
			t.Fatal("native journal driver does not admit", action)
		}
	}
	plan := rbdJournalPlan{Image: "i"}
	if rbdJournalReadOnly("change-primary") || !rbdJournalReadOnly("observe-resubmitted") || rbdJournalInspected("change-primary") || rbdJournalInspected("observe-resubmitted") ||
		rbdJournalTarget(plan, "change-primary") != "i:osd-out,acting-primary" || rbdJournalTarget(plan, "observe-resubmitted") != "i" {
		t.Fatal("primary-change action classification or target differs")
	}
	if !slices.Equal(rbdJournalPrimaryObjects(3, "ab12"), []string{"rbd_header.ab12", "journal_data.3.ab12.0", "journal.ab12"}) {
		t.Fatal("followed objects differ")
	}
}

// The owner-loss and forced-break sequences are admitted only under their
// exact selectors, each on the single-OSD RBD fixture, and change no other
// sequence. The owner-loss sequence is the lost-append sequence with the
// takeover in place of the replay.
func TestRBDJournalOwnerLossSelectorsAreExclusive(t *testing.T) {
	base := map[string]string{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_RBD": "1"}
	load := func(selector string) (profile, error) {
		return loadProfile(func(key string) string {
			switch key {
			case "CEPH_MSGR_TEST_RUN":
				return selector
			case "CEPH_MSGR_TEST_CAPABILITIES":
				return catalogCapabilities(t, selector)
			}
			return base[key]
		})
	}
	for selector, want := range map[string][]string{rbdJournalOwnerLossSelector: rbdJournalOwnerLossActions, rbdJournalForcedBreakSelector: rbdJournalForcedBreakActions} {
		p, err := load(selector)
		if err != nil || p.testRun != selector || p.rbdReplicas() != "1" || !slices.Equal(rbdJournalActionsFor(p), want) || !slices.Equal(rbdLockActionsFor(p), rbdLockActions) {
			t.Fatal("journal owner-loss selector admission differs", selector, p.testRun, err)
		}
		if row := rbdSelectionProfile(t, selector); !slices.Equal(rbdJournalActionsFor(row), want) {
			t.Fatal("catalog row of", selector, "selects another sequence")
		}
		for _, widened := range []string{strings.TrimSuffix(selector, "$"), strings.TrimPrefix(selector, "^"), "^TestCephRBDJournal(OwnerLoss|ForcedBreak)Integration$"} {
			if _, err := load(widened); err == nil {
				t.Fatal("unanchored or widened journal owner-loss selector admitted", widened)
			}
		}
	}
	for _, selector := range []string{"", rbdJournalLostAppendSelector, rbdLockOwnerLossSelector} {
		p, err := load(selector)
		if err != nil || slices.Equal(rbdJournalActionsFor(p), rbdJournalOwnerLossActions) || slices.Equal(rbdJournalActionsFor(p), rbdJournalForcedBreakActions) {
			t.Fatal("another RBD selection runs a journal owner-loss sequence", selector, err)
		}
	}
	want := append(slices.Clone(rbdJournalLostAppendActions[:len(rbdJournalLostAppendActions)-1]), "takeover")
	if !slices.Equal(rbdJournalOwnerLossActions, want) {
		t.Fatal("owner-loss sequence differs from the lost-append sequence", rbdJournalOwnerLossActions)
	}
	token := strings.Repeat("a1", 16)
	for next := 0; next <= len(rbdJournalLostAppendActions); next++ {
		for _, action := range []string{"observe-appended", "force-break", "observe-fenced", "takeover"} {
			for _, actions := range [][]string{rbdJournalActions, rbdJournalLostAppendActions, rbdJournalPrimaryChangeActions} {
				if _, _, err := parseRBDJournalRequest(actions, []string{"7", token, action}, token, 6, next); err == nil {
					t.Fatal("owner-loss action admitted in another sequence", action, next)
				}
			}
		}
		if _, _, err := parseRBDJournalRequest(rbdJournalOwnerLossActions, []string{"7", token, "replay"}, token, 6, next); err == nil {
			t.Fatal("replay admitted in the owner-loss sequence", next)
		}
	}
	for _, action := range slices.Concat(rbdJournalOwnerLossActions, rbdJournalForcedBreakActions) {
		if !strings.Contains(rbdJournalNativePython, "'"+action+"'") {
			t.Fatal("native journal driver does not admit", action)
		}
	}
	plan := rbdJournalPlan{Image: "i"}
	if !rbdJournalReadOnly("observe-appended") || !rbdJournalReadOnly("observe-fenced") || rbdJournalReadOnly("force-break") || rbdJournalReadOnly("takeover") ||
		!rbdJournalInspected("takeover") || rbdJournalInspected("force-break") || rbdJournalInspected("observe-fenced") ||
		rbdJournalBlocklisted("observe-locked") || rbdJournalBlocklisted("replay") || !rbdJournalBlocklisted("takeover") || !rbdJournalBlocklisted("observe-appended") ||
		rbdJournalTarget(plan, "force-break") != "i:exclusive-lock-break,osd-blocklist" || rbdJournalTarget(plan, "takeover") != "i:exclusive-lock-break,osd-blocklist,journal-replay,native-write" {
		t.Fatal("owner-loss action classification or target differs")
	}
	if !rbdJournalLockLogged("force-break") || !rbdJournalLockLogged("takeover") || rbdJournalLockLogged("observe-fenced") || rbdJournalLockLogged("replay") {
		t.Fatal("lock log classification differs")
	}
	for _, field := range []string{"image.lock_break(rbd.RBD_LOCK_MODE_EXCLUSIVE, listed[0]['owner'])", "plan['takeover_events']", "'osd blocklist ls'",
		"conf.update(debug_rbd='10/10', log_to_file='true', log_file=LOCK_LOG)", "    cluster.shutdown()\nif action in ('force-break', 'takeover'):\n    out['lock_log'] = lock_log()",
		"out['boundary'] = {'lockers': lockers(image), 'watchers': watchers(image)}\n                started = time.monotonic_ns()\n                out['events'] = [apply(image, 'takeover', e)"} {
		if !strings.Contains(rbdJournalNativePython, field) {
			t.Fatal("native journal driver lacks", field)
		}
	}
}

// change-primary marks the objects' one common primary out and succeeds only
// when each object is on the two other members alone.
func TestRBDJournalChangePrimary(t *testing.T) {
	objects := []string{"h", "j", "m"}
	type cluster struct {
		epoch    uint32
		acting   map[string][]int
		out      map[int]bool
		commands []int
		recorded []int
		stuck    bool
		placeErr error
		outErr   error
		markErr  error
		foreign  bool
		dropped  bool
	}
	ops := func(c *cluster) rbdJournalPrimaryOps {
		return rbdJournalPrimaryOps{
			place: func(_ context.Context, object string) (rbdJournalPlacement, error) {
				// Marked out, an OSD leaves the up set; the new primary keeps
				// it as the last member of the acting set.
				up := slices.Clone(c.acting[object])
				acting := slices.Clone(up)
				if !c.stuck {
					up = slices.DeleteFunc(up, func(osd int) bool { return c.out[osd] })
					acting = slices.Clone(up)
					for _, osd := range c.acting[object] {
						if c.out[osd] && !c.dropped {
							acting = append(acting, osd)
						}
					}
				}
				if c.foreign && len(c.commands) != 0 {
					up, acting = []int{7, 8}, []int{7, 8, 1}
				}
				p := rbdJournalPlacement{Epoch: c.epoch, Pool: "p", PoolID: 3, Object: object, RawPGID: "3.1", PGID: "3.0", Up: up, Acting: acting}
				if len(acting) != 0 && len(up) != 0 {
					p.UpPrimary, p.ActingPrimary = up[0], acting[0]
				}
				return p, c.placeErr
			},
			out: func(_ context.Context, osd int) error {
				c.commands = append(c.commands, osd)
				if c.outErr != nil {
					return c.outErr
				}
				c.out[osd] = true
				c.epoch++
				return nil
			},
			marking: func(osd int) error {
				c.recorded = append(c.recorded, osd)
				return c.markErr
			},
			bound: 20 * time.Millisecond, pause: time.Millisecond,
		}
	}
	fresh := func(header, journal, meta []int) *cluster {
		return &cluster{epoch: 40, acting: map[string][]int{"h": header, "j": journal, "m": meta}, out: map[int]bool{}}
	}
	one := func() *cluster { return fresh([]int{1, 0, 2}, []int{1, 0, 2}, []int{1, 0, 2}) }
	c := one()
	r, err := rbdJournalChangePrimary(t.Context(), "p", objects, ops(c))
	if err != nil || r.Out != 1 || !slices.Equal(c.recorded, []int{1}) || !slices.Equal(c.commands, []int{1}) || len(r.Objects) != 3 || !slices.Equal(r.Commands, []string{"osd out 1"}) {
		t.Fatal("primary change differs", r, err)
	}
	for _, o := range r.Objects {
		if !o.Moved || o.Before.ActingPrimary != 1 || o.After.ActingPrimary != 0 || !slices.Equal(o.After.Up, []int{0, 2}) || !slices.Equal(o.After.Acting, []int{0, 2, 1}) ||
			o.After.Epoch != 41 || o.Before.Epoch != 40 {
			t.Fatal("object did not move to the next member", o)
		}
	}
	failing := map[string]*cluster{
		"primary does not move":           one(),
		"objects led by different OSDs":   fresh([]int{1, 0, 2}, []int{0, 2, 1}, []int{1, 0, 2}),
		"two-member placement":            fresh([]int{1, 0}, []int{1, 0}, []int{1, 0}),
		"duplicate member":                fresh([]int{1, 1, 2}, []int{1, 1, 2}, []int{1, 1, 2}),
		"placement unavailable":           one(),
		"command refused":                 one(),
		"intent not recorded":             one(),
		"another acting set afterwards":   one(),
		"old primary left the acting set": one(),
		"epoch unknown":                   one(),
	}
	failing["primary does not move"].stuck = true
	failing["placement unavailable"].placeErr = fmt.Errorf("no map")
	failing["command refused"].outErr = fmt.Errorf("EPERM")
	failing["intent not recorded"].markErr = fmt.Errorf("disk")
	failing["another acting set afterwards"].foreign = true
	failing["old primary left the acting set"].dropped = true
	failing["epoch unknown"].epoch = 0
	for name, c := range failing {
		r, err := rbdJournalChangePrimary(t.Context(), "p", objects, ops(c))
		if err == nil {
			t.Error("primary change hid a failure:", name, r)
		}
		// No command without its recorded intent, and none after a refused
		// record.
		if len(c.commands) > len(c.recorded) || c.markErr != nil && len(c.commands) != 0 || len(c.commands) > 1 {
			t.Error("osd out without a recorded intent, or more than once:", name)
		}
	}
	if _, err := rbdJournalChangePrimary(t.Context(), "p", nil, ops(one())); err == nil {
		t.Fatal("primary change without an object admitted")
	}
	if _, err := rbdJournalChangePrimary(t.Context(), "other", objects, ops(one())); err == nil {
		t.Fatal("placement of another pool admitted")
	}
}

// The cleanup marks every OSD in again and reports only what the OSDMap
// confirms.
func TestRBDJournalMarkIn(t *testing.T) {
	state := map[int]bool{0: true, 1: false, 2: true}
	in := func(_ context.Context, osd int) error {
		state[osd] = true
		return nil
	}
	read := func(context.Context) (map[int]bool, error) { return state, nil }
	restored, err := rbdJournalMarkIn(t.Context(), []int{1}, in, read)
	if err != nil || !slices.Equal(restored, []int{1}) || !state[1] {
		t.Fatal("OSD not marked in", restored, err)
	}
	calls := 0
	if restored, err := rbdJournalMarkIn(t.Context(), nil, func(context.Context, int) error { calls++; return nil }, read); err != nil || restored != nil || calls != 0 {
		t.Fatal("cleanup without an OSD marked out issued commands", restored, err)
	}
	state = map[int]bool{0: true, 1: false, 2: true}
	if restored, err := rbdJournalMarkIn(t.Context(), []int{1}, func(context.Context, int) error { return fmt.Errorf("EPERM") }, read); err == nil || len(restored) != 0 {
		t.Fatal("an OSD left out was reported in", restored, err)
	}
	if restored, err := rbdJournalMarkIn(t.Context(), []int{1}, in, func(context.Context) (map[int]bool, error) { return nil, fmt.Errorf("no map") }); err == nil || restored != nil {
		t.Fatal("an unread OSDMap confirmed an OSD in", restored, err)
	}
	if restored, err := rbdJournalMarkIn(t.Context(), []int{5}, func(context.Context, int) error { return nil }, read); err == nil || len(restored) != 0 {
		t.Fatal("an OSD missing from the OSDMap was reported in", restored, err)
	}
}

// rbdJournalOutput is a well-formed output of one journal action.
func rbdJournalOutput(token string, plan rbdJournalPlan, action string) map[string]any {
	id := "5678ef01"
	header, prefix := "journal."+id, "journal_data.3."+id+"."
	calls := []map[string]any{}
	for _, method := range rbdJournalCalls {
		calls = append(calls, map[string]any{"object": header, "class": "journal", "method": method, "input_hex": "", "code": 0, "output_hex": "00"})
	}
	objects, offset := []map[string]any{}, uint64(0)
	for i := 0; i < rbdJournalObjectCount; i++ {
		size := uint64(0)
		if i < 4 {
			size = 8300
		}
		o := map[string]any{"number": i, "object": fmt.Sprintf("%s%d", prefix, i), "exists": size != 0, "size": size, "offset": offset, "sha256": strings.Repeat("0", 64)}
		if size == 0 {
			o["sha256"] = ""
		}
		offset += size
		objects = append(objects, o)
	}
	cli := func(args ...string) map[string]any {
		return map[string]any{"argv": append([]string{"--rbd_cache=false", "--log_to_stderr=false", "--err_to_stderr=true", "journal"}, args...),
			"returncode": 0, "stdout": "{}", "stderr": "", "elapsed_ns": 3, "parsed": map[string]any{}}
	}
	spec := plan.Pool + "/" + id
	value := map[string]any{
		"action": action, "token": token, "request_id": 9, "pool": plan.Pool, "pool_id": 3, "image": plan.Image, "native_global_id": 4242, "started_ns": 10, "finished_ns": 20,
		"image_id": id, "header_object": "rbd_header." + id, "journal_header": header, "journal_prefix": prefix,
		"stat":     map[string]any{"size": plan.Size, "obj_size": 65536, "num_objs": 4, "order": 16, "block_name_prefix": "rbd_data." + id},
		"features": 69, "flags": 0, "lockers": map[string]any{"empty": true, "tag": "", "exclusive": false, "lockers": []map[string]any{}}, "watchers": []map[string]any{},
		"class_calls": calls, "journal_objects": objects, "info": cli("info", "--format", "json", spec), "status": cli("status", "--format", "json", spec),
		"blobs": map[string]any{
			"image_head":      map[string]any{"name": rbdJournalBlobName(action, "image_head"), "length": plan.Size, "sha256": strings.Repeat("3", 64)},
			"journal_objects": map[string]any{"name": rbdJournalBlobName(action, "journal_objects"), "length": offset, "sha256": strings.Repeat("4", 64)},
		},
	}
	if rbdJournalInspected(action) {
		value["inspect"] = cli("inspect", "--verbose", spec)
	}
	if rbdJournalBlocklisted(action) {
		value["blocklist"] = []map[string]any{{"addr": "10.0.0.9:0/11", "until": "2023-11-14T23:13:20.000000+0000"}}
	}
	if action == "takeover" {
		value["blocklist_before"] = []map[string]any{}
	}
	if rbdJournalLockLogged(action) {
		value["lock_log"] = map[string]any{"present": true, "matched": 2, "truncated": false, "lines": []map[string]any{
			{"level": 10, "component": "librbd::managed_lock::AcquireRequest", "text": "@0 handle_get_locker: r=-2"},
			{"level": 10, "component": "librbd::managed_lock::AcquireRequest", "text": "@0 handle_lock: r=0"}}}
	}
	events := func(phase string, planned []rbdJournalEvent) []map[string]any {
		result := []map[string]any{}
		for _, e := range planned {
			sum := strings.Repeat("1", 64)
			if e.Kind == "discard" {
				sum = ""
			}
			result = append(result, map[string]any{"phase": phase, "kind": e.Kind, "offset": e.Offset, "length": e.Length, "sha256": sum, "started_ns": 11, "finished_ns": 12})
		}
		return result
	}
	switch action {
	case "setup":
		value["oracle_register"] = map[string]any{"object": header, "class": "journal", "method": "client_register", "input_hex": "00", "code": 0, "output_hex": ""}
		value["events"], value["close_ns"] = events("setup", plan.SetupEvents), 5
	case "write":
		value["events"], value["close_ns"] = events("write", plan.WriteEvents), 5
	case "replay":
		value["acquire_ns"], value["owner"], value["close_ns"] = 7, true, 5
	case "takeover":
		value["events"], value["write_ns"], value["owner"], value["close_ns"] = events("takeover", plan.TakeoverEvents), 8, true, 5
		value["boundary"] = map[string]any{
			"lockers":  map[string]any{"empty": false, "tag": "internal", "exclusive": true, "lockers": []map[string]any{{"client": "client.4321", "cookie": "auto 77", "addr": "10.0.0.9:0/11"}}},
			"watchers": []map[string]any{{"addr": "10.0.0.9:0/11", "id": 4321, "cookie": 77}, {"addr": "10.0.0.3:0/3", "id": 4400, "cookie": 88}}}
	case "force-break":
		owner := "10.0.0.9:0/11"
		value["force_break"] = map[string]any{"owners": []map[string]any{{"mode": 0, "owner": owner}}, "watchers_before": []map[string]any{{"addr": owner, "id": 4321, "cookie": 77}},
			"lockers_before":   map[string]any{"empty": false, "tag": "internal", "exclusive": true, "lockers": []map[string]any{{"client": "client.4321", "cookie": "auto 77", "addr": owner}}},
			"blocklist_before": []map[string]any{}, "break_ns": 9, "owners_after": []map[string]any{}, "is_owner": false}
	case "concurrent":
		owner, issued, completed := events("concurrent-owner", plan.ConcurrentOwnerEvents), events("concurrent", plan.ConcurrentEvents), events("concurrent", plan.ConcurrentEvents)
		for _, e := range issued {
			delete(e, "finished_ns")
		}
		watchers := []map[string]any{{"addr": "10.0.0.1:0/1", "id": 4242, "cookie": 7}}
		returns := make([]int, len(plan.ConcurrentEvents))
		value["events"], value["close_ns"], value["aio_return_values"], value["final_wait_ns"] = slices.Concat(owner, completed), 5, returns, 6
		value["phases"] = []map[string]any{
			{"phase": "owner", "time_ns": 1, "is_owner": true, "owners": []map[string]any{{"mode": 0, "owner": "10.0.0.1:0/1"}}, "watchers": watchers, "events": owner},
			{"phase": "issued", "time_ns": 2, "is_owner": false, "owners": []map[string]any{}, "watchers": watchers, "wait_ns": 3, "events": issued},
			{"phase": "completed", "time_ns": 3, "is_owner": true, "owners": []map[string]any{}, "watchers": watchers, "events": completed, "aio_return_values": returns},
			{"phase": "closed", "time_ns": 4, "is_owner": false, "owners": []map[string]any{}, "watchers": []map[string]any{}, "wait_ns": 4},
		}
	}
	return value
}

func TestRBDJournalNativeOutputShapeIsClosed(t *testing.T) {
	token := strings.Repeat("a1", 16)
	plan := rbdJournalPlanFor(token)
	encode := func(value map[string]any) []byte {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	fb := func(v map[string]any) map[string]any { return v["force_break"].(map[string]any) }
	for _, action := range slices.Concat(rbdJournalActions, rbdJournalLostAppendActions, []string{"change-primary", "observe-resubmitted"}, rbdJournalOwnerLossActions, rbdJournalForcedBreakActions) {
		if _, err := decodeRBDJournalNative(action, token, 9, plan, encode(rbdJournalOutput(token, plan, action))); err != nil {
			t.Fatal("well-formed journal output refused", action, err)
		}
	}
	// A checkpoint must carry its inspection; the lost-append observations
	// must not carry one, native events or a lock acquisition.
	checkpoint := rbdJournalOutput(token, plan, "checkpoint")
	delete(checkpoint, "inspect")
	if _, err := decodeRBDJournalNative("checkpoint", token, 9, plan, encode(checkpoint)); err == nil {
		t.Fatal("checkpoint without rbd journal inspect admitted")
	}
	for name, change := range map[string]func(map[string]any){
		"inspect":      func(v map[string]any) { v["inspect"] = rbdJournalOutput(token, plan, "checkpoint")["inspect"] },
		"events":       func(v map[string]any) { v["events"] = rbdJournalOutput(token, plan, "write")["events"] },
		"acquisition":  func(v map[string]any) { v["acquire_ns"], v["owner"] = 7, true },
		"other action": func(v map[string]any) { v["action"] = "observe-recorded" },
	} {
		value := rbdJournalOutput(token, plan, "observe-locked")
		change(value)
		if _, err := decodeRBDJournalNative("observe-locked", token, 9, plan, encode(value)); err == nil {
			t.Fatal("malformed lost-append observation admitted", name)
		}
	}
	for name, change := range map[string]func(map[string]any){
		"unknown field":      func(v map[string]any) { v["extra"] = 1 },
		"other image":        func(v map[string]any) { v["image"] = plan.BaseImage },
		"other prefix":       func(v map[string]any) { v["journal_prefix"] = "journal_data.4.5678ef01." },
		"no journaling":      func(v map[string]any) { v["features"] = 5 },
		"missing object":     func(v map[string]any) { v["journal_objects"] = v["journal_objects"].([]map[string]any)[:7] },
		"missing inspect":    func(v map[string]any) { delete(v, "inspect") },
		"failed register":    func(v map[string]any) { v["oracle_register"].(map[string]any)["code"] = -17 },
		"short events":       func(v map[string]any) { v["events"] = v["events"].([]map[string]any)[:2] },
		"foreign class call": func(v map[string]any) { v["class_calls"].([]map[string]any)[0]["class"] = "rbd" },
		"other cli argv":     func(v map[string]any) { v["status"].(map[string]any)["argv"] = []string{"journal", "status"} },
		"cli without output": func(v map[string]any) { delete(v["info"].(map[string]any), "parsed") },
	} {
		value := rbdJournalOutput(token, plan, "setup")
		change(value)
		if _, err := decodeRBDJournalNative("setup", token, 9, plan, encode(value)); err == nil {
			t.Fatal("malformed journal output admitted", name)
		}
	}
	observe := rbdJournalOutput(token, plan, "observe-registered")
	observe["events"] = rbdJournalOutput(token, plan, "write")["events"]
	if _, err := decodeRBDJournalNative("observe-registered", token, 9, plan, encode(observe)); err == nil {
		t.Fatal("observation with native events admitted")
	}
	if _, err := decodeRBDJournalNative("replay", token, 9, plan, encode(rbdJournalOutput(token, plan, "write"))); err == nil {
		t.Fatal("write output admitted as replay")
	}
	phases := func(v map[string]any) []map[string]any { return v["phases"].([]map[string]any) }
	for name, change := range map[string]func(map[string]any){
		"missing phase":          func(v map[string]any) { v["phases"] = phases(v)[:3] },
		"reordered phases":       func(v map[string]any) { p := phases(v); p[1], p[2] = p[2], p[1] },
		"owner without lock":     func(v map[string]any) { phases(v)[0]["is_owner"] = false },
		"completed without lock": func(v map[string]any) { phases(v)[2]["is_owner"] = false },
		"issued already done":    func(v map[string]any) { phases(v)[1]["events"].([]map[string]any)[0]["finished_ns"] = 9 },
		"watcher after close":    func(v map[string]any) { phases(v)[3]["watchers"] = phases(v)[0]["watchers"] },
		"missing aio results":    func(v map[string]any) { delete(v, "aio_return_values") },
		"no final signal":        func(v map[string]any) { delete(v, "final_wait_ns") },
		"no inspect":             func(v map[string]any) { delete(v, "inspect") },
		"native events only":     func(v map[string]any) { v["events"] = v["events"].([]map[string]any)[2:] },
	} {
		value := rbdJournalOutput(token, plan, "concurrent")
		change(value)
		if _, err := decodeRBDJournalNative("concurrent", token, 9, plan, encode(value)); err == nil {
			t.Fatal("malformed concurrent journal output admitted", name)
		}
	}
	// The owner-loss and forced-break actions carry the blocklist, the
	// takeover its own writes and lock ownership, the forced break its
	// break; no other action carries any of them.
	for name, change := range map[string]struct {
		action string
		change func(map[string]any)
	}{
		"takeover without its writes":         {"takeover", func(v map[string]any) { delete(v, "events") }},
		"takeover with the write events":      {"takeover", func(v map[string]any) { v["events"] = rbdJournalOutput(token, plan, "write")["events"] }},
		"takeover without ownership":          {"takeover", func(v map[string]any) { delete(v, "owner") }},
		"takeover without a blocklist before": {"takeover", func(v map[string]any) { delete(v, "blocklist_before") }},
		"takeover without a blocklist":        {"takeover", func(v map[string]any) { delete(v, "blocklist") }},
		"takeover without inspect":            {"takeover", func(v map[string]any) { delete(v, "inspect") }},
		"takeover without write time":         {"takeover", func(v map[string]any) { delete(v, "write_ns") }},
		"takeover without its boundary":       {"takeover", func(v map[string]any) { delete(v, "boundary") }},
		"boundary without its lock record": {"takeover", func(v map[string]any) {
			delete(v["boundary"].(map[string]any), "lockers")
		}},
		"boundary lock record inconsistent": {"takeover", func(v map[string]any) {
			v["boundary"].(map[string]any)["lockers"].(map[string]any)["empty"] = true
		}},
		"boundary without watchers": {"takeover", func(v map[string]any) {
			v["boundary"].(map[string]any)["watchers"] = []map[string]any{}
		}},
		"boundary watcher without address": {"takeover", func(v map[string]any) {
			v["boundary"].(map[string]any)["watchers"].([]map[string]any)[0]["addr"] = ""
		}},
		"replay with a boundary": {"replay", func(v map[string]any) {
			v["boundary"] = rbdJournalOutput(token, plan, "takeover")["boundary"]
		}},
		"break without its output": {"force-break", func(v map[string]any) { delete(v, "force_break") }},
		"break of two owners": {"force-break", func(v map[string]any) {
			fb(v)["owners"] = append(fb(v)["owners"].([]map[string]any), fb(v)["owners"].([]map[string]any)[0])
		}},
		"break of another owner":               {"force-break", func(v map[string]any) { fb(v)["owners"].([]map[string]any)[0]["owner"] = "10.0.0.2:0/2" }},
		"owner left after the break":           {"force-break", func(v map[string]any) { fb(v)["owners_after"] = fb(v)["owners"] }},
		"breaker owns the lock":                {"force-break", func(v map[string]any) { fb(v)["is_owner"] = true }},
		"break with native events":             {"force-break", func(v map[string]any) { v["events"] = rbdJournalOutput(token, plan, "takeover")["events"] }},
		"break without its blocklist":          {"force-break", func(v map[string]any) { delete(v, "blocklist") }},
		"fenced observation without blocklist": {"observe-fenced", func(v map[string]any) { delete(v, "blocklist") }},
		"appended observation with a break":    {"observe-appended", func(v map[string]any) { v["force_break"] = rbdJournalOutput(token, plan, "force-break")["force_break"] }},
		"locked observation with a blocklist":  {"observe-locked", func(v map[string]any) { v["blocklist"] = []map[string]any{} }},
		"replay with a blocklist":              {"replay", func(v map[string]any) { v["blocklist"] = []map[string]any{} }},
		"replay with a write time":             {"replay", func(v map[string]any) { v["write_ns"] = 3 }},
		"takeover without its lock log":        {"takeover", func(v map[string]any) { delete(v, "lock_log") }},
		"break without its lock log":           {"force-break", func(v map[string]any) { delete(v, "lock_log") }},
		"observation with a lock log":          {"observe-fenced", func(v map[string]any) { v["lock_log"] = rbdJournalOutput(token, plan, "takeover")["lock_log"] }},
		"lock log miscounted": {"takeover", func(v map[string]any) {
			v["lock_log"].(map[string]any)["matched"] = 1
		}},
		"lock log truncation unstated": {"takeover", func(v map[string]any) {
			v["lock_log"].(map[string]any)["matched"] = 3
		}},
		"lock log line of another library": {"takeover", func(v map[string]any) {
			v["lock_log"].(map[string]any)["lines"].([]map[string]any)[0]["component"] = "osdc::Objecter"
		}},
		"lock log line too long": {"takeover", func(v map[string]any) {
			v["lock_log"].(map[string]any)["lines"].([]map[string]any)[0]["text"] = strings.Repeat("x", rbdJournalLockLogText+1)
		}},
	} {
		value := rbdJournalOutput(token, plan, change.action)
		change.change(value)
		if _, err := decodeRBDJournalNative(change.action, token, 9, plan, encode(value)); err == nil {
			t.Fatal("malformed owner-loss journal output admitted", name)
		}
	}
	replay := rbdJournalOutput(token, plan, "replay")
	replay["phases"] = rbdJournalOutput(token, plan, "concurrent")["phases"]
	if _, err := decodeRBDJournalNative("replay", token, 9, plan, encode(replay)); err == nil {
		t.Fatal("replay output with concurrent phases admitted")
	}
	if err := (&fixtureCluster{profile: profile{rbdBase: "1"}}).rbdJournalControl(t.Context(), t.TempDir(), []string{"1", token, "setup"}); err == nil {
		t.Fatal("journal control admitted without a prepared scope")
	}
}

// The embedded journal driver must at least be valid Python where an
// interpreter is available; the live acceptance runs it against native
// librbd.
func TestRBDJournalNativeDriverParses(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 unavailable")
	}
	cmd := exec.Command(python, "-c", "import ast, sys; ast.parse(sys.stdin.read())")
	cmd.Stdin = strings.NewReader(rbdJournalNativePython)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatal("native RBD journal driver is not valid Python", err, string(output))
	}
}
