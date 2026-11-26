package integration_test

import (
	"fmt"
	"slices"
	"time"
)

// rbdJournalBeforeDownWindowClaims checks the pre-down restart cell from the
// nodown flag to the end of the window after the kill: the fixture flagged
// exactly P nodown before the appends, and for the whole window, which began
// after the kill request and ended rbdJournalBeforeDownWindow or more after
// it, every OSDMap read listed P up, flagged, at the address the subject
// routed to before the kill, with every followed object on the original
// acting set led by P and no OSD down. The subject's own reread in the
// window found that address too. The caller outcomes and the kill itself are
// the restart cell's claims.
func rbdJournalBeforeDownWindowClaims(r *rbdJournalLostAppendRecord, acting []int32, fail func(string, ...any)) {
	h, p := r.Held, r.Held.Pending
	text := rbdJournalHeldText
	address := p.AddressBefore
	k := p.KeepUp
	if k == nil || k.OSD != h.Primary || !slices.Equal(k.Commands, []string{fmt.Sprintf("osd add-nodown %d", h.Primary)}) || !k.Before.Up || slices.Contains(k.Before.State, "nodown") ||
		!k.After.Up || !slices.Contains(k.After.State, "nodown") || k.After.Epoch <= k.Before.Epoch || address == "" || k.Before.Address != address || k.After.Address != address ||
		k.Before.OSD != h.Primary || k.After.OSD != h.Primary || len(k.After.Down) != 0 {
		fail("keep-up: osd.%d at %s was not flagged nodown by exactly one command, up before and after: %s", h.Primary, address, text(k))
		return
	}
	if h.SentAtEpoch < k.After.Epoch {
		fail("keep-up: the appends were placed at epoch %d, before the flag's epoch %d", h.SentAtEpoch, k.After.Epoch)
	}
	d := p.Dead
	if d == nil || h.Kill == nil {
		fail("observe-primary-dead: the fixture's read or the kill is missing: %s", text(d))
		return
	}
	killed, killErr := time.Parse(time.RFC3339Nano, h.Kill.RequestedUTC)
	first, firstErr := time.Parse(time.RFC3339Nano, d.FirstUTC)
	last, lastErr := time.Parse(time.RFC3339Nano, d.LastUTC)
	if killErr != nil || firstErr != nil || lastErr != nil || !first.After(killed) || last.Sub(killed) < rbdJournalBeforeDownWindow || d.Polls < 2 {
		fail("observe-primary-dead: the window did not begin after the kill and last %v after it: %s (killed %s)", rbdJournalBeforeDownWindow, text(d), h.Kill.RequestedUTC)
	}
	for _, info := range []nativeRBDJournalOSDInfo{d.First, d.Last} {
		if info.OSD != h.Primary || !info.Up || info.Address != address || !slices.Contains(info.State, "nodown") || len(info.Down) != 0 || info.Epoch < k.After.Epoch {
			fail("observe-primary-dead: the OSDMap did not list osd.%d up and flagged at %s with every OSD up: %s", h.Primary, address, text(info))
		}
	}
	if d.OSD != h.Primary || d.Address != address || !d.Up || d.Last.Epoch < d.First.Epoch {
		fail("observe-primary-dead: some read of the window listed osd.%d down or at another address: %s", h.Primary, text(d))
	}
	if len(d.Objects) != 3 {
		fail("observe-primary-dead: the placements of the three followed objects are missing: %s", text(d.Objects))
	}
	for _, o := range d.Objects {
		if !slices.Equal(o.Acting, acting) || !slices.Equal(o.Up, acting) || o.ActingPrimary != h.Primary || o.Epoch < d.First.Epoch {
			fail("observe-primary-dead: %s is not on the original acting set %v led by osd.%d: %s", o.Object, acting, h.Primary, text(o))
		}
	}
	if p.AddressDead != address {
		fail("observe-primary-dead: the subject's reread routes osd.%d to %q, not the killed instance's %q", h.Primary, p.AddressDead, address)
	}
}

// rbdJournalBeforeDownBackClaims checks how P came back: started after the
// window, then its entry in the OSDMap shows the old instance marked down at
// an epoch after every read of the window and the new one up from a later
// epoch, at a new address, which the subject's reread found; the PG is
// clean in an interval that began no earlier than that and later than the
// one the lock was taken in. It returns false if the read is missing.
func rbdJournalBeforeDownBackClaims(r *rbdJournalLostAppendRecord, placed *nativeRBDJournalPGQuery, fail func(string, ...any)) bool {
	h, p := r.Held, r.Held.Pending
	text := rbdJournalHeldText
	back := p.Back
	if back == nil || len(back.Objects) != 3 || back.Query == nil || back.OSDInfo == nil || p.Dead == nil {
		fail("observe-primary-back: the fixture's read is missing: %s", text(back))
		return false
	}
	started, err := time.Parse(time.RFC3339Nano, p.StartedUTC)
	windowEnd, endErr := time.Parse(time.RFC3339Nano, p.Dead.LastUTC)
	if err != nil || endErr != nil || !started.After(windowEnd) {
		fail("observe-primary-back: osd.%d was not started after the window (started %q, window to %q)", h.Primary, p.StartedUTC, p.Dead.LastUTC)
	}
	if !back.Settled || len(back.Down) != 0 || back.OSD != -1 || len(back.Ops) != 0 {
		fail("observe-primary-back: osd.%d is not up again with every OSD up: %s", h.Primary, text(back))
	}
	info := back.OSDInfo
	if info.OSD != h.Primary || !info.Up || info.DownAt <= p.Dead.Last.Epoch || info.UpFrom <= info.DownAt || info.Epoch < info.UpFrom {
		fail("observe-primary-back: osd.%d was not marked down after the window (last read at epoch %d) and up again later: %s", h.Primary, p.Dead.Last.Epoch, text(info))
	}
	if p.AddressAfter == "" || p.AddressAfter == p.AddressBefore || info.Address != p.AddressAfter {
		fail("observe-primary-back: the restarted osd.%d kept its address %q (now %q, OSDMap %q)", h.Primary, p.AddressBefore, p.AddressAfter, info.Address)
	}
	if q := back.Query; placed != nil && (q.SameIntervalSince <= placed.SameIntervalSince || q.SamePrimarySince <= placed.SamePrimarySince || q.SameIntervalSince < info.UpFrom) {
		fail("observe-primary-back: the PG is not in an interval that began once osd.%d was up again (epoch %d): %s, before %s", h.Primary, info.UpFrom, text(q), text(placed))
	}
	return true
}
