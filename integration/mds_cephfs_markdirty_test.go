package integration_test

import (
	"reflect"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
)

// A rejected write publishes nothing: with FILE_WR regranted over latched
// buffered data, markDirty must leave the inode, dirty state, latch and events
// exactly as they were. The same holds for data still buffered or writing.
func TestCephFSMarkDirtyRejectionLeavesLatchedStateUnchanged(t *testing.T) {
	h := newCephFSLatchHarness(t)
	h.buffer(&cephfsWritebackStopped{Phase: "b", Outcomes: []string{cephfsWriteUncertain}})
	all := cephCapsForMode(cephFileModeRDWR) | cephCapPin
	h.push(cephmsgr.MDSCapOpRevoke, all&^(cephCapFileWR|cephCapFileBuffer|cephCapFileExcl))
	h.waitLatch()
	// FILE_WR and FILE_BUFFER come back while the latch is unresolved.
	h.push(cephmsgr.MDSCapOpGrant, all)
	if c, _ := h.a.cap(cephfsLatchTestIno); c.Issued&cephCapFileWR == 0 {
		t.Fatal("FILE_WR was not regranted", cephCapString(c.Issued))
	}
	type state struct {
		cap   cephfsCap
		dirty cephfsDirtyFile
	}
	take := func(a *cephfsCapAgent) state {
		c, _ := a.cap(cephfsLatchTestIno)
		d, _ := a.dirtyState(cephfsLatchTestIno)
		c.Open = nil
		return state{c, d}
	}
	reject := func(what string, a *cephfsCapAgent, buffered bool) {
		t.Helper()
		before := take(a)
		if err := a.markDirty(cephfsLatchTestIno, before.cap.Size+1000, cephmsgr.MDSTime{Seconds: 9999}, 3, buffered, 0, func() error { return nil }); err == nil {
			t.Fatal("write admitted:", what)
		}
		if after := take(a); !reflect.DeepEqual(before, after) {
			t.Fatalf("%s: rejected write changed state\nbefore %+v\nafter  %+v", what, before, after)
		}
	}
	reject("latched, buffered write", h.a, true)
	reject("latched, unbuffered write", h.a, false)

	// Buffered but not latched, and mid-writeback.
	h2 := newCephFSLatchHarness(t)
	h2.buffer(nil)
	for _, writing := range []bool{false, true} {
		h2.a.mu.Lock()
		h2.a.dirty[cephfsLatchTestIno].writing = writing
		h2.a.mu.Unlock()
		reject("buffered data", h2.a, false)
	}
	// A rejection for missing caps does not create dirty state either.
	h3 := newCephFSLatchHarness(t)
	h3.a.caps[cephfsLatchTestIno].Issued = 0
	if err := h3.a.markDirty(cephfsLatchTestIno, 500, cephmsgr.MDSTime{Seconds: 1}, 1, false, 0, nil); err == nil || len(h3.a.dirty) != 0 {
		t.Fatal("write without caps admitted or created dirty state", err)
	}
}
