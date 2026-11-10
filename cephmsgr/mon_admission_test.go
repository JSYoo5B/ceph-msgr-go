package cephmsgr

import (
	"errors"
	"reflect"
	"testing"

	"github.com/JSYoo5B/ceph-msgr-go/internal/cephx"
	"github.com/JSYoo5B/ceph-msgr-go/internal/maps"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/session"
)

func monAdmissionUnit() (*Client, *monAdmission) {
	c := &Client{changed: make(chan struct{}), wake: make(chan struct{}, 1), auth: &cephx.Client{Name: "client.unit", Tickets: make(map[uint32]cephx.Ticket)}}
	a := newMonAdmission(c)
	a.session = new(session.Session)
	c.mon = a.session
	return c, a
}

func monAdmissionReady(a *monAdmission) bool {
	select {
	case <-a.ready:
		return true
	default:
		return false
	}
}

func TestMonAdmissionCarriesFirstValidatedMonMapAndHighestEarlyManagerMap(t *testing.T) {
	c, a := monAdmissionUnit()
	for _, epoch := range []uint32{3, 9, 4} {
		if err := a.handle(msgr.MessageData{Type: msgr.MgrMapMessage, Front: mockMgrMap(epoch, 100+uint64(epoch), 6800)}); err != nil {
			t.Fatal(err)
		}
	}
	// A subscription payload before admission is dropped without decoding.
	if err := a.handle(msgr.MessageData{Type: msgr.ConfigMessage, Front: []byte{0}}); err != nil {
		t.Fatal("unadmitted subscription payload was decoded", err)
	}
	if c.mgrMap.Epoch != 0 || monAdmissionReady(a) {
		t.Fatal("early manager map escaped admission", c.mgrMap.Epoch)
	}
	front := epochMonMap(5, 2)
	if err := a.handle(msgr.MessageData{Type: msgr.MonMapMessage, Front: front}); err != nil {
		t.Fatal(err)
	}
	want, err := maps.DecodeMon(front)
	if err != nil {
		t.Fatal(err)
	}
	if !monAdmissionReady(a) || !reflect.DeepEqual(a.result(), want) {
		t.Fatal("admission did not carry the validated MonMap", a.result(), want)
	}
	if c.monMap.Epoch != 5 || c.mgrMap.Epoch != 9 || c.mgrMap.GlobalID != 109 {
		t.Fatal("admission did not apply the highest buffered manager map", c.monMap.Epoch, c.mgrMap.Epoch, c.mgrMap.GlobalID)
	}
	// Later maps are ordinary updates; the admission result stays the first map.
	if err := a.handle(msgr.MessageData{Type: msgr.MonMapMessage, Front: epochMonMap(6, 3)}); err != nil {
		t.Fatal(err)
	}
	if err := a.handle(msgr.MessageData{Type: msgr.MgrMapMessage, Front: mockMgrMap(10, 110, 6800)}); err != nil {
		t.Fatal(err)
	}
	if a.result().Epoch != 5 || a.result().AuthEpoch != 2 || c.monMap.Epoch != 6 || c.mgrMap.Epoch != 10 {
		t.Fatal("post-admission maps changed the admission result or were not applied", a.result(), c.monMap.Epoch, c.mgrMap.Epoch)
	}
}

func TestMonAdmissionRejectedOrStaleCandidatePublishesNothing(t *testing.T) {
	early := msgr.MessageData{Type: msgr.MgrMapMessage, Front: mockMgrMap(9, 4000, 6810)}
	t.Run("FSID mismatch", func(t *testing.T) {
		c, a := monAdmissionUnit()
		c.fsid = [16]byte{2}
		if err := a.handle(early); err != nil {
			t.Fatal(err)
		}
		if err := a.handle(msgr.MessageData{Type: msgr.MonMapMessage, Front: mockMonMap(20, [16]byte{1})}); err == nil {
			t.Fatal("foreign cluster candidate was admitted")
		}
		if monAdmissionReady(a) || a.bufferedMgr != nil || c.mgrMap.Epoch != 0 || c.monMap.Epoch != 0 {
			t.Fatal("rejected candidate published or retained its maps", c.mgrMap.Epoch, c.monMap.Epoch)
		}
	})
	t.Run("release", func(t *testing.T) {
		c, a := monAdmissionUnit()
		if err := a.handle(early); err != nil {
			t.Fatal(err)
		}
		if err := a.handle(msgr.MessageData{Type: msgr.MonMapMessage, Front: mockMonMap(19, [16]byte{1})}); !errors.Is(err, maps.ErrRelease) {
			t.Fatal("pre-Tentacle candidate lost its release cause", err)
		}
		if monAdmissionReady(a) || a.bufferedMgr != nil || c.mgrMap.Epoch != 0 {
			t.Fatal("rejected candidate published its early manager map")
		}
	})
	t.Run("malformed early manager map", func(t *testing.T) {
		c, a := monAdmissionUnit()
		if err := a.handle(early); err != nil {
			t.Fatal(err)
		}
		if err := a.handle(msgr.MessageData{Type: msgr.MgrMapMessage, Front: []byte{0}}); !errors.Is(err, msgr.ErrFrame) {
			t.Fatal("malformed early manager map lost frame provenance", err)
		}
		if a.bufferedMgr != nil || c.mgrMap.Epoch != 0 {
			t.Fatal("malformed early map kept an earlier buffered map")
		}
	})
	t.Run("stale candidate", func(t *testing.T) {
		c, a := monAdmissionUnit()
		c.mon = new(session.Session) // Rolled back or replaced before its maps arrive.
		if err := a.handle(early); err != nil {
			t.Fatal(err)
		}
		if err := a.handle(msgr.MessageData{Type: msgr.MonMapMessage, Front: mockMonMap(20, [16]byte{1})}); err != nil {
			t.Fatal(err)
		}
		if monAdmissionReady(a) || a.bufferedMgr != nil || c.mgrMap.Epoch != 0 || c.monMap.Epoch != 0 {
			t.Fatal("stale candidate was admitted or published maps")
		}
	})
}
