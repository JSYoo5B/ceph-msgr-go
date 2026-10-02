package cephmsgr

import (
	"encoding/binary"
	"errors"
	"net/netip"
	"reflect"
	"testing"

	"github.com/jsyoo5b/ceph-msgr-go/internal/msgr"
)

func TestNamedMonitorBindingChangeAfterTellPreservesUnknownOutcome(t *testing.T) {
	for _, change := range []string{"removed", "renamed", "address reassigned"} {
		t.Run(change, func(t *testing.T) {
			address := namedTestAddressB()
			f := namedTestFixtureFor(t, address, nil, nil)
			if err := f.c.WaitMgrReady(f.ctx); err != nil {
				t.Fatal(err)
			}
			before := f.c.Snapshot()
			f.c.mu.Lock()
			primary, mgr, auth := f.c.mon, f.c.mgr, f.c.auth
			monMap, mgrMap := f.c.monMap, f.c.mgrMap
			f.c.mu.Unlock()
			finished := make(chan error, 1)
			go func() {
				_, err := f.c.MonTellTo(f.ctx, "b", Command{JSON: []byte(`{"prefix":"hold mutation"}`)})
				finished <- err
			}()
			p := namedTestTarget(t, f)
			p.next(t, f.ctx, msgr.MonGetMapMessage)
			request := p.next(t, f.ctx, msgr.TellCommandMessage)
			if request.Transaction == 0 {
				t.Fatal("private peer did not receive a transactional Tell")
			}
			// Receipt of the complete Tell proves both independent admission and
			// transmission. The peer holds its reply, so this map is the first
			// terminal event for an already-started, still-pending command.
			members := []namedTestMember{
				{"a", []msgr.Address{{Type: 2, Endpoint: netip.MustParseAddrPort("192.0.2.1:3300")}}},
				{"b", []msgr.Address{address}},
				{"c", []msgr.Address{{Type: 2, Endpoint: netip.MustParseAddrPort("192.0.2.3:3302")}}},
			}
			switch change {
			case "removed":
				members = []namedTestMember{members[0], members[2]}
			case "renamed":
				members[1].name = "replacement"
			case "address reassigned":
				members[1].addresses, members[2].addresses = members[2].addresses, members[1].addresses
			}
			front := namedTestMap([16]byte{1}, 20, members)
			// Independent fixture layout: bufferlist U32 + envelope U8/U8/U32
			// + raw FSID16 precede the MonMap epoch. Advance it from 1 to 2.
			binary.LittleEndian.PutUint32(front[4+6+16:], 2)
			p.send(t, f.ctx, msgr.MessageData{Type: msgr.MonMapMessage, Version: 1, Front: front})
			err := admissionResult(t, f.ctx, finished)
			var unknown *OutcomeUnknownError
			if !errors.As(err, &unknown) || !errors.Is(err, ErrMonitorTargetChanged) || !errors.Is(unknown.Cause, ErrMonitorTargetChanged) {
				t.Fatal("post-transmission binding change lost uncertainty or its cause", err)
			}
			select {
			case <-p.done:
			case <-f.ctx.Done():
				t.Fatal("private session outlived its binding refusal", f.ctx.Err())
			}
			namedTestPrimaryHealthy(t, f)
			if _, err := f.c.MgrCommand(f.ctx, Command{JSON: []byte(`{"prefix":"status"}`)}); err != nil {
				t.Fatal("private binding change damaged ordinary MGR commands", err)
			}
			f.c.mu.Lock()
			preserved := f.c.mon == primary && f.c.mgr == mgr && f.c.auth == auth && reflect.DeepEqual(monMap, f.c.monMap) && reflect.DeepEqual(mgrMap, f.c.mgrMap)
			f.c.mu.Unlock()
			if !preserved || !reflect.DeepEqual(before, f.c.Snapshot()) || p.tellCount.Load() != 1 || f.namedDials.Load() != 1 || f.monDials.Load() != 1 || f.mgrDials.Load() != 1 {
				t.Fatal("private binding refusal changed primary state or retried a mutation", err, preserved, before, f.c.Snapshot(), p.tellCount.Load(), f.namedDials.Load(), f.monDials.Load(), f.mgrDials.Load())
			}
		})
	}
}
