package cephmsgr

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/cephx"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
)

// renewalTargetFixture serves synthetic MONs at two learned addresses. The
// seed MON can be stopped so that each later dial to it consumes the whole
// setup timeout, as a failed TCP dial to a stopped host can.
type renewalTargetFixture struct {
	mu        sync.Mutex
	dials     []string
	seedPeers []net.Conn
	seedDown  bool
	refuseB   bool
	proofs    []renewalProof
}

type renewalProof struct {
	endpoint         string
	previousGlobalID uint64
	oldSecretID      uint64
	oldTicket        bool
}

const (
	renewalSeedMON  = "192.0.2.1:3300"
	renewalOtherMON = "192.0.2.3:3300"
)

func (f *renewalTargetFixture) dial(ctx context.Context, _, endpoint string) (net.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.dials = append(f.dials, endpoint)
	seedDown, refuseB := f.seedDown, f.refuseB
	f.mu.Unlock()
	switch {
	case endpoint == renewalSeedMON && seedDown:
		<-ctx.Done()
		return nil, ctx.Err()
	case endpoint == renewalOtherMON && refuseB:
		return nil, errors.New("synthetic connection refused")
	case endpoint != renewalSeedMON && endpoint != renewalOtherMON:
		return nil, errors.New("unexpected synthetic endpoint " + endpoint)
	}
	client, peer := net.Pipe()
	if endpoint == renewalSeedMON {
		f.mu.Lock()
		f.seedPeers = append(f.seedPeers, peer)
		f.mu.Unlock()
	}
	addresses := []msgr.Address{
		{Type: 2, Endpoint: netip.MustParseAddrPort(renewalSeedMON)},
		{Type: 2, Endpoint: netip.MustParseAddrPort(renewalOtherMON)},
	}
	cfg := peerConfig{fsid: [16]byte{1}, release: 20, role: 1, authTTL: 2 * time.Second, mgrTTL: 2 * time.Second,
		monMap: mockMonMapAddresses(20, [16]byte{1}, addresses),
		authProof: func(previous, secretID uint64, ticket []byte) {
			f.mu.Lock()
			f.proofs = append(f.proofs, renewalProof{endpoint: endpoint, previousGlobalID: previous, oldSecretID: secretID, oldTicket: len(ticket) > 0})
			f.mu.Unlock()
		}}
	go func() { mockDaemon(peer, cfg) }()
	return client, nil
}

// reset returns and clears the dial and proof records.
func (f *renewalTargetFixture) reset() ([]string, []renewalProof) {
	f.mu.Lock()
	defer f.mu.Unlock()
	dials, proofs := f.dials, f.proofs
	f.dials, f.proofs = nil, nil
	return dials, proofs
}

// Routine renewal must first re-authenticate to the MON that currently admits
// the client. A stopped earlier seed is only a fallback; dialing it first
// would spend the setup timeout before every renewal.
func TestRoutineRenewalPrefersAdmittedMonitor(t *testing.T) {
	fixture := &renewalTargetFixture{}
	installed := make(chan authIssuerObservation, 16)
	hooks := &authTraceHooks{installed: func(o authIssuerObservation, _ *cephx.Client) {
		select {
		case installed <- o:
		default:
			t.Error("installation observations overflowed")
		}
	}}
	options := mockOptions(t, 20, [16]byte{1})
	options.Monitors = []string{renewalSeedMON}
	options.DialContext = fixture.dial
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	c, err := Dial(context.WithValue(ctx, authTraceContextKey{}, hooks), options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	initial := c.Snapshot()
	next := func(phase string) authIssuerObservation {
		t.Helper()
		select {
		case o := <-installed:
			return o
		case <-ctx.Done():
			t.Fatal(phase, ctx.Err())
		}
		panic("unreachable")
	}
	if o := next("initial admission"); o.endpoint != renewalSeedMON {
		t.Fatal("initial admission used", o.endpoint)
	}
	checkIdentity := func(phase string, proofs []renewalProof, endpoint string) {
		t.Helper()
		if got := c.Snapshot(); got.GlobalID != initial.GlobalID || got.GlobalID == 0 {
			t.Fatal(phase, "changed GlobalID", initial.GlobalID, got.GlobalID)
		}
		c.mu.Lock()
		authErr := c.authErr
		c.mu.Unlock()
		if authErr != nil {
			t.Fatal(phase, "recorded an authentication rejection", authErr)
		}
		i := slices.IndexFunc(proofs, func(p renewalProof) bool { return p.endpoint == endpoint })
		if i < 0 || proofs[i].previousGlobalID != initial.GlobalID || proofs[i].oldSecretID == 0 || !proofs[i].oldTicket {
			t.Fatal(phase, "did not present the previous GlobalID and AUTH ticket", proofs)
		}
	}

	// Stop the seed MON. Its session ends and later dials to it hang until
	// the setup timeout. Recovery has no healthy admitted MON, so it uses
	// the ordinary order: seed first, then the other learned MON.
	fixture.reset()
	fixture.mu.Lock()
	fixture.seedDown = true
	peers := fixture.seedPeers
	fixture.mu.Unlock()
	for _, peer := range peers {
		peer.Close()
	}
	if o := next("recovery"); o.endpoint != renewalOtherMON {
		t.Fatal("recovery admitted", o.endpoint)
	}
	dials, proofs := fixture.reset()
	if !slices.Equal(dials, []string{renewalSeedMON, renewalOtherMON}) {
		t.Fatal("recovery dial order", dials)
	}
	checkIdentity("recovery", proofs, renewalOtherMON)

	// The next routine renewal starts while the recovered session is healthy.
	if o := next("renewal"); o.endpoint != renewalOtherMON {
		t.Fatal("renewal admitted", o.endpoint)
	}
	dials, proofs = fixture.reset()
	if !slices.Equal(dials, []string{renewalOtherMON}) {
		t.Fatal("renewal did not go to the admitted MON first", dials)
	}
	checkIdentity("renewal", proofs, renewalOtherMON)

	// When the admitted MON accepts no new connection, renewal falls back
	// to the remaining targets in their ordinary order.
	fixture.mu.Lock()
	fixture.seedDown, fixture.refuseB = false, true
	fixture.mu.Unlock()
	if o := next("fallback renewal"); o.endpoint != renewalSeedMON {
		t.Fatal("fallback renewal admitted", o.endpoint)
	}
	dials, proofs = fixture.reset()
	if !slices.Equal(dials, []string{renewalOtherMON, renewalSeedMON}) {
		t.Fatal("fallback renewal dial order", dials)
	}
	checkIdentity("fallback renewal", proofs, renewalSeedMON)
}

func TestMonitorTargetsPreferHealthyAdmittedTarget(t *testing.T) {
	seed := "192.0.2.1:3300"
	learned := []msgr.Address{
		{Type: 2, Endpoint: netip.MustParseAddrPort("192.0.2.1:3300")},
		{Type: 2, Nonce: 7, Endpoint: netip.MustParseAddrPort("[2001:db8::3]:3300"), ScopeID: 4},
	}
	admitted := monTarget{endpoint: dialAddress(learned[1]), address: learned[1]}
	endpoints := func(targets []monTarget) []string {
		var out []string
		for _, target := range targets {
			out = append(out, target.endpoint)
		}
		return out
	}
	targets, failures := monTargets([]string{seed}, learned, admitted, true)
	if len(failures) != 0 || len(targets) != 3 || targets[0] != admitted || !slices.Equal(endpoints(targets[1:]), []string{seed, "192.0.2.1:3300"}) {
		t.Fatal("healthy admitted target not first with full identity", targets, failures)
	}
	if targets, _ := monTargets([]string{seed}, learned, admitted, false); targets[0].endpoint != seed {
		t.Fatal("unhealthy admitted session reordered targets", targets)
	}
	// A target whose full identity is no longer listed is not invented.
	other := admitted
	other.address.Nonce = 8
	if targets, _ := monTargets([]string{seed}, learned, other, true); len(targets) != 3 || targets[0].endpoint != seed || slices.Contains(targets, other) {
		t.Fatal("unlisted admitted target was used", targets)
	}
}
