package integration_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// OMAP and xattr writes committed with ONDISK survive SIGKILL of the object's
// primary, and an OMAP compare-then-set vector whose reply was lost resolves
// through an explicit resubmission: re-executing it would fail its compare.
func TestCephOSDOmapDurabilityIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" {
		t.Skip("daemon termination persistence needs the BlueStore fixture")
	}
	c, _, base, original := osdFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	prepared := false
	t.Cleanup(func() {
		if !prepared {
			return
		}
		cleanup, stop := context.WithTimeout(context.Background(), 40*time.Second)
		defer stop()
		if result := nativeOmapWrite(t, cleanup, base, "cleanup", omapWriteInput{}); !result.Cleanup.Removed {
			t.Error("owned objects were not removed", result.Cleanup)
		}
	})
	seedModel := map[string][]byte{"k0": []byte("zero"), "k1": []byte("one")}
	owned := nativeOmapWrite(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: []byte("omap-durability"), Entries: omapWriteEntries(seedModel)}, Reserve: 1})
	prepared = true
	if owned.SeedCode == nil || *owned.SeedCode != 0 || len(owned.Objects) != 2 {
		t.Fatal("native seed differs", owned.SeedCode)
	}
	subject := owned.Objects[0]
	inventory := [][]byte{[]byte(""), []byte("k0"), []byte("k1"), []byte("k2")}
	observe := func() omapWriteState {
		return nativeOmapWrite(t, ctx, base, "observe", omapWriteInput{Object: subject, InventoryKeys: inventory}).State
	}
	route := func() osdFixtureOracle {
		return nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": subject, "namespace": ""}).Placement
	}
	header := []byte{0, 0xff, 'H'}
	want := map[string][]byte{"k1": []byte("one"), "k2": {0, 1, 2}}

	t.Run("committed OMAP and xattr survive SIGKILL", func(t *testing.T) {
		target := route()
		o := mutationConnection(t, c, ctx, target)
		r := osdObjectRequest(target,
			cephmsgr.OSDOperation{Code: cephmsgr.OSDSetOmapHeader, Omap: cephmsgr.OSDOmapInput{Header: header}},
			cephmsgr.OSDOperation{Code: cephmsgr.OSDSetOmapValues, Omap: cephmsgr.OSDOmapInput{Entries: []cephmsgr.OSDOmapEntry{{Key: "k2", Value: []byte{0, 1, 2}}}}},
			cephmsgr.OSDOperation{Code: cephmsgr.OSDRemoveOmapKeys, Omap: cephmsgr.OSDOmapInput{Keys: []string{"k0"}}},
			cephmsgr.OSDOperation{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: "durable"}, Data: []byte("yes")})
		r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700005000}
		reply, err := o.Request(ctx, r)
		o.Close()
		if err != nil || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
			t.Fatal("ONDISK OMAP vector", err)
		}
		before := observe()
		if !equalOmapModel(omapWriteInventory(t, before), want) || !bytes.Equal(*before.Header, header) || xattrInventory(before.nativeXattrSnapshot)["durable"] != "yes" || before.Version != reply.UserVersion {
			t.Fatal("native state before the crash differs")
		}
		name := strconv.FormatInt(int64(target.ID), 10)
		if err := testcluster.ControlDaemon(ctx, dir, "kill", "osd", name); err != nil {
			t.Fatal(err)
		}
		recoverMutationFixture(t, c, base, original, name, true)
		after := observe()
		if after.Version != before.Version || !reflect.DeepEqual(omapWriteInventory(t, after), omapWriteInventory(t, before)) || !bytes.Equal(*after.Header, header) || xattrInventory(after.nativeXattrSnapshot)["durable"] != "yes" {
			t.Fatal("OMAP/xattr state changed across SIGKILL and restart", before.Version, after.Version)
		}
	})

	t.Run("lost compare-then-set reply resolved by resubmission", func(t *testing.T) {
		target := route()
		options := integrationOptions(t)
		options.EnableOSD = true
		var armed atomic.Bool
		blocked := make(chan struct{}, 1)
		dial := options.DialContext
		options.DialContext = func(ctx context.Context, network, endpoint string) (net.Conn, error) {
			conn, err := dial(ctx, network, endpoint)
			if err != nil {
				return nil, err
			}
			if _, kind, _ := testcluster.SplitPeer(endpoint); kind != "osd."+strconv.Itoa(int(target.ID)) {
				return conn, nil
			}
			return &testcluster.LostReplyConn{Conn: conn, Armed: &armed, Closed: make(chan struct{}), SignalBlocked: func() {
				select {
				case blocked <- struct{}{}:
				default:
				}
			}}, nil
		}
		client, err := cephmsgr.Dial(ctx, options)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		o := mutationConnection(t, client, ctx, target)
		armed.Store(true)
		// Re-executing this vector would fail its compare (k1 is no longer "one").
		vector := osdObjectRequest(target,
			cephmsgr.OSDOperation{Code: cephmsgr.OSDCompareOmap, Omap: cephmsgr.OSDOmapInput{Assertions: []cephmsgr.OSDOmapAssertion{{Key: "k1", Value: []byte("one"), Comparison: cephmsgr.OSDOmapEqual}}}},
			cephmsgr.OSDOperation{Code: cephmsgr.OSDSetOmapValues, Omap: cephmsgr.OSDOmapInput{Entries: []cephmsgr.OSDOmapEntry{{Key: "k1", Value: []byte("two")}}}})
		vector.ClientIncarnation, vector.ModificationTime = 3, cephmsgr.OSDTime{Seconds: 1700005001}
		callCtx, stop := context.WithCancel(ctx)
		defer stop()
		lost := make(chan struct {
			r cephmsgr.OSDReply
			e error
		}, 1)
		go func() {
			r, e := o.Request(callCtx, vector)
			lost <- struct {
				r cephmsgr.OSDReply
				e error
			}{r, e}
		}()
		select {
		case <-blocked:
		case <-ctx.Done():
			t.Fatal("reply fault not activated")
		}
		var executed omapWriteState
		for {
			executed = observe()
			if bytes.Equal(omapWriteInventory(t, executed)["k1"], []byte("two")) {
				break
			}
			select {
			case <-ctx.Done():
				t.Fatal("the vector never executed")
			case <-time.After(100 * time.Millisecond):
			}
		}
		stop()
		got := <-lost
		var unknown *cephmsgr.OutcomeUnknownError
		if !errors.As(got.e, &unknown) || got.r.Submission.Transaction == 0 {
			t.Fatal("expected an unknown outcome with identity", got.e)
		}
		o.Close()
		armed.Store(false)
		o = mutationConnection(t, client, ctx, target)
		defer o.Close()
		again := vector
		again.Resubmit, again.RetryAttempt = &got.r.Submission, 1
		resolved, err := o.Request(ctx, again)
		if err != nil || resolved.UserVersion != executed.Version || resolved.Flags&cephmsgr.OSDReplyOnDisk == 0 {
			t.Fatalf("resubmission was not answered from the log: %v (user %d native %d)", err, resolved.UserVersion, executed.Version)
		}
		// Control: the same vector as a new request now fails its compare.
		fresh := vector
		fresh.ModificationTime = cephmsgr.OSDTime{Seconds: 1700005002}
		_, err = o.Request(ctx, fresh)
		var server *cephmsgr.OSDError
		if !errors.As(err, &server) || server.Code != -125 {
			t.Fatal("a fresh compare-then-set did not fail its compare", err)
		}
		if after := observe(); after.Version != executed.Version {
			t.Fatal("native state changed after the resubmission and refused control", executed.Version, after.Version)
		}
	})
}
