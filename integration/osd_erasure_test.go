package integration_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

// An erasure-coded pool (k=2, m=1, OSD failure domain, overwrites enabled)
// makes MON and OSDs require CRUSH_V2 from every client. Object I/O to it goes
// to the PG primary with that primary's shard in spg_t (replicated-PG
// profiles) or with the raw hash the OSD maps itself (object-hash profile).
// The independent native client observes every result.
func TestCephOSDErasureIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != "3" || os.Getenv("CEPH_MSGR_TEST_OSD_STORE") != "bluestore" {
		t.Skip("k=2,m=1 with overwrites needs three BlueStore OSDs")
	}
	fixture, request, hash := osdFixture, osdObjectRequest, false
	if isReplicatedPGFixtureProfile() {
		fixture, request, hash = replicatedPGFixture, replicatedPGRequest, true
	}
	c, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 90*time.Second)
		defer stop()
		if r := nativePool(t, cleanup, "cleanup", nil); !r.PoolsRemoved {
			t.Error("owned erasure-coded pool was not removed")
		}
	})
	pg := 1
	if hash {
		pg = 8
	}
	created := nativePool(t, ctx, "create", map[string]any{"slot": 1, "pg_num": pg, "hashpspool": hash, "erasure": true})
	if created.PoolType != 3 || created.Size != 3 {
		t.Fatal("native pool is not k=2,m=1 erasure-coded", created.PoolType, created.Size)
	}
	t.Logf("EC pool %d rule %s", created.PoolID, created.CrushRule)

	t.Run("admission with an erasure-code rule present", func(t *testing.T) {
		options := integrationOptions(t)
		options.EnableOSD = true
		if hash {
			options.OSDProfile = replicatedPGFixtureOSDProfile(t)
		}
		fresh, err := cephmsgr.Dial(ctx, options)
		if err != nil {
			t.Fatal("MON admission refused the client", err)
		}
		defer fresh.Close()
		o := mutationConnection(t, fresh, ctx, base)
		defer o.Close()
		if _, err := o.Request(ctx, request(base, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat})); err != nil {
			t.Fatal("replicated-pool I/O after the EC rule appeared", err)
		}
	})

	const object = "erasure-object"
	route := poolPlace(t, ctx, 1, object, "")
	if route.PoolType != 3 || route.Shard < 0 || len(route.Acting) != 3 || route.Acting[route.Shard] != route.ID {
		t.Fatalf("native EC placement differs: %+v", route)
	}
	o := mutationConnection(t, c, ctx, route)
	defer o.Close()
	send := func(timeout time.Duration, edit func(*cephmsgr.OSDRequest), ops ...cephmsgr.OSDOperation) (cephmsgr.OSDReply, error) {
		r := request(route, ops...)
		if ops[0].Code != cephmsgr.OSDRead && ops[0].Code != cephmsgr.OSDStat && ops[0].Code != cephmsgr.OSDGetXattr {
			r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700002000}
		}
		if edit != nil {
			edit(&r)
		}
		callCtx, stop := context.WithTimeout(ctx, timeout)
		defer stop()
		return o.Request(callCtx, r)
	}
	must := func(t *testing.T, name string, ops ...cephmsgr.OSDOperation) cephmsgr.OSDReply {
		t.Helper()
		reply, err := send(30*time.Second, nil, ops...)
		if err != nil {
			t.Fatal(name, err)
		}
		return reply
	}
	native := func() nativeXattrSnapshot { return poolObserve(t, ctx, 1, object, "", "") }
	payload := make([]byte, 10000)
	for i := range payload {
		payload[i] = byte(i*7 + i/251)
	}
	want := bytes.Clone(payload)

	t.Run("object I/O matches the native client", func(t *testing.T) {
		reply := must(t, "writefull", cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(payload)), Data: payload})
		if reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
			t.Fatal("writefull lacks ONDISK")
		}
		if n := native(); !bytes.Equal(n.Data, want) || n.Version != reply.UserVersion {
			t.Fatal("native bytes/version after writefull differ", n.Version, reply.UserVersion)
		}
		read := must(t, "read", cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Offset: 100, Length: 5000})
		if !bytes.Equal(read.Operations[0].Data, want[100:5100]) {
			t.Fatal("partial read differs")
		}
		patch := bytes.Repeat([]byte{0xee, 0x11}, 50)
		must(t, "partial overwrite", cephmsgr.OSDOperation{Code: cephmsgr.OSDWrite, Offset: 3000, Length: uint64(len(patch)), Data: patch})
		copy(want[3000:], patch)
		tail := []byte("erasure tail\x00\xff")
		must(t, "append", cephmsgr.OSDOperation{Code: cephmsgr.OSDAppend, Length: uint64(len(tail)), Data: tail})
		want = append(want, tail...)
		must(t, "setxattr", cephmsgr.OSDOperation{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: "ec-attr"}, Data: []byte{0, 0xff, 'E'}})
		got := must(t, "getxattr", cephmsgr.OSDOperation{Code: cephmsgr.OSDGetXattr, Xattr: cephmsgr.OSDXattrInput{Name: "ec-attr", MaxOutputBytes: 64}})
		if !bytes.Equal(got.Operations[0].Data, []byte{0, 0xff, 'E'}) {
			t.Fatal("getxattr differs", got.Operations[0].Data)
		}
		n := native()
		if !bytes.Equal(n.Data, want) || xattrInventory(n)["ec-attr"] != string([]byte{0, 0xff, 'E'}) {
			t.Fatal("native state after overwrite, append and xattr differs")
		}
		stat := must(t, "stat", cephmsgr.OSDOperation{Code: cephmsgr.OSDStat})
		if len(stat.Operations[0].Data) < 8 || uint64(stat.Operations[0].Data[0])|uint64(stat.Operations[0].Data[1])<<8 != uint64(len(want)) {
			t.Fatal("stat size differs", stat.Operations[0].Data)
		}
		must(t, "truncate", cephmsgr.OSDOperation{Code: cephmsgr.OSDTruncate, Offset: 4000})
		if n := native(); !bytes.Equal(n.Data, want[:4000]) {
			t.Fatal("native bytes after truncate differ", len(n.Data))
		}
	})

	t.Run("OMAP is refused by an erasure-coded pool", func(t *testing.T) {
		_, err := send(30*time.Second, nil, cephmsgr.OSDOperation{Code: cephmsgr.OSDSetOmapValues, Omap: cephmsgr.OSDOmapInput{Entries: []cephmsgr.OSDOmapEntry{{Key: "k", Value: []byte("v")}}}})
		var server *cephmsgr.OSDError
		var unknown *cephmsgr.OutcomeUnknownError
		if !errors.As(err, &server) || errors.As(err, &unknown) {
			t.Fatal("OMAP write on an EC pool was not a known refusal", err)
		}
		t.Logf("OMAP on EC pool: %d", server.Code)
	})

	if hash {
		t.Run("the OSD requires the primary's shard", func(t *testing.T) {
			for name, edit := range map[string]func(*cephmsgr.OSDRequest){
				"NO_SHARD":    func(r *cephmsgr.OSDRequest) { r.Placement.Erasure, r.Placement.Shard = false, 0 },
				"other shard": func(r *cephmsgr.OSDRequest) { r.Placement.Shard = uint8((int(route.Shard) + 1) % 3) },
			} {
				reply, err := send(5*time.Second, edit, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: 16})
				t.Logf("%s: code=%d err=%v", name, reply.Code, err)
				if err == nil {
					t.Fatal("a read with the wrong shard succeeded", name)
				}
			}
		})
	}

	t.Run("watch and notify on an EC object", func(t *testing.T) {
		watchRequest := request(route)
		w, _, err := o.WatchObject(ctx, watchRequest, cephmsgr.OSDWatchOptions{TimeoutSeconds: 30})
		if err != nil {
			t.Fatal("watch", err)
		}
		defer w.Close()
		payload := []byte{0, 0xff, 'e', 'c'}
		n, _, err := o.NotifyObject(ctx, request(route), payload, cephmsgr.OSDWatchOptions{TimeoutSeconds: 10})
		if err != nil {
			t.Fatal("notify", err)
		}
		defer n.Close()
		event, err := w.Next(ctx)
		if err != nil || !bytes.Equal(event.Payload, payload) || event.Cookie != w.Cookie() {
			t.Fatal("notification", event, err)
		}
		if _, err := w.Acknowledge(ctx, route.Epoch, event, []byte("ack")); err != nil {
			t.Fatal("acknowledge", err)
		}
		done, err := n.Next(ctx)
		if err != nil || done.ReturnCode != 0 || len(done.Replies) != 1 || !bytes.Equal(done.Replies[0].Data, []byte("ack")) || len(done.Missed) != 0 {
			t.Fatal("notify completion", done, err)
		}
	})

	t.Run("delete", func(t *testing.T) {
		must(t, "delete", cephmsgr.OSDOperation{Code: cephmsgr.OSDDelete})
		if n := native(); n.Code != -2 {
			t.Fatal("object survived the delete", n.Code)
		}
	})
}
