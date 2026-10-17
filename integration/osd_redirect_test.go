package integration_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

// SET_REDIRECT makes an object a manifest pointing at another pool's object:
// the OSD truncates it and serves its reads from the target, for the Go and
// the native client alike. UNSET_MANIFEST makes it a plain (now empty) object
// again. Refusals are the known codes of PrimaryLogPG.
func TestCephOSDRedirectIntegration(t *testing.T) {
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	c, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	pools := createOwnedPools(t, ctx, base, 1, 2)
	target := []byte("redirect target bytes\x00\xff")
	poolSeed(t, ctx, 2, "redirect-target", "", target, nil)
	targetState := poolObserve(t, ctx, 2, "redirect-target", "", "")
	poolSeed(t, ctx, 1, "redirected", "", []byte("original bytes"), nil)
	poolSeed(t, ctx, 1, "plain", "", []byte("plain"), nil)
	connections := map[int32]*cephmsgr.OSDConnection{}
	defer func() {
		for _, o := range connections {
			o.Close()
		}
	}()
	send := func(object string, op cephmsgr.OSDOperation) (cephmsgr.OSDReply, error) {
		route := poolPlace(t, ctx, 1, object, "")
		if connections[route.ID] == nil {
			connections[route.ID] = mutationConnection(t, c, ctx, route)
		}
		r := pools.request(route, op)
		if op.Code != cephmsgr.OSDRead {
			r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700007000}
		}
		return connections[route.ID].Request(ctx, r)
	}
	redirect := func(name string) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDSetRedirect, Redirect: cephmsgr.OSDRedirectInput{TargetPool: pools.ids[2], TargetName: name, TargetVersion: targetState.Version}}
	}
	read := cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: 4096}

	reply, err := send("redirected", redirect("redirect-target"))
	if err != nil || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
		t.Fatal("set redirect", err)
	}
	got, err := send("redirected", read)
	if err != nil || !bytes.Equal(got.Operations[0].Data, target) {
		t.Fatalf("Go read through the redirect: err=%v code=%d flags=%#x", err, got.Code, got.Flags)
	}
	if n := poolObserve(t, ctx, 1, "redirected", "", ""); n.Code != 0 || !bytes.Equal(n.Data, target) {
		t.Fatalf("native read through the redirect: code %d %q", n.Code, n.Data)
	}
	if n := poolObserve(t, ctx, 2, "redirect-target", "", ""); !bytes.Equal(n.Data, target) || n.Version != targetState.Version {
		t.Fatal("the target changed")
	}
	// IGNORE_REDIRECT serves the manifest object itself (truncated, empty);
	// RWORDERED changes nothing visible.
	flagged := func(edit func(*cephmsgr.OSDRequest)) []byte {
		route := poolPlace(t, ctx, 1, "redirected", "")
		r := pools.request(route, read)
		edit(&r)
		reply, err := connections[route.ID].Request(ctx, r)
		if err != nil {
			t.Fatal("flagged read", err)
		}
		return reply.Operations[0].Data
	}
	if got := flagged(func(r *cephmsgr.OSDRequest) { r.IgnoreRedirect = true }); len(got) != 0 {
		t.Fatalf("IGNORE_REDIRECT still followed the redirect: %q", got)
	}
	if got := flagged(func(r *cephmsgr.OSDRequest) { r.OrderReadsWrites = true }); !bytes.Equal(got, target) {
		t.Fatalf("RWORDERED read differs: %q", got)
	}

	written := []byte("written through the redirect")
	wreply, werr := send("redirected", cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(written)), Data: written})
	tgt := poolObserve(t, ctx, 2, "redirect-target", "", "")
	src := poolObserve(t, ctx, 1, "redirected", "", "")
	// The OSD proxies the write to the target: the target changes and the
	// redirect keeps reading it.
	if werr != nil || wreply.Flags&cephmsgr.OSDReplyOnDisk == 0 || !bytes.Equal(tgt.Data, written) || !bytes.Equal(src.Data, written) {
		t.Fatalf("write through the redirect: err=%v target %q redirected %q", werr, tgt.Data, src.Data)
	}

	if _, err := send("redirected", cephmsgr.OSDOperation{Code: cephmsgr.OSDUnsetManifest}); err != nil {
		t.Fatal("unset manifest", err)
	}
	got, err = send("redirected", read)
	if err != nil || len(got.Operations[0].Data) != 0 {
		t.Fatalf("read after unset: %q %v", got.Operations, err)
	}
	if n := poolObserve(t, ctx, 1, "redirected", "", ""); n.Code != 0 || len(n.Data) != 0 {
		t.Fatalf("native object after unset: code %d %q", n.Code, n.Data)
	}

	var server *cephmsgr.OSDError
	var unknown *cephmsgr.OutcomeUnknownError
	for name, tc := range map[string]struct {
		object string
		op     cephmsgr.OSDOperation
		code   int32
	}{
		"redirect of an absent object": {"redirect-absent", redirect("redirect-target"), -2},
		"unset on a plain object":      {"plain", cephmsgr.OSDOperation{Code: cephmsgr.OSDUnsetManifest}, -95},
	} {
		if _, err := send(tc.object, tc.op); !errors.As(err, &server) || errors.As(err, &unknown) || server.Code != tc.code {
			t.Fatal(name, "expected a known refusal", tc.code, err)
		}
	}
	self := cephmsgr.OSDOperation{Code: cephmsgr.OSDSetRedirect, Redirect: cephmsgr.OSDRedirectInput{TargetPool: pools.ids[1], TargetName: "plain"}}
	if _, err := send("plain", self); !errors.As(err, &server) || errors.As(err, &unknown) || server.Code != -22 {
		t.Fatal("a self redirect must be a known -EINVAL", err)
	}
	// Local refusals: a target without the opcode, and the opcode with COPY_FROM input.
	for _, op := range []cephmsgr.OSDOperation{{Code: cephmsgr.OSDRead, Length: 1, Redirect: cephmsgr.OSDRedirectInput{TargetName: "x"}}, {Code: cephmsgr.OSDSetRedirect}} {
		if reply, err := send("plain", op); err == nil || reply.Submission != (cephmsgr.OSDSubmission{}) {
			t.Fatal("a malformed redirect was submitted", op.Code, err)
		}
	}
}
