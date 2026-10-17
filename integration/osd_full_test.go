package integration_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

// A pool over its object quota is flagged full_quota. Writes without a full
// flag are never answered; FULL_TRY gets the server's -EDQUOT with no effect;
// FULL_FORCE proceeds. Native observation checks every outcome independently.
func TestCephOSDFullPoolFlagsIntegration(t *testing.T) {
	fixture := osdFixture
	if isReplicatedPGFixtureProfile() {
		fixture = replicatedPGFixture
	}
	c, _, base, _ := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 420*time.Second)
	defer cancel()
	pools := createOwnedPools(t, ctx, base, 1)
	for _, name := range []string{"full-a", "full-b", "full-c"} {
		poolSeed(t, ctx, 1, name, "", []byte("seed-"+name), nil)
	}
	nativePool(t, ctx, "quota", map[string]any{"slot": 1, "max_objects": 2})
	deadline := time.Now().Add(240 * time.Second)
	for {
		info := nativePool(t, ctx, "poolinfo", map[string]any{"slot": 1})
		if strings.Contains(info.FlagNames, "full_quota") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("pool never reached full_quota", info.FlagNames)
		}
		time.Sleep(3 * time.Second)
	}
	connections := map[int32]*cephmsgr.OSDConnection{}
	defer func() {
		for _, connection := range connections {
			connection.Close()
		}
	}()
	send := func(timeout time.Duration, object string, configure func(*cephmsgr.OSDRequest), ops ...cephmsgr.OSDOperation) (cephmsgr.OSDReply, error) {
		route := poolPlace(t, ctx, 1, object, "")
		if connections[route.ID] == nil {
			connections[route.ID] = mutationConnection(t, c, ctx, route)
		}
		r := pools.request(route, ops...)
		r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700000800}
		if configure != nil {
			configure(&r)
		}
		callCtx, stop := context.WithTimeout(ctx, timeout)
		defer stop()
		return connections[route.ID].Request(callCtx, r)
	}
	write := func(data string) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(data)), Data: []byte(data)}
	}
	t.Run("plain write is never answered", func(t *testing.T) {
		_, err := send(5*time.Second, "full-plain", nil, write("plain"))
		var unknown *cephmsgr.OutcomeUnknownError
		if !errors.As(err, &unknown) {
			t.Fatal("a write to a full pool must end as an unknown outcome, not a reply", err)
		}
		if state := poolObserve(t, ctx, 1, "full-plain", "", ""); state.Code != -2 {
			t.Fatal("the dropped write created an object", state.Code)
		}
	})
	t.Run("FULL_TRY gets the quota refusal", func(t *testing.T) {
		_, err := send(20*time.Second, "full-try", func(r *cephmsgr.OSDRequest) { r.FullTry = true }, write("try"))
		var server *cephmsgr.OSDError
		var unknown *cephmsgr.OutcomeUnknownError
		if !errors.As(err, &server) || errors.As(err, &unknown) || server.Code != -122 {
			t.Fatal("expected a known -EDQUOT", err)
		}
		if state := poolObserve(t, ctx, 1, "full-try", "", ""); state.Code != -2 {
			t.Fatal("the refused write created an object", state.Code)
		}
	})
	t.Run("FULL_FORCE writes and deletes despite the flag", func(t *testing.T) {
		reply, err := send(20*time.Second, "full-force", func(r *cephmsgr.OSDRequest) { r.FullForce = true }, write("forced"))
		if err != nil || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
			t.Fatal("forced write", err)
		}
		if state := poolObserve(t, ctx, 1, "full-force", "", ""); state.Code != 0 || !bytes.Equal(state.Data, []byte("forced")) || reply.UserVersion != state.Version {
			t.Fatal("native object differs after the forced write", state.Code)
		}
		reply, err = send(20*time.Second, "full-a", func(r *cephmsgr.OSDRequest) { r.FullForce = true }, cephmsgr.OSDOperation{Code: cephmsgr.OSDDelete})
		if err != nil || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
			t.Fatal("forced delete", err)
		}
		if state := poolObserve(t, ctx, 1, "full-a", "", ""); state.Code != -2 {
			t.Fatal("forced delete left the object", state.Code)
		}
	})
	t.Run("misuse is refused before submission", func(t *testing.T) {
		route := poolPlace(t, ctx, 1, "full-b", "")
		o := mutationConnection(t, c, ctx, route)
		defer o.Close()
		for _, edit := range []func(*cephmsgr.OSDRequest){
			func(r *cephmsgr.OSDRequest) { r.FullTry, r.FullForce = true, true },
		} {
			r := pools.request(route, write("x"))
			r.ModificationTime = cephmsgr.OSDTime{Seconds: 1700000801}
			edit(&r)
			if reply, err := o.Request(ctx, r); err == nil || reply.Submission != (cephmsgr.OSDSubmission{}) {
				t.Fatal("conflicting full flags were submitted", err)
			}
		}
		read := pools.request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: 4})
		read.FullTry = true
		if reply, err := o.Request(ctx, read); err == nil || reply.Submission != (cephmsgr.OSDSubmission{}) {
			t.Fatal("a read vector carried a full flag", err)
		}
	})
}
