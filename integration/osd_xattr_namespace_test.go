package integration_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

func TestCephOSDXattrNamespacesIntegration(t *testing.T) {
	c, _, base, original := osdFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	names := []string{"ns-binary", "ns-empty", "ns-remove"}
	encodedNames := make([]string, len(names))
	for i, name := range names {
		encodedNames[i] = base64.StdEncoding.EncodeToString([]byte(name))
	}
	observe := func(call context.Context, namespace string) nativeXattrSnapshot {
		return nativeXattrCompare(t, call, base, xattrNativeInput{
			Object: base.Name, Namespace: namespace, Kind: "read", Names: encodedNames,
		}).Actual
	}
	defaultBefore := observe(ctx, "")
	if defaultBefore.Code != 0 || !bytes.Equal(defaultBefore.Data, original) {
		t.Fatal("default namespace object differs from native fixture before isolation")
	}
	namespaces := []string{"ceph-msgr/ns-alpha", "ceph-msgr/네임스페이스"}
	type namespaceState struct {
		oracle     osdFixtureOracle
		connection *cephmsgr.OSDConnection
		owned      bool
		data       []byte
		attributes map[string]string
		previous   nativeXattrSnapshot
	}
	states := make([]namespaceState, len(namespaces))
	// The consumer prepares routing separately for each exact namespace/name.
	// No hash, target, epoch or nonce is inferred from the default object.
	for i, namespace := range namespaces {
		native := nativeMutation(t, ctx, map[string]any{
			"compare": false, "placement": true, "object": base.Name, "namespace": namespace,
		})
		fresh := native.Placement
		if native.Actual.Code != -2 || fresh.Pool != base.Pool || fresh.Name != base.Name || fresh.Namespace != namespace || fresh.Epoch < base.Epoch || fresh.ID < 0 || fresh.Address == "" || fresh.RawPGID != fmt.Sprintf("%d.%x", fresh.Pool, fresh.Hash) || fresh.PGID != fmt.Sprintf("%d.%x", fresh.Pool, fresh.PG) {
			t.Fatalf("native namespace preparation identity/absence differs: namespace=%q placement=%+v actual=%d", namespace, fresh, native.Actual.Code)
		}
		acting := false
		for _, id := range fresh.Acting {
			acting = acting || id == fresh.ID
		}
		if !acting {
			t.Fatal("native namespace primary is absent from acting set")
		}
		states[i].oracle = fresh
		states[i].connection = mutationConnection(t, c, ctx, fresh)
		t.Logf("caller prepared namespace=%q name=%q rawPG=%s mappedPG=%s epoch=%d primary=%d endpoint=%s", namespace, fresh.Name, fresh.RawPGID, fresh.PGID, fresh.Epoch, fresh.ID, fresh.Address)
	}
	defer func() {
		restore, stop := context.WithTimeout(context.Background(), 30*time.Second)
		defer stop()
		for i := range states {
			state := &states[i]
			if state.connection != nil {
				if err := state.connection.Close(); err != nil {
					t.Error("close namespace subject session", err)
				}
			}
			if !state.owned {
				continue
			}
			// One independent caller-selected DELETE cleans the new object.
			// This is not a replay of a tested SET or uncertain create outcome.
			cleanup, err := c.OpenOSD(restore, cephmsgr.OSDTarget{ID: state.oracle.ID, Address: state.oracle.Address})
			if err != nil {
				t.Error("open explicitly targeted namespace cleanup", err)
				continue
			}
			reply, err := cleanup.Request(restore, osdObjectRequest(state.oracle, cephmsgr.OSDOperation{Code: cephmsgr.OSDDelete}))
			var absent *cephmsgr.OSDError
			if err != nil && !(errors.As(err, &absent) && absent.Code == -2) {
				t.Error("delete owned namespace object", err)
			} else if err == nil && reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
				t.Error("namespace cleanup lacks ONDISK completion")
			}
			if err := cleanup.Close(); err != nil {
				t.Error("close namespace cleanup session", err)
			}
			if after := observe(restore, state.oracle.Namespace); after.Code != -2 {
				t.Error("native namespace object remains after explicit cleanup", state.oracle.Namespace, after.Code)
			}
		}
		if after := observe(restore, ""); !xattrSnapshotEqual(defaultBefore, after, true) {
			t.Error("namespace cases/cleanup changed original default object bytes, attrs, version or stat2")
		}
	}()
	assertIsolation := func() {
		for i := range states {
			state := &states[i]
			native := observe(ctx, state.oracle.Namespace)
			if !state.owned {
				if native.Code != -2 {
					t.Fatal("uncreated namespace was modified by another namespace", state.oracle.Namespace)
				}
				continue
			}
			if native.Code != 0 || !bytes.Equal(native.Data, state.data) || !reflect.DeepEqual(xattrInventory(native), state.attributes) {
				t.Fatal("native namespace state differs from caller bytes/inventory", state.oracle.Namespace)
			}
			if state.previous.Code == 0 && !xattrSnapshotEqual(state.previous, native, true) {
				t.Fatal("untouched namespace changed during another namespace operation", state.oracle.Namespace)
			}
			state.previous = native
		}
		if after := observe(ctx, ""); !xattrSnapshotEqual(defaultBefore, after, true) {
			t.Fatal("namespace vector changed original default object")
		}
	}
	set := func(name string, value []byte) cephmsgr.OSDOperation {
		return cephmsgr.OSDOperation{Code: cephmsgr.OSDSetXattr, Xattr: cephmsgr.OSDXattrInput{Name: name}, Data: value}
	}
	for i := range states {
		state := &states[i]
		state.data = []byte{0, 0xff, byte('A' + i), 0, 10, 0x80}
		binaryValue := []byte{0xff, byte('a' + i), 0, byte(i), 0x80}
		state.attributes = map[string]string{names[0]: string(binaryValue), names[1]: "", names[2]: "remove-me"}
		request := osdObjectRequest(state.oracle,
			cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: uint64(len(state.data)), Data: state.data},
			set(names[0], binaryValue), set(names[1], nil), set(names[2], []byte("remove-me")),
		)
		request.ModificationTime = cephmsgr.OSDTime{Seconds: 1700000100 + uint32(i), Nanoseconds: 24680 + uint32(i)}
		// Both namespaced objects were independently observed absent. A sent
		// outcome may still create this owned object even when the wait fails.
		state.owned = true
		reply, err := state.connection.Request(ctx, request)
		if err != nil || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
			t.Fatal("create/write xattrs in explicit namespace", err)
		}
		checkOSDMetadata(t, reply, state.oracle)
		state.previous = observe(ctx, state.oracle.Namespace)
		if state.previous.Seconds != int64(request.ModificationTime.Seconds) || state.previous.Nanos != int64(request.ModificationTime.Nanoseconds) || state.previous.Version != reply.UserVersion {
			t.Fatal("native namespace metadata differs from ONDISK reply/explicit mtime")
		}
		assertIsolation()
	}
	for i := range states {
		state := &states[i]
		read := osdObjectRequest(state.oracle,
			cephmsgr.OSDOperation{Code: cephmsgr.OSDGetXattr, Xattr: cephmsgr.OSDXattrInput{Name: names[0], MaxOutputBytes: 1024}},
			cephmsgr.OSDOperation{Code: cephmsgr.OSDGetXattrs, Xattr: cephmsgr.OSDXattrInput{MaxOutputBytes: 64 << 10}},
			cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: uint64(len(state.data))},
		)
		reply, err := state.connection.Request(ctx, read)
		if err != nil || len(reply.Operations) != 3 || !bytes.Equal(reply.Operations[0].Data, []byte(state.attributes[names[0]])) || !reflect.DeepEqual(xattrEncodedInventory(t, reply.Operations[1].Data), state.attributes) || !bytes.Equal(reply.Operations[2].Data, state.data) {
			t.Fatal("namespaced GET/GETALL/READ boundaries differ from independent native state", err)
		}
		checkOSDMetadata(t, reply, state.oracle)
		assertIsolation()
		replacement := []byte{0, byte('X' + i), 0xff, 0}
		request := osdObjectRequest(state.oracle, set(names[0], replacement), cephmsgr.OSDOperation{Code: cephmsgr.OSDRemoveXattr, Xattr: cephmsgr.OSDXattrInput{Name: names[2]}})
		request.ModificationTime = cephmsgr.OSDTime{Seconds: 1700000200 + uint32(i), Nanoseconds: 13579 + uint32(i)}
		reply, err = state.connection.Request(ctx, request)
		if err != nil || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
			t.Fatal("namespaced SET/RM", err)
		}
		checkOSDMetadata(t, reply, state.oracle)
		previous := state.previous
		state.previous = observe(ctx, state.oracle.Namespace)
		if state.previous.Version <= previous.Version || state.previous.Version != reply.UserVersion || state.previous.Seconds != int64(request.ModificationTime.Seconds) || state.previous.Nanos != int64(request.ModificationTime.Nanoseconds) {
			t.Fatal("native namespace SET/RM version or mtime differs")
		}
		state.attributes[names[0]] = string(replacement)
		delete(state.attributes, names[2])
		assertIsolation()
		missing := osdObjectRequest(state.oracle, cephmsgr.OSDOperation{Code: cephmsgr.OSDGetXattr, Xattr: cephmsgr.OSDXattrInput{Name: names[2], MaxOutputBytes: 1024}})
		reply, err = state.connection.Request(ctx, missing)
		var server *cephmsgr.OSDError
		if !errors.As(err, &server) || server.Code != -61 || reply.Code != -61 || len(reply.Operations) != 1 || reply.Operations[0].Result != -61 {
			t.Fatal("removed namespaced attribute is not known ENODATA", err)
		}
		assertIsolation()
	}
	t.Log("same bare object name independently routed in slash/Unicode namespaces; GET/GETALL/SET/RM and content matched native clients while both peer namespace and default object remained unchanged")
}
