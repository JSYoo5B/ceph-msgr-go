package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

func pgListV8NamespaceNative(t *testing.T, ctx context.Context, name, namespace string) mutationOracle {
	t.Helper()
	if namespace == "" {
		return replicatedPGNative(t, ctx, name)
	}
	var result mutationOracle
	replicatedPGControl(t, ctx, "osd-mutation", "compare", "osd-mutation-input.json", "osd-mutation-result.json", map[string]any{"compare": false, "placement": true, "object": name, "namespace": namespace}, &result)
	r := result.Placement
	if r.Name != name || r.Namespace != namespace || r.Pool < 0 || r.PG >= 8 || r.Hash == r.PG || r.Epoch == 0 || r.ID < 0 || r.Address == "" || len(r.Acting) == 0 || r.Acting[0] != r.ID {
		t.Fatal("native isolated namespace placement differs", result)
	}
	return result
}

type pgListV8Evidence struct {
	Name, Namespace           string
	Placement                 osdFixtureOracle
	Native                    nativePGList
	Pages                     []cephmsgr.OSDPGListReply
	NativeAfter               nativePGList
	ObjectBefore, ObjectAfter mutationSnapshot
}

func TestCephOSDPGListV8Integration(t *testing.T) {
	if !isReplicatedPGFixtureProfile() {
		t.Skip("requires an explicit replicated-PG fixture profile")
	}
	c, _, base, _ := replicatedPGFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	t.Cleanup(cancel)
	baseline := replicatedPGNative(t, ctx, base.Name).Actual
	prepared, namespaceOwned, defaultClean, namespaceClean := false, false, false, false
	namespace := "pg-list-v8-isolated"
	var nsRoute osdFixtureOracle
	var nsConn *cephmsgr.OSDConnection
	connections := map[int32]*cephmsgr.OSDConnection{}
	var evidence []pgListV8Evidence
	t.Cleanup(func() {
		defer func() {
			data, err := json.Marshal(struct {
				Profile                      string             `json:"profile"`
				Cases                        []pgListV8Evidence `json:"cases"`
				DefaultClean, NamespaceClean bool
			}{os.Getenv("CEPH_MSGR_TEST_OSD_PROFILE"), evidence, defaultClean, namespaceClean})
			if err == nil {
				err = testcluster.WritePublicEvidence(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "osd-pg-list-v8-results.json"), data)
			}
			if err != nil {
				t.Error(err)
			}
		}()
		cleanup, stop := context.WithTimeout(context.Background(), 60*time.Second)
		defer stop()
		// OpenOSD aliases one connection per target. Remove the isolated head
		// before closing any shared alias, then run independent native cleanup.
		if namespaceOwned && nsConn != nil {
			_, err := nsConn.Request(cleanup, replicatedPGRequest(nsRoute, cephmsgr.OSDOperation{Code: cephmsgr.OSDDelete}))
			var server *cephmsgr.OSDError
			if err != nil && !(errors.As(err, &server) && server.Code == -2) {
				t.Error("isolated v8 listing head cleanup", err)
			}
			after := pgListV8NamespaceNative(t, cleanup, nsRoute.Name, namespace)
			namespaceClean = after.Actual.Code == -2
			if !namespaceClean {
				t.Error("native isolated v8 listing head remains")
			}
		}
		for _, conn := range connections {
			_ = conn.Close()
		}
		if prepared {
			defaultClean = replicatedPGOwned(t, cleanup, base, "cleanup", omapWriteInput{}).Cleanup.Removed
			if !defaultClean {
				t.Error("native default v8 listing cleanup incomplete")
			}
		}
		final := replicatedPGNative(t, cleanup, base.Name).Actual
		if !reflect.DeepEqual(baseline, final) {
			t.Error("PG listings or owned cleanup altered the original native fixture")
		}
	})
	owned := replicatedPGOwned(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: []byte("v8 namespace inventory")}, Reserve: 8})
	prepared = true
	groups := map[uint32][]osdFixtureOracle{}
	connection := func(route osdFixtureOracle) *cephmsgr.OSDConnection {
		if conn := connections[route.ID]; conn != nil {
			return conn
		}
		conn := replicatedPGConnection(t, c, ctx, route)
		connections[route.ID] = conn
		return conn
	}
	for _, name := range owned.Objects {
		observed := replicatedPGNative(t, ctx, name)
		route := observed.Placement
		if route.PG >= 8 || route.Hash == route.PG {
			t.Fatal("native object does not use the eight-PG explicit placement fixture", route)
		}
		groups[route.PG] = append(groups[route.PG], route)
		if observed.Actual.Code == -2 {
			ops := []cephmsgr.OSDOperation{{Code: cephmsgr.OSDCreate, Flags: cephmsgr.OSDExclusiveCreate}, {Code: cephmsgr.OSDWriteFull, Length: 1, Data: []byte{42}}}
			if reply, err := connection(route).Request(ctx, replicatedPGRequest(route, ops...)); err != nil || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
				t.Fatal("owned v8 listing object create", reply, err)
			}
		} else if observed.Actual.Code != 0 {
			t.Fatal("owned native listing seed observation failed", observed)
		}
	}
	// Nine independently named objects in eight actual PGs guarantee a group
	// with at least two members. Prefer a nonzero PG when equally populated.
	var selected []osdFixtureOracle
	for _, group := range groups {
		if len(group) > len(selected) || len(group) == len(selected) && group[0].PG != 0 && len(selected) > 0 && selected[0].PG == 0 {
			selected = group
		}
	}
	if len(selected) < 2 {
		t.Fatal("owned nine-object/eight-PG fixture lacks a pagination group")
	}
	defaultRoute := selected[0]
	defaultConn := connection(defaultRoute)
	nsNative := pgListV8NamespaceNative(t, ctx, defaultRoute.Name, namespace)
	nsRoute = nsNative.Placement
	nsConn = connection(nsRoute)
	if nsNative.Actual.Code != -2 {
		t.Fatal("new isolated namespace head already exists", nsNative)
	}
	namespaceOwned = true
	create := replicatedPGRequest(nsRoute, cephmsgr.OSDOperation{Code: cephmsgr.OSDCreate, Flags: cephmsgr.OSDExclusiveCreate}, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: 2, Data: []byte{0, 255}})
	if reply, err := nsConn.Request(ctx, create); err != nil || reply.Flags&cephmsgr.OSDReplyOnDisk == 0 {
		t.Fatal("isolated v8 listing head create", reply, err)
	}
	for _, tc := range []struct {
		name, namespace string
		route           osdFixtureOracle
		conn            *cephmsgr.OSDConnection
	}{
		{"default actual PG count-one pages", "", defaultRoute, defaultConn},
		{"isolated namespace actual PG count-one pages", namespace, nsRoute, nsConn},
	} {
		t.Run(tc.name, func(t *testing.T) {
			record := pgListV8Evidence{Name: tc.name, Namespace: tc.namespace, Placement: tc.route}
			defer func() { evidence = append(evidence, record) }()
			record.ObjectBefore = pgListV8NamespaceNative(t, ctx, tc.route.Name, tc.namespace).Actual
			record.Native = pgListOracle(t, ctx, tc.route.PG, tc.namespace)
			if record.Native.Pool != uint64(tc.route.Pool) {
				t.Fatal("native PG inventory pool differs")
			}
			request := cephmsgr.OSDPGListRequest{ClientIncarnation: 81, MapEpoch: tc.route.Epoch, PG: cephmsgr.OSDPGListTarget{Pool: uint64(tc.route.Pool), Seed: tc.route.PG, Primary: tc.route.ID}, Namespace: tc.namespace, Count: 1, StartEpoch: tc.route.Epoch, MaxOutputBytes: 8192}
			var got []cephmsgr.OSDPGListEntry
			seen := map[string]bool{}
			for n := 0; n < 128; n++ {
				reply, err := tc.conn.ListPG(ctx, request)
				record.Pages = append(record.Pages, reply)
				if err != nil {
					t.Fatal("v8 actual-PG listing", reply, err)
				}
				if reply.PG.Pool != uint64(tc.route.Pool) || reply.PG.Seed != tc.route.PG || reply.MapEpoch < tc.route.Epoch || reply.Submission.MapEpoch != tc.route.Epoch || reply.Submission.ClientIncarnation != 81 || reply.Submission.Transaction == 0 || reply.Transaction != reply.Submission.Transaction || reply.GlobalID != reply.Submission.GlobalID || len(reply.Entries) > 1 || len(reply.Cursor.Raw) < 39 || len(reply.RawData) < 49 {
					t.Fatal("v8 page lost actual PG/cursor/epoch/request identity", reply)
				}
				got = append(got, reply.Entries...)
				if reply.End {
					break
				}
				if seen[string(reply.Cursor.Raw)] || bytes.Equal(request.Cursor, reply.Cursor.Raw) {
					t.Fatal("v8 actual PG cursor failed to advance", reply)
				}
				seen[string(reply.Cursor.Raw)] = true
				request.Cursor = reply.Cursor.Raw
			}
			if len(record.Pages) == 0 || !record.Pages[len(record.Pages)-1].End {
				t.Fatal("bounded v8 PG enumeration failed to complete")
			}
			if !reflect.DeepEqual(sortedPGList(got), sortedPGList(record.Native.Entries)) {
				t.Fatal("v8 full PG inventory differs from independent native PGNLS", got, record.Native.Entries)
			}
			if tc.namespace == "" && (len(got) < 2 || len(record.Pages) < 2) {
				t.Fatal("default v8 count-one request did not exercise cursor continuation")
			}
			matched := false
			for _, entry := range got {
				if entry.Namespace == tc.namespace && entry.Object == tc.route.Name && entry.Key == "" {
					matched = true
				}
			}
			if !matched {
				t.Fatal("targeted owned namespace head absent from native/Go inventory")
			}
			record.NativeAfter = pgListOracle(t, ctx, tc.route.PG, tc.namespace)
			record.ObjectAfter = pgListV8NamespaceNative(t, ctx, tc.route.Name, tc.namespace).Actual
			if !reflect.DeepEqual(sortedPGList(record.Native.Entries), sortedPGList(record.NativeAfter.Entries)) || !reflect.DeepEqual(record.ObjectBefore, record.ObjectAfter) {
				t.Fatal("read-only v8 PG listing changed namespace inventory or native object state")
			}
			t.Logf("native PGNLS complete inventory equals v8 cursor pages: actualPG=%s rawObjectPG=%s namespace=%q entries=%d pages=%d", tc.route.PGID, tc.route.RawPGID, tc.namespace, len(got), len(record.Pages))
		})
	}
	t.Run("wrong explicit primary is a local refusal", func(t *testing.T) {
		reply, err := defaultConn.ListPG(ctx, cephmsgr.OSDPGListRequest{MapEpoch: defaultRoute.Epoch, PG: cephmsgr.OSDPGListTarget{Pool: uint64(defaultRoute.Pool), Seed: defaultRoute.PG, Primary: defaultRoute.ID + 100}, Count: 1, MaxOutputBytes: 8192})
		if !errors.Is(err, cephmsgr.ErrOSDPGListTarget) || reply.Submission.Transaction != 0 || len(reply.RawFront) != 0 {
			t.Fatal("invalid v8 primary reached submission", reply, err)
		}
	})
}
