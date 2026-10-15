package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

type nativePGList struct {
	Commit    string                    `json:"source_commit"`
	Pool      uint64                    `json:"pool_id"`
	PG        uint32                    `json:"pg_seed"`
	Namespace string                    `json:"namespace"`
	Entries   []cephmsgr.OSDPGListEntry `json:"entries"`
	Scope     string                    `json:"scope"`
}

func pgListOracle(t *testing.T, ctx context.Context, pg uint32, namespace string) nativePGList {
	t.Helper()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	data, err := json.Marshal(map[string]any{"pg_seed": pg, "namespace": namespace})
	if err != nil {
		t.Fatal(err)
	}
	if err := testcluster.WritePublicEvidence(filepath.Join(dir, "osd-pg-list-input.json"), data); err != nil {
		t.Fatal(err)
	}
	if err := testcluster.ControlDaemon(ctx, dir, "verify", "osd-pg-list", "observe"); err != nil {
		t.Fatal(err)
	}
	data, err = os.ReadFile(filepath.Join(dir, "osd-pg-list-result.json"))
	if err != nil {
		t.Fatal(err)
	}
	var result nativePGList
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	if result.Commit != "7f793731f1b39eb4f465e960113d2363c311b964" || result.PG != pg || result.Namespace != namespace || result.Scope == "" {
		t.Fatal("native PG inventory identity differs", result)
	}
	return result
}
func sortedPGList(entries []cephmsgr.OSDPGListEntry) []cephmsgr.OSDPGListEntry {
	result := append([]cephmsgr.OSDPGListEntry{}, entries...)
	sort.Slice(result, func(i, j int) bool {
		a, b := result[i], result[j]
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		if a.Object != b.Object {
			return a.Object < b.Object
		}
		return a.Key < b.Key
	})
	return result
}
func TestCephOSDPGListIntegration(t *testing.T) {
	c, _, base, _ := osdFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	t.Cleanup(cancel)
	prepared := false
	connections := map[string]*cephmsgr.OSDConnection{}
	var nsRoute *osdFixtureOracle
	var nsConn *cephmsgr.OSDConnection
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 40*time.Second)
		defer stop()
		if nsConn != nil {
			if _, err := nsConn.Request(cleanup, osdObjectRequest(*nsRoute, cephmsgr.OSDOperation{Code: cephmsgr.OSDDelete})); err != nil {
				t.Error(err)
			}
		}
		// OpenOSD reuses the same connection for one OSD target. Delete the
		// isolated head before closing any alias retained in connections.
		if nsConn != nil {
			nsConn.Close()
		}
		for _, conn := range connections {
			conn.Close()
		}
		if prepared && !nativeOmapWrite(t, cleanup, base, "cleanup", omapWriteInput{}).Cleanup.Removed {
			t.Error("PG listing object cleanup failed")
		}
	})
	owned := nativeOmapWrite(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: []byte("PG listing baseline")}, Reserve: 4})
	prepared = true
	routes := map[string]osdFixtureOracle{}
	for _, name := range owned.Objects {
		route := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": name, "namespace": ""}).Placement
		routes[name] = route
		connections[name] = mutationConnection(t, c, ctx, route)
		if name != owned.Objects[0] {
			if _, err := connections[name].Request(ctx, osdObjectRequest(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: 1, Data: []byte{42}})); err != nil {
				t.Fatal(err)
			}
		}
	}
	namespace := "pg-list-isolated"
	route := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": owned.Objects[0], "namespace": namespace}).Placement
	connection := mutationConnection(t, c, ctx, route)
	if _, err := connection.Request(ctx, osdObjectRequest(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDWriteFull, Length: 2, Data: []byte{0, 255}})); err != nil {
		connection.Close()
		t.Fatal(err)
	}
	nsRoute, nsConn = &route, connection
	type evidence struct {
		Namespace string                    `json:"namespace"`
		Native    nativePGList              `json:"native"`
		Pages     []cephmsgr.OSDPGListReply `json:"go_pages"`
	}
	var receipts []evidence
	defer func() {
		data, err := json.Marshal(receipts)
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "osd-pg-list-results.json"), data)
		}
		if err != nil {
			t.Error(err)
		}
	}()
	for _, tc := range []struct {
		name, namespace string
		route           osdFixtureOracle
		conn            *cephmsgr.OSDConnection
	}{
		{"default namespace with cursor pages", "", routes[owned.Objects[0]], connections[owned.Objects[0]]},
		{"explicit isolated namespace", namespace, route, connection},
		{"empty namespace", "pg-list-empty-" + owned.Objects[0], route, connection},
	} {
		t.Run(tc.name, func(t *testing.T) {
			native := pgListOracle(t, ctx, tc.route.PG, tc.namespace)
			request := cephmsgr.OSDPGListRequest{MapEpoch: tc.route.Epoch, PG: cephmsgr.OSDPGListTarget{Pool: uint64(tc.route.Pool), Seed: tc.route.PG, Primary: tc.route.ID}, Namespace: tc.namespace, Count: 1, StartEpoch: tc.route.Epoch, MaxOutputBytes: 8192}
			record := evidence{Namespace: tc.namespace, Native: native}
			var got []cephmsgr.OSDPGListEntry
			seen := map[string]bool{}
			for n := 0; n < 128; n++ {
				reply, err := tc.conn.ListPG(ctx, request)
				record.Pages = append(record.Pages, reply)
				if err != nil {
					t.Fatal(err)
				}
				if reply.PG.Pool != uint64(tc.route.Pool) || reply.PG.Seed != tc.route.PG || reply.Submission.MapEpoch != tc.route.Epoch || reply.Submission.Transaction == 0 || reply.MapEpoch < tc.route.Epoch || len(reply.Entries) > 1 || len(reply.Cursor.Raw) < 39 || len(reply.RawData) < 49 {
					t.Fatal("PG identity/cursor/bound lost", reply)
				}
				got = append(got, reply.Entries...)
				if reply.End {
					break
				}
				if seen[string(reply.Cursor.Raw)] || bytes.Equal(request.Cursor, reply.Cursor.Raw) {
					t.Fatal("nonadvancing PG cursor", reply.Cursor)
				}
				seen[string(reply.Cursor.Raw)] = true
				request.Cursor = reply.Cursor.Raw
			}
			receipts = append(receipts, record)
			if len(record.Pages) == 0 || !record.Pages[len(record.Pages)-1].End {
				t.Fatal("bounded PG enumeration never completed")
			}
			if !reflect.DeepEqual(sortedPGList(got), sortedPGList(native.Entries)) {
				t.Fatal("native complete PG inventory differs", got, native.Entries)
			}
			after := pgListOracle(t, ctx, tc.route.PG, tc.namespace)
			if !reflect.DeepEqual(sortedPGList(after.Entries), sortedPGList(native.Entries)) {
				t.Fatal("PG inventory changed during observation")
			}
			if len(native.Entries) > 1 && len(record.Pages) < 2 {
				t.Fatal("count-one listing did not paginate")
			}
			t.Logf("native explicit-PG inventory matches: namespace=%q PG=%d.%x entries=%d Go pages=%d", tc.namespace, tc.route.Pool, tc.route.PG, len(got), len(record.Pages))
		})
	}
	t.Run("wrong explicit primary rejected locally", func(t *testing.T) {
		target := routes[owned.Objects[0]]
		reply, err := connections[owned.Objects[0]].ListPG(ctx, cephmsgr.OSDPGListRequest{MapEpoch: target.Epoch, PG: cephmsgr.OSDPGListTarget{Pool: uint64(target.Pool), Seed: target.PG, Primary: target.ID + 100}, Count: 1, MaxOutputBytes: 8192})
		if !errors.Is(err, cephmsgr.ErrOSDPGListTarget) || reply.Submission.Transaction != 0 {
			t.Fatal("invalid PG target submitted", reply, err)
		}
	})
}
