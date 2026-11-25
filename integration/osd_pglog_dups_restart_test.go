package integration_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// persistedDupRestart compares independent store copies, not the live duplicate
// index. No write or resubmission is allowed between these two captures.
func persistedDupRestart(before, after shardHistorySnapshot) error {
	if !before.Complete || !after.Complete || !before.AllActingShards || !after.AllActingShards || len(before.Failures)+len(after.Failures) != 0 || before.PGID != after.PGID || !slices.Equal(before.Acting, after.Acting) || len(before.Acting) != 4 || len(before.Shards) != 4 || len(after.Shards) != 4 || after.IntervalSince <= before.IntervalSince || after.Before.Epoch <= before.After.Epoch {
		return fmt.Errorf("restart did not retain the four-shard PG in a newer interval")
	}
	for i, a := range after.Shards {
		b := before.Shards[i]
		if !a.Complete || !b.Complete || len(a.Failures)+len(b.Failures) != 0 || a.Index != i || b.Index != i || a.OSD != after.Acting[i] || b.OSD != before.Acting[i] || a.Index != b.Index || a.OSD != b.OSD || a.SPG != b.SPG || a.Head != b.Head || a.Tail != b.Tail || a.LogEntries != b.LogEntries || a.DupEntries != b.DupEntries || a.ExtraReqidEntries != b.ExtraReqidEntries {
			return fmt.Errorf("shard %d identity or history boundary changed", i)
		}
		var parsed [2][]pgLogRecord
		for j, shard := range []shardHistoryShard{b, a} {
			raw, err := base64.StdEncoding.DecodeString(shard.Raw)
			if err != nil {
				return err
			}
			head, tail, records, err := parsePGLogRecords(raw)
			if err != nil {
				return err
			}
			if head != shard.Head || tail != shard.Tail {
				return fmt.Errorf("shard %d raw boundary differs", i)
			}
			parsed[j] = records
		}
		if !reflect.DeepEqual(parsed[0], parsed[1]) {
			return fmt.Errorf("shard %d persisted request records changed", i)
		}
	}
	return nil
}

func dupRestartBoot(before, after osdMapState) error {
	if before.OSD != after.OSD || !before.Up || !before.In || !after.Up || !after.In || after.Epoch <= before.Epoch || after.UpFrom <= before.Epoch || after.UpFrom > after.Epoch {
		return fmt.Errorf("OSD restart not independently observed: before %+v after %+v", before, after)
	}
	return nil
}

// Both checkpoints stop all four owned OSDs before starting any, so no live
// survivor supplies the old duplicate index. Fresh store copies after peering
// must preserve the exact persisted records. This does not inspect the rebuilt
// live nonprimary index or resubmit a request after restart. Both phases use
// optimized isa k=2,m=2 and deliberately lowered PG-log bounds. K is the
// known APPEND; P is COPY_FROM2 with source request S at the same version.
// Before restart each store holds P alone at that shared duplicate key.
// Independent native reads and fresh Go reads of source, destination, known
// APPEND and filler objects must retain their bytes and user versions.
// Cleanup restores all OSDs, the original bounds, and removes owned pools.
// The receipts are published only after those cleanup verdicts. No default
// retention bound, crash durability, or request replay after reboot is claimed.
//
//scenario:row osd.aes256k.ip4.container.osd-store-bluestore.osd-replicas-4.osd-profile-replicated-pg-v8.t-osderasureduphistoryrestart key=aes256k service=aes256k family=4 stress= runtime=container osd=1 osd_store=bluestore osd_replicas=4 osd_profile=replicated-pg-v8 capabilities=osd-erasure-width,osd-shard-history
//scenario:covers CLIENT to OSD
//fixture:isolated stops and restarts all four owned OSDs and copies their stores with temporary PG-log bounds
func TestCephOSDErasureDupHistoryRestartIntegration(t *testing.T) {
	for _, evicted := range []bool{false, true} {
		if !t.Run(fmt.Sprintf("evicted-%t", evicted), func(t *testing.T) {
			checkpoint := &dupHistoryCheckpoint{}
			complete, recovered := false, false
			proof := map[string]any{"evicted": evicted}
			publishAfterCleanups(t, func() {
				if !complete || !recovered || !checkpoint.boundsRestored || !checkpoint.poolsRemoved {
					t.Error("restart checkpoint or owned cleanup incomplete")
				}
				proof["complete"], proof["daemons_recovered"], proof["bounds_restored"], proof["pools_removed"] = complete, recovered, checkpoint.boundsRestored, checkpoint.poolsRemoved
				raw, err := json.MarshalIndent(proof, "", "  ")
				if err == nil {
					err = testcluster.WritePublicEvidence(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), fmt.Sprintf("osd-dups-restart-%t.json", evicted)), append(raw, '\n'))
				}
				if err != nil {
					t.Error(err)
				}
				t.Logf("restart cleanup: daemons=%t bounds=%t pools=%t", recovered, checkpoint.boundsRestored, checkpoint.poolsRemoved)
			})
			checkpoint.run = func(r *dupHistoryRun, sourceConn *cephmsgr.OSDConnection, base osdFixtureOracle, before shardHistorySnapshot, targets []pgHistoryTarget, phase bool) bool {
				if phase != evicted {
					return false
				}
				dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
				states := make(map[int32]osdMapState)
				state := func(id int32) osdMapState {
					var result struct {
						State *osdMapState `json:"osd_state"`
					}
					nativePoolResult(t, r.ctx, "osdstate", map[string]any{"osd": id}, &result)
					if result.State == nil || result.State.OSD != id {
						t.Fatal("missing native OSD state", id)
					}
					return *result.State
				}
				objects := []struct {
					slot   int
					name   string
					before nativeXattrSnapshot
				}{
					{1, "dup-history-source", nativeXattrSnapshot{}}, {2, "dup-history-known", nativeXattrSnapshot{}},
					{2, "dup-history-copy", nativeXattrSnapshot{}}, {2, "dup-history-filler", nativeXattrSnapshot{}},
				}
				for i := range objects {
					o := &objects[i]
					o.before = poolObserve(t, r.ctx, o.slot, o.name, "", "")
					if o.before.Code != 0 {
						t.Fatal("pre-restart readback", o.name)
					}
				}
				for _, id := range before.Acting {
					states[id] = state(id)
				}
				stopped := map[int32]bool{}
				t.Cleanup(func() {
					ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
					defer cancel()
					for _, id := range before.Acting {
						if stopped[id] {
							if err := testcluster.ControlDaemon(ctx, dir, "start", "osd", fmt.Sprint(id)); err != nil {
								t.Error(err)
							} else {
								stopped[id] = false
							}
						}
					}
					if err := testcluster.ControlDaemon(ctx, dir, "verify", "osd-ready", "clean"); err != nil {
						t.Error(err)
						return
					}
					fresh := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true})
					publishReplicaOSDRouting(t, dir, adoptMutationPlacement(t, base, fresh.Placement))
					recovered = true
				})
				if err := sourceConn.Close(); err != nil {
					t.Fatal(err)
				}
				if err := r.conn.Close(); err != nil {
					t.Fatal(err)
				}
				for _, id := range before.Acting {
					stopped[id] = true
					if err := testcluster.ControlDaemon(r.ctx, dir, "stop", "osd", fmt.Sprint(id)); err != nil {
						t.Fatal(err)
					}
				}
				for _, id := range before.Acting {
					if err := testcluster.ControlDaemon(r.ctx, dir, "start", "osd", fmt.Sprint(id)); err != nil {
						t.Fatal(err)
					}
					stopped[id] = false
				}
				if err := testcluster.ControlDaemon(r.ctx, dir, "verify", "osd-ready", "clean"); err != nil {
					t.Fatal(err)
				}
				boots := make(map[int32][2]osdMapState)
				for _, id := range before.Acting {
					after := state(id)
					boots[id] = [2]osdMapState{states[id], after}
					if err := dupRestartBoot(states[id], after); err != nil {
						t.Fatal(err)
					}
					t.Logf("restart osd.%d: up_from %d -> %d, map %d -> %d", id, states[id].UpFrom, after.UpFrom, states[id].Epoch, after.Epoch)
				}
				proof["osd_boots"] = boots
				fresh := poolPlace(t, r.ctx, r.slot, r.route.Name, "")
				if fresh.Pool != r.route.Pool || fresh.Hash != r.route.Hash || fresh.PGID != r.route.PGID || fresh.PrimaryID != r.route.PrimaryID || fresh.Shard != r.route.Shard || !slices.Equal(fresh.Acting, r.route.Acting) || !slices.Equal(fresh.Up, r.route.Up) || fresh.Epoch <= r.epoch {
					t.Fatal("restart changed object or final placement")
				}
				after := shardPGHistory(t, r.ctx, r.slot, "dup-history-filler", fresh, targets, nil)
				if err := persistedDupRestart(before, after); err != nil {
					t.Fatal(err)
				}
				digests := map[string][2]string{}
				for _, o := range objects {
					native := poolObserve(t, r.ctx, o.slot, o.name, "", "")
					digests[o.name] = [2]string{nativeDigest(o.before), nativeDigest(native)}
					if !sameNative(native, o.before) {
						t.Fatal("native object changed after restart", o.name)
					}
					route := poolPlace(t, r.ctx, o.slot, o.name, "")
					conn := mutationConnection(t, r.c, r.ctx, route)
					reply, err := conn.Request(r.ctx, r.request(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: uint64(len(native.Data))}))
					conn.Close()
					if err != nil || reply.Code != 0 || reply.UserVersion != native.Version || len(reply.Operations) != 1 || !slices.Equal(reply.Operations[0].Data, native.Data) {
						t.Fatal("fresh Go read differs from native", o.name, err)
					}
					if !sameNative(poolObserve(t, r.ctx, o.slot, o.name, "", ""), native) {
						t.Fatal("readback changed object", o.name)
					}
				}
				proof["pg"], proof["epochs"], proof["intervals"] = after.PGID, [2]uint32{before.After.Epoch, after.Before.Epoch}, [2]uint32{before.IntervalSince, after.IntervalSince}
				var shards []map[string]any
				for i, a := range after.Shards {
					b := before.Shards[i]
					raw, _ := base64.StdEncoding.DecodeString(a.Raw)
					_, _, records, _ := parsePGLogRecords(raw)
					shards = append(shards, map[string]any{"osd": a.OSD, "spg": a.SPG, "head": a.Head, "tail": a.Tail, "before_sha256": b.SHA256, "after_sha256": a.SHA256, "records": records})
				}
				proof["shards"], proof["object_digests"] = shards, digests
				complete = true
				return true
			}
			dupHistoryScenario(t, &erasureK2M2, checkpoint)
		}) {
			return
		}
	}
}

func TestPersistedDupRestart(t *testing.T) {
	raw := `{"pg_log_t":{"head":"30'5","tail":"30'4","log":[{"op":"modify","object":"x","version":"30'5","prior_version":"30'4","reqid":"client.4700.11:5","extra_reqids":[],"mtime":"0.0","return_code":0}],"dups":[{"reqid":"client.4700.11:4","version":"30'4","user_version":"4","return_code":"0"}]}}`
	makeSnapshot := func(epoch uint32) shardHistorySnapshot {
		s := shardHistorySnapshot{Complete: true, AllActingShards: true, PGID: "2.0", Acting: []int32{0, 1, 2, 3}, IntervalSince: epoch, Before: osdFixtureOracle{Epoch: epoch}, After: osdFixtureOracle{Epoch: epoch}}
		for i := 0; i < 4; i++ {
			s.Shards = append(s.Shards, shardHistoryShard{Complete: true, Index: i, OSD: int32(i), SPG: fmt.Sprintf("2.0s%d", i), Head: "30'5", Tail: "30'4", LogEntries: 1, DupEntries: 1, Raw: base64.StdEncoding.EncodeToString([]byte(raw))})
		}
		return s
	}
	before := makeSnapshot(40)
	if err := persistedDupRestart(before, makeSnapshot(50)); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*shardHistorySnapshot){
		"same interval": func(s *shardHistorySnapshot) { s.IntervalSince = 40 },
		"old epoch":     func(s *shardHistorySnapshot) { s.Before.Epoch = 40 },
		"other PG":      func(s *shardHistorySnapshot) { s.PGID = "3.0" },
		"other acting":  func(s *shardHistorySnapshot) { s.Acting[3] = 4 },
		"missing shard": func(s *shardHistorySnapshot) { s.Shards = s.Shards[:3] },
		"incomplete":    func(s *shardHistorySnapshot) { s.Complete = false },
		"wrong OSD":     func(s *shardHistorySnapshot) { s.Shards[2].OSD = 3 },
		"wrong index":   func(s *shardHistorySnapshot) { s.Shards[2].Index = 3 },
		"changed bound": func(s *shardHistorySnapshot) { s.Shards[2].Tail = "30'3" },
		"changed request": func(s *shardHistorySnapshot) {
			s.Shards[2].Raw = base64.StdEncoding.EncodeToString([]byte(strings.ReplaceAll(raw, "client.4700.11:4", "client.4700.12:4")))
		},
		"changed version": func(s *shardHistorySnapshot) {
			s.Shards[2].Raw = base64.StdEncoding.EncodeToString([]byte(strings.ReplaceAll(raw, `"user_version":"4"`, `"user_version":"5"`)))
		},
	} {
		t.Run(name, func(t *testing.T) {
			after := makeSnapshot(50)
			mutate(&after)
			if persistedDupRestart(before, after) == nil {
				t.Fatal("accepted invalid restart evidence")
			}
		})
	}
}

func TestDupRestartBoot(t *testing.T) {
	before := osdMapState{OSD: 2, Epoch: 40, Up: true, In: true, UpFrom: 10}
	valid := osdMapState{OSD: 2, Epoch: 50, Up: true, In: true, UpFrom: 48}
	if err := dupRestartBoot(before, valid); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*osdMapState){
		"same boot": func(a *osdMapState) { a.UpFrom = 10 }, "old boot": func(a *osdMapState) { a.UpFrom = 40 }, "future boot": func(a *osdMapState) { a.UpFrom = 51 }, "wrong OSD": func(a *osdMapState) { a.OSD = 3 }, "down": func(a *osdMapState) { a.Up = false }, "out": func(a *osdMapState) { a.In = false }, "old map": func(a *osdMapState) { a.Epoch = 40 },
	} {
		t.Run(name, func(t *testing.T) {
			after := valid
			mutate(&after)
			if dupRestartBoot(before, after) == nil {
				t.Fatal("accepted missing restart proof")
			}
		})
	}
}
