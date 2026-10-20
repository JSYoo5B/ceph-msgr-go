package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

type nativeWatchers struct {
	Object   string   `json:"object"`
	GlobalID uint64   `json:"global_id"`
	Cookies  []uint64 `json:"cookies"`
	Timeouts []uint32 `json:"timeouts"`
	Stopped  bool     `json:"stopped"`
	Native   struct {
		Entries []struct {
			Address  string `json:"address"`
			GlobalID uint64 `json:"global_id"`
			Cookie   uint64 `json:"cookie"`
		} `json:"entries"`
	} `json:"native"`
}

func watcherOracle(t *testing.T, ctx context.Context, action string) nativeWatchers {
	t.Helper()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if err := testcluster.ControlDaemon(ctx, dir, "verify", "osd-watchers", action); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "osd-watchers-result.json"))
	if err != nil {
		t.Fatal(err)
	}
	var result nativeWatchers
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestCephOSDListWatchersIntegration(t *testing.T) {
	c, _, base, _ := osdFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	prepared, watching := false, false
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 40*time.Second)
		defer stop()
		if watching {
			watcherOracle(t, cleanup, "stop")
		}
		if prepared && !nativeOmapWrite(t, cleanup, base, "cleanup", omapWriteInput{}).Cleanup.Removed {
			t.Error("watcher object cleanup failed")
		}
	})
	owned := nativeOmapWrite(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: []byte("watcher baseline")}, Reserve: 1})
	prepared = true
	if len(owned.Objects) != 2 {
		t.Fatal("owned watcher fixture missing")
	}
	main, absent := owned.Objects[0], owned.Objects[1]
	routes := map[string]osdFixtureOracle{}
	connections := map[string]*cephmsgr.OSDConnection{}
	defer func() {
		for _, conn := range connections {
			conn.Close()
		}
	}()
	for _, name := range []string{main, absent} {
		route := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": name, "namespace": ""}).Placement
		routes[name] = route
		connections[name] = mutationConnection(t, c, ctx, route)
	}
	var evidence []cephmsgr.OSDReply
	list := func(name string, budget uint32) (cephmsgr.OSDReply, error) {
		reply, err := connections[name].Request(ctx, osdObjectRequest(routes[name], cephmsgr.OSDOperation{Code: cephmsgr.OSDListWatchers, ListWatchers: cephmsgr.OSDListWatchersInput{MaxOutputBytes: budget}}))
		evidence = append(evidence, reply)
		return reply, err
	}
	t.Run("empty committed head", func(t *testing.T) {
		reply, err := list(main, 4096)
		if err != nil || len(reply.Operations[0].Watchers) != 0 || len(reply.Operations[0].Data) != 10 {
			t.Fatal("empty watcher listing", reply, err)
		}
	})
	t.Run("absent object signed ENOENT", func(t *testing.T) {
		reply, err := list(absent, 4096)
		var server *cephmsgr.OSDError
		if !errors.As(err, &server) || server.Code != -2 || reply.Code != -2 || len(reply.RawData) != 0 {
			t.Fatal("absent watcher listing", reply, err)
		}
	})
	native := watcherOracle(t, ctx, "start")
	watching = true
	if native.GlobalID == 0 || len(native.Cookies) != 2 || len(native.Native.Entries) != 2 {
		t.Fatal("native watch registration receipt", native)
	}
	t.Run("two live native registrations match CLI", func(t *testing.T) {
		reply, err := list(main, 4096)
		if err != nil {
			t.Fatal(err)
		}
		entries := reply.Operations[0].Watchers
		if len(entries) != 2 {
			t.Fatal("lost watchers", entries)
		}
		sort.Slice(entries, func(i, j int) bool { return entries[i].Cookie < entries[j].Cookie })
		for _, got := range entries {
			idx := -1
			for i, cookie := range native.Cookies {
				if cookie == got.Cookie {
					idx = i
				}
			}
			if idx < 0 || got.NameType != 8 || uint64(got.NameNumber) != native.GlobalID || got.TimeoutSeconds != native.Timeouts[idx] || !got.Address.Endpoint.IsValid() {
				t.Fatal("native identity/timeout/address differs", got, native)
			}
			matched := false
			for _, cli := range native.Native.Entries {
				if cli.Cookie == got.Cookie && cli.GlobalID == uint64(got.NameNumber) {
					addr := fmt.Sprintf("%s/%d", got.Address.Endpoint, got.Address.Nonce)
					if !strings.HasSuffix(cli.Address, addr) {
						t.Fatal("native CLI address differs", cli.Address, addr)
					}
					matched = true
				}
			}
			if !matched {
				t.Fatal("watcher missing from CLI", got)
			}
		}
		if reply.GlobalID == native.GlobalID {
			t.Fatal("observer and watcher identities conflated")
		}
		t.Logf("two native watchers matched: observer=%d watcher=%d raw=%d bytes", reply.GlobalID, native.GlobalID, len(reply.RawData))
	})
	t.Run("populated output over local budget fails only its connection", func(t *testing.T) {
		options := integrationOptions(t)
		options.EnableOSD = true
		independent, err := cephmsgr.Dial(ctx, options)
		if err != nil {
			t.Fatal(err)
		}
		defer independent.Close()
		limited := mutationConnection(t, independent, ctx, routes[main])
		defer limited.Close()
		reply, err := limited.Request(ctx, osdObjectRequest(routes[main], cephmsgr.OSDOperation{Code: cephmsgr.OSDListWatchers, ListWatchers: cephmsgr.OSDListWatchersInput{MaxOutputBytes: 10}}))
		var unknown *cephmsgr.OutcomeUnknownError
		if !errors.As(err, &unknown) || reply.Submission.Transaction == 0 || len(reply.RawData) <= 10 {
			t.Fatal("populated listing escaped local budget", err)
		}
		evidence = append(evidence, reply)
		// The original connection still observes the same registrations.
		if reply, err := list(main, 4096); err != nil || len(reply.Operations[0].Watchers) != 2 {
			t.Fatal("local budget failure damaged other connection", err)
		}
	})
	stopped := watcherOracle(t, ctx, "stop")
	watching = false
	if !stopped.Stopped {
		t.Fatal("native unwatch incomplete")
	}
	t.Run("native unwatch leaves empty listing", func(t *testing.T) {
		reply, err := list(main, 10)
		if err != nil || len(reply.Operations[0].Watchers) != 0 || len(reply.RawData) != 10 {
			t.Fatal("unwatch listing", reply, err)
		}
	})
	t.Run("completed native scope can start again", func(t *testing.T) {
		second := watcherOracle(t, ctx, "start")
		watching = true
		if second.GlobalID == 0 || len(second.Cookies) != 2 {
			t.Fatal("second scope lacks native registrations")
		}
		reply, err := list(main, 4096)
		if err != nil || len(reply.Operations[0].Watchers) != 2 {
			t.Fatal("second watcher scope listing", err)
		}
		for _, watcher := range reply.Operations[0].Watchers {
			if uint64(watcher.NameNumber) != second.GlobalID {
				t.Fatal("stale watcher identity in new scope")
			}
		}
		watcherOracle(t, ctx, "stop")
		watching = false
	})
	data, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}
	if err = testcluster.WritePublicEvidence(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "osd-watchers-results.json"), data); err != nil {
		t.Fatal(err)
	}
}
