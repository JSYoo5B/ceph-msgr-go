package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

const chunkReceiptChild = "CEPH_MSGR_CHUNK_RECEIPT_CHILD"

// Exercise JSON publication of each replicated receipt shape after actual Go
// subtest cleanups, including Fatal in the body and a failed restore/readback.
// The controller substitute keeps its effective setting separately from the
// advertised restoration flag, so that a stale or false flag cannot pass.
func TestCopyChunkCleanupPublication(t *testing.T) {
	if name := os.Getenv(chunkReceiptChild); name != "" {
		copyChunkReceiptChild(t, name)
		return
	}
	for _, shape := range []string{"loss", "three", "source", "omap", "removal"} {
		for _, outcome := range []string{"verified", "body-fatal", "lower-fatal", "restore-refused", "readback-mismatch", "invalid-json"} {
			t.Run(shape+"/"+outcome, func(t *testing.T) {
				dir := t.TempDir()
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCopyChunkCleanupPublication$", "-test.count=1", "-test.timeout=25s")
				cmd.Env = append(os.Environ(), chunkReceiptChild+"="+shape+"/"+outcome, "CEPH_MSGR_CONTROL_DIR="+dir)
				out, err := cmd.CombinedOutput()
				if (err == nil) != (outcome == "verified") {
					t.Fatalf("unexpected child verdict: %v\n%s", err, out)
				}
				raw, err := os.ReadFile(filepath.Join(dir, "published.json"))
				if err != nil {
					t.Fatal(err, string(out))
				}
				var published struct {
					Restored bool              `json:"chunk_restored"`
					Readback *copyChunkSetting `json:"chunk_restore_readback"`
				}
				if err := json.Unmarshal(raw, &published); err != nil {
					t.Fatal(err)
				}
				state, err := os.ReadFile(filepath.Join(dir, "effective.json"))
				if err != nil {
					t.Fatal(err)
				}
				var effective uint64
				if err := json.Unmarshal(state, &effective); err != nil {
					t.Fatal(err)
				}
				want := outcome == "verified" || outcome == "body-fatal" || outcome == "lower-fatal"
				if published.Restored != want {
					t.Fatalf("published restored=%t, effective=%d: %s", published.Restored, effective, raw)
				}
				if outcome == "invalid-json" {
					if published.Readback != nil {
						t.Fatal("aborted readback was published as observed", string(raw))
					}
				} else if published.Readback == nil || published.Readback.Value != effective || published.Restored != copyChunkRestored(*published.Readback, 1) {
					t.Fatalf("published status differs from independent effective readback %d: %s", effective, raw)
				}
			})
		}
	}
}

func copyChunkReceiptChild(t *testing.T, name string) {
	parts := strings.Split(name, "/")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		fakeCopyChunkReceiptControl(t, ctx, os.Getenv("CEPH_MSGR_CONTROL_DIR"), parts[1])
	}()
	defer func() { cancel(); <-done }()
	t.Run("cell", func(t *testing.T) {
		var value any
		var restored *bool
		var readback **copyChunkSetting
		switch parts[0] {
		case "loss":
			r := &copyChunkLossReceipt{}
			value, restored, readback = r, &r.ChunkRestore, &r.ChunkReadback
		case "three":
			r := &copyThreeChunkReceipt{}
			value, restored, readback = r, &r.ChunkRestore, &r.ChunkReadback
		case "source":
			r := &copySourceChangeReceipt{}
			value, restored, readback = r, &r.ChunkRestore, &r.ChunkReadback
		default:
			r := &copyChunkMutationReceipt{}
			value, restored, readback = r, &r.ChunkRestore, &r.ChunkReadback
		}
		publishAfterCleanups(t, func() {
			raw, err := json.Marshal(value)
			if err == nil {
				err = testcluster.WritePublicEvidence(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "published.json"), raw)
			}
			if err != nil {
				t.Error(err)
			}
		})
		r := &copyPrimaryChangeRun{t: t, ctx: ctx, dstRoute: osdFixtureOracle{ID: 1}, recovery: &copyPrimaryRecovery{}}
		r.lowerCopyChunk(restored, readback)
		if parts[1] == "body-fatal" {
			t.Fatal("injected failure after lowering the setting")
		}
	})
}

func fakeCopyChunkReceiptControl(t *testing.T, ctx context.Context, dir, outcome string) {
	const prior = 8 << 20
	value := uint64(prior)
	for ctx.Err() == nil {
		requests, err := filepath.Glob(filepath.Join(dir, "verify-osd-pool.*.request"))
		if err != nil {
			t.Error(err)
			return
		}
		for _, request := range requests {
			id := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(request), "verify-osd-pool."), ".request")
			var input struct {
				Action string `json:"action"`
			}
			raw, err := os.ReadFile(filepath.Join(dir, "osd-pool-input.json."+id))
			if err == nil {
				err = json.Unmarshal(raw, &input)
			}
			if err != nil {
				t.Error(err)
				return
			}
			setting := copyChunkSetting{OSD: 1, Action: input.Action, Limit: multiChunkLimit, Prior: prior}
			switch input.Action {
			case "lower":
				value = multiChunkLimit
				setting.Lowered, setting.Claimed = outcome != "lower-fatal", true
			case "restore":
				if outcome == "restore-refused" {
					setting.Claimed = true
					setting.Failures = []string{"injected config set refusal"}
				} else if outcome == "readback-mismatch" {
					setting.Restored = true
				} else {
					value = prior
					setting.Restored = true
				}
			default:
				t.Error("unexpected control", input.Action)
				return
			}
			setting.Value = value
			raw, err = json.Marshal(map[string]any{"schema_version": 1, "source_commit": "7f793731f1b39eb4f465e960113d2363c311b964", "mode": "copychunk", "fixture_daemon_version": "ceph version 20.2.4 (7f793731f1b39eb4f465e960113d2363c311b964)", "copy_chunk": setting})
			if outcome == "invalid-json" && input.Action == "restore" {
				raw = []byte("{")
			}
			for path, data := range map[string][]byte{"osd-pool-result.json": raw, "effective.json": []byte(fmt.Sprint(value))} {
				if err == nil {
					err = os.WriteFile(filepath.Join(dir, path), data, 0600)
				}
			}
			if err == nil {
				err = os.Remove(request)
			}
			if err == nil {
				err = os.WriteFile(filepath.Join(dir, "osd-pool-verify."+id), nil, 0600)
			}
			if err != nil {
				t.Error(err)
				return
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Millisecond):
		}
	}
}
