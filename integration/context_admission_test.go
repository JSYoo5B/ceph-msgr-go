package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

// A context may inspect the client while checking its own lifetime. Snapshot
// reads session readiness, so an Err callback under a session lock deadlocks.
type snapshotAdmissionContext struct {
	context.Context
	client  *cephmsgr.Client
	fsid    string
	mgr     bool
	checks  atomic.Uint32
	invalid atomic.Bool
}

func (ctx *snapshotAdmissionContext) Err() error {
	state := ctx.client.Snapshot()
	ctx.checks.Add(1)
	if state.Closed || state.FSID != ctx.fsid || !state.Monitor.Ready || ctx.mgr && !state.Manager.Ready {
		ctx.invalid.Store(true)
	}
	return ctx.Context.Err()
}

func TestCephContextAdmissionIntegration(t *testing.T) {
	c, ctx := fixtureClient(t, 30*time.Second)
	for _, mgr := range []bool{false, true} {
		name := "MON"
		if mgr {
			name = "MGR"
		}
		t.Run(name, func(t *testing.T) {
			call, warm := c.MonCommand, c.WaitMonReady
			query := cephmsgr.Command{JSON: []byte(`{"prefix":"status","format":"json"}`)}
			if mgr {
				call, warm = c.MgrCommand, c.WaitMgrReady
				query.JSON = []byte(`{"prefix":"pg stat","format":"json"}`)
			}
			if err := warm(ctx); err != nil {
				t.Fatal("prepare context callback target", err)
			}
			fsid := c.Snapshot().FSID
			payload := []byte{0, 255, 13, 10, 1, 0}
			binary := cephmsgr.Command{JSON: []byte(`{"prefix":"pg getmap"}`)}
			if !mgr {
				key := fmt.Sprintf("native-context-admission-%d", time.Now().UnixNano())
				encoded, _ := json.Marshal(map[string]string{"prefix": "config-key set", "key": key})
				if _, err := c.MonCommand(ctx, cephmsgr.Command{JSON: encoded, Input: payload}); err != nil {
					t.Fatal("seed binary context callback response", err)
				}
				binary.JSON, _ = json.Marshal(map[string]string{"prefix": "config-key get", "key": key})
			}
			for _, tc := range []struct {
				name    string
				command cephmsgr.Command
			}{
				{"JSON", query},
				{"binary", binary},
				{"server-error", cephmsgr.Command{JSON: []byte(`{"prefix":"__ceph_msgr_missing_command__"}`)}},
			} {
				t.Run(tc.name, func(t *testing.T) {
					operation, cancel := context.WithTimeout(ctx, 5*time.Second)
					defer cancel()
					callback := &snapshotAdmissionContext{Context: operation, client: c, fsid: fsid, mgr: mgr}
					result, err := call(callback, tc.command)
					if tc.name == "server-error" {
						var server *cephmsgr.CommandError
						var unknown *cephmsgr.OutcomeUnknownError
						if !errors.As(err, &server) || result.Code != -22 || server.Code != result.Code || server.Message != result.Message || result.Message == "" || errors.As(err, &unknown) {
							t.Fatal("reentrant context lost the server rejection", result.Code, err)
						}
					} else {
						if err != nil || result.Code != 0 {
							t.Fatal("reentrant context prevented a real command reply", result.Code, err)
						}
						valid := json.Valid(result.Data)
						if tc.name == "binary" {
							valid = bytes.Equal(result.Data, payload)
							if mgr {
								valid = len(result.Data) >= 16 && !json.Valid(result.Data) && bytes.ContainsRune(result.Data, 0)
							}
						}
						if !valid {
							t.Fatal("reentrant context changed raw command output", result.Code, len(result.Data))
						}
					}
					// A warmed MON command checks Err twice at the public API,
					// once in Session.Call and again at writer admission. MGR
					// additionally checks the operation before ready-session use.
					minimum := uint32(4)
					if mgr {
						minimum++
					}
					if callback.checks.Load() < minimum || callback.invalid.Load() {
						t.Fatal("context did not inspect ready state at command admission", callback.checks.Load(), callback.invalid.Load())
					}
				})
			}
		})
	}
}
