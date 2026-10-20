package integration_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

type nativeNotifyReceipt struct {
	Object     string `json:"object"`
	GlobalID   uint64 `json:"global_id"`
	Done       bool   `json:"done"`
	ReturnCode int32  `json:"return_code"`
	Replies    []struct {
		GlobalID   uint64 `json:"global_id"`
		Cookie     uint64 `json:"cookie"`
		PayloadHex string `json:"payload_hex"`
	} `json:"replies"`
	Missed []struct {
		GlobalID uint64 `json:"global_id"`
		Cookie   uint64 `json:"cookie"`
	} `json:"missed"`
}

func notifyOracle(t *testing.T, ctx context.Context, action string) nativeNotifyReceipt {
	t.Helper()
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if err := testcluster.ControlDaemon(ctx, dir, "verify", "osd-watch-notify", action); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "osd-watch-notify-result.json"))
	if err != nil {
		t.Fatal(err)
	}
	var result nativeNotifyReceipt
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func TestCephOSDWatchNotifyIntegration(t *testing.T) {
	c, _, base, _ := osdFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()
	prepared, watching, notifying := false, false, false
	var w *cephmsgr.OSDWatch
	var conn *cephmsgr.OSDConnection
	var epoch uint32
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 45*time.Second)
		defer stop()
		if notifying {
			notifyOracle(t, cleanup, "finish")
		}
		if w != nil {
			if _, err := w.Unwatch(cleanup, epoch); err != nil {
				t.Error("Go unwatch cleanup", err)
			}
		}
		if conn != nil {
			conn.Close()
		}
		if watching {
			watcherOracle(t, cleanup, "stop")
		}
		if prepared && !nativeOmapWrite(t, cleanup, base, "cleanup", omapWriteInput{}).Cleanup.Removed {
			t.Error("watch/notify object cleanup failed")
		}
	})
	owned := nativeOmapWrite(t, ctx, base, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: []byte("watch/notify baseline")}, Reserve: 1})
	prepared = true
	name := owned.Objects[0]
	route := nativeMutation(t, ctx, map[string]any{"compare": false, "placement": true, "object": name, "namespace": ""}).Placement
	conn = mutationConnection(t, c, ctx, route)
	request := osdObjectRequest(route)
	request.Operations = nil
	epoch = request.MapEpoch
	native := watcherOracle(t, ctx, "start")
	watching = true
	if native.Object != name || native.GlobalID == 0 || len(native.Cookies) != 2 {
		t.Fatal("native watchers do not own object", native)
	}
	var replies []cephmsgr.OSDReply
	var events []cephmsgr.OSDWatchEvent
	payload := []byte{0, 255, 'n', 'o', 't', 'i', 'f', 'y', 0}
	t.Run("Go notifier completes through two native watchers", func(t *testing.T) {
		n, reply, err := conn.NotifyObject(ctx, request, payload, cephmsgr.OSDWatchOptions{TimeoutSeconds: 3})
		if err != nil {
			t.Fatal(err)
		}
		defer n.Close()
		replies = append(replies, reply)
		completion, err := n.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, completion)
		if completion.Opcode != cephmsgr.OSDWatchCompletion || completion.ReturnCode != 0 || completion.NotifyID == 0 || completion.Cookie != n.Cookie() || len(completion.Replies) != 2 || len(completion.Missed) != 0 || len(completion.Payload) != 0 {
			t.Fatal("native completion differs", completion)
		}
		for _, r := range completion.Replies {
			found := false
			for _, cookie := range native.Cookies {
				if r.GlobalID == native.GlobalID && r.Cookie == cookie && len(r.Data) == 0 {
					found = true
				}
			}
			if !found {
				t.Fatal("native watcher ACK identity lost", r)
			}
		}
		if _, err := n.Next(ctx); !errors.Is(err, cephmsgr.ErrOSDNotifyComplete) {
			t.Fatal(err)
		}
	})
	var err error
	w, _, err = conn.WatchObject(ctx, request, cephmsgr.OSDWatchOptions{TimeoutSeconds: 37})
	if err != nil {
		t.Fatal(err)
	}
	t.Run("explicit watch ping", func(t *testing.T) {
		reply, err := w.Ping(ctx, epoch)
		if err != nil {
			t.Fatal(err)
		}
		replies = append(replies, reply)
	})
	t.Run("partial timeout preserves native replies and unacknowledged Go watch", func(t *testing.T) {
		n, reply, err := conn.NotifyObject(ctx, request, payload, cephmsgr.OSDWatchOptions{TimeoutSeconds: 1})
		if err != nil {
			t.Fatal(err)
		}
		defer n.Close()
		replies = append(replies, reply)
		notification, err := w.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, notification)
		if notification.Opcode != cephmsgr.OSDWatchNotification || notification.Cookie != w.Cookie() || !bytes.Equal(notification.Payload, payload) || notification.NotifierGlobalID != reply.GlobalID {
			t.Fatal("Go notification binary metadata lost", notification)
		}
		completion, err := n.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, completion)
		if completion.ReturnCode != -110 || len(completion.Replies) != 2 || len(completion.Missed) != 1 || completion.Missed[0].Cookie != w.Cookie() || completion.Missed[0].GlobalID != reply.GlobalID {
			t.Fatal("partial timeout receipt differs", completion)
		}
	})
	t.Run("native notifier receives explicit binary Go ACK", func(t *testing.T) {
		input, err := json.Marshal(map[string]any{"payload_hex": hex.EncodeToString(payload), "timeout_ms": 10000})
		if err != nil {
			t.Fatal(err)
		}
		if err := testcluster.WritePublicEvidence(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "osd-watch-notify-input.json"), input); err != nil {
			t.Fatal(err)
		}
		started := notifyOracle(t, ctx, "start")
		notifying = true
		if started.Object != name || started.GlobalID == 0 {
			t.Fatal("native notifier scope differs", started)
		}
		notification, err := w.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, notification)
		if !bytes.Equal(notification.Payload, payload) || notification.NotifierGlobalID != started.GlobalID || notification.Cookie != w.Cookie() || notification.Source.ID != route.ID {
			t.Fatal("native notify source/payload lost", notification)
		}
		// Native completion stays pending until the user elects to ACK.
		if snapshot := notifyOracle(t, ctx, "observe"); snapshot.Done {
			t.Fatal("notification automatically acknowledged")
		}
		ackPayload := []byte{0, 255, 'a', 'c', 'k', 0}
		reply, err := w.Acknowledge(ctx, epoch, notification, ackPayload)
		if err != nil {
			t.Fatal(err)
		}
		replies = append(replies, reply)
		finished := notifyOracle(t, ctx, "finish")
		notifying = false
		if !finished.Done || finished.ReturnCode != 0 || len(finished.Missed) != 0 || len(finished.Replies) != 3 {
			t.Fatal("native completion receipt differs", finished)
		}
		matched := false
		for _, r := range finished.Replies {
			if r.GlobalID == reply.GlobalID && r.Cookie == w.Cookie() {
				if r.PayloadHex != hex.EncodeToString(ackPayload) {
					t.Fatal("native ACK blob differs", r)
				}
				matched = true
			}
		}
		if !matched {
			t.Fatal("native notifier lost Go watcher identity")
		}
	})
	t.Run("explicit unwatch removes only Go registration", func(t *testing.T) {
		reply, err := w.Unwatch(ctx, epoch)
		if err != nil {
			t.Fatal(err)
		}
		replies = append(replies, reply)
		if _, err := w.Next(ctx); !errors.Is(err, cephmsgr.ErrClosed) {
			t.Fatal(err)
		}
		w = nil
		listed, err := conn.Request(ctx, osdObjectRequest(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDListWatchers, ListWatchers: cephmsgr.OSDListWatchersInput{MaxOutputBytes: 4096}}))
		if err != nil || len(listed.Operations[0].Watchers) != 2 {
			t.Fatal("unwatch affected native registrations", err)
		}
	})
	data, err := json.Marshal(struct {
		Replies []cephmsgr.OSDReply
		Events  []cephmsgr.OSDWatchEvent
	}{replies, events})
	if err != nil {
		t.Fatal(err)
	}
	if err := testcluster.WritePublicEvidence(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "osd-watch-notify-go.json"), data); err != nil {
		t.Fatal(err)
	}
	t.Logf("watch/notify preserved %d operation receipts and %d independent events", len(replies), len(events))
}
