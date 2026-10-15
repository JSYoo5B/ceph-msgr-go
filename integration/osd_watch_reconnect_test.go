package integration_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

const watchReconnectSource = "7f793731f1b39eb4f465e960113d2363c311b964"

type nativeReconnectWatchList struct {
	Source  string `json:"source_commit"`
	Object  string `json:"object"`
	Entries []struct {
		Address  string `json:"address"`
		GlobalID uint64 `json:"global_id"`
		Cookie   uint64 `json:"cookie"`
	} `json:"entries"`
}

type nativeReconnectNotify struct {
	nativeNotifyReceipt
	Source string `json:"source_commit"`
	Token  string `json:"token"`
}

// This uses only the existing owned native list/notifier controls. Returning
// errors lets cleanup attempt UNWATCH and close both connections even when the
// notifier's completion cannot be confirmed.
func watchReconnectControl(ctx context.Context, kind, action, resultName string, result any) error {
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if err := os.Remove(filepath.Join(dir, resultName)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := testcluster.ControlDaemon(ctx, dir, "verify", kind, action); err != nil {
		return err
	}
	file, err := os.Open(filepath.Join(dir, resultName))
	if err != nil {
		return err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, (512<<10)+1))
	if err != nil {
		return err
	}
	if len(data) > 512<<10 {
		return errors.New("native watch reconnect receipt exceeds evidence bound")
	}
	return json.Unmarshal(data, result)
}

func watchReconnectList(ctx context.Context, name string) (nativeReconnectWatchList, error) {
	var result nativeReconnectWatchList
	err := watchReconnectControl(ctx, "osd-watchers", "observe", "osd-watchers-result.json", &result)
	if err == nil && (result.Source != watchReconnectSource || result.Object != name || len(result.Entries) > 64) {
		err = errors.New("native reconnect watcher list provenance or ownership differs")
	}
	return result, err
}

func watchReconnectNotify(ctx context.Context, action, name string) (nativeReconnectNotify, error) {
	var result nativeReconnectNotify
	err := watchReconnectControl(ctx, "osd-watch-notify", action, "osd-watch-notify-result.json", &result)
	if err == nil && (result.Source != watchReconnectSource || result.Object != name || len(result.Token) != 32 || result.GlobalID == 0) {
		err = errors.New("native reconnect notifier provenance or ownership differs")
	}
	return result, err
}

func TestCephOSDWatchReconnectIntegration(t *testing.T) {
	modern := os.Getenv("CEPH_MSGR_TEST_OSD_PROFILE") == "replicated-pg-v8"
	var c *cephmsgr.Client
	var base osdFixtureOracle
	if modern {
		c, _, base, _ = replicatedPGFixture(t)
	} else {
		c, _, base, _ = osdFixture(t)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	t.Cleanup(cancel)
	owned := func(call context.Context, mode string, input omapWriteInput) omapWriteReceipt {
		if modern {
			return replicatedPGOwned(t, call, base, mode, input)
		}
		return nativeOmapWrite(t, call, base, mode, input)
	}
	native := func(call context.Context, name string) mutationOracle {
		if modern {
			return replicatedPGNative(t, call, name)
		}
		return nativeMutation(t, call, map[string]any{"compare": false, "placement": true, "object": name, "namespace": ""})
	}
	requestFor := func(route osdFixtureOracle, ops ...cephmsgr.OSDOperation) cephmsgr.OSDRequest {
		if modern {
			return replicatedPGRequest(route, ops...)
		}
		return osdObjectRequest(route, ops...)
	}
	checkReply := func(reply cephmsgr.OSDReply, route osdFixtureOracle) {
		if modern {
			checkReplicatedPGReply(t, reply, route)
		} else {
			checkOSDMetadata(t, reply, route)
		}
	}
	var oldConnection, newConnection *cephmsgr.OSDConnection
	var oldWatch, currentWatch *cephmsgr.OSDWatch
	var route osdFixtureOracle
	var name string
	prepared, watchMayExist, notifying, unwatched, cleaned := false, false, false, false, false
	var initial, ping, reconnect, unwatch cephmsgr.OSDReply
	var acknowledgements, listings []cephmsgr.OSDReply
	var oldEvent, newEvent cephmsgr.OSDWatchEvent
	var before, after, removed nativeReconnectWatchList
	var notifications []nativeReconnectNotify
	var finalNative mutationSnapshot
	var handoffDuration time.Duration
	var cleanupErrors []string
	var oldHandleFenced, oldEventFenced bool
	globalID := c.Snapshot().GlobalID
	t.Cleanup(func() {
		defer func() {
			data, err := json.Marshal(struct {
				Profile                           string `json:"profile"`
				GlobalID                          uint64 `json:"global_id"`
				Name                              string `json:"object"`
				Initial, Ping, Reconnect, Unwatch cephmsgr.OSDReply
				Acknowledgements, Listings        []cephmsgr.OSDReply
				OldEvent, NewEvent                cephmsgr.OSDWatchEvent
				Before, After, Removed            nativeReconnectWatchList
				Notifications                     []nativeReconnectNotify
				FinalNative                       mutationSnapshot
				HandoffMilliseconds               int64 `json:"close_to_reconnect_milliseconds"`
				OldHandleFenced, OldEventFenced   bool
				Unwatched, Cleaned                bool
				CleanupErrors                     []string
			}{os.Getenv("CEPH_MSGR_TEST_OSD_PROFILE"), globalID, name, initial, ping, reconnect, unwatch, acknowledgements, listings, oldEvent, newEvent, before, after, removed, notifications, finalNative, handoffDuration.Milliseconds(), oldHandleFenced, oldEventFenced, unwatched, cleaned, cleanupErrors})
			if err == nil && len(data) > 1<<20 {
				err = errors.New("watch reconnect evidence exceeds output bound")
			}
			if err == nil {
				err = testcluster.WritePublicEvidence(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "osd-watch-reconnect-results.json"), data)
			}
			if err != nil {
				t.Error("preserve native watch reconnect evidence", err)
			}
		}()
		cleanup, stop := context.WithTimeout(context.Background(), 40*time.Second)
		defer stop()
		if notifying {
			result, err := watchReconnectNotify(cleanup, "finish", name)
			notifications = append(notifications, result)
			if err != nil {
				cleanupErrors = append(cleanupErrors, err.Error())
				t.Error("finish owned native notifier", err)
			} else {
				notifying = !result.Done
			}
		}
		if currentWatch != nil && watchMayExist {
			reply, err := currentWatch.Unwatch(cleanup, route.Epoch)
			unwatch = reply
			if err != nil {
				cleanupErrors = append(cleanupErrors, err.Error())
				t.Error("explicit reconnect watch cleanup", err)
			} else {
				watchMayExist, unwatched = false, true
			}
		}
		if newConnection != nil {
			_ = newConnection.Close()
		}
		if oldConnection != nil {
			_ = oldConnection.Close()
		}
		if prepared && !watchMayExist && !notifying {
			cleaned = owned(cleanup, "cleanup", omapWriteInput{}).Cleanup.Removed
			if !cleaned {
				t.Error("watch reconnect owned object cleanup incomplete")
			}
		}
		if prepared && (watchMayExist || notifying) {
			t.Error("owned watch object retained because remote UNWATCH or native notifier completion was not confirmed")
		}
	})
	baseline := []byte{0, 255, 128, 'r', 'e', 'c', 'o', 'n', 'n', 'e', 'c', 't'}
	seeded := owned(ctx, "prepare", omapWriteInput{Seed: &omapWriteSeed{Baseline: baseline}})
	prepared = true
	if len(seeded.Objects) != 1 || !bytes.Equal(seeded.State.Data, baseline) {
		t.Fatal("watch reconnect fixture ownership or baseline differs", seeded)
	}
	name = seeded.Objects[0]
	route = native(ctx, name).Placement
	var err error
	oldConnection, err = c.OpenOSD(ctx, cephmsgr.OSDTarget{ID: route.ID, Address: route.Address})
	if err != nil {
		t.Fatal(err)
	}
	request := requestFor(route)
	request.ClientIncarnation = 91
	watchMayExist = true // An uncertain initial registration is never replayed.
	oldWatch, initial, err = oldConnection.WatchObject(ctx, request, cephmsgr.OSDWatchOptions{TimeoutSeconds: 37, RegistrationGeneration: 9})
	currentWatch = oldWatch
	if err != nil {
		t.Fatal("initial native WATCH registration", initial, err)
	}
	checkReply(initial, route)
	if oldWatch == nil || oldWatch.Cookie() == 0 || len(initial.Operations) != 1 || initial.Operations[0].Watch.Cookie != oldWatch.Cookie() || initial.Operations[0].Watch.Generation != 9 {
		t.Fatal("initial watcher receipt lost cookie or registration generation", initial)
	}
	before, err = watchReconnectList(ctx, name)
	if err != nil || len(before.Entries) != 1 || before.Entries[0].GlobalID != globalID || before.Entries[0].Cookie != oldWatch.Cookie() || before.Entries[0].Address == "" {
		t.Fatal("independent native original watcher identity differs", before, err)
	}
	// An independent native notification proves the original handle's session
	// generation before that connection is closed. Every ACK is explicit.
	notify := func(watch *cephmsgr.OSDWatch, payload, ack []byte) cephmsgr.OSDWatchEvent {
		t.Helper()
		input, err := json.Marshal(map[string]any{"payload_hex": hex.EncodeToString(payload), "timeout_ms": 10000})
		if err != nil {
			t.Fatal(err)
		}
		if err := testcluster.WritePublicEvidence(filepath.Join(os.Getenv("CEPH_MSGR_CONTROL_DIR"), "osd-watch-notify-input.json"), input); err != nil {
			t.Fatal(err)
		}
		started, err := watchReconnectNotify(ctx, "start", name)
		notifications = append(notifications, started)
		notifying = true
		if err != nil || started.Done || started.GlobalID == globalID {
			t.Fatal("independent native notifier start", started, err)
		}
		event, err := watch.Next(ctx)
		if err != nil {
			t.Fatal("native notification delivery", err)
		}
		if event.Opcode != cephmsgr.OSDWatchNotification || event.Cookie != watch.Cookie() || event.GlobalID != globalID || event.NotifierGlobalID != started.GlobalID || event.NotifyID == 0 || event.Source != (cephmsgr.OSDTarget{ID: route.ID, Address: route.Address}) || event.SessionGeneration == 0 || !bytes.Equal(event.Payload, payload) || len(event.RawFront) == 0 {
			t.Fatal("native notification identity or binary payload differs", event)
		}
		pending, err := watchReconnectNotify(ctx, "observe", name)
		notifications = append(notifications, pending)
		if err != nil || pending.Token != started.Token || pending.Done {
			t.Fatal("notification was acknowledged without the explicit user reply", pending, err)
		}
		reply, err := watch.Acknowledge(ctx, route.Epoch, event, ack)
		acknowledgements = append(acknowledgements, reply)
		if err != nil {
			t.Fatal("explicit native notification ACK", reply, err)
		}
		checkReply(reply, route)
		finished, err := watchReconnectNotify(ctx, "finish", name)
		notifications = append(notifications, finished)
		if err != nil || finished.Token != started.Token || !finished.Done || finished.ReturnCode != 0 || len(finished.Missed) != 0 || len(finished.Replies) != 1 || finished.Replies[0].GlobalID != globalID || finished.Replies[0].Cookie != watch.Cookie() || finished.Replies[0].PayloadHex != hex.EncodeToString(ack) {
			t.Fatal("native notifier lost explicit binary ACK or reconnected identity", finished, err)
		}
		notifying = false
		return event
	}
	oldEvent = notify(oldWatch, []byte{0, 255, 'o', 'l', 'd', 0}, []byte{0, 128, 'o', 'l', 'd', '-', 'A', 'C', 'K'})
	ping, err = oldWatch.Ping(ctx, route.Epoch)
	if err != nil {
		t.Fatal("explicit original watch ping before disconnect", err)
	}
	checkReply(ping, route)
	// Keep the native 37-second registration grace window independent of slow
	// observer setup: between Close and RECONNECT there is no native helper call.
	handoff, stopHandoff := context.WithTimeout(ctx, 10*time.Second)
	startedHandoff := time.Now()
	if err := oldConnection.Close(); err != nil {
		stopHandoff()
		t.Fatal(err)
	}
	newConnection, err = c.OpenOSD(handoff, cephmsgr.OSDTarget{ID: route.ID, Address: route.Address})
	if err != nil {
		stopHandoff()
		t.Fatal("same-client replacement OSD connection", err)
	}
	if newConnection == oldConnection || c.Snapshot().GlobalID != globalID {
		stopHandoff()
		t.Fatal("replacement did not preserve client identity and replace the closed connection")
	}
	currentWatch, reconnect, err = newConnection.ReconnectWatch(handoff, oldWatch, request)
	handoffDuration = time.Since(startedHandoff)
	stopHandoff()
	if err != nil {
		t.Fatal("explicit same-cookie native RECONNECT", reconnect, err)
	}
	checkReply(reconnect, route)
	if currentWatch == nil || currentWatch == oldWatch || currentWatch.Cookie() != oldWatch.Cookie() || reconnect.GlobalID != globalID || len(reconnect.Operations) != 1 || reconnect.Operations[0].Watch.Action != cephmsgr.OSDWatchReconnect || reconnect.Operations[0].Watch.Cookie != oldWatch.Cookie() || reconnect.Operations[0].Watch.Generation != initial.Operations[0].Watch.Generation+1 || handoffDuration >= 37*time.Second {
		t.Fatal("native reconnect changed cookie/global identity or lost generation increment/grace", reconnect, handoffDuration)
	}
	if reply, err := oldWatch.Ping(ctx, route.Epoch); !errors.Is(err, cephmsgr.ErrOSDWatchSuperseded) || reply.Submission != (cephmsgr.OSDSubmission{}) {
		t.Fatal("superseded old watch retained remote operation authority", reply, err)
	}
	oldHandleFenced = true
	if reply, err := currentWatch.Acknowledge(ctx, route.Epoch, oldEvent, []byte("stale")); err == nil || reply.Submission != (cephmsgr.OSDSubmission{}) {
		t.Fatal("reconnected handle accepted an old-session notification", reply, err)
	}
	oldEventFenced = true
	after, err = watchReconnectList(ctx, name)
	if err != nil || len(after.Entries) != 1 || after.Entries[0].GlobalID != globalID || after.Entries[0].Cookie != oldWatch.Cookie() {
		t.Fatal("native RECONNECT replaced or duplicated watcher identity", after, err)
	}
	listed, err := newConnection.Request(ctx, requestFor(route, cephmsgr.OSDOperation{Code: cephmsgr.OSDListWatchers, ListWatchers: cephmsgr.OSDListWatchersInput{MaxOutputBytes: 4096}}))
	listings = append(listings, listed)
	if err != nil || len(listed.Operations) != 1 || len(listed.Operations[0].Watchers) != 1 || listed.Operations[0].Watchers[0].NameType != 8 || uint64(listed.Operations[0].Watchers[0].NameNumber) != globalID || listed.Operations[0].Watchers[0].Cookie != currentWatch.Cookie() || listed.Operations[0].Watchers[0].TimeoutSeconds != 37 {
		t.Fatal("reconnected wire watcher metadata differs from independent CLI", listed, err)
	}
	checkReply(listed, route)
	newEvent = notify(currentWatch, []byte{0, 255, 128, 'n', 'e', 'w', 0}, []byte{0, 255, 'n', 'e', 'w', '-', 'A', 'C', 'K', 0})
	if newEvent.SessionGeneration <= oldEvent.SessionGeneration || newEvent.Cookie != oldEvent.Cookie || newEvent.GlobalID != oldEvent.GlobalID || newEvent.NotifyID == oldEvent.NotifyID {
		t.Fatal("new native notification did not cross a session-generation handoff", oldEvent, newEvent)
	}
	unwatch, err = currentWatch.Unwatch(ctx, route.Epoch)
	if err != nil {
		t.Fatal("explicit reconnected UNWATCH", unwatch, err)
	}
	checkReply(unwatch, route)
	watchMayExist, unwatched = false, true
	if _, err := currentWatch.Next(ctx); !errors.Is(err, cephmsgr.ErrClosed) {
		t.Fatal("explicit UNWATCH did not end the new handle", err)
	}
	currentWatch = nil
	removed, err = watchReconnectList(ctx, name)
	if err != nil || len(removed.Entries) != 0 {
		t.Fatal("native watcher registration survived explicit reconnected UNWATCH", removed, err)
	}
	finalNative = native(ctx, name).Actual
	if finalNative.Code != 0 || !bytes.Equal(finalNative.Data, baseline) {
		t.Fatal("watch reconnect/notification changed native object bytes", finalNative)
	}
	t.Logf("native WATCH cookie=%d client=%d survived explicit RECONNECT in %s: registration generation %d/%d, session generation %d/%d, binary notify/ACK and UNWATCH confirmed for %s", oldWatch.Cookie(), globalID, handoffDuration, initial.Operations[0].Watch.Generation, reconnect.Operations[0].Watch.Generation, oldEvent.SessionGeneration, newEvent.SessionGeneration, route.PGID)
}
