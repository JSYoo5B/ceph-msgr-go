package cephmsgr

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/session"
)

// serviceMapLargeSelector exclusively selects the large-map cell.
const serviceMapLargeSelector = "^TestCephMonServiceMapLargeMapIntegration$"

const (
	// serviceMapLargeBudget is below the large native map and far above the
	// fixture's ordinary ServiceMap.
	serviceMapLargeBudget = 256 << 10
	// serviceMapLargeFrameLimit is below the large native map and above every
	// other frame of the fixture (MonMap, MgrMap, tickets).
	serviceMapLargeFrameLimit = 1 << 20
)

// A native holder registers about 1.55 MiB of metadata, so the MON publishes a
// ServiceMap larger than 1 MiB. Three Go clients watch it:
//
//   - default limits: the map is admitted, and its daemon GID and every
//     metadata value equal the native registration and the MON's own dump;
//   - a 256 KiB watch budget: the watch alone ends with ErrServiceMapOverflow
//     after the maps it had accepted, while its client keeps working and a
//     new watch with the default budget receives the same large map;
//   - a 1 MiB MaxFrameSize: the MON session that carries the frame fails with
//     the limit error, which becomes the watch's cause; the client recovers
//     and, once the large registration is gone, a new watch works again.
//
// A second native holder then registers 65537 metadata keys, one more than
// the decoder admits in a collection. The native map is valid and smaller
// than the default frame limit, yet the default-limit watch ends with the
// limit error of its failed session; the client recovers and, once that
// registration is gone, a new watch works again.
//
// It runs only when exclusively selected: these maps would otherwise reach
// every ServiceMap watch of a broad suite.
func TestCephMonServiceMapLargeMapIntegration(t *testing.T) {
	if selected := flag.Lookup("test.run"); selected == nil || selected.Value.String() != serviceMapLargeSelector {
		t.Skip("large ServiceMap cell requires its exclusive selector " + serviceMapLargeSelector)
	}
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if control == "" {
		t.Skip("large ServiceMap cell requires the disposable fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()
	dial := func(configure func(*Options)) *Client {
		t.Helper()
		options := integrationOptions(t)
		configure(&options)
		c, err := Dial(ctx, options)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		return c
	}
	open := func(c *Client, options ServiceMapOptions) *ServiceMapStream {
		t.Helper()
		watch, err := c.WatchServiceMap(ctx, options)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { watch.Close() })
		return watch
	}
	admitted := dial(func(*Options) {})
	budgeted := dial(func(*Options) {})
	framed := dial(func(o *Options) { o.MaxFrameSize = serviceMapLargeFrameLimit })
	identities := map[*Client]State{admitted: admitted.Snapshot(), budgeted: budgeted.Snapshot(), framed: framed.Snapshot()}
	admittedWatch := open(admitted, ServiceMapOptions{})
	budgetedWatch := open(budgeted, ServiceMapOptions{MaxBufferedBytes: serviceMapLargeBudget})
	framedWatch := open(framed, ServiceMapOptions{})

	token := serviceLifecycleToken(t)
	daemon := "holder-" + token[:16]
	absent := func(m ServiceMap) bool {
		_, found := m.Services["ceph-msgr"].Daemons[daemon]
		return m.Epoch != 0 && len(m.Raw) != 0 && !found
	}
	// Every watch first holds an ordinary map. The budgeted watch keeps it
	// unread: it must still be delivered after the overflow.
	baseline := waitServiceLifecycleMap(t, ctx, admittedWatch, absent)
	framedBaseline := waitServiceLifecycleMap(t, ctx, framedWatch, absent)
	for {
		budgeted.mu.Lock()
		pending := budgetedWatch.pending != nil
		budgeted.mu.Unlock()
		if pending {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("budgeted watch received no ordinary map", ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
	if len(baseline.Raw)+512 > serviceMapLargeBudget {
		t.Fatal("the fixture's ordinary ServiceMap already exceeds the cell's budget", len(baseline.Raw))
	}
	budgeted.mu.Lock()
	budgetedSource := budgetedWatch.watch.source
	budgeted.mu.Unlock()
	framed.mu.Lock()
	framedSource := framedWatch.watch.source
	framed.mu.Unlock()
	if budgetedSource == nil || framedSource == nil {
		t.Fatal("limited watches have no registered MON source")
	}

	wideToken := serviceLifecycleToken(t)
	for _, owned := range []string{token, wideToken} {
		t.Cleanup(func() {
			cleanup, stop := context.WithTimeout(context.Background(), 90*time.Second)
			defer stop()
			r, err := observeNativeServiceHolder(cleanup, control, owned, "cleanup")
			if err != nil {
				t.Error("native holder cleanup", err)
				return
			}
			if r.Holder != nil && r.Holder.Alive && !r.Killed {
				t.Error("native holder survived cleanup")
			}
			t.Logf("holder cleanup state=%s killed=%t restored=%v", r.State, r.Killed, r.Restored != nil && *r.Restored)
		})
	}
	start, err := observeNativeServiceHolder(ctx, control, token, "start-large")
	if err != nil {
		t.Fatal("start large native holder", err)
	}
	ready, err := serviceHolderReady(start)
	if err != nil {
		t.Fatal(err)
	}
	padding := serviceHolderPadding(token, "start-large")
	registered := summarizeServiceHolderPadding(padding)
	native := waitServiceHolder(t, ctx, control, token, "large registration", func(r nativeServiceHolderReceipt) bool {
		return serviceHolderPresent(r, ready) && r.Holder != nil && r.Holder.Alive
	})
	if ready.serviceHolderPadSummary != registered || native.View.Entry.serviceHolderPadSummary != registered {
		t.Fatal("native registration or dump differs from the recomputed padding", ready.serviceHolderPadSummary, native.View.Entry.serviceHolderPadSummary, registered)
	}

	// Default limits: admitted with the native identity and raw metadata.
	holds := func(m ServiceMap) bool { return serviceLifecyclePresent(m, native, ready) }
	large := waitServiceLifecycleMap(t, ctx, admittedWatch, holds)
	requireServiceMapLargeDaemon(t, large, daemon, ready, padding)
	if len(large.Raw) <= serviceMapLargeFrameLimit || len(large.Raw)+512 <= serviceMapLargeBudget || len(large.Raw) <= registered.Bytes {
		t.Fatal("native map is not above the cell's limits", len(large.Raw), registered.Bytes)
	}
	t.Logf("admitted large map: Go epoch=%d native epoch=%d GID=%d raw=%d bytes padding keys=%d bytes=%d sha256=%s (Go decode, native dump and holder registration agree)",
		large.Epoch, *native.View.Epoch, ready.GID, len(large.Raw), registered.Keys, registered.Bytes, registered.SHA256)

	// Watch budget: accepted maps first, then the stable overflow.
	drained := 0
	var overflow error
	for overflow == nil {
		m, err := budgetedWatch.Next(ctx)
		if err != nil {
			overflow = err
			break
		}
		if !absent(m) || len(m.Raw)+512 > serviceMapLargeBudget {
			t.Fatal("budgeted watch delivered a map it could not retain", m.Epoch, len(m.Raw))
		}
		drained++
	}
	if again, err := budgetedWatch.Next(ctx); overflow != ErrServiceMapOverflow || err != ErrServiceMapOverflow || again.Raw != nil || drained == 0 {
		t.Fatal("budget overflow was not the stable cause after the accepted maps", overflow, err, drained)
	}
	// Ticket renewal may have retired the source meanwhile; it must not have failed.
	if err := budgetedSource.Err(); err != nil && !errors.Is(err, session.ErrRetired) {
		t.Fatal("watch budget overflow failed the shared MON session", err)
	}
	requireServiceMapLargeClient(t, ctx, budgeted, identities[budgeted], "after the watch budget overflow")
	budgetedAgain := open(budgeted, ServiceMapOptions{})
	again := waitServiceLifecycleMap(t, ctx, budgetedAgain, holds)
	requireServiceMapLargeDaemon(t, again, daemon, ready, padding)
	budgetedAgain.Close()
	t.Logf("watch budget %d: drained %d accepted map(s), then %v; client GID=%d kept, new default-budget watch read epoch=%d raw=%d",
		serviceMapLargeBudget, drained, overflow, identities[budgeted].GlobalID, again.Epoch, len(again.Raw))

	// Frame limit: the carrying session fails; the watch reports its cause.
	var limit error
	for limit == nil {
		m, err := framedWatch.Next(ctx)
		if err != nil {
			limit = err
			break
		}
		if !absent(m) {
			t.Fatal("frame-limited watch delivered the oversized map", m.Epoch, len(m.Raw))
		}
	}
	if _, err := framedWatch.Next(ctx); !errors.Is(limit, ErrLimitExceeded) || errors.Is(limit, ErrServiceMapOverflow) || err != limit {
		t.Fatal("frame limit was not the stable session cause", limit, err)
	}
	requireServiceMapLargeClient(t, ctx, framed, identities[framed], "after the frame limit")
	t.Logf("frame limit %d: watch ended with %v after baseline epoch=%d; source session error=%v; client GID=%d recovered",
		serviceMapLargeFrameLimit, limit, framedBaseline.Epoch, framedSource.Err(), identities[framed].GlobalID)

	// Removal: the admitted watch follows it, and the frame-limited client
	// can watch again once the map fits its frames.
	released, err := observeNativeServiceHolder(ctx, control, token, "release")
	if err != nil || released.State != "released" || released.Exited == nil || released.Exited.Reason != "released" || !released.Exited.ShutdownReturned {
		t.Fatal("native holder release did not return a released shutdown", err)
	}
	absenceCtx, stopAbsence := context.WithTimeout(ctx, 120*time.Second)
	defer stopAbsence()
	var since time.Time
	gone := waitServiceHolder(t, absenceCtx, control, token, "stable native removal", func(r nativeServiceHolderReceipt) bool {
		if !serviceHolderAbsent(r) {
			since = time.Time{}
			return false
		}
		if since.IsZero() {
			since = time.Now()
		}
		return time.Since(since) >= 2*time.Second
	})
	removed := waitServiceLifecycleMap(t, absenceCtx, admittedWatch, func(m ServiceMap) bool {
		return serviceLifecycleAbsent(m, large.Epoch, daemon)
	})
	framedAgain := open(framed, ServiceMapOptions{})
	small := waitServiceLifecycleMap(t, absenceCtx, framedAgain, func(m ServiceMap) bool {
		return serviceLifecycleAbsent(m, large.Epoch, daemon)
	})
	framedAgain.Close()
	if r, err := observeNativeServiceHolder(ctx, control, token, "cleanup"); err != nil || !serviceHolderRestored(r) || r.Holder == nil || r.Holder.Alive {
		t.Fatal("released holder baseline was not restored without a kill", err)
	}
	for c, before := range identities {
		requireServiceMapLargeClient(t, ctx, c, before, "after removal")
	}
	t.Logf("removal: Go epoch=%d raw=%d native epoch=%d; frame-limited client's new watch read epoch=%d raw=%d",
		removed.Epoch, len(removed.Raw), *gone.View.Epoch, small.Epoch, len(small.Raw))

	// Collection cap: a valid native map with 65537 metadata entries in one
	// daemon. Only the admitted client still watches, with default limits.
	wideDaemon := "holder-" + wideToken[:16]
	wideStart, err := observeNativeServiceHolder(ctx, control, wideToken, "start-wide")
	if err != nil {
		t.Fatal("start wide native holder", err)
	}
	wideReady, err := serviceHolderReady(wideStart)
	if err != nil {
		t.Fatal(err)
	}
	wideNative := waitServiceHolder(t, ctx, control, wideToken, "wide registration", func(r nativeServiceHolderReceipt) bool {
		return serviceHolderPresent(r, wideReady) && r.Holder != nil && r.Holder.Alive
	})
	if wideReady.Keys != 1<<16+1 || wideNative.View.Entry.Keys != wideReady.Keys {
		t.Fatal("native registration is not one entry above the collection cap", wideReady.Keys, wideNative.View.Entry.Keys)
	}
	var capped error
	for capped == nil {
		m, err := admittedWatch.Next(ctx)
		if err != nil {
			capped = err
			break
		}
		if _, found := m.Services["ceph-msgr"].Daemons[wideDaemon]; found {
			t.Fatal("watch delivered a map above the collection cap", m.Epoch, len(m.Raw))
		}
	}
	if _, err := admittedWatch.Next(ctx); !errors.Is(capped, ErrLimitExceeded) || !errors.Is(capped, ErrMalformedMessage) || errors.Is(capped, ErrServiceMapOverflow) || err != capped {
		t.Fatal("collection cap was not the stable session cause", capped, err)
	}
	settleServiceMapLargeSession(admitted)
	requireServiceMapLargeClient(t, ctx, admitted, identities[admitted], "after the collection cap")
	t.Logf("collection cap: native epoch=%d GID=%d metadata keys=%d bytes=%d (valid native map); default-limit watch ended with %v; client GID=%d recovered",
		*wideNative.View.Epoch, wideReady.GID, wideNative.View.Entry.Keys, wideNative.View.Entry.Bytes, capped, identities[admitted].GlobalID)
	wideReleased, err := observeNativeServiceHolder(ctx, control, wideToken, "release")
	if err != nil || wideReleased.State != "released" || wideReleased.Exited == nil || wideReleased.Exited.Reason != "released" || !wideReleased.Exited.ShutdownReturned {
		t.Fatal("wide native holder release did not return a released shutdown", err)
	}
	wideCtx, stopWide := context.WithTimeout(ctx, 120*time.Second)
	defer stopWide()
	since = time.Time{}
	wideGone := waitServiceHolder(t, wideCtx, control, wideToken, "stable native removal of the wide registration", func(r nativeServiceHolderReceipt) bool {
		if !serviceHolderAbsent(r) {
			since = time.Time{}
			return false
		}
		if since.IsZero() {
			since = time.Now()
		}
		return time.Since(since) >= 2*time.Second
	})
	// The watch opens only after native removal, so the MON's current map
	// is already below the cap.
	final := waitServiceLifecycleMap(t, wideCtx, open(admitted, ServiceMapOptions{}), func(m ServiceMap) bool {
		return serviceLifecycleAbsent(m, removed.Epoch, daemon, wideDaemon)
	})
	if r, err := observeNativeServiceHolder(ctx, control, wideToken, "cleanup"); err != nil || !serviceHolderRestored(r) || r.Holder == nil || r.Holder.Alive {
		t.Fatal("released wide holder baseline was not restored without a kill", err)
	}
	for c, before := range identities {
		requireServiceMapLargeClient(t, ctx, c, before, "after the wide removal")
	}
	t.Logf("wide removal: native epoch=%d; new default-limit watch read epoch=%d raw=%d", *wideGone.View.Epoch, final.Epoch, len(final.Raw))
}

// settleServiceMapLargeSession waits for the MON session that carried an
// undecodable map to fail. The decoder's refusal ends the watch first and
// fails that session immediately afterwards; a command issued in between
// would be admitted to the failing session and end as an unknown outcome.
// The bound only covers a session that was already replaced.
func settleServiceMapLargeSession(c *Client) {
	c.mu.Lock()
	current := c.mon
	c.mu.Unlock()
	if current == nil {
		return
	}
	select {
	case <-current.Done():
	case <-time.After(2 * time.Second):
	}
}

// requireServiceMapLargeDaemon checks the owned daemon of a delivered map
// against the holder's GID and the independently recomputed padding, and
// that the retained raw map decodes to the same daemon on its own.
func requireServiceMapLargeDaemon(t *testing.T, m ServiceMap, daemon string, ready nativeServiceHolderReady, padding map[string]string) {
	t.Helper()
	d := m.Services["ceph-msgr"].Daemons[daemon]
	if d.GID != ready.GID || summarizeServiceHolderPadding(d.Metadata) != ready.serviceHolderPadSummary {
		t.Fatal("Go map differs from the native registration", d.GID, ready.GID, summarizeServiceHolderPadding(d.Metadata))
	}
	for key, value := range padding {
		if d.Metadata[key] != value {
			t.Fatal("raw metadata value changed", key)
		}
	}
	decoded, err := msgr.DecodeServiceMap(msgr.MessageData{Type: msgr.ServiceMapMessage, Front: m.Raw}, 0)
	raw := decoded.Services["ceph-msgr"].Daemons[daemon]
	if err != nil || decoded.Epoch != m.Epoch || raw.GID != d.GID || summarizeServiceHolderPadding(raw.Metadata) != ready.serviceHolderPadSummary || !bytes.Equal(decoded.Raw, m.Raw) {
		t.Fatal("retained raw map does not hold the delivered daemon", err)
	}
}

// requireServiceMapLargeClient checks that a client kept its identity, was
// not rejected and still completes a command on an admitted MON session.
// The command is not "status": its reply embeds the whole ServiceMap and
// would itself exceed the frame-limited client's MaxFrameSize.
func requireServiceMapLargeClient(t *testing.T, ctx context.Context, c *Client, before State, phase string) {
	t.Helper()
	result, err := c.MonCommand(ctx, Command{JSON: []byte(`{"prefix":"mon dump","format":"json"}`)})
	after := c.Snapshot()
	if err != nil || !json.Valid(result.Data) || after.GlobalID != before.GlobalID || after.FSID != before.FSID || after.AuthRejection != nil {
		t.Fatal("client is not usable with its original identity", phase, err, before.GlobalID, after.GlobalID, after.AuthRejection)
	}
}
