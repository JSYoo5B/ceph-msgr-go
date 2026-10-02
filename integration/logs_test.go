package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
)

func ordinaryLogFixture(t *testing.T) string {
	t.Helper()
	control := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if control == "" || os.Getenv("CEPH_MSGR_TEST_MGR_COUNT") != "2" || os.Getenv("CEPH_MSGR_TEST_MAPPED_IPV6") == "1" {
		t.Skip("requires the ordinary two-MGR disposable fixture")
	}
	for _, name := range []string{"CEPH_MSGR_TEST_EXPIRE_TICKETS", "CEPH_MSGR_TEST_IDLE_SESSIONS", "CEPH_MSGR_TEST_AUTH_EPOCH", "CEPH_MSGR_TEST_SHORT_TICKETS"} {
		if os.Getenv(name) == "1" {
			t.Skip("requires ordinary secure authentication")
		}
	}
	if os.Getenv("CEPH_MSGR_TEST_MODE_REJECTION") != "" {
		t.Skip("requires secure MON and MGR listeners")
	}
	return control
}

func publishLogOnce(t *testing.T, ctx context.Context, c *cephmsgr.Client, text string) {
	t.Helper()
	command, err := json.Marshal(map[string]any{"prefix": "log", "logtext": []string{text}})
	if err != nil {
		t.Fatal(err)
	}
	// A diagnostic log append is a mutation. Submit once; no uncertain retry.
	result, err := c.MonCommand(ctx, cephmsgr.Command{JSON: command})
	if err != nil || result.Code != 0 {
		t.Fatal("once-only fixture log append", result.Code, err)
	}
}

func waitLogEntry(t *testing.T, ctx context.Context, stream *cephmsgr.LogStream, text string) (cephmsgr.LogBatch, cephmsgr.LogEntry) {
	t.Helper()
	for {
		batch, err := stream.Next(ctx)
		if err != nil {
			t.Fatal("receive expected fixture log", err)
		}
		for _, entry := range batch.Entries {
			if entry.Message == text {
				return batch, entry
			}
		}
	}
}

func TestCephLogWatchIntegration(t *testing.T) {
	control := ordinaryLogFixture(t)
	data, err := os.ReadFile(filepath.Join(control, "log-oracle.json"))
	var oracle struct {
		Sentinel string `json:"sentinel"`
		Entry    struct {
			Name, Rank, Stamp, Channel, Priority, Message string
			Sequence                                      uint64 `json:"seq"`
			Addresses                                     struct {
				Vector []struct {
					Type, Addr string
					Nonce      uint32
				} `json:"addrvec"`
			} `json:"addrs"`
		} `json:"entry"`
	}
	if err != nil || json.Unmarshal(data, &oracle) != nil || !strings.HasPrefix(oracle.Sentinel, "ceph-msgr-log-oracle-") || oracle.Entry.Message != oracle.Sentinel {
		t.Fatal("independent native CLI log oracle failed", err)
	}
	c, ctx := fixtureClient(t, time.Minute)
	watchContext, cancelWatch := context.WithCancel(ctx)
	defer cancelWatch()
	stream, err := c.WatchLogs(watchContext, cephmsgr.LogOptions{StartVersion: 1})
	if err != nil {
		t.Fatal("register log watch", err)
	}
	t.Cleanup(func() { stream.Close() })
	if _, err := c.WatchLogs(ctx, cephmsgr.LogOptions{}); !errors.Is(err, cephmsgr.ErrLogWatchActive) {
		t.Fatal("second watch was admitted", err)
	}
	batch, entry := waitLogEntry(t, ctx, stream, oracle.Sentinel)
	rank := int64(-1)
	if oracle.Entry.Rank != "client.?" {
		rank, err = strconv.ParseInt(strings.TrimPrefix(oracle.Entry.Rank, "client."), 10, 64)
		if err != nil {
			t.Fatal("native rank", err)
		}
	}
	stamp, err := time.Parse("2006-01-02T15:04:05.999999999-0700", oracle.Entry.Stamp)
	if err != nil {
		t.Fatal("native timestamp", err)
	}
	if batch.FSID != c.Snapshot().FSID || batch.Version == 0 || entry.NameType != 8 || entry.NameID != "test" || oracle.Entry.Name != "client.test" || entry.RankType != 8 || entry.RankNumber != rank || entry.Sequence != oracle.Entry.Sequence || entry.Priority != 1 || oracle.Entry.Priority != "[INF]" || entry.Channel != oracle.Entry.Channel || entry.Seconds != uint32(stamp.Unix()) || entry.Nanoseconds/1000 != uint32(stamp.Nanosecond())/1000 {
		t.Fatal("native CLI log identity, time or cursor disagrees", batch.Version, entry)
	}
	// Ceph's CLI stamp displays microseconds. The event retains raw nanoseconds;
	// compare to the CLI's displayed precision without truncating the event.
	if len(entry.Addresses) != len(oracle.Entry.Addresses.Vector) {
		t.Fatal("native address count")
	}
	for i, address := range entry.Addresses {
		native := oracle.Entry.Addresses.Vector[i]
		endpoint, err := netip.ParseAddrPort(native.Addr)
		if err != nil || address.Endpoint != endpoint || address.Nonce != native.Nonce || address.Type != map[string]uint32{"v1": 1, "v2": 2, "any": 3}[native.Type] || address.FlowInfo != 0 || address.ScopeID != 0 {
			t.Fatal("native source address metadata disagrees", address, err)
		}
	}
	localWait, cancelWait := context.WithCancel(ctx)
	cancelWait()
	if _, err := stream.Next(localWait); !errors.Is(err, context.Canceled) {
		t.Fatal("individual Next cancellation", err)
	}
	text := fmt.Sprintf("ceph-msgr-go-log-live-%d", time.Now().UnixNano())
	publishLogOnce(t, ctx, c, text)
	live, got := waitLogEntry(t, ctx, stream, text)
	if live.Version <= batch.Version || got.Sequence != 0 || got.Priority != 1 || got.Channel != "cluster" || got.NameID != "test" {
		t.Fatal("live log did not advance service cursor independently of entry seq", live, got)
	}
	for _, mgr := range []bool{false, true} {
		if err := fixtureRecoveryRead(ctx, c, mgr); err != nil {
			t.Fatal("ordinary command with log watch", mgr, err)
		}
	}
	cancelWatch()
	for i := 0; ; i++ {
		_, err := stream.Next(ctx)
		if errors.Is(err, context.Canceled) {
			break
		}
		if err != nil || i >= 64 {
			t.Fatal("watch cancellation failed to finish accepted queue", i, err)
		}
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	overflow, err := c.WatchLogs(ctx, cephmsgr.LogOptions{StartVersion: live.Version + 1, MaxBufferedBytes: 1024})
	if err != nil {
		t.Fatal("reuse canceled watch slot", err)
	}
	t.Cleanup(func() { overflow.Close() })
	// One entry plus its retained metadata exceeds the explicit 1KiB budget.
	publishLogOnce(t, ctx, c, fmt.Sprintf("ceph-msgr-go-log-overflow-%d", time.Now().UnixNano()))
	if _, err := overflow.Next(ctx); !errors.Is(err, cephmsgr.ErrLogOverflow) {
		t.Fatal("actual log buffer overflow was not explicit", err)
	}
	if err := fixtureRecoveryRead(ctx, c, false); err != nil {
		t.Fatal("MON session after log overflow", err)
	}
	readonlyOptions := integrationOptions(t)
	key, err := os.ReadFile(filepath.Join(control, "readonly.key"))
	if err != nil {
		t.Fatal(err)
	}
	readonlyOptions.Key, err = cephmsgr.ParseKey(strings.TrimSpace(string(key)))
	if err != nil {
		t.Fatal(err)
	}
	readonlyOptions.Identity = "client.readonly"
	readonly, err := cephmsgr.Dial(ctx, readonlyOptions)
	if err != nil {
		t.Fatal("authenticate read-only log viewer", err)
	}
	t.Cleanup(func() { readonly.Close() })
	// Watch registration is local and asynchronous. Start from a known earlier
	// cursor so a late SUBSCRIBE cannot skip this once-only producer entry.
	viewer, err := readonly.WatchLogs(ctx, cephmsgr.LogOptions{StartVersion: live.Version + 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { viewer.Close() })
	viewText := fmt.Sprintf("ceph-msgr-readonly-log-view-%d", time.Now().UnixNano())
	publishLogOnce(t, ctx, c, viewText)
	viewBatch, viewEntry := waitLogEntry(t, ctx, viewer, viewText)
	if viewBatch.FSID != c.Snapshot().FSID || viewEntry.NameID != "test" || readonly.Snapshot().AuthRejection != nil {
		t.Fatal("read-only log viewer lost producer identity or authentication", viewBatch, viewEntry)
	}
	if err := fixtureRecoveryRead(ctx, readonly, false); err != nil {
		t.Fatal("read-only command alongside actual log delivery", err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.WatchLogs(ctx, cephmsgr.LogOptions{}); !errors.Is(err, cephmsgr.ErrClosed) {
		t.Fatal("watch after client Close", err)
	}
	t.Log("native CLI log fields, source address, raw timestamp, service cursor, local wait/watch cancellation, buffer overflow and ordinary commands passed")
}
