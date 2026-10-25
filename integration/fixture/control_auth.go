package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

// managerAuthRejection recognizes an explicit native MonClient refusal in the
// latest ceph-mgr process only. Old refusals from an earlier respawn cannot
// authorize another restart. No Go subject's authentication is retried here.
// Pinned MonClient.cc:1968-1998 and CephxServiceHandler.cc:253-260 explain the
// failure after the MON no longer retains the native daemon's AUTH secret.
// It also returns the latest process-start line, which bounds MON attribution.
func managerAuthRejection(log string) (rejection, start string) {
	started := strings.LastIndex(log, " ceph version ")
	if started < 0 {
		return "", ""
	}
	lineStart := strings.LastIndexByte(log[:started], '\n') + 1
	lines := strings.Split(log[lineStart:], "\n")
	for _, line := range lines {
		if strings.Contains(line, "monclient(hunting): handle_auth_bad_method failed to auth with my available methods: (13) Permission denied") {
			rejection = line
		}
	}
	return rejection, lines[0]
}

func nativeManagerRejection(ctx context.Context, daemon testcontainers.Container) (rejection, start string, err error) {
	logs, err := daemon.Logs(ctx)
	if err != nil {
		return "", "", err
	}
	data, readErr := io.ReadAll(io.LimitReader(logs, (8<<20)+1))
	closeErr := logs.Close()
	if readErr != nil || closeErr != nil {
		return "", "", errors.Join(readErr, closeErr)
	}
	if len(data) > 8<<20 {
		return "", "", errors.New("native MGR preparation log exceeds bound")
	}
	rejection, start = managerAuthRejection(string(data))
	return rejection, start, nil
}

// Pinned src/log/LogClock.h:139-164 prints coarse millisecond or fine
// microsecond local time with a numeric zone. Docker daemons on one host share
// the kernel clock, so MON and MGR entries are comparable instants.
var cephLogEntry = regexp.MustCompile(`^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.(?:\d{3}|\d{6})[+-]\d{4}) [0-9a-f]+ +-?\d+ (.*)$`)

func cephLogTime(stamp string) (time.Time, error) {
	layout := "2006-01-02T15:04:05.000-0700"
	if len(stamp) == len("2006-01-02T15:04:05.000000-0700") {
		layout = "2006-01-02T15:04:05.000000-0700"
	}
	return time.Parse(layout, stamp)
}

// cephLogEntryTime parses the leading timestamp of one native log entry.
func cephLogEntryTime(line string) (time.Time, string, error) {
	entry := cephLogEntry.FindStringSubmatch(strings.TrimSuffix(line, "\r"))
	if entry == nil {
		return time.Time{}, "", errors.New("line is not a timestamped native Ceph log entry")
	}
	stamp, err := cephLogTime(entry[1])
	return stamp, entry[2], err
}

// monitorReclaimLogLimit bounds candidate refusal evidence from each MON.
// Unrelated debug history is streamed without being retained by the filter.
const monitorReclaimLogLimit = 32 << 20

// The three refusals in pinned CephxServiceHandler.cc:111-113, 126-128 and
// 141-143 share this dout prefix (line 30) and name the global_id whose
// reclaim the MON refused with -EACCES.
var monitorReclaimSuffixes = []string{" without presenting ticket", " using bad ticket", " using mismatching ticket"}

type countedReader struct {
	r io.Reader
	n int64
}

func (c *countedReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// scanMonitorReclaimRefusals returns, sorted and unique, the global IDs one MON
// log refused to reclaim for mgr.<name> at or after since. A matching line that
// is not an exact native entry is an explicit error rather than silent absence.
func scanMonitorReclaimRefusals(r io.Reader, name string, since time.Time, limit int) ([]uint64, error) {
	if name == "" || limit <= 0 || since.IsZero() {
		return nil, errors.New("invalid native MON reclaim attribution scope")
	}
	prefix := "cephx server mgr." + name + ":  attempt to reclaim global_id "
	counted := &countedReader{r: io.LimitReader(r, int64(limit)+1)}
	scanner := bufio.NewScanner(counted)
	scanner.Buffer(make([]byte, 0, 64<<10), limit+1)
	found := make(map[uint64]bool)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.Contains(line, prefix) {
			continue
		}
		stamp, message, err := cephLogEntryTime(line)
		if err != nil || !strings.HasPrefix(message, prefix) {
			return nil, fmt.Errorf("malformed native MON reclaim refusal for mgr.%s: %q", name, line)
		}
		if stamp.Before(since) {
			continue
		}
		value, suffix, _ := strings.Cut(message[len(prefix):], " ")
		suffix = " " + suffix
		id, err := strconv.ParseUint(value, 10, 64)
		if err != nil || id == 0 || value[0] == '0' || !slices.Contains(monitorReclaimSuffixes, suffix) {
			return nil, fmt.Errorf("malformed native MON reclaim global_id for mgr.%s: %q", name, line)
		}
		found[id] = true
	}
	if counted.n > int64(limit) {
		return nil, fmt.Errorf("native MON log exceeds %d-byte reclaim attribution bound", limit)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return slices.Sorted(maps.Keys(found)), nil
}

type monitorLogSource struct {
	name string
	logs func(context.Context) (io.ReadCloser, error)
}

// monitorReclaimRefusals unions every owned MON's attribution. A missing MON
// set is an error: no observation must never read as "nothing refused".
func monitorReclaimRefusals(ctx context.Context, monitors []monitorLogSource, name string, since time.Time) ([]uint64, error) {
	if len(monitors) == 0 {
		return nil, errors.New("no owned MON logs for native MGR refusal attribution")
	}
	found := make(map[uint64]bool)
	for _, mon := range monitors {
		logs, err := mon.logs(ctx)
		if err != nil {
			return nil, fmt.Errorf("read mon.%s log for native MGR refusal attribution: %w", mon.name, err)
		}
		stopClose := context.AfterFunc(ctx, func() { _ = logs.Close() })
		ids, scanErr := scanMonitorReclaimRefusals(newMonitorReclaimLogReader(ctx, logs, name), name, since, monitorReclaimLogLimit)
		stopClose()
		if err := errors.Join(scanErr, logs.Close()); err != nil {
			return nil, fmt.Errorf("mon.%s native MGR refusal attribution: %w", mon.name, err)
		}
		for _, id := range ids {
			found[id] = true
		}
	}
	return slices.Sorted(maps.Keys(found)), nil
}

// nativeManagerRefusal reads one owned MGR's latest-process MonClient -13 line
// and attributes it to MonClient global IDs using MON evidence from the same
// process only. With scanAlways the MONs are read even without a -13 line.
func (c *fixtureCluster) nativeManagerRefusal(ctx context.Context, name string, daemon testcontainers.Container, scanAlways bool) (managerRefusal, error) {
	line, start, err := nativeManagerRejection(ctx, daemon)
	if err != nil || line == "" && !scanAlways {
		return managerRefusal{Line: line}, err
	}
	started, _, err := cephLogEntryTime(start)
	if err != nil {
		return managerRefusal{}, fmt.Errorf("native mgr.%s process start %q: %w", name, start, err)
	}
	var monitors []monitorLogSource
	for _, mon := range c.cluster.Monitors() {
		monitors = append(monitors, monitorLogSource{name: mon.DaemonName, logs: mon.Logs})
	}
	refused, err := monitorReclaimRefusals(ctx, monitors, name, started)
	if err != nil {
		return managerRefusal{}, err
	}
	return managerRefusal{Line: line, Refused: refused}, nil
}

func (c *fixtureCluster) controlManagerAuthRequest(ctx context.Context, out string) error {
	var fields []string
	var err error
	fields, err = c.request(out, "verify-mgr-auth")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || c.profile.nativeMgrPreparation != "1" {
			return errors.New("native MGR rejection observation is restricted to its opt-in acceptance fixture")
		}
		daemon, err := c.daemon("mgr", fields[1])
		if err != nil {
			return err
		}
		status, err := c.cluster.ManagerStatus(ctx)
		if err != nil {
			return err
		}
		rejection, _, err := nativeManagerRejection(ctx, daemon)
		if err != nil {
			return err
		}
		present := status.ActiveName == fields[1]
		for _, standby := range status.Standbys {
			present = present || standby.Name == fields[1]
		}
		data, err := json.Marshal(map[string]any{"daemon": "mgr." + fields[1], "missing": !present, "native_rejection": rejection, "status": status})
		if err != nil {
			return err
		}
		if err = os.WriteFile(filepath.Join(out, "manager-native-auth.json"), data, 0644); err != nil {
			return err
		}
		if err = publishAck(out, "mgr-auth-verify."+fields[0]); err != nil {
			return err
		}
	}
	return nil
}

func (c *fixtureCluster) controlManagerKeyringRequest(ctx context.Context, out string) error {
	var fields []string
	var err error
	fields, err = c.request(out, "verify-mgr-keyring")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 || !shortTicketGateLabels[fields[1]] || c.profile.shortTickets != "1" {
			return errors.New("native MGR keyring gate is restricted to the short-ticket fixture")
		}
		if err = c.verifyShortTicketGate(ctx, out, fields[1]); err != nil {
			return err
		}
		if err = publishAck(out, "mgr-keyring-verify."+fields[0]); err != nil {
			return err
		}
	}
	return nil
}

func (c *fixtureCluster) controlAuthBundleRequest(ctx context.Context, out string) error {
	var fields []string
	var err error
	fields, err = c.request(out, "collect-auth-bundle")
	if err != nil {
		return err
	}
	if fields != nil {
		if len(fields) != 2 {
			return errors.New("invalid auth first-failure bundle request")
		}
		if err = c.collectAuthBundleControl(ctx, out, fields[0], fields[1]); err != nil {
			return err
		}
		if err = publishAck(out, "auth-bundle-collect."+fields[0]); err != nil {
			return err
		}
	}
	return nil
}
