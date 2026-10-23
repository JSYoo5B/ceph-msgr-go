package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// The interrupted-writeback/fencing recipe (issue #10 checkpoint 2) shares the
// DATA scope and adds closed controls, admitted only for its explicit
// selector:
//
//   - osd-drop-on/osd-drop-off: a daemon-local override of the single owned
//     OSD, debug_ms=10/10 then ms_blackhole_client=true, read back from its
//     admin socket and restored (blackhole first) with readback. At most two
//     drops are issued per run, one at a time: the second only after the
//     first is restored. Native
//     Connection::is_blackhole applies ms_blackhole_client to client peers
//     only, and ProtocolV2::handle_message drops such a message after
//     Messenger receipt and before dispatch, logging it at debug_ms 10. MDS,
//     MON and MGR peers of the OSD are unaffected.
//   - osd-log <gid> <nonce>: the attributed drop evidence after the mark taken
//     when the drop was enabled. At that point an MOSDOp is still undecoded
//     (MOSDOp::print shows "osd_op()"), so evidence is the same-thread pair
//     of the "received message ... from=client.<gid> type=42" line and the
//     blackhole line, both naming the registered peer address nonce.
//   - evict <gid>: one admin-socket `session evict` of a registered owned Go
//     session with mds_session_blocklist_on_evict=true, waiting until the
//     session is gone and its exact address is blocklisted. The entry is
//     removed by cleanup as an owned-session entry.
//   - reader-start/reader-status/reader-wait: one background native reader
//     of shared-file (the read-shared driver action), started while no OSD
//     drop is active. LibCephFS checks pool permission with its own OSD
//     operations before it waits for capabilities (Client::check_pool_perm),
//     so those must not meet a drop; its data reads wait for the Go writer to
//     release FILE_BUFFER. An OSD drop may follow while the reader waits, and
//     the reader's result is collected only after that drop is restored.
//     Status never blocks; wait returns the reader's terminal result.
//     Control requests stay one at a time while the reader runs.
//
// References at mdsFreshSource (LGPL-2.1, no code copied): msg/Connection.cc
// is_blackhole, msg/async/ProtocolV2.cc handle_message and _conn_prefix,
// messages/MOSDOp.h print, osd/PrimaryLogPG.cc do_op (EBLOCKLISTED),
// mds/MDSRank.cc evict_client and mds/Server.cc kill_session.
const mdsCephFSWritebackSelector = "^TestCephMDSCephFSWritebackFenceIntegration$"

var mdsCephFSDataOSDConfigNames = []string{"debug_ms", "ms_blackhole_client"}

// mdsCephFSDataOSDDropLimit bounds the OSD drops of one run.
const mdsCephFSDataOSDDropLimit = 2

// mdsCephFSDataReaderTimeout bounds the background reader's native process.
const mdsCephFSDataReaderTimeout = 75 * time.Second

type mdsCephFSDataOSDDrop struct {
	OSD              string            `json:"osd"`
	RequestID        int64             `json:"request_id"`
	State            string            `json:"state"`
	Settings         map[string]string `json:"settings"`
	Baseline         map[string]string `json:"baseline"`
	Readback         map[string]string `json:"readback,omitempty"`
	LogMark          int               `json:"log_mark"`
	RestoreRequestID int64             `json:"restore_request_id,omitempty"`
	RestoreReadback  map[string]string `json:"restore_readback,omitempty"`
	Error            string            `json:"error,omitempty"`
}

func (d *mdsCephFSDataOSDDrop) active() bool {
	return d != nil && (d.State == "intended" || d.State == "applied" || d.State == "uncertain" || d.State == "restore-intended" || d.State == "restore-uncertain")
}

// mdsCephFSDataOSDDropLog is the attributed drop evidence returned by osd-log.
type mdsCephFSDataOSDDropLog struct {
	Mark     int      `json:"mark"`
	Total    int      `json:"total"`
	Lines    []string `json:"lines"`
	Dropped  int      `json:"dropped"`
	GlobalID uint64   `json:"global_id"`
	Nonce    uint32   `json:"nonce"`
}

// The single owned OSD of the legacy-v6 DATA fixture, by its exact name.
func (c *fixtureCluster) mdsCephFSDataOSD() (*ceph.OSDContainer, string, error) {
	var owned []*ceph.OSDContainer
	for _, d := range c.cluster.OSDs() {
		if d != nil && d.Container != nil && d.GetContainerID() != "" {
			owned = append(owned, d)
		}
	}
	if len(owned) != 1 {
		return nil, "", errors.New("DATA OSD drop requires exactly one owned OSD")
	}
	return owned[0], "osd." + strconv.Itoa(owned[0].ID), nil
}

func (c *fixtureCluster) mdsCephFSDataOSDConfig(ctx context.Context, daemon string) (map[string]string, error) {
	values := map[string]string{}
	for _, name := range mdsCephFSDataOSDConfigNames {
		call, stop := context.WithTimeout(ctx, 15*time.Second)
		data, err := c.cluster.AdminSocket(call, daemon, "config", "get", name)
		stop()
		if err != nil {
			return nil, err
		}
		value, err := parseMDSCephFSCompletionConfig(data, name)
		if err != nil {
			return nil, err
		}
		values[name] = value
	}
	return values, nil
}

// mdsCephFSDataOSDApply sets values in the given order, each read back.
func (c *fixtureCluster) mdsCephFSDataOSDApply(ctx context.Context, daemon string, order []string, values map[string]string, readback *map[string]string) error {
	*readback = map[string]string{}
	for _, name := range order {
		if !slices.Contains(mdsCephFSDataOSDConfigNames, name) || values[name] == "" {
			return errors.New("DATA OSD setting is outside closed scope")
		}
		call, stop := context.WithTimeout(ctx, 15*time.Second)
		_, err := c.cluster.AdminSocket(call, daemon, "config", "set", name, values[name])
		stop()
		if err != nil {
			return err
		}
		call, stop = context.WithTimeout(ctx, 15*time.Second)
		data, err := c.cluster.AdminSocket(call, daemon, "config", "get", name)
		stop()
		if err != nil {
			return err
		}
		value, err := parseMDSCephFSCompletionConfig(data, name)
		if err != nil {
			return err
		}
		(*readback)[name] = value
		if value != values[name] {
			return fmt.Errorf("DATA OSD reports %s=%s after setting %s", name, value, values[name])
		}
	}
	return nil
}

func (c *fixtureCluster) mdsCephFSDataOSDLogLines(ctx context.Context) ([]string, error) {
	d, _, err := c.mdsCephFSDataOSD()
	if err != nil {
		return nil, err
	}
	reader, err := d.Logs(ctx)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	data, err := io.ReadAll(io.LimitReader(reader, mdsCephFSDataCapLogBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > mdsCephFSDataCapLogBytes {
		return nil, errors.New("DATA OSD log exceeds bounded drop-log input")
	}
	return strings.Split(strings.TrimRight(string(data), "\r\n"), "\n"), nil
}

// newOSDDrop admits one OSD drop of a run before any native change: never
// beside another drop that is not restored, never beyond the run's limit.
func (x *mdsCephFSDataScope) newOSDDrop(id int64, daemon string, baseline map[string]string) (*mdsCephFSDataOSDDrop, error) {
	if x.osdDrop != nil && (x.osdDrop.State != "restored" || len(x.osdDropHistory)+1 >= mdsCephFSDataOSDDropLimit) {
		return nil, errors.New("DATA OSD drop is unrestored or the run's drops are used")
	}
	if x.restart.pending() || x.activeRuntime("client-drop-on", x.current.GID) != nil ||
		x.activeRuntime("log-pause", x.current.GID) != nil || x.activeRuntime("blackhole-on", x.current.GID) != nil {
		return nil, errors.New("DATA OSD drop refused while a restart or MDS client blackhole is in effect")
	}
	if baseline["ms_blackhole_client"] != "false" || baseline["debug_ms"] == "" {
		return nil, errors.New("DATA OSD client transport must be running from a false baseline")
	}
	return &mdsCephFSDataOSDDrop{OSD: daemon, RequestID: id, State: "intended", Settings: map[string]string{"debug_ms": "10/10", "ms_blackhole_client": "true"},
		Baseline: maps.Clone(baseline)}, nil
}

func (c *fixtureCluster) mdsCephFSDataWritebackAct(ctx context.Context, id int64, a mdsCephFSDataAction, result *mdsCephFSDataResult) error {
	x := c.mdsCephFSData
	if c.profile.testRun != mdsCephFSWritebackSelector {
		return errors.New("DATA writeback controls require the explicit writeback-fence selector")
	}
	switch a.name {
	case "osd-drop-on":
		_, daemon, err := c.mdsCephFSDataOSD()
		if err != nil {
			return err
		}
		baseline, err := c.mdsCephFSDataOSDConfig(ctx, daemon)
		if err != nil {
			return err
		}
		d, err := x.newOSDDrop(id, daemon, baseline)
		if err != nil {
			return err
		}
		lines, err := c.mdsCephFSDataOSDLogLines(ctx)
		if err != nil {
			return err
		}
		d.LogMark = len(lines)
		if x.osdDrop != nil {
			x.osdDropHistory = append(x.osdDropHistory, x.osdDrop)
		}
		x.osdDrop, result.OSDDrop = d, d
		if err := c.mdsCephFSDataJournal(); err != nil {
			return err
		}
		// Debug first, so that the first dropped message is logged.
		err = c.mdsCephFSDataOSDApply(ctx, daemon, []string{"debug_ms", "ms_blackhole_client"}, d.Settings, &d.Readback)
		d.State = "applied"
		if err != nil {
			d.State, d.Error = "uncertain", err.Error()
		}
		return errors.Join(err, c.mdsCephFSDataJournal())
	case "osd-drop-off":
		d := x.osdDrop
		if d == nil || d.State != "applied" || d.RestoreRequestID != 0 {
			return errors.New("DATA OSD drop restore requires its applied, unrestored override")
		}
		d.State, d.RestoreRequestID = "restore-intended", id
		result.OSDDrop = d
		if err := c.mdsCephFSDataJournal(); err != nil {
			return err
		}
		err := c.mdsCephFSDataOSDApply(ctx, d.OSD, []string{"ms_blackhole_client", "debug_ms"}, d.Baseline, &d.RestoreReadback)
		d.State = "restored"
		if err != nil {
			d.State, d.Error = "restore-uncertain", err.Error()
		}
		return errors.Join(err, c.mdsCephFSDataJournal())
	case "osd-log":
		d := x.osdDrop
		if d == nil || d.State != "applied" && d.State != "restored" || !x.capLogIdentity(a.gid, a.nonce) {
			return errors.New("DATA OSD drop log requires its confirmed override and exact registered owned Go identity")
		}
		lines, err := c.mdsCephFSDataOSDLogLines(ctx)
		if err != nil {
			return err
		}
		if len(lines) < d.LogMark {
			return errors.New("DATA OSD log shortened after mark")
		}
		kept, dropped := filterMDSCephFSDataOSDDropLog(lines[d.LogMark:], a.gid, a.nonce)
		result.OSDDrop = d
		result.OSDLog = &mdsCephFSDataOSDDropLog{Mark: d.LogMark, Total: len(lines), Lines: kept, Dropped: dropped, GlobalID: a.gid, Nonce: a.nonce}
		return nil
	case "evict":
		return c.mdsCephFSDataEvict(ctx, id, a.gid, result)
	case "reader-start":
		if err := x.readerReady(); err != nil {
			return err
		}
		r := newMDSCephFSDataReaderRun(id)
		x.reader = r
		go func() {
			// The reader outlives this control request, bounded by its own
			// process timeout.
			ctx, stop := context.WithTimeout(context.Background(), mdsCephFSDataReaderTimeout+15*time.Second)
			defer stop()
			process, native, err := c.mdsCephFSDataRunNativeAdmitted(ctx, "read-shared", "shared-file")
			r.finish(process, native, err)
		}()
		snapshot := r.snapshot()
		result.Reader = &snapshot
		return nil
	case "reader-status":
		if x.reader == nil {
			return errors.New("DATA background reader was not started")
		}
		snapshot := x.reader.snapshot()
		result.Reader = &snapshot
		return nil
	case "reader-wait":
		return x.reader.wait(ctx, id, x.osdDrop.active(), result)
	}
	return errors.New("DATA writeback action is not admitted")
}

// mdsCephFSDataReader is the receipt of the background native reader.
type mdsCephFSDataReader struct {
	RequestID     int64  `json:"request_id"`
	State         string `json:"state"`
	StartedUTC    string `json:"started_utc"`
	ElapsedMillis int64  `json:"elapsed_ms"`
	WaitRequestID int64  `json:"wait_request_id,omitempty"`
	Error         string `json:"error,omitempty"`
}

// mdsCephFSDataReaderRun tracks the reader goroutine. done closes after the
// native process ended and its output was decoded.
type mdsCephFSDataReaderRun struct {
	mu      sync.Mutex
	receipt mdsCephFSDataReader
	started time.Time
	done    chan struct{}
	process *ceph.NativeCommandResult
	native  mdsCephFSDataNative
	err     error
}

func newMDSCephFSDataReaderRun(id int64) *mdsCephFSDataReaderRun {
	now := time.Now()
	return &mdsCephFSDataReaderRun{receipt: mdsCephFSDataReader{RequestID: id, State: "running", StartedUTC: now.UTC().Format(time.RFC3339Nano)}, started: now, done: make(chan struct{})}
}

func (r *mdsCephFSDataReaderRun) running() bool {
	if r == nil {
		return false
	}
	select {
	case <-r.done:
		return false
	default:
		return true
	}
}

func (r *mdsCephFSDataReaderRun) finish(process *ceph.NativeCommandResult, native mdsCephFSDataNative, err error) {
	r.mu.Lock()
	r.process, r.native, r.err = process, native, err
	r.receipt.ElapsedMillis = time.Since(r.started).Milliseconds()
	r.receipt.State = "completed"
	if err != nil {
		r.receipt.State, r.receipt.Error = "failed", err.Error()
	}
	r.mu.Unlock()
	close(r.done)
}

func (r *mdsCephFSDataReaderRun) snapshot() mdsCephFSDataReader {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.receipt
	if s.State == "running" {
		s.ElapsedMillis = time.Since(r.started).Milliseconds()
	}
	return s
}

// readerReady admits the single background reader while no OSD drop is
// active, no restart is pending and no MDS fault is in effect.
func (x *mdsCephFSDataScope) readerReady() error {
	if x.reader != nil {
		return errors.New("DATA background reader was already started")
	}
	if x.restart.pending() || x.activeRuntime("log-pause", x.current.GID) != nil || x.activeRuntime("blackhole-on", x.current.GID) != nil ||
		x.activeRuntime("client-drop-on", x.current.GID) != nil {
		return errors.New("DATA background reader refused while a restart or MDS fault is in effect")
	}
	if x.osdDrop.active() {
		return errors.New("DATA background reader refused while the OSD drops client messages")
	}
	return nil
}

// wait returns the reader's terminal result once, after the OSD drop is
// restored, bounded by the control request.
func (r *mdsCephFSDataReaderRun) wait(ctx context.Context, id int64, dropActive bool, result *mdsCephFSDataResult) error {
	if r == nil {
		return errors.New("DATA background reader was not started")
	}
	if dropActive {
		return errors.New("DATA background reader wait requires the restored OSD drop")
	}
	r.mu.Lock()
	collected := r.receipt.WaitRequestID != 0
	r.mu.Unlock()
	if collected {
		return errors.New("DATA background reader result was already collected")
	}
	select {
	case <-r.done:
	case <-ctx.Done():
		return fmt.Errorf("DATA background reader did not end: %w", ctx.Err())
	}
	r.mu.Lock()
	r.receipt.WaitRequestID = id
	result.Process = r.process
	err := r.err
	if err == nil {
		native := r.native
		result.Native = &native
	}
	r.mu.Unlock()
	snapshot := r.snapshot()
	result.Reader = &snapshot
	return err
}

// awaitCleanup lets a running reader end before cleanup's native work.
func (r *mdsCephFSDataReaderRun) awaitCleanup(ctx context.Context, receipt *mdsCephFSDataCleanup) error {
	if r == nil {
		return nil
	}
	select {
	case <-r.done:
	case <-ctx.Done():
		return errors.New("DATA background reader did not end before cleanup")
	}
	snapshot := r.snapshot()
	receipt.Reader = &snapshot
	return nil
}

var dataOSDLogReceived = regexp.MustCompile(`\.handle_message received message m=0x[0-9a-fA-F]+ seq=[0-9]+ from=client\.([0-9]+) type=42 osd_op\(`)

// filterMDSCephFSDataOSDDropLog keeps same-thread pairs of one received MOSDOp
// from client.<gid> and its blackhole line, both on a connection whose peer
// address (between ">>" and "conn(") carries the registered nonce.
func filterMDSCephFSDataOSDDropLog(lines []string, gid uint64, nonce uint32) ([]string, int) {
	address := regexp.MustCompile(`/` + strconv.FormatUint(uint64(nonce), 10) + `$`)
	peer := func(line string) bool {
		parts := strings.SplitN(line, " >> ", 2)
		if len(parts) != 2 {
			return false
		}
		p := strings.SplitN(parts[1], " conn(", 2)
		return len(p) == 2 && !strings.Contains(p[0], " ") && address.MatchString(p[0])
	}
	received := map[string]string{}
	kept, dropped, groups := []string{}, 0, 0
	for _, raw := range lines {
		line := strings.TrimRight(raw, "\r\n")
		thread := dataCapLogThread.FindStringSubmatch(line)
		if thread == nil {
			continue
		}
		key := thread[1]
		if m := dataOSDLogReceived.FindStringSubmatch(line); m != nil {
			delete(received, key)
			if m[1] != strconv.FormatUint(gid, 10) || !peer(line) {
				continue
			}
			if len(line) > mdsCephFSDataCapLogLineBytes || len(received) >= 64 {
				dropped++
				continue
			}
			received[key] = line
			continue
		}
		if strings.Contains(line, ".handle_message blackhole osd_op(") {
			source, ok := received[key]
			delete(received, key)
			if !ok || !peer(line) {
				continue
			}
			if len(line) > mdsCephFSDataCapLogLineBytes || groups == mdsCephFSDataCapLogGroups {
				dropped++
				continue
			}
			kept = append(kept, source, line)
			groups++
			continue
		}
		if strings.Contains(line, ".handle_message ") {
			// Any other message handled on this thread ends the pairing.
			delete(received, key)
		}
	}
	return kept, dropped
}

// mdsCephFSDataEvict evicts one registered owned Go session once through the
// owned MDS admin socket and waits until it is gone and its exact address is
// blocklisted. It runs only on an adopted, proven incarnation with every
// client blackhole inactive.
func (c *fixtureCluster) mdsCephFSDataEvict(ctx context.Context, id int64, gid uint64, result *mdsCephFSDataResult) error {
	s, x := c.mdsFresh, c.mdsCephFSData
	if err := x.nativeReady(); err != nil {
		return err
	}
	if x.mutated("eviction") {
		return errors.New("DATA eviction was already issued")
	}
	data, err := c.mdsCephFSDataAsok(ctx, "config", "get", "mds_session_blocklist_on_evict")
	if err != nil {
		return err
	}
	if value, err := parseMDSCephFSCompletionConfig(data, "mds_session_blocklist_on_evict"); err != nil || value != "true" {
		return errors.Join(err, errors.New("DATA eviction requires mds_session_blocklist_on_evict=true"))
	}
	sessions, _, err := c.mdsCephFSDataSessions(ctx)
	if err != nil {
		return err
	}
	var victim *mdsSessionsNative
	for i := range sessions {
		if sessions[i].ID == gid {
			victim = &sessions[i]
		}
	}
	if victim == nil || victim.State != "open" && victim.State != "stale" || victim.AuthID != "mds-cephfs-data-"+s.token || x.owned[victim.Address] != gid {
		return errors.New("DATA eviction target is not a registered session of the owned Go identity")
	}
	before, err := c.mdsSessionsBlocklist(ctx)
	if err != nil {
		return err
	}
	if mdsSessionsFindBlock(before, victim.Address) != nil || mdsSessionsFindBlock(x.baselineBlocklist, victim.Address) != nil {
		return errors.New("DATA eviction target address is already blocklisted")
	}
	m := &mdsCephFSMutation{Kind: "eviction", RequestID: id, Target: strconv.FormatUint(gid, 10), State: "intended", GlobalID: gid, Address: victim.Address}
	x.mutations = append(x.mutations, m)
	result.Mutation = m
	if err := c.mdsCephFSDataJournal(); err != nil {
		return err
	}
	output, err := c.mdsCephFSDataAsokTimeout(ctx, 30*time.Second, "session", "evict", "id="+strconv.FormatUint(gid, 10))
	if len(output) != 0 {
		m.Output = strings.TrimSpace(string(output[:min(len(output), 4096)]))
	}
	if err != nil {
		m.State, m.Error = "uncertain", err.Error()
		return errors.Join(err, c.mdsCephFSDataJournal())
	}
	m.State = "requested"
	if err := c.mdsCephFSDataJournal(); err != nil {
		return err
	}
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for {
		sessions, _, err := c.mdsCephFSDataSessions(ctx)
		if err != nil {
			return err
		}
		present := slices.ContainsFunc(sessions, func(n mdsSessionsNative) bool { return n.ID == gid })
		blocks, err := c.mdsSessionsBlocklist(ctx)
		if err != nil {
			return err
		}
		if entry := mdsSessionsFindBlock(blocks, victim.Address); entry != nil && !present {
			m.State, m.Until = "evicted", entry.Until
			result.Sessions, result.Blocklist = sessions, blocks
			return c.mdsCephFSDataJournal()
		}
		select {
		case <-ctx.Done():
			err := fmt.Errorf("DATA eviction did not remove the session and blocklist its address: %w", ctx.Err())
			m.State, m.Error = "uncertain", err.Error()
			return errors.Join(err, c.mdsCephFSDataJournal())
		case <-tick.C:
		}
	}
}

// mdsCephFSDataRestoreOSDDrop restores an override still active at cleanup.
// An OSD is not restarted by this fixture, so there is no discard path.
func (c *fixtureCluster) mdsCephFSDataRestoreOSDDrop(ctx context.Context, receipt *mdsCephFSDataCleanup) error {
	d := c.mdsCephFSData.osdDrop
	receipt.OSDDrop = d
	if d == nil || d.State == "restored" {
		receipt.OSDConfigRestored = true
		return nil
	}
	_, daemon, err := c.mdsCephFSDataOSD()
	if err == nil && daemon != d.OSD {
		err = errors.New("DATA OSD drop cleanup cannot prove its owned OSD")
	}
	if err != nil {
		return err
	}
	d.State = "restore-intended"
	if err := c.mdsCephFSDataJournal(); err != nil {
		return err
	}
	err = c.mdsCephFSDataOSDApply(ctx, daemon, []string{"ms_blackhole_client", "debug_ms"}, d.Baseline, &d.RestoreReadback)
	d.State = "restored"
	if err != nil {
		d.State, d.Error = "restore-uncertain", err.Error()
	}
	receipt.OSDConfigRestored = err == nil
	return errors.Join(err, c.mdsCephFSDataJournal())
}
