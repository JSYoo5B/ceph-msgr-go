package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
)

// Only daemon-local runtime changes are used. Native references at
// mdsFreshSource: MDLog.cc _submit_thread (pause), Locker.cc
// handle_client_caps/_do_snap_update/file_update_finish (journaled FLUSHSNAP
// ACK), DispatchQueue.cc pre_dispatch and MClientCaps.h print (log identity).
// LGPL-2.1 source references; no native code copied. Pausing the journal does
// not pause Messenger/MDSMap processing; no central override survives respawn.
const (
	mdsCephFSDataCapLogBytes     = 16 << 20
	mdsCephFSDataCapLogGroups    = 64
	mdsCephFSDataCapLogLineBytes = 2048
)

var mdsCephFSDataRuntimeConfigNames = []string{"mds_log_pause", "debug_mds", "debug_ms", "ms_blackhole_client"}

type mdsCephFSDataRuntime struct {
	Kind             string            `json:"kind"`
	Settings         map[string]string `json:"settings"`
	Baseline         map[string]string `json:"baseline"`
	IncarnationGID   uint64            `json:"incarnation_gid"`
	RequestID        int64             `json:"request_id"`
	State            string            `json:"state"`
	Readback         map[string]string `json:"readback,omitempty"`
	RestoreRequestID int64             `json:"restore_request_id,omitempty"`
	RestoreReadback  map[string]string `json:"restore_readback,omitempty"`
	Error            string            `json:"error,omitempty"`
}

func (r *mdsCephFSDataRuntime) active() bool {
	return r != nil && (r.State == "intended" || r.State == "applied" || r.State == "uncertain" || r.State == "restore-intended" || r.State == "restore-uncertain")
}
func (x *mdsCephFSDataScope) activeRuntime(kind string, gid uint64) *mdsCephFSDataRuntime {
	for _, r := range x.runtime {
		if r.Kind == kind && r.IncarnationGID == gid && r.active() {
			return r
		}
	}
	return nil
}
func (x *mdsCephFSDataScope) nativeReady() error {
	if x.restart.pending() || x.activeRuntime("log-pause", x.current.GID) != nil || x.activeRuntime("blackhole-on", x.current.GID) != nil ||
		x.activeRuntime("client-drop-on", x.current.GID) != nil {
		return errors.New("MDS CephFS data native action refused while restart, journal pause or client blackhole is in effect")
	}
	return nil
}

// The name is the exact owned container name, never a caller-provided daemon.
func (c *fixtureCluster) mdsCephFSDataDaemon() (*cephfs.MDS, error) {
	for _, d := range c.mdsFresh.fs.MDSs() {
		if d != nil && d.Container != nil && d.GetContainerID() != "" && d.ID == c.mdsCephFSData.current.Name {
			return d, nil
		}
	}
	return nil, errors.New("owned MDS CephFS data daemon container is unavailable")
}
func (c *fixtureCluster) mdsCephFSDataAsok(ctx context.Context, args ...string) ([]byte, error) {
	return c.mdsCephFSDataAsokTimeout(ctx, 15*time.Second, args...)
}
func (c *fixtureCluster) mdsCephFSDataAsokTimeout(ctx context.Context, timeout time.Duration, args ...string) ([]byte, error) {
	d, err := c.mdsCephFSDataDaemon()
	if err != nil {
		return nil, err
	}
	p, err := c.cluster.RunNativeCommand(ctx, d.Container, ceph.NativeCommand{Argv: append([]string{"ceph", "--admin-daemon", "/var/run/ceph/ceph-mds." + d.ID + ".asok"}, args...), Timeout: timeout, MaxStdout: mdsCephFSDataJSONLimit, MaxStderr: 16 << 10, Label: "owned DATA MDS admin socket " + args[0]})
	if err != nil || !p.Complete() || p.ExitCode != 0 {
		return nil, errors.Join(err, errors.New("owned DATA MDS admin socket command did not complete"))
	}
	return bytes.TrimSpace(p.Stdout), nil
}
func (c *fixtureCluster) mdsCephFSDataCheckDaemon(ctx context.Context, d mdsFreshDaemon) error {
	data, err := c.mdsCephFSDataAsok(ctx, "status")
	if err != nil {
		return err
	}
	status, err := parseMDSCephFSCompletionDaemonStatus(data)
	if err != nil {
		return err
	}
	if status.GID != d.GID || status.Rank != 0 || status.FSName != c.mdsFresh.name || status.State != "up:active" {
		return errors.New("DATA admin socket no longer names the proven active incarnation")
	}
	return nil
}
func (c *fixtureCluster) mdsCephFSDataConfig(ctx context.Context, d mdsFreshDaemon) (map[string]string, error) {
	if err := c.mdsCephFSDataCheckDaemon(ctx, d); err != nil {
		return nil, err
	}
	values := map[string]string{}
	for _, name := range mdsCephFSDataRuntimeConfigNames {
		data, err := c.mdsCephFSDataAsok(ctx, "config", "get", name)
		if err != nil {
			return nil, err
		}
		value, err := parseMDSCephFSCompletionConfig(data, name)
		if err != nil {
			return nil, err
		}
		values[name] = value
	}
	return values, c.mdsCephFSDataCheckDaemon(ctx, d)
}
func (c *fixtureCluster) mdsCephFSDataApplyRuntime(ctx context.Context, d mdsFreshDaemon, settings map[string]string, readback *map[string]string) error {
	*readback = map[string]string{}
	for _, name := range slices.Sorted(maps.Keys(settings)) {
		if !slices.Contains(mdsCephFSDataRuntimeConfigNames, name) {
			return errors.New("DATA runtime setting is outside closed scope")
		}
		if err := c.mdsCephFSDataCheckDaemon(ctx, d); err != nil {
			return err
		}
		if _, err := c.mdsCephFSDataAsok(ctx, "config", "set", name, settings[name]); err != nil {
			return err
		}
		data, err := c.mdsCephFSDataAsok(ctx, "config", "get", name)
		if err != nil {
			return err
		}
		value, err := parseMDSCephFSCompletionConfig(data, name)
		if err != nil {
			return err
		}
		(*readback)[name] = value
		if value != settings[name] {
			return fmt.Errorf("DATA daemon reports %s=%s after setting %s", name, value, settings[name])
		}
	}
	return c.mdsCephFSDataCheckDaemon(ctx, d)
}
func (x *mdsCephFSDataScope) newRuntime(id int64, kind string) (*mdsCephFSDataRuntime, error) {
	if x.restart.pending() || len(x.runtime) >= 4 || slices.ContainsFunc(x.runtime, func(r *mdsCephFSDataRuntime) bool { return r.Kind == kind && r.IncarnationGID == x.current.GID }) {
		return nil, errors.New("DATA runtime change was already issued or restart is pending")
	}
	settings := map[string]string{}
	switch kind {
	case "log-pause":
		if x.baselineConfig["mds_log_pause"] != "false" {
			return nil, errors.New("DATA owned journal baseline must be false")
		}
		// The journal-pause recipe needs its own later blackhole-on, which an
		// active (even uncertain) cap-send client drop would refuse.
		if x.activeRuntime("client-drop-on", x.current.GID) != nil {
			return nil, errors.New("DATA journal pause refused while the cap-send client drop is active")
		}
		settings["mds_log_pause"] = "true"
	case "blackhole-on":
		pause := x.activeRuntime("log-pause", x.current.GID)
		if x.baselineConfig["ms_blackhole_client"] != "false" || pause == nil || pause.State != "applied" || pause.Readback["mds_log_pause"] != "true" ||
			x.activeRuntime("client-drop-on", x.current.GID) != nil {
			return nil, errors.New("DATA client blackhole requires a proven owned journal pause and false baseline")
		}
		settings["ms_blackhole_client"] = "true"
	case "client-drop-on":
		// The cap-send recovery recipe drops incoming client messages after
		// Messenger receipt and before dispatch (ProtocolV2::handle_message
		// logs that branch at debug_ms 10). It needs the owned debug change for
		// handler evidence and never combines with the journal-pause recipe.
		debug := x.activeRuntime("debug-on", x.current.GID)
		if x.baselineConfig["ms_blackhole_client"] != "false" || debug == nil || debug.State != "applied" || debug.Readback["debug_mds"] != "10/10" ||
			x.activeRuntime("log-pause", x.current.GID) != nil || x.activeRuntime("blackhole-on", x.current.GID) != nil {
			return nil, errors.New("DATA client drop requires the owned debug change, a false baseline and no journal-pause recipe")
		}
		settings["ms_blackhole_client"], settings["debug_ms"] = "true", "10/10"
	case "debug-on":
		settings["debug_mds"], settings["debug_ms"] = "10/10", "1/5"
	default:
		return nil, errors.New("DATA runtime kind is not admitted")
	}
	baseline := map[string]string{}
	for name := range settings {
		value := x.baselineConfig[name]
		if value == "" {
			return nil, errors.New("DATA runtime baseline unavailable")
		}
		baseline[name] = value
	}
	r := &mdsCephFSDataRuntime{Kind: kind, Settings: settings, Baseline: baseline, IncarnationGID: x.current.GID, RequestID: id, State: "intended"}
	x.runtime = append(x.runtime, r)
	return r, nil
}
func (c *fixtureCluster) mdsCephFSDataRuntimeAct(ctx context.Context, id int64, a mdsCephFSDataAction, result *mdsCephFSDataResult) error {
	x := c.mdsCephFSData
	switch a.name {
	case "debug-on", "log-pause", "blackhole-on", "client-drop-on":
		r, err := x.newRuntime(id, a.name)
		if err != nil {
			return err
		}
		result.Runtime = r
		if err := c.mdsCephFSDataJournal(); err != nil {
			return err
		}
		err = c.mdsCephFSDataApplyRuntime(ctx, x.current, r.Settings, &r.Readback)
		r.State = "applied"
		if err != nil {
			r.State, r.Error = "uncertain", err.Error()
		}
		return errors.Join(err, c.mdsCephFSDataJournal())
	case "flush-journal":
		return c.mdsCephFSDataFlushJournal(ctx, id, result)
	case "debug-off", "log-resume":
		kind := "debug-on"
		if a.name == "log-resume" {
			kind = "log-pause"
			if err := x.canResumeLog(); err != nil {
				return err
			}
		}
		r := x.activeRuntime(kind, x.current.GID)
		if r == nil {
			return errors.New("DATA has no owned runtime change on this incarnation")
		}
		r.State, r.RestoreRequestID = "restore-intended", id
		result.Runtime = r
		if err := c.mdsCephFSDataJournal(); err != nil {
			return err
		}
		err := c.mdsCephFSDataApplyRuntime(ctx, x.current, r.Baseline, &r.RestoreReadback)
		r.State = "restored"
		if err != nil {
			r.State, r.Error = "restore-uncertain", err.Error()
		}
		return errors.Join(err, c.mdsCephFSDataJournal())
	case "cap-log-mark":
		if x.activeRuntime("debug-on", x.current.GID) == nil {
			return errors.New("DATA cap log mark requires its owned debug change")
		}
		lines, err := c.mdsCephFSDataLogLines(ctx)
		if err != nil {
			return err
		}
		x.logMark, x.logMarkGID, x.haveLogMark = len(lines), x.current.GID, true
		result.CapLog = &mdsCephFSCompletionLog{Mark: x.logMark, Total: len(lines), Lines: []string{}}
		return nil
	case "cap-log":
		if !x.haveLogMark || x.logMarkGID != x.current.GID || !x.capLogIdentity(a.gid, a.nonce) {
			return errors.New("DATA cap log requires a mark and exact registered owned Go identity")
		}
		lines, err := c.mdsCephFSDataLogLines(ctx)
		if err != nil {
			return err
		}
		if len(lines) < x.logMark {
			return errors.New("DATA daemon log shortened after mark")
		}
		kept, dropped := filterMDSCephFSDataCapLog(lines[x.logMark:], a.gid, a.nonce)
		result.CapLog = &mdsCephFSCompletionLog{Mark: x.logMark, Total: len(lines), Lines: kept, Dropped: dropped, GlobalID: a.gid, Nonce: a.nonce}
		return nil
	}
	return errors.New("DATA runtime action is not admitted")
}
func (x *mdsCephFSDataScope) capLogIdentity(gid uint64, nonce uint32) bool {
	if gid == 0 || nonce == 0 {
		return false
	}
	suffix := "/" + strconv.FormatUint(uint64(nonce), 10)
	for address, ownedGID := range x.owned {
		if ownedGID == gid && strings.HasSuffix(address, suffix) {
			return true
		}
	}
	return false
}
func (c *fixtureCluster) mdsCephFSDataLogLines(ctx context.Context) ([]string, error) {
	d, err := c.mdsCephFSDataDaemon()
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
		return nil, errors.New("DATA daemon log exceeds bounded cap-log input")
	}
	return strings.Split(strings.TrimRight(string(data), "\r\n"), "\n"), nil
}

// Sessions and status stay on the owned admin socket while client Messenger
// messages are deliberately blackholed; no native tell RPC is used here.
func (c *fixtureCluster) mdsCephFSDataSessions(ctx context.Context) ([]mdsSessionsNative, json.RawMessage, error) {
	data, err := c.mdsCephFSDataAsok(ctx, "session", "ls")
	if err != nil {
		return nil, nil, err
	}
	sessions, err := parseMDSSessionsList(data)
	if err != nil {
		return nil, nil, err
	}
	return sessions, json.RawMessage(data), nil
}
func (x *mdsCephFSDataScope) canResumeLog() error {
	if x.restart.pending() {
		return errors.New("DATA log resume refused during restart")
	}
	pause := x.activeRuntime("log-pause", x.current.GID)
	blackhole := x.activeRuntime("blackhole-on", x.current.GID)
	if pause == nil || pause.State != "applied" || pause.Readback["mds_log_pause"] != "true" || pause.RestoreRequestID != 0 || blackhole == nil || blackhole.State != "applied" || blackhole.Readback["ms_blackhole_client"] != "true" {
		return errors.New("DATA log resume requires its unresumed pause and proven client blackhole")
	}
	return nil
}
func (x *mdsCephFSDataScope) canFlushJournal() error {
	if x.flushJournal != nil || x.restart.pending() {
		return errors.New("DATA journal barrier already issued or restart pending")
	}
	var pause *mdsCephFSDataRuntime
	for _, r := range x.runtime {
		if r.Kind == "log-pause" && r.IncarnationGID == x.current.GID {
			pause = r
		}
	}
	blackhole := x.activeRuntime("blackhole-on", x.current.GID)
	if pause == nil || pause.State != "restored" || pause.RestoreReadback["mds_log_pause"] != "false" || blackhole == nil || blackhole.State != "applied" || blackhole.Readback["ms_blackhole_client"] != "true" {
		return errors.New("DATA journal barrier requires a proven log resume and active client blackhole")
	}
	return nil
}

type mdsCephFSDataFlushJournal struct {
	RequestID      int64                     `json:"request_id"`
	IncarnationGID uint64                    `json:"incarnation_gid"`
	State          string                    `json:"state"`
	Result         *mdsCephFSCompletionFlush `json:"result,omitempty"`
	Error          string                    `json:"error,omitempty"`
}

func decodeMDSCephFSDataFlushJournal(data []byte) (*mdsCephFSCompletionFlush, error) {
	var native struct {
		ReturnCode *int64 `json:"return_code"`
		Message    string `json:"message"`
	}
	if len(data) == 0 || len(data) > 4096 || json.Unmarshal(data, &native) != nil || native.ReturnCode == nil || len(native.Message) > 2048 {
		return nil, errors.New("DATA journal barrier output differs or exceeds its bound")
	}
	result := &mdsCephFSCompletionFlush{ReturnCode: *native.ReturnCode, Message: native.Message}
	if result.ReturnCode != 0 {
		return result, fmt.Errorf("DATA journal barrier returned %d", result.ReturnCode)
	}
	return result, nil
}
func (c *fixtureCluster) mdsCephFSDataFlushJournal(ctx context.Context, id int64, result *mdsCephFSDataResult) error {
	x := c.mdsCephFSData
	if err := x.canFlushJournal(); err != nil {
		return err
	}
	f := &mdsCephFSDataFlushJournal{RequestID: id, IncarnationGID: x.current.GID, State: "intended"}
	x.flushJournal = f
	if err := c.mdsCephFSDataJournal(); err != nil {
		return err
	}
	err := c.mdsCephFSDataCheckDaemon(ctx, x.current)
	if err == nil {
		var data []byte
		data, err = c.mdsCephFSDataAsokTimeout(ctx, 60*time.Second, "flush", "journal")
		if err == nil {
			f.Result, err = decodeMDSCephFSDataFlushJournal(data)
			result.FlushJournal = f.Result
		}
	}
	if err == nil {
		err = c.mdsCephFSDataCheckDaemon(ctx, x.current)
	}
	f.State = "applied"
	if err != nil {
		f.State, f.Error = "uncertain", err.Error()
	}
	return errors.Join(err, c.mdsCephFSDataJournal())
}

var dataCapLogThread = regexp.MustCompile(`(?:^|\s)([0-9a-fA-F]{6,16})\s+-?[0-9]+\s`)
var dataCapLogMessage = regexp.MustCompile(`client_caps\(([a-z_]+) ino (0x[0-9a-fA-F]+|[0-9]+) .*\btid ([0-9]+) .*\bfollows ([0-9a-fA-F]+)\b`)
var dataCapLogHandler = regexp.MustCompile(`handle_client_caps\s+on (0x[0-9a-fA-F]+|[0-9]+) tid ([0-9]+) follows ([0-9a-fA-F]+) op ([a-z_]+)\b`)
var dataCapLogSnap = regexp.MustCompile(`\bflushsnap snap ([0-9a-fA-F]+) client\.([0-9]+) on .*\binode (0x[0-9a-fA-F]+|[0-9]+)\b`)
var dataCapLogDedupe = regexp.MustCompile(`handle_client_caps already flushed tid ([0-9]+) for client\.([0-9]+)\b`)
var dataCapLogFlush = regexp.MustCompile(`\bflush client\.([0-9]+) dirty ([A-Za-z]+) seq [0-9]+ on \[inode (0x[0-9a-fA-F]+|[0-9]+)\b`)

type dataCapLogTuple struct {
	op                string
	ino, tid, follows uint64
}

func dataCapLogNumber(value string) (uint64, error) {
	base := 10
	if strings.HasPrefix(value, "0x") {
		base = 16
		value = value[2:]
	}
	return strconv.ParseUint(value, base, 64)
}

// Native snapid_t prints hexadecimal without a 0x prefix; TID is decimal.
func dataCapLogTupleFrom(op, ino, tid, follows string) (dataCapLogTuple, bool) {
	i, e1 := dataCapLogNumber(ino)
	t, e2 := dataCapLogNumber(tid)
	f, e3 := strconv.ParseUint(follows, 16, 64)
	return dataCapLogTuple{op, i, t, f}, e1 == nil && e2 == nil && e3 == nil && i != 0 && t != 0
}

// Incoming evidence consists of a registered source, its exact handler on
// the same native thread and at most one expected-snap or completed-flush
// branch. Dropped ACK evidence is a separate same-thread triple: counted
// client GID, exact ACK tuple sent to the registered peer nonce, and native
// blackhole of that same tuple. A bare ACK/drop/dedupe line is never evidence.
// An incoming client message dropped after Messenger receipt is one
// ProtocolV2 handle_message blackhole line whose peer address carries the
// registered nonce; it has no GlobalID, so only the exact peer address and
// tuple attribute it. An UPDATE handler may be followed by its completed-flush
// branch or by the dirty-flush branch naming the client GID and inode.
func filterMDSCephFSDataCapLog(lines []string, gid uint64, nonce uint32) ([]string, int) {
	type pending struct {
		tuple           dataCapLogTuple
		source          string
		handled, branch bool
	}
	type outgoing struct {
		tuple         dataCapLogTuple
		counted, peer string
	}
	contexts := map[string]*pending{}
	outgoingContexts := map[string]*outgoing{}
	kept, dropped, groups := []string{}, 0, 0
	client := regexp.MustCompile(`\bclient\.` + strconv.FormatUint(gid, 10) + `(?:\s|\[|,|\)|:|$)`)
	address := regexp.MustCompile(`/` + strconv.FormatUint(uint64(nonce), 10) + `(?:\s|\]|,|\)|$)`)
	messageTuple := func(line string) (dataCapLogTuple, bool) {
		m := dataCapLogMessage.FindStringSubmatch(line)
		if m == nil {
			return dataCapLogTuple{}, false
		}
		return dataCapLogTupleFrom(m[1], m[2], m[3], m[4])
	}
	for _, raw := range lines {
		line := strings.TrimRight(raw, "\r\n")
		thread := dataCapLogThread.FindStringSubmatch(line)
		if thread == nil {
			continue
		}
		key := thread[1]
		if strings.Contains(line, "<==") {
			delete(contexts, key)
			// A different dispatch on this thread also ends any uncompleted send
			// attribution. Unrelated threads may legitimately interleave log lines.
			delete(outgoingContexts, key)
			if !client.MatchString(line) || !address.MatchString(line) {
				continue
			}
			if len(line) > mdsCephFSDataCapLogLineBytes {
				dropped++
				continue
			}
			tuple, ok := messageTuple(line)
			if !ok {
				continue
			}
			if len(contexts) >= 64 {
				dropped++
				continue
			}
			contexts[key] = &pending{tuple: tuple, source: line}
			continue
		}
		if strings.Contains(line, "send_message_client_counted ") {
			delete(outgoingContexts, key)
			tuple, ok := messageTuple(line)
			if !ok || tuple.op != "flushsnap_ack" || !client.MatchString(line) {
				continue
			}
			if len(line) > mdsCephFSDataCapLogLineBytes || len(outgoingContexts) >= 64 {
				dropped++
				continue
			}
			outgoingContexts[key] = &outgoing{tuple: tuple, counted: line}
			continue
		}
		if strings.Contains(line, " --> ") {
			out := outgoingContexts[key]
			if out == nil {
				continue
			}
			out.peer = ""
			tuple, ok := messageTuple(line)
			parts := strings.SplitN(line, " --> ", 2)
			peer := strings.SplitN(parts[1], " -- ", 2)
			if !ok || tuple != out.tuple || len(peer) != 2 || !address.MatchString(peer[0]+" ") {
				delete(outgoingContexts, key)
				continue
			}
			if len(line) > mdsCephFSDataCapLogLineBytes {
				dropped++
				delete(outgoingContexts, key)
				continue
			}
			out.peer = line
			continue
		}
		if strings.Contains(line, ".handle_message blackhole ") {
			delete(outgoingContexts, key)
			parts := strings.SplitN(line, " >> ", 2)
			if len(parts) != 2 {
				continue
			}
			peer := strings.SplitN(parts[1], " conn(", 2)
			tuple, ok := messageTuple(line)
			if !ok || tuple.op != "update" && tuple.op != "flushsnap" || len(peer) != 2 || strings.Contains(peer[0], " ") || !address.MatchString(peer[0]+" ") {
				continue
			}
			if len(line) > mdsCephFSDataCapLogLineBytes || groups == mdsCephFSDataCapLogGroups {
				dropped++
				continue
			}
			kept = append(kept, line)
			groups++
			continue
		}
		if strings.Contains(line, " blackhole ") {
			out := outgoingContexts[key]
			delete(outgoingContexts, key)
			if out == nil || out.peer == "" {
				continue
			}
			tuple, ok := messageTuple(line)
			if !ok || tuple != out.tuple {
				continue
			}
			if len(line) > mdsCephFSDataCapLogLineBytes || groups == mdsCephFSDataCapLogGroups {
				dropped++
				continue
			}
			kept = append(kept, out.counted, out.peer, line)
			groups++
			continue
		}
		p := contexts[key]
		if p == nil {
			continue
		}
		if m := dataCapLogHandler.FindStringSubmatch(line); m != nil {
			tuple, ok := dataCapLogTupleFrom(m[4], m[1], m[2], m[3])
			if !ok || tuple != p.tuple || p.handled {
				delete(contexts, key)
				continue
			}
			if len(line) > mdsCephFSDataCapLogLineBytes || groups == mdsCephFSDataCapLogGroups {
				dropped++
				delete(contexts, key)
				continue
			}
			kept = append(kept, p.source, line)
			groups++
			p.handled = true
			continue
		}
		if !p.handled || p.branch || p.tuple.op != "flushsnap" && p.tuple.op != "update" {
			continue
		}
		if m := dataCapLogDedupe.FindStringSubmatch(line); m != nil {
			tid, e1 := strconv.ParseUint(m[1], 10, 64)
			observedGID, e2 := strconv.ParseUint(m[2], 10, 64)
			if e1 != nil || e2 != nil || tid != p.tuple.tid || observedGID != gid {
				continue
			}
			if len(line) > mdsCephFSDataCapLogLineBytes {
				dropped++
				continue
			}
			kept = append(kept, line)
			p.branch = true
			continue
		}
		if p.tuple.op == "update" {
			m := dataCapLogFlush.FindStringSubmatch(line)
			if m == nil {
				continue
			}
			observedGID, e1 := strconv.ParseUint(m[1], 10, 64)
			ino, e2 := dataCapLogNumber(m[3])
			if e1 != nil || e2 != nil || observedGID != gid || ino != p.tuple.ino {
				continue
			}
			if len(line) > mdsCephFSDataCapLogLineBytes {
				dropped++
				continue
			}
			kept = append(kept, line)
			p.branch = true
			continue
		}
		if m := dataCapLogSnap.FindStringSubmatch(line); m != nil {
			observedGID, e1 := strconv.ParseUint(m[2], 10, 64)
			ino, e2 := dataCapLogNumber(m[3])
			if e1 != nil || e2 != nil || observedGID != gid || ino != p.tuple.ino {
				continue
			}
			if len(line) > mdsCephFSDataCapLogLineBytes {
				dropped++
				continue
			}
			kept = append(kept, line)
			p.branch = true
		}
	}
	return kept, dropped
}

// A runtime change is discarded only after the new incarnation reports every
// baseline value, never merely because an exact-GID fail command returned.
func (x *mdsCephFSDataScope) discardRestartRuntime(gid uint64, config map[string]string) error {
	for _, name := range mdsCephFSDataRuntimeConfigNames {
		if config[name] != x.baselineConfig[name] || config[name] == "" {
			return fmt.Errorf("DATA new incarnation %s differs from baseline", name)
		}
	}
	for _, r := range x.runtime {
		if r.active() {
			if r.IncarnationGID == gid {
				return errors.New("DATA runtime incarnation did not change")
			}
		}
	}
	for _, r := range x.runtime {
		if r.active() {
			r.State = "discarded-by-restart"
		}
	}
	return nil
}
func (c *fixtureCluster) mdsCephFSDataRestoreRuntime(ctx context.Context, receipt *mdsCephFSDataCleanup) error {
	x := c.mdsCephFSData
	if len(x.runtime) == 0 {
		receipt.ConfigRestored = true
		return nil
	}
	status, err := c.mdsCephFSDataRankStatus(ctx)
	if err != nil {
		return err
	}
	if status.Rank0 == nil || status.Rank0.Name != x.current.Name || status.Rank0.State != "up:active" {
		return errors.New("DATA runtime cleanup cannot prove its active owned rank")
	}
	if status.Rank0.GID != x.current.GID && status.ArmedPresent {
		return errors.New("DATA old runtime incarnation remains mapped; refusing discard")
	}
	d := x.current
	d.GID = status.Rank0.GID
	d.Incarnation = status.Rank0.Incarnation
	var errs []error
	for _, r := range x.runtime {
		if !r.active() || r.IncarnationGID != d.GID {
			continue
		}
		r.State = "restore-intended"
		if err := c.mdsCephFSDataJournal(); err != nil {
			errs = append(errs, err)
			continue
		}
		err := c.mdsCephFSDataApplyRuntime(ctx, d, r.Baseline, &r.RestoreReadback)
		r.State = "restored"
		if err != nil {
			r.State, r.Error = "restore-uncertain", err.Error()
			errs = append(errs, err)
		}
	}
	config, err := c.mdsCephFSDataConfig(ctx, d)
	if err != nil {
		errs = append(errs, err)
	} else {
		receipt.Config = config
		for _, name := range mdsCephFSDataRuntimeConfigNames {
			if config[name] != x.baselineConfig[name] || config[name] == "" {
				errs = append(errs, fmt.Errorf("DATA cleanup %s differs from baseline", name))
			}
		}
		if len(errs) == 0 {
			for _, r := range x.runtime {
				if r.active() && r.IncarnationGID != d.GID {
					r.State = "discarded-by-restart"
				}
			}
		}
	}
	receipt.ConfigRestored = len(errs) == 0
	return errors.Join(errors.Join(errs...), c.mdsCephFSDataJournal())
}
