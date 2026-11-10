package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// Closed, test-only native observations for one applied OSD, PG or MDS command.
//
// A request names one owned daemon and, for the PG cell, one placement group
// of that OSD. Every read and both mutations go through that daemon's own
// admin socket, a local Unix socket independent of every Messenger connection
// of the Go client under test. Each observation is bracketed by two fresh
// native map reads of the daemon's identity:
//
//   - osd and pg: the OSD's entry in "osd dump" (up, up_from epoch and public
//     v2 address with nonce);
//   - mds: the daemon's entry in "fs dump" (up:active, GID and v2 address with
//     nonce).
//
// The osd and mds cells share the daemon-local option, bound and mutations of
// the manager Tell cell (mgr_tell_outcome.go): overwrite sets baseline+2 and
// restore unsets or re-sets the one original runtime override.
//
// The pg cell observes the one reversible PG-scoped state that the pinned OSD
// exposes to PG-routed commands: the scrubber's "sessions" debug flag, which
// only decides whether the primary's "query" output lists a "test_sequence"
// counter. Its baseline must be the default (not listed). Overwrite and
// restore are the same native "scrubdebug <pgid> unset sessions", so a replay
// of the Go client's "set" after the overwrite would list the counter again.
//
// The consumer supplies a token, a phase and the closed target, never a
// command, an option or a value. Each mutation is claimed before its native
// call and submitted at most once per token; one that did not return
// successfully is recorded as it returned and is never repeated.
//
// The baseline belongs to one daemon incarnation, recorded with it: the OSD's
// up_from epoch or the MDS GID, and the public address with its nonce.
// Immediately before either mutation a fresh native map read must show that
// same up incarnation; otherwise the mutation is refused, nothing is sent and
// nothing is claimed. The reads after the call are compared with the recorded
// incarnation again. The admin socket is addressed by daemon name and does
// not report that incarnation, so identity is not proven atomically with the
// command: a daemon replaced between the map read and the admin-socket call
// is not prevented, only reported by the reads after the call.
//
// Native behavior relied upon (Tentacle v20.2.4): OSD and MDS queue MCommand to
// the common admin-socket handlers (src/osd/OSD.cc, src/mds/MDSDaemon.cc,
// src/common/admin_socket.cc), PG-routed commands reach PrimaryLogPG::do_command
// (src/osd/OSD.cc, src/osd/PrimaryLogPG.cc), and the scrubber flag is set in
// PgScrubber::asok_debug and read in PgScrubber::dump_scrubber
// (src/osd/scrubber/pg_scrubber.cc).
const (
	// daemonCommandMDSSelector is the one further selector the owned MDS
	// command fixture admits (profile.go).
	daemonCommandMDSSelector    = "^TestCephAppliedMDSCommandIsNotReplayedIntegration$"
	daemonCommandPrefix         = "native-daemon-command-no-replay-"
	daemonCommandPGFlag         = "sessions"
	daemonCommandPGCounter      = "test_sequence"
	daemonCommandMapTimeout     = 15 * time.Second
	daemonCommandRequestTimeout = 90 * time.Second
)

var (
	daemonCommandPhases     = map[string]bool{"initial": true, "applied": true, "overwrite": true, "recovered": true, "restore": true, "restored": true}
	daemonCommandOSDPattern = regexp.MustCompile(`^(0|[1-9][0-9]{0,3})$`)
	daemonCommandPGPattern  = regexp.MustCompile(`^(0|[1-9][0-9]{0,8})\.[0-9a-f]{1,8}$`)
	daemonCommandMDSPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
)

// daemonCommandTarget is the closed target of one cell.
type daemonCommandTarget struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
	PGID string `json:"pgid,omitempty"`
}

func (t daemonCommandTarget) daemon() string {
	if t.Kind == "mds" {
		return "mds." + t.ID
	}
	return "osd." + t.ID
}

func parseDaemonCommandTarget(fields []string) (daemonCommandTarget, error) {
	invalid := errors.New("invalid owned daemon command observation")
	if len(fields) < 2 {
		return daemonCommandTarget{}, invalid
	}
	target := daemonCommandTarget{Kind: fields[0], ID: fields[1]}
	switch {
	case target.Kind == "osd" && len(fields) == 2 && daemonCommandOSDPattern.MatchString(target.ID):
	case target.Kind == "mds" && len(fields) == 2 && daemonCommandMDSPattern.MatchString(target.ID):
	case target.Kind == "pg" && len(fields) == 3 && daemonCommandOSDPattern.MatchString(target.ID) && daemonCommandPGPattern.MatchString(fields[2]):
		target.PGID = fields[2]
	default:
		return daemonCommandTarget{}, invalid
	}
	return target, nil
}

// daemonCommandIdentity is one fresh native map read of the target daemon.
type daemonCommandIdentity struct {
	OK     bool   `json:"ok"`
	Error  string `json:"error"`
	Daemon string `json:"daemon"`
	Up     bool   `json:"up"`
	// Incarnation is the OSD's up_from epoch or the MDS GID. With the address
	// nonce it changes when the daemon process is replaced.
	Incarnation uint64 `json:"incarnation"`
	// Address is the daemon's public v2 address as "<ip:port>/<nonce>".
	Address string `json:"address"`
}

// daemonCommandPGState is the projection of one primary "query" reply. No other
// part of the reply is persisted.
type daemonCommandPGState struct {
	OK        bool   `json:"ok"`
	Error     string `json:"error"`
	DataBytes int    `json:"data_bytes"`
	Valid     bool   `json:"valid"`
	PGID      string `json:"pgid"`
	State     string `json:"state"`
	// ScrubberPresent is false unless the OSD is the active primary of the PG.
	ScrubberPresent bool `json:"scrubber_present"`
	FlagPresent     bool `json:"flag_present"`
}

type daemonCommandViews struct {
	DaemonBefore daemonCommandIdentity `json:"daemon_before"`
	ConfigGet    *mgrTellCall          `json:"config_get,omitempty"`
	ConfigDiff   *mgrTellDiff          `json:"config_diff,omitempty"`
	PGQuery      *daemonCommandPGState `json:"pg_query,omitempty"`
	DaemonAfter  daemonCommandIdentity `json:"daemon_after"`
}

type daemonCommandReceipt struct {
	RequestID int64               `json:"request_id"`
	Token     string              `json:"token"`
	Phase     string              `json:"phase"`
	Target    daemonCommandTarget `json:"target"`
	// Restorable reports that the initial observation recorded a baseline one
	// closed mutation can restore. Baseline is nil for the pg cell, whose only
	// restorable baseline is the unset flag.
	Restorable bool               `json:"restorable"`
	Baseline   *mgrTellBaseline   `json:"baseline,omitempty"`
	Views      daemonCommandViews `json:"views"`
	Mutation   *mgrTellMutation   `json:"mutation,omitempty"`
	State      string             `json:"state,omitempty"`
}

type daemonCommandOwner struct {
	Token  string              `json:"token"`
	Target daemonCommandTarget `json:"target"`
	// Incarnation and Address are the daemon process that supplied the
	// baseline.
	Incarnation uint64           `json:"incarnation"`
	Address     string           `json:"address"`
	Baseline    *mgrTellBaseline `json:"baseline,omitempty"`
}

// incarnation reports whether one map read shows the baseline's daemon.
func (o daemonCommandOwner) incarnation(read daemonCommandIdentity) bool {
	return o.Incarnation != 0 && o.Address != "" && read.OK && read.Up && read.Daemon == o.Target.daemon() &&
		read.Incarnation == o.Incarnation && read.Address == o.Address
}

// daemonCommandBackend is the native surface of one request for one target.
type daemonCommandBackend struct {
	identity    func(context.Context) daemonCommandIdentity
	adminSocket func(ctx context.Context, args ...string) ([]byte, error)
}

func (b daemonCommandBackend) call(ctx context.Context, args ...string) ([]byte, error) {
	call, cancel := context.WithTimeout(ctx, mgrTellCallTimeout)
	defer cancel()
	return b.adminSocket(call, args...)
}

// daemonCommandPGQuery keeps the identity, state and scrubber flag of a query
// reply. The reply enumerates peering and log details that stay in memory.
func daemonCommandPGQuery(data []byte, err error) *daemonCommandPGState {
	observed := &daemonCommandPGState{OK: err == nil, Error: mgrTellDiagnostic(err), DataBytes: len(data)}
	var reply struct {
		State string `json:"state"`
		Info  struct {
			PGID string `json:"pgid"`
		} `json:"info"`
		Scrubber map[string]json.RawMessage `json:"scrubber"`
	}
	if err != nil || len(data) > mgrTellReplyLimit || json.Unmarshal(data, &reply) != nil || !daemonCommandPGPattern.MatchString(reply.Info.PGID) || len(reply.State) > 128 {
		return observed
	}
	observed.Valid, observed.PGID, observed.State, observed.ScrubberPresent = true, reply.Info.PGID, reply.State, reply.Scrubber != nil
	_, observed.FlagPresent = reply.Scrubber[daemonCommandPGCounter]
	return observed
}

func (b daemonCommandBackend) views(ctx context.Context, target daemonCommandTarget) daemonCommandViews {
	views := daemonCommandViews{DaemonBefore: b.identity(ctx)}
	if target.Kind == "pg" {
		views.PGQuery = daemonCommandPGQuery(b.call(ctx, "pg", target.PGID, "query"))
	} else {
		data, err := b.call(ctx, "config", "get", mgrTellOption)
		get := mgrTellCall{OK: err == nil, Error: mgrTellDiagnostic(err)}
		if err == nil && len(data) <= 4096 {
			get.Data = data
		} else if err == nil {
			get.OK, get.Error = false, "config get reply exceeds its bound"
		}
		diff := mgrTellOptionDiff(b.call(ctx, "config", "diff", "get", mgrTellOption))
		views.ConfigGet, views.ConfigDiff = &get, &diff
	}
	views.DaemonAfter = b.identity(ctx)
	return views
}

// daemonCommandStable reports whether the same up incarnation of the target
// daemon brackets the views.
func daemonCommandStable(views daemonCommandViews, target daemonCommandTarget) bool {
	before, after := views.DaemonBefore, views.DaemonAfter
	return before.OK && after.OK && before.Up && after.Up && before.Daemon == target.daemon() && after.Daemon == target.daemon() &&
		before.Incarnation != 0 && before.Incarnation == after.Incarnation && before.Address != "" && before.Address == after.Address
}

// daemonCommandRestorable returns whether the bracketed initial views are a
// baseline one closed mutation can restore, and for the option cells that
// baseline.
func daemonCommandRestorable(views daemonCommandViews, target daemonCommandTarget) (bool, *mgrTellBaseline) {
	if !daemonCommandStable(views, target) {
		return false, nil
	}
	if target.Kind == "pg" {
		query := views.PGQuery
		return query != nil && query.OK && query.Valid && query.PGID == target.PGID && query.ScrubberPresent && !query.FlagPresent, nil
	}
	if views.ConfigGet == nil || views.ConfigDiff == nil {
		return false, nil
	}
	effective := mgrTellEffective(*views.ConfigGet)
	diff := *views.ConfigDiff
	if effective == nil || !mgrTellInBound(*effective) || !diff.OK || !diff.Valid {
		return false, nil
	}
	if diff.Entry == nil || !diff.Entry.OverridePresent {
		return true, &mgrTellBaseline{Effective: *effective}
	}
	if diff.Entry.Override != nil && *diff.Entry.Override == *effective && (diff.Entry.Final == nil || *diff.Entry.Final == *effective) {
		return true, &mgrTellBaseline{Present: true, Value: *effective, Effective: *effective}
	}
	return false, nil
}

func daemonCommandMutationArgs(owner daemonCommandOwner, phase string) ([]string, error) {
	if owner.Target.Kind == "pg" {
		if owner.Baseline != nil {
			return nil, errors.New("recorded PG command baseline is not the unset flag")
		}
		return []string{"scrubdebug", owner.Target.PGID, "unset", daemonCommandPGFlag}, nil
	}
	if owner.Baseline == nil {
		return nil, errors.New("recorded daemon command baseline is missing")
	}
	if phase == "overwrite" {
		return mgrTellOverwriteArgs(*owner.Baseline)
	}
	return mgrTellRestoreArgs(*owner.Baseline)
}

func daemonCommandJournal(out string, receipt daemonCommandReceipt) error {
	data, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(filepath.Join(out, "daemon-command-outcome-native.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(append(data, '\n'))
	return errors.Join(writeErr, file.Close())
}

// daemonCommandOutcome serves one request for an already parsed target.
func daemonCommandOutcome(ctx context.Context, out string, requestID int64, token, phase string, target daemonCommandTarget, backend daemonCommandBackend) error {
	if requestID <= 0 || !mgrTellTokenPattern.MatchString(token) || !daemonCommandPhases[phase] {
		return errors.New("invalid owned daemon command observation")
	}
	receipt := daemonCommandReceipt{RequestID: requestID, Token: token, Phase: phase, Target: target}
	ownerPath := filepath.Join(out, daemonCommandPrefix+token+".baseline.json")
	if phase == "initial" {
		receipt.Views = backend.views(ctx, target)
		receipt.Restorable, receipt.Baseline = daemonCommandRestorable(receipt.Views, target)
		if receipt.Restorable {
			// A repeated initial request cannot replace the first baseline.
			before := receipt.Views.DaemonBefore
			if err := mgrTellWriteExclusive(ownerPath, daemonCommandOwner{token, target, before.Incarnation, before.Address, receipt.Baseline}); err != nil {
				return fmt.Errorf("record the daemon command baseline: %w", err)
			}
		}
	} else {
		data, err := os.ReadFile(ownerPath)
		var owner daemonCommandOwner
		if err != nil || len(data) > 4096 || json.Unmarshal(data, &owner) != nil || owner.Token != token || owner.Target != target {
			return errors.New("daemon command outcome has no matching baseline receipt for this target")
		}
		receipt.Restorable, receipt.Baseline = true, owner.Baseline
		if phase == "overwrite" || phase == "restore" {
			args, err := daemonCommandMutationArgs(owner, phase)
			if err != nil {
				return err
			}
			receipt.Mutation = &mgrTellMutation{Action: phase, Args: args}
			// The values come from one process's baseline. A stale or
			// unverifiable owner is refused before anything is claimed or sent.
			if !owner.incarnation(backend.identity(ctx)) {
				receipt.Mutation.Refused, receipt.Mutation.Error = true, mgrTellRefusal
			} else {
				// Claim the one allowed mutation before crossing the native
				// call. A call that does not return leaves this marker
				// uncertain; no retry may repeat it.
				started := receipt
				started.State = phase + "-started-outcome-unknown"
				if err := mgrTellWriteExclusive(filepath.Join(out, daemonCommandPrefix+token+"."+phase+"-started.json"), started); err != nil {
					return fmt.Errorf("daemon command %s was already claimed: %w", phase, err)
				}
				if err := daemonCommandJournal(out, started); err != nil {
					return err
				}
				output, callErr := backend.call(ctx, args...)
				receipt.Mutation.OK, receipt.Mutation.Error, receipt.Mutation.OutputBytes = callErr == nil, mgrTellDiagnostic(callErr), len(output)
			}
		}
		receipt.Views = backend.views(ctx, target)
		if m := receipt.Mutation; m != nil && !m.Refused {
			m.SameIncarnationAfter = owner.incarnation(receipt.Views.DaemonBefore) && owner.incarnation(receipt.Views.DaemonAfter)
		}
	}
	if err := mgrTellPublish(out, daemonCommandPrefix+token+"."+phase+".json", receipt); err != nil {
		return err
	}
	return daemonCommandJournal(out, receipt)
}

type daemonCommandAddress struct {
	Type  string `json:"type"`
	Addr  string `json:"addr"`
	Nonce uint64 `json:"nonce"`
}

func daemonCommandV2(addresses []daemonCommandAddress) string {
	for _, address := range addresses {
		if address.Type == "v2" && address.Addr != "" && len(address.Addr) <= 64 {
			return address.Addr + "/" + strconv.FormatUint(address.Nonce, 10)
		}
	}
	return ""
}

// daemonCommandOSDIdentity projects one OSD's entry of a native "osd dump".
func daemonCommandOSDIdentity(id string, data []byte, err error) daemonCommandIdentity {
	observed := daemonCommandIdentity{OK: err == nil, Error: mgrTellDiagnostic(err), Daemon: "osd." + id}
	var dump struct {
		OSDs []struct {
			ID     int64  `json:"osd"`
			Up     int    `json:"up"`
			UpFrom uint64 `json:"up_from"`
			Public struct {
				Addrvec []daemonCommandAddress `json:"addrvec"`
			} `json:"public_addrs"`
		} `json:"osds"`
	}
	if err != nil {
		return observed
	}
	if json.Unmarshal(data, &dump) != nil {
		observed.OK, observed.Error = false, "osd dump is not the expected JSON"
		return observed
	}
	for _, osd := range dump.OSDs {
		if strconv.FormatInt(osd.ID, 10) == id {
			observed.Up, observed.Incarnation, observed.Address = osd.Up == 1, osd.UpFrom, daemonCommandV2(osd.Public.Addrvec)
		}
	}
	return observed
}

// daemonCommandMDSIdentity projects one named MDS's entry of a native "fs dump".
func daemonCommandMDSIdentity(name string, data []byte, err error) daemonCommandIdentity {
	observed := daemonCommandIdentity{OK: err == nil, Error: mgrTellDiagnostic(err), Daemon: "mds." + name}
	var dump struct {
		Filesystems []struct {
			Map struct {
				Info map[string]struct {
					GID   uint64 `json:"gid"`
					Name  string `json:"name"`
					State string `json:"state"`
					Addrs struct {
						Addrvec []daemonCommandAddress `json:"addrvec"`
					} `json:"addrs"`
				} `json:"info"`
			} `json:"mdsmap"`
		} `json:"filesystems"`
	}
	if err != nil {
		return observed
	}
	if json.Unmarshal(data, &dump) != nil {
		observed.OK, observed.Error = false, "fs dump is not the expected JSON"
		return observed
	}
	matches := 0
	for _, filesystem := range dump.Filesystems {
		for _, info := range filesystem.Map.Info {
			if info.Name == name {
				matches++
				observed.Up, observed.Incarnation, observed.Address = info.State == "up:active", info.GID, daemonCommandV2(info.Addrs.Addrvec)
			}
		}
	}
	if matches > 1 {
		return daemonCommandIdentity{OK: false, Error: "fs dump lists the daemon name more than once", Daemon: "mds." + name}
	}
	return observed
}

func (c *fixtureCluster) daemonCommandBackend(ctx context.Context, target daemonCommandTarget) (daemonCommandBackend, error) {
	mapRead := func(call context.Context, args ...string) ([]byte, error) {
		bounded, cancel := context.WithTimeout(call, daemonCommandMapTimeout)
		defer cancel()
		return c.cluster.Ceph(bounded, args...)
	}
	if target.Kind == "mds" {
		if c.mdsFresh == nil || c.mdsFresh.fs == nil {
			return daemonCommandBackend{}, errors.New("invalid owned daemon command observation: no owned MDS scope")
		}
		for _, daemon := range c.mdsFresh.fs.MDSs() {
			if daemon == nil || daemon.Container == nil || daemon.ID != target.ID {
				continue
			}
			return daemonCommandBackend{
				identity: func(call context.Context) daemonCommandIdentity {
					data, err := mapRead(call, "fs", "dump", "--format", "json")
					return daemonCommandMDSIdentity(target.ID, data, err)
				},
				adminSocket: func(call context.Context, args ...string) ([]byte, error) {
					argv := append([]string{"ceph", "--admin-daemon", "/var/run/ceph/ceph-mds." + daemon.ID + ".asok"}, args...)
					process, err := c.cluster.RunNativeCommand(call, daemon.Container, ceph.NativeCommand{Argv: argv, Timeout: mgrTellCallTimeout,
						MaxStdout: mgrTellReplyLimit, MaxStderr: 16 << 10, Label: "owned MDS admin socket " + args[0]})
					if err != nil || !process.Complete() || process.ExitCode != 0 {
						return nil, errors.Join(err, fmt.Errorf("owned MDS admin socket %s did not complete (exit %d, timed out %v)", args[0], process.ExitCode, process.TimedOut))
					}
					return bytes.TrimSpace(process.Stdout), nil
				},
			}, nil
		}
		return daemonCommandBackend{}, errors.New("invalid owned daemon command observation: the MDS is not owned by this fixture")
	}
	osds, err := c.cluster.OSDs(ctx)
	if err != nil {
		return daemonCommandBackend{}, err
	}
	for _, osd := range osds {
		if strconv.Itoa(osd.ID) != target.ID {
			continue
		}
		return daemonCommandBackend{
			identity: func(call context.Context) daemonCommandIdentity {
				data, err := mapRead(call, "osd", "dump", "--format", "json")
				return daemonCommandOSDIdentity(target.ID, data, err)
			},
			adminSocket: func(call context.Context, args ...string) ([]byte, error) {
				return c.adminSocket(call, "osd."+target.ID, args...)
			},
		}, nil
	}
	return daemonCommandBackend{}, errors.New("invalid owned daemon command observation: the OSD is not owned by this fixture")
}

func (c *fixtureCluster) controlDaemonCommandRequest(ctx context.Context, out string) error {
	fields, err := c.request(out, "observe-daemon-command")
	if err != nil || fields == nil {
		return err
	}
	if len(fields) < 5 {
		return errors.New("invalid owned daemon command observation")
	}
	requestID, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil {
		return errors.New("invalid owned daemon command observation")
	}
	target, err := parseDaemonCommandTarget(fields[3:])
	if err != nil {
		return err
	}
	// Fixtures that rotate tickets, expire sessions or restrict modes keep
	// their own cells.
	if c.profile.authEpoch == "1" || c.profile.idleSessions == "1" || c.profile.shortTickets == "1" || c.profile.expireTickets == "1" || c.profile.modeRejection != "" {
		return errors.New("invalid owned daemon command observation: not an ordinary fixture")
	}
	call, cancel := context.WithTimeout(ctx, daemonCommandRequestTimeout)
	defer cancel()
	backend, err := c.daemonCommandBackend(call, target)
	if err != nil {
		return err
	}
	if err := daemonCommandOutcome(call, out, requestID, fields[1], fields[2], target, backend); err != nil {
		return err
	}
	return publishAck(out, "daemon-command-observed."+fields[0])
}
