package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
)

// The authentication first-failure bundle is a diagnostic control: right after
// a Go client's first cold MGR authentication was refused, the Go test freezes
// what it expects and asks the fixture to observe what the stock images can
// show around that rejection. It follows the ceph-testcontainers-go v0.3.0
// recipe (MIT, internal/integration/auth_first_failure_integration_helpers_test.go
// collectAuthFirstFailureBundle/classifyAuthBundle/checkAuthBundleSecrecy and
// docs/guides/messenger-client-fixtures.md "Authentication first-failure
// bundle"); the code here is written for this fixture.
//
// The collection never repeats the tested authentication, never submits a
// wipe, never restarts or reconfigures a daemon (debug_auth prints keys in
// this image, so no debug level is raised), and never retries a query: each
// query runs once within its bound, all within one budget, and records its
// status. It never establishes that the issuing MON and the verifying MGR used
// the same rotating-key generation; no stock command shows that without the
// keys, so generation_equality is always unavailable/unverified.
const (
	authBundleSchema          = "ceph-msgr-go/auth-first-failure/v1"
	authBundleFile            = "auth-first-failure-bundle.json"
	authGenerationUnavailable = "unavailable/unverified"
	authBundleBudget          = time.Minute
	authBundleQueryBound      = 15 * time.Second
	// authBundleStopAllowance bounds the stop of a native control that
	// RunNativeCommand could not confirm as stopped, outside the budget.
	authBundleStopAllowance = time.Minute
	authBundleExcerptLimit  = 1024
	authBundleControlOutput = 4 << 10
	authBundleRequestLimit  = 48 << 10
	authTextWithheld        = "text withheld: redaction did not finish within the collection budget"
)

// authBundleLabels names the profiles allowed to request a bundle.
var authBundleLabels = map[string]func(profile) bool{
	"auth-epoch":    func(p profile) bool { return p.authEpoch == "1" },
	"short-tickets": func(p profile) bool { return p.shortTickets == "1" },
}

type authBundleProvenance struct {
	Producer string `json:"producer"`
	Subject  string `json:"subject"`
	Run      string `json:"run"`
	Report   string `json:"report,omitempty"`
}

// authBundleExpected is what the Go test froze right after the rejection,
// before any diagnostic: the cluster, its MONs and the target MGR's GID. The
// fixture adds the target's container instance as the first query.
type authBundleExpected struct {
	FSID        string   `json:"fsid"`
	Monitors    []string `json:"monitors"`
	Manager     string   `json:"manager"`
	GID         uint64   `json:"gid"`
	ContainerID string   `json:"container_id,omitempty"`
	StartedAt   string   `json:"started_at,omitempty"`
}

// authBundleRejection is the Go test's record of the first rejection. The
// ticket label is an opaque run-scoped HMAC, never the ticket.
type authBundleRejection struct {
	Run             string    `json:"run"`
	Cycle           int       `json:"cycle"`
	Attempt         int       `json:"attempt"`
	At              time.Time `json:"at"`
	Target          string    `json:"target"`
	TargetGID       uint64    `json:"target_gid"`
	ClientID        uint64    `json:"client_id,omitempty"`
	SecretID        *uint64   `json:"secret_id,omitempty"`
	TicketLabel     string    `json:"ticket_label,omitempty"`
	RefusedGlobalID *uint64   `json:"refused_global_id,omitempty"`
	Result          int       `json:"result"`
	Error           string    `json:"error"`
}

type authBundleHeld struct {
	ClientID uint64 `json:"client_id"`
	Usable   bool   `json:"usable"`
}

type authBundleTicket struct {
	Run             string `json:"run"`
	Cycle           int    `json:"cycle"`
	Service         int    `json:"service"`
	SecretID        uint64 `json:"secret_id"`
	Label           string `json:"label"`
	IssuerMonmap    uint64 `json:"issuer_monmap_epoch,omitempty"`
	IssuerAuthEpoch uint64 `json:"issuer_auth_epoch,omitempty"`
}

type authBundleRequest struct {
	Label      string               `json:"label"`
	Provenance authBundleProvenance `json:"provenance"`
	Expected   authBundleExpected   `json:"expected_target"`
	Rejection  authBundleRejection  `json:"first_failure"`
	Held       *authBundleHeld      `json:"held_session,omitempty"`
	Tickets    []authBundleTicket   `json:"consumer_tickets,omitempty"`
}

type authBundleQuery struct {
	Name     string    `json:"name"`
	Kind     string    `json:"kind"`
	Started  time.Time `json:"started"`
	Finished time.Time `json:"finished"`
	Status   string    `json:"status"`
	Error    string    `json:"error,omitempty"`
	Excerpt  string    `json:"excerpt,omitempty"`
}

// authBundleMonitor is one MON's own mon_status through its admin socket, and
// its container's start time. AuthEpoch is nil when the MON does not report it.
type authBundleMonitor struct {
	Name          string  `json:"name"`
	Rank          int     `json:"rank"`
	State         string  `json:"state"`
	ElectionEpoch uint64  `json:"election_epoch"`
	Quorum        []int   `json:"quorum"`
	MonmapEpoch   uint64  `json:"monmap_epoch"`
	AuthEpoch     *uint64 `json:"auth_epoch"`
	FSID          string  `json:"fsid"`
	StartedAt     string  `json:"container_started_at,omitempty"`
}

// authBundleManager is one owned MGR: its MgrMap GID, container instance and,
// before the controls, the global IDs of its beacon and module MON sessions.
type authBundleManager struct {
	Name            string   `json:"name"`
	GID             uint64   `json:"gid"`
	Active          bool     `json:"active"`
	ContainerID     string   `json:"container_id"`
	StartedAt       string   `json:"started_at"`
	BeaconGlobalIDs []uint64 `json:"beacon_global_ids"`
	ModuleGlobalIDs []uint64 `json:"module_global_ids"`
}

type authBundleControl struct {
	Target   string    `json:"target"`
	Command  string    `json:"command"`
	Phase    string    `json:"phase"`
	Started  time.Time `json:"started"`
	ExitCode int       `json:"exit_code"`
	Complete bool      `json:"complete"`
}

type authBundleVerdict struct {
	Complete           bool     `json:"complete"`
	Problems           []string `json:"problems,omitempty"`
	Target             string   `json:"target"`
	RefusedClient      string   `json:"refused_client"`
	ReusedSecretIDs    []uint64 `json:"reused_secret_ids,omitempty"`
	ControlsSucceeded  bool     `json:"controls_succeeded"`
	Result             int      `json:"result"`
	GenerationEquality string   `json:"generation_equality"`
}

type authFirstFailureBundle struct {
	Schema             string               `json:"schema"`
	RequestID          string               `json:"request_id"`
	Label              string               `json:"label"`
	Provenance         authBundleProvenance `json:"provenance"`
	Release            string               `json:"release,omitempty"`
	Expected           authBundleExpected   `json:"expected_target"`
	Budget             string               `json:"budget"`
	QueryBound         string               `json:"query_bound"`
	Started            time.Time            `json:"started"`
	Ended              time.Time            `json:"ended"`
	Rejection          authBundleRejection  `json:"first_failure"`
	Held               *authBundleHeld      `json:"held_session,omitempty"`
	Tickets            []authBundleTicket   `json:"consumer_tickets,omitempty"`
	Monitors           []authBundleMonitor  `json:"monitors"`
	Quorum             []string             `json:"quorum_names,omitempty"`
	MgrMapEpoch        uint32               `json:"mgr_map_epoch,omitempty"`
	MgrMapEpochAfter   uint32               `json:"mgr_map_epoch_after,omitempty"`
	Managers           []authBundleManager  `json:"managers"`
	ManagersAfter      []authBundleManager  `json:"managers_after"`
	Controls           []authBundleControl  `json:"native_controls"`
	Queries            []authBundleQuery    `json:"queries"`
	CleanupErrors      []string             `json:"cleanup_errors,omitempty"`
	CollectionComplete bool                 `json:"collection_complete"`
	GenerationEquality string               `json:"generation_equality"`
	Verdict            authBundleVerdict    `json:"verdict"`
}

// authBundleSources is what a collection reads. Each method waits at most
// until its context ends.
type authBundleSources interface {
	AdminSocket(ctx context.Context, daemon string, args ...string) ([]byte, error)
	ManagerMap(ctx context.Context) (managerPreparationMap, error)
	QuorumStatus(ctx context.Context) (ceph.QuorumStatus, error)
	OwnedManagers() []string
	// ContainerInstance returns the container ID and start time of an owned
	// "mon" or "mgr" daemon.
	ContainerInstance(ctx context.Context, kind, name string) (string, string, error)
	// TellVersion runs one fresh native "ceph tell <target> version" from the
	// control container, bounded by timeout and output.
	TellVersion(ctx context.Context, target string, timeout time.Duration, output int) (ceph.NativeCommandResult, error)
	Redact(ctx context.Context, data []byte) ([]byte, error)
}

// decodeAuthBundleRequest decodes the base64url JSON request strictly.
func decodeAuthBundleRequest(encoded string) (authBundleRequest, error) {
	var request authBundleRequest
	if len(encoded) == 0 || len(encoded) > authBundleRequestLimit {
		return request, errors.New("invalid auth bundle request size")
	}
	data, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return request, errors.New("invalid auth bundle request encoding")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return request, fmt.Errorf("invalid auth bundle request: %w", err)
	}
	return request, nil
}

// validateAuthBundleRequest checks the request against the fixture's own
// identities. It does not require a complete rejection record: an incomplete
// one is kept as given and reported by the classifier.
func validateAuthBundleRequest(request authBundleRequest, p profile, monitors, managers []string) error {
	allowed, ok := authBundleLabels[request.Label]
	if !ok || !allowed(p) {
		return errors.New("auth first-failure bundle is restricted to the auth-epoch and short-ticket fixtures")
	}
	expected := request.Expected
	if expected.FSID == "" || expected.FSID != p.fsid {
		return errors.New("auth bundle expected FSID differs from the fixture")
	}
	if expected.ContainerID != "" || expected.StartedAt != "" {
		return errors.New("auth bundle container identity is frozen by the fixture")
	}
	want, got := slices.Sorted(slices.Values(monitors)), slices.Sorted(slices.Values(expected.Monitors))
	if len(got) == 0 || !slices.Equal(want, got) {
		return errors.New("auth bundle expected MONs differ from the owned MONs")
	}
	if expected.Manager != "" && !slices.Contains(managers, expected.Manager) {
		return errors.New("auth bundle target is not an owned MGR")
	}
	if request.Rejection.Target != "" && request.Rejection.Target != "mgr."+expected.Manager {
		return errors.New("auth bundle rejection target differs from the expected MGR")
	}
	return nil
}

type authBundleCollector struct {
	sources authBundleSources
	ctx     context.Context
	bound   time.Duration
	bundle  *authFirstFailureBundle
}

func (c *authBundleCollector) redact(text string) string {
	redacted, err := c.sources.Redact(c.ctx, []byte(text))
	if err != nil {
		return authTextWithheld
	}
	return string(redacted[:min(len(redacted), authBundleExcerptLimit)])
}

// query runs one query once within the per-query bound and records its
// status; output is kept, redacted, only for a query that did not complete.
func (c *authBundleCollector) query(name, kind string, run func(context.Context) ([]byte, error)) bool {
	ctx, cancel := context.WithTimeout(c.ctx, c.bound)
	defer cancel()
	entry := authBundleQuery{Name: name, Kind: kind, Started: time.Now().UTC(), Status: queryComplete}
	output, err := run(ctx)
	if err != nil {
		entry.Status = boundedQueryStatus(err, ctx.Err() != nil)
		switch {
		case errors.Is(err, errAuthControlTimedOut):
			entry.Status = queryTimedOut
		case errors.Is(err, ceph.ErrNativeCommandIncomplete):
			entry.Status = queryTruncated
		}
		entry.Error = c.redact(err.Error())
		if len(output) > 0 {
			entry.Excerpt = c.redact(string(output))
		}
	}
	entry.Finished = time.Now().UTC()
	c.bundle.Queries = append(c.bundle.Queries, entry)
	return err == nil
}

// errAuthControlTimedOut and errAuthControlTruncated mark a native control
// whose result is not complete; the query then records that status.
var (
	errAuthControlTimedOut  = errors.New("native control timed out")
	errAuthControlTruncated = fmt.Errorf("native control output truncated: %w", ceph.ErrNativeCommandIncomplete)
)

func (c *authBundleCollector) observeManagers(phase string, sessions []monitorManagerSessions) ([]authBundleManager, uint32) {
	var state managerPreparationMap
	mapped := c.query("mgr dump "+phase, "observation", func(ctx context.Context) ([]byte, error) {
		var err error
		state, err = c.sources.ManagerMap(ctx)
		return nil, err
	})
	gids := map[string]uint64{}
	if mapped {
		if state.ActiveName != "" {
			gids[state.ActiveName] = state.ActiveGID
		}
		for _, standby := range state.Standbys {
			gids[standby.Name] = standby.GID
		}
	}
	observed := []authBundleManager{}
	for _, name := range c.sources.OwnedManagers() {
		entry := authBundleManager{Name: name, GID: gids[name], Active: mapped && name == state.ActiveName, BeaconGlobalIDs: []uint64{}, ModuleGlobalIDs: []uint64{}}
		c.query("container of mgr."+name+" "+phase, "observation", func(ctx context.Context) ([]byte, error) {
			var err error
			entry.ContainerID, entry.StartedAt, err = c.sources.ContainerInstance(ctx, "mgr", name)
			return nil, err
		})
		if sessions != nil {
			for _, monitor := range sessions {
				for _, beacon := range monitor.Beacons {
					if beacon.Entity == "mgr."+name {
						entry.BeaconGlobalIDs = append(entry.BeaconGlobalIDs, beacon.GlobalID)
					}
				}
			}
			slices.Sort(entry.BeaconGlobalIDs)
			entry.BeaconGlobalIDs = slices.Compact(entry.BeaconGlobalIDs)
			if ids := moduleGlobalIDs(sessions, name); ids != nil {
				entry.ModuleGlobalIDs = ids
			}
		}
		observed = append(observed, entry)
	}
	return observed, state.Epoch
}

// parseAuthMonStatus reads a MON's mon_status. Empty output, non-JSON output
// and a status without its name, FSID or MonMap epoch are errors.
func parseAuthMonStatus(data []byte) (authBundleMonitor, error) {
	var status struct {
		Name          string `json:"name"`
		Rank          int    `json:"rank"`
		State         string `json:"state"`
		ElectionEpoch uint64 `json:"election_epoch"`
		Quorum        []int  `json:"quorum"`
		Monmap        *struct {
			Epoch     uint64  `json:"epoch"`
			AuthEpoch *uint64 `json:"auth_epoch"`
			FSID      string  `json:"fsid"`
		} `json:"monmap"`
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return authBundleMonitor{}, errors.New("mon_status printed nothing")
	}
	if err := json.Unmarshal(data, &status); err != nil {
		return authBundleMonitor{}, fmt.Errorf("mon_status is not JSON: %w", err)
	}
	if status.Name == "" || status.Monmap == nil || status.Monmap.FSID == "" || status.Monmap.Epoch == 0 {
		return authBundleMonitor{}, errors.New("mon_status lacks its name, FSID or MonMap epoch")
	}
	return authBundleMonitor{Name: status.Name, Rank: status.Rank, State: status.State, ElectionEpoch: status.ElectionEpoch, Quorum: status.Quorum,
		MonmapEpoch: status.Monmap.Epoch, AuthEpoch: status.Monmap.AuthEpoch, FSID: status.Monmap.FSID}, nil
}

// collectAuthFirstFailureBundle collects one bundle within budget from a
// context of its own and bounds each query by bound.
func collectAuthFirstFailureBundle(parent context.Context, sources authBundleSources, id string, request authBundleRequest, budget, bound time.Duration) authFirstFailureBundle {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), budget)
	defer cancel()
	bundle := authFirstFailureBundle{Schema: authBundleSchema, RequestID: id, Label: request.Label, Provenance: request.Provenance, Expected: request.Expected,
		Budget: budget.String(), QueryBound: bound.String(), Started: time.Now().UTC(), Rejection: request.Rejection, Held: request.Held,
		Tickets: request.Tickets, Monitors: []authBundleMonitor{}, Controls: []authBundleControl{}, GenerationEquality: authGenerationUnavailable}
	collector := &authBundleCollector{sources: sources, ctx: ctx, bound: bound, bundle: &bundle}
	// The target's container instance is frozen first, before any other
	// diagnostic, since the Go test cannot observe it.
	if name := request.Expected.Manager; name != "" {
		collector.query("container of mgr."+name+" frozen", "identity", func(ctx context.Context) ([]byte, error) {
			var err error
			bundle.Expected.ContainerID, bundle.Expected.StartedAt, err = sources.ContainerInstance(ctx, "mgr", name)
			return nil, err
		})
	}
	var sessions []monitorManagerSessions
	for _, name := range request.Expected.Monitors {
		var observed authBundleMonitor
		statusOK := collector.query("mon."+name+" mon_status", "local observation", func(ctx context.Context) ([]byte, error) {
			out, err := sources.AdminSocket(ctx, "mon."+name, "mon_status")
			if err != nil {
				return out, err
			}
			observed, err = parseAuthMonStatus(out)
			return out, err
		})
		collector.query("container of mon."+name, "observation", func(ctx context.Context) ([]byte, error) {
			var err error
			_, observed.StartedAt, err = sources.ContainerInstance(ctx, "mon", name)
			return nil, err
		})
		if statusOK {
			bundle.Monitors = append(bundle.Monitors, observed)
		}
		collector.query("mon."+name+" sessions", "local observation", func(ctx context.Context) ([]byte, error) {
			out, err := sources.AdminSocket(ctx, "mon."+name, "sessions")
			if err != nil {
				return out, err
			}
			decoded, err := decodeManagerSessions(name, out)
			if err == nil {
				sessions = append(sessions, decoded)
			}
			return out, err
		})
	}
	collector.query("quorum", "messenger observation", func(ctx context.Context) ([]byte, error) {
		quorum, err := sources.QuorumStatus(ctx)
		if err == nil {
			bundle.Quorum = quorum.QuorumNames
		}
		return nil, err
	})
	if sessions == nil {
		sessions = []monitorManagerSessions{}
	}
	bundle.Managers, bundle.MgrMapEpoch = collector.observeManagers("before", sessions)
	// A standby MGR answers no tell, so the controls address the tested
	// target and the active MGR.
	var targets []string
	if request.Rejection.Target != "" {
		targets = append(targets, request.Rejection.Target)
	}
	for _, manager := range bundle.Managers {
		if manager.Active && !slices.Contains(targets, "mgr."+manager.Name) {
			targets = append(targets, "mgr."+manager.Name)
		}
	}
	for _, target := range targets {
		collector.query("tell "+target+" version", "behavioral control", func(ctx context.Context) ([]byte, error) {
			started := time.Now().UTC()
			result, err := sources.TellVersion(ctx, target, bound, authBundleControlOutput)
			var unstopped *ceph.NativeCommandUnstoppedError
			if errors.As(err, &unstopped) {
				stopCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), authBundleStopAllowance)
				if stopErr := unstopped.Stop(stopCtx); stopErr != nil {
					bundle.CleanupErrors = append(bundle.CleanupErrors, fmt.Sprintf("stop %s: %v", target, stopErr))
				}
				stop()
			}
			bundle.Controls = append(bundle.Controls, authBundleControl{Target: target, Command: "version", Phase: "after first failure", Started: started,
				ExitCode: result.ExitCode, Complete: err == nil && result.Complete()})
			if err == nil {
				switch {
				case result.TimedOut:
					err = errAuthControlTimedOut
				case !result.Complete():
					err = errAuthControlTruncated
				case result.ExitCode != 0:
					err = fmt.Errorf("native control exit %d", result.ExitCode)
				}
			}
			if err == nil && bundle.Release == "" {
				var version struct {
					Version string `json:"version"`
					Release string `json:"release"`
				}
				if json.Unmarshal(result.Stdout, &version) == nil {
					bundle.Release = strings.TrimSpace(version.Version + " " + version.Release)
				}
			}
			return append(result.Stdout, result.Stderr...), err
		})
	}
	bundle.ManagersAfter, bundle.MgrMapEpochAfter = collector.observeManagers("after", nil)
	bundle.Ended = time.Now().UTC()
	bundle.CollectionComplete = len(bundle.CleanupErrors) == 0 &&
		!slices.ContainsFunc(bundle.Queries, func(q authBundleQuery) bool { return q.Status != queryComplete })
	bundle.Verdict = classifyAuthBundle(bundle)
	return bundle
}

// classifyAuthBundle checks a bundle's completeness and associations. It
// never establishes generation equality.
func classifyAuthBundle(bundle authFirstFailureBundle) authBundleVerdict {
	rejection := bundle.Rejection
	verdict := authBundleVerdict{Target: queryUnavailable, RefusedClient: "unknown", Result: rejection.Result, GenerationEquality: authGenerationUnavailable}
	problem := func(format string, args ...any) {
		verdict.Problems = append(verdict.Problems, fmt.Sprintf(format, args...))
	}
	if bundle.Schema != authBundleSchema {
		problem("schema %q", bundle.Schema)
	}
	if rejection.Target == "" || rejection.TargetGID == 0 || rejection.Result == 0 || rejection.At.IsZero() {
		problem("no tested rejection")
	}
	if bundle.GenerationEquality != authGenerationUnavailable {
		problem("generation equality %q: a stock bundle cannot establish it", bundle.GenerationEquality)
	}
	for _, query := range bundle.Queries {
		if query.Status != queryComplete {
			problem("%s: %s %s", query.Name, query.Status, query.Error)
		}
	}
	for _, cleanup := range bundle.CleanupErrors {
		problem("cleanup: %s", cleanup)
	}
	expected := bundle.Expected
	for _, name := range expected.Monitors {
		index := slices.IndexFunc(bundle.Monitors, func(m authBundleMonitor) bool { return m.Name == name })
		switch {
		case index < 0:
			problem("mon.%s unavailable", name)
		case bundle.Monitors[index].FSID != expected.FSID:
			problem("mon.%s reports FSID %q, not %q", name, bundle.Monitors[index].FSID, expected.FSID)
		}
	}
	for _, before := range bundle.Managers {
		index := slices.IndexFunc(bundle.ManagersAfter, func(after authBundleManager) bool { return after.Name == before.Name })
		if index < 0 {
			problem("mgr.%s unavailable after the collection", before.Name)
			continue
		}
		if after := bundle.ManagersAfter[index]; after.GID != before.GID || after.ContainerID != before.ContainerID || after.StartedAt != before.StartedAt {
			problem("mgr.%s changed during the collection: GID %d -> %d, container %s/%s -> %s/%s",
				before.Name, before.GID, after.GID, before.ContainerID, before.StartedAt, after.ContainerID, after.StartedAt)
		}
	}
	mapRead := slices.ContainsFunc(bundle.Queries, func(q authBundleQuery) bool { return q.Name == "mgr dump before" && q.Status == queryComplete })
	if index := slices.IndexFunc(bundle.Managers, func(m authBundleManager) bool { return "mgr."+m.Name == rejection.Target }); index >= 0 && bundle.Managers[index].GID != 0 {
		target := bundle.Managers[index]
		verdict.Target = gateSlotRegistered
		if target.GID != rejection.TargetGID || target.GID != expected.GID || expected.ContainerID == "" ||
			target.ContainerID != expected.ContainerID || target.StartedAt != expected.StartedAt {
			verdict.Target = "changed"
		}
		if refused := rejection.RefusedGlobalID; refused != nil {
			switch {
			case *refused == target.GID:
				verdict.RefusedClient = gateRefusalRegistered
			case slices.Contains(target.ModuleGlobalIDs, *refused):
				verdict.RefusedClient = gateRefusalModuleClient
			}
		}
	} else if mapRead {
		verdict.Target = gateSlotAbsent
	}
	if verdict.Target != gateSlotRegistered {
		problem("target %s is %s", rejection.Target, verdict.Target)
	}
	type origin struct {
		run   string
		cycle int
	}
	origins := map[uint64]map[origin]bool{}
	record := func(run string, cycle int, secretID uint64) {
		if origins[secretID] == nil {
			origins[secretID] = map[origin]bool{}
		}
		origins[secretID][origin{run, cycle}] = true
	}
	for _, ticket := range bundle.Tickets {
		record(ticket.Run, ticket.Cycle, ticket.SecretID)
	}
	if rejection.SecretID != nil {
		record(rejection.Run, rejection.Cycle, *rejection.SecretID)
	}
	for secretID, seen := range origins {
		if len(seen) > 1 {
			verdict.ReusedSecretIDs = append(verdict.ReusedSecretIDs, secretID)
		}
	}
	slices.Sort(verdict.ReusedSecretIDs)
	verdict.ControlsSucceeded = len(bundle.Controls) > 0 && !slices.ContainsFunc(bundle.Controls, func(c authBundleControl) bool { return c.ExitCode != 0 || !c.Complete })
	verdict.Complete = len(verdict.Problems) == 0
	return verdict
}

// checkAuthBundleSecrecy refuses a bundle whose JSON holds any of secrets, or
// a field the bundle does not define.
func checkAuthBundleSecrecy(data []byte, secrets ...string) error {
	for _, secret := range secrets {
		if secret != "" && bytes.Contains(data, []byte(secret)) {
			return errors.New("the auth bundle holds a secret")
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var bundle authFirstFailureBundle
	if err := decoder.Decode(&bundle); err != nil {
		return fmt.Errorf("the auth bundle has fields beyond its schema: %w", err)
	}
	return nil
}

// fixtureSecrets returns the consumer keys the fixture wrote, for the secrecy
// check. Missing files are skipped; the cluster's own secrets are redacted by
// RedactSecrets.
func fixtureSecrets(out string) []string {
	var secrets []string
	for _, name := range []string{"key", "readonly.key", "revocable.key", "configwatch.key"} {
		if data, err := os.ReadFile(filepath.Join(out, name)); err == nil {
			if secret := strings.TrimSpace(string(data)); secret != "" {
				secrets = append(secrets, secret)
			}
		}
	}
	return secrets
}

// fixtureAuthBundleSources reads a bundle from the owned cluster.
type fixtureAuthBundleSources struct {
	c       *fixtureCluster
	secrets []string
}

func (s fixtureAuthBundleSources) AdminSocket(ctx context.Context, daemon string, args ...string) ([]byte, error) {
	return s.c.cluster.AdminSocket(ctx, daemon, args...)
}
func (s fixtureAuthBundleSources) ManagerMap(ctx context.Context) (managerPreparationMap, error) {
	return s.c.observeManagerMap(ctx)
}
func (s fixtureAuthBundleSources) QuorumStatus(ctx context.Context) (ceph.QuorumStatus, error) {
	return s.c.cluster.QuorumStatus(ctx)
}
func (s fixtureAuthBundleSources) OwnedManagers() []string {
	var names []string
	for _, manager := range s.c.cluster.Managers() {
		names = append(names, manager.DaemonName)
	}
	slices.Sort(names)
	return names
}
func (s fixtureAuthBundleSources) ContainerInstance(ctx context.Context, kind, name string) (string, string, error) {
	daemon, err := s.c.daemon(kind, name)
	if err != nil {
		return "", "", err
	}
	state, err := daemon.State(ctx)
	if err != nil {
		return "", "", err
	}
	if state == nil || state.StartedAt == "" {
		return "", "", errQueryMissing
	}
	return daemon.GetContainerID(), state.StartedAt, nil
}
func (s fixtureAuthBundleSources) TellVersion(ctx context.Context, target string, timeout time.Duration, output int) (ceph.NativeCommandResult, error) {
	return s.c.cluster.RunNativeCommand(ctx, s.c.cluster.ControlContainer(), ceph.NativeCommand{
		Argv: authBundleControlArgv(target), Timeout: timeout, MaxStdout: output, MaxStderr: 16 << 10, Label: "auth bundle tell " + target + " version",
	})
}
func (s fixtureAuthBundleSources) Redact(ctx context.Context, data []byte) ([]byte, error) {
	return s.c.cluster.RedactSecrets(ctx, data, s.secrets...)
}

// authBundleControlArgv is the only native command a collection runs. It
// sets no option: in particular no debug level, since debug_auth prints keys.
func authBundleControlArgv(target string) []string {
	return []string{"ceph", "--connect-timeout", "5", "tell", target, "version"}
}

// collectAuthBundleControl serves one "collect-auth-bundle" request: it
// validates the request, collects the bundle, checks its secrecy and writes
// it. Collection failures stay inside the bundle; only an invalid request or
// a bundle that fails the secrecy check is a control error.
func (c *fixtureCluster) collectAuthBundleControl(ctx context.Context, out, id, encoded string) error {
	request, err := decodeAuthBundleRequest(encoded)
	if err != nil {
		return err
	}
	var monitors []string
	for _, mon := range c.cluster.Monitors() {
		monitors = append(monitors, mon.DaemonName)
	}
	sources := fixtureAuthBundleSources{c: c, secrets: fixtureSecrets(out)}
	if err := validateAuthBundleRequest(request, c.profile, monitors, sources.OwnedManagers()); err != nil {
		return err
	}
	bundle := collectAuthFirstFailureBundle(ctx, sources, id, request, authBundleBudget, authBundleQueryBound)
	data, err := json.Marshal(bundle)
	if err != nil {
		return err
	}
	if err := checkAuthBundleSecrecy(data, sources.secrets...); err != nil {
		return err
	}
	temporary := filepath.Join(out, authBundleFile+".tmp")
	if err := os.WriteFile(temporary, data, 0644); err != nil {
		return err
	}
	return os.Rename(temporary, filepath.Join(out, authBundleFile))
}
