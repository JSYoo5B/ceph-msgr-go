package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
)

// The CephFS session acceptance drives an owned, closed set of native
// filesystem mutations. Every mutation is journaled before it is issued, is
// never replayed after an uncertain reply, and is reconciled against an
// independent native readback during cleanup. Behavior relied upon is
// documented at mdsFreshSource: mds/Server.cc (handle_client_session,
// find_idle_sessions, update_required_client_features), mds/MDSRank.cc
// (evict_client, dump_sessions), mds/SessionMap.cc (Session::dump) and
// mon/FSCommands.cc. LGPL-2.1 references; no code copied.

const mdsSessionsJSONLimit = 512 << 10
const mdsSessionsMaxGlobalIDs = 8

// mdsSessionsRequirement is one owned required_client_features toggle. The
// key is the cephfs_dump_features name in the MDSMap dump. reply_encoding is
// withheld by the session profile (a REJECT); reclaim_client is declared by
// both profiles (an admission); multi_reconnect is CephFS-only.
type mdsSessionsRequirement struct {
	feature, key, kind string
}

var mdsSessionsRequirements = map[string]mdsSessionsRequirement{
	"require":         {"reply_encoding", "feature_9", "required_client_features"},
	"require-reclaim": {"reclaim_client", "feature_10", "required_client_features_reclaim_client"},
	"require-multi":   {"multi_reconnect", "feature_12", "required_client_features_multi_reconnect"},
}

// mdsSessionsRequirementFor maps a require-*-on/off action to its toggle.
func mdsSessionsRequirementFor(action string) (mdsSessionsRequirement, bool) {
	base, ok := strings.CutSuffix(action, "-on")
	if !ok {
		base, ok = strings.CutSuffix(action, "-off")
	}
	r, found := mdsSessionsRequirements[base]
	return r, ok && found
}

const mdsSessionsDeferOption = "mds_defer_session_stale"

// The stale window shortens session_timeout to the native minimum and keeps
// autoclose well beyond the observation. mds_defer_session_stale=false is
// required because Server::find_idle_sessions otherwise never marks a session
// without capabilities stale; it evicts it at autoclose instead.
var mdsSessionsStaleWindow = cephfs.SessionTimeouts{Timeout: 30 * time.Second, Autoclose: 120 * time.Second}

// The autoclose window keeps the native minimum timeout and shortens autoclose
// (also at least 30 s in mon/FSCommands.cc) so a silent session is evicted
// within the observation; it is applied after the stale window is restored.
var mdsSessionsAutocloseWindow = cephfs.SessionTimeouts{Timeout: 30 * time.Second, Autoclose: 40 * time.Second}

type mdsSessionsMap struct {
	Epoch     uint32            `json:"mdsmap_epoch"`
	Refuse    bool              `json:"refuse_client_session"`
	Required  map[string]string `json:"required_client_features"`
	Timeout   int64             `json:"session_timeout"`
	Autoclose int64             `json:"session_autoclose"`
}

type mdsSessionsBlock struct {
	Address string `json:"address"`
	Range   bool   `json:"range"`
	Until   string `json:"until"`
}

type mdsSessionsBaseline struct {
	Map                  mdsSessionsMap     `json:"map"`
	DeferStale           string             `json:"daemon_mds_defer_session_stale"`
	BlocklistOnEvict     string             `json:"daemon_mds_session_blocklist_on_evict"`
	BlocklistOnTimeout   string             `json:"daemon_mds_session_blocklist_on_timeout"`
	CentralDeferEntries  int                `json:"central_mds_defer_session_stale_entries"`
	Blocklist            []mdsSessionsBlock `json:"blocklist"`
	BlocklistObservedUTC string             `json:"blocklist_observed_utc"`
}

// One journal record per owned native mutation. State moves from intended to
// applied or uncertain, then restore-intended to restored or
// restore-uncertain. Evictions end evicted (blocklist entry recorded) or
// uncertain. Cleanup reconciles every non-baseline record from native state.
type mdsSessionsMutation struct {
	Kind      string `json:"kind"`
	RequestID int64  `json:"request_id"`
	Target    string `json:"target"`
	State     string `json:"state"`
	GlobalID  uint64 `json:"global_id,omitempty"`
	Address   string `json:"address,omitempty"`
	Until     string `json:"blocklist_until,omitempty"`
	Output    string `json:"native_output,omitempty"`
	Error     string `json:"error,omitempty"`
}

type mdsSessionsScope struct {
	out, foreignFS       string
	foreign              *ceph.ClientConfig
	lastRequestID        int64
	baseline             mdsSessionsBaseline
	mutations            []*mdsSessionsMutation
	refuse               string
	required             map[string]string
	stale                string
	windowKind           string
	timeouts             *cephfs.SessionTimeoutsOverride
	deferStale           *ceph.ConfigOverride
	owned                map[string]uint64
	baselineReady        bool
	identityCreateIntent bool
}

func mdsSessionsForeignCaps(foreignFS string) ceph.ClientCaps {
	// MON read for authentication and an MDS read grant for a different
	// filesystem name: Server::handle_client_session rejects every session
	// message from this identity with "client doesn't have caps for FS".
	return ceph.ClientCaps{Mon: "allow r", MDS: "allow r fsname=" + foreignFS}
}

func mdsSessionsKeyringSecret(keyring []byte) (string, error) {
	var key string
	for _, line := range strings.Split(string(keyring), "\n") {
		k, v, ok := strings.Cut(line, "=")
		if ok && strings.TrimSpace(k) == "key" {
			if key != "" {
				return "", errors.New("dedicated MDS sessions keyring has multiple keys")
			}
			key = strings.TrimSpace(v)
		}
	}
	if key == "" {
		return "", errors.New("dedicated MDS sessions key missing")
	}
	return key, nil
}

func parseMDSSessionsMap(data []byte, fscid int64, name string) (mdsSessionsMap, error) {
	var result mdsSessionsMap
	if len(data) == 0 || len(data) > mdsFreshJSONLimit || !json.Valid(data) {
		return result, errors.New("MDS sessions native fs get exceeds its bound or is invalid")
	}
	var native struct {
		ID  *int64 `json:"id"`
		Map struct {
			Epoch      *uint32 `json:"epoch"`
			Name       string  `json:"fs_name"`
			FlagsState *struct {
				Refuse *bool `json:"refuse_client_session"`
			} `json:"flags_state"`
			Required  map[string]string `json:"required_client_features"`
			Timeout   *int64            `json:"session_timeout"`
			Autoclose *int64            `json:"session_autoclose"`
		} `json:"mdsmap"`
	}
	if err := json.Unmarshal(data, &native); err != nil || native.ID == nil || *native.ID != fscid || native.Map.Name != name || native.Map.Epoch == nil || *native.Map.Epoch == 0 ||
		native.Map.FlagsState == nil || native.Map.FlagsState.Refuse == nil || native.Map.Required == nil || native.Map.Timeout == nil || native.Map.Autoclose == nil || len(native.Map.Required) > 64 {
		return result, errors.New("MDS sessions native map identity or session fields differ")
	}
	return mdsSessionsMap{Epoch: *native.Map.Epoch, Refuse: *native.Map.FlagsState.Refuse, Required: native.Map.Required, Timeout: *native.Map.Timeout, Autoclose: *native.Map.Autoclose}, nil
}

// mdsSessionsNative is one native Session::dump entry, reduced to the fields
// the acceptance compares. Metadata holds every client key/value; the
// formatter's client_features and metric_spec objects are kept separately.
type mdsSessionsNative struct {
	ID             uint64            `json:"id"`
	State          string            `json:"state"`
	AuthType       string            `json:"auth_type"`
	AuthID         string            `json:"auth_id"`
	EntityType     string            `json:"entity_type"`
	AddressType    string            `json:"address_type"`
	Address        string            `json:"address"`
	Caps           int64             `json:"num_caps"`
	Leases         int64             `json:"num_leases"`
	ClientFeatures string            `json:"client_features"`
	MetricFlags    string            `json:"metric_flags"`
	Metadata       map[string]string `json:"metadata"`
}

func mdsSessionsAddress(addr string, nonce uint32) (string, error) {
	endpoint, err := netip.ParseAddrPort(addr)
	if err != nil || endpoint.Addr().Zone() != "" {
		return "", errors.New("MDS session entity address is not a numeric unscoped endpoint")
	}
	// Same exact IP:port/nonce form as the provider's normalized blocklist.
	return endpoint.String() + "/" + strconv.FormatUint(uint64(nonce), 10), nil
}

func parseMDSSessionsList(data []byte) ([]mdsSessionsNative, error) {
	if len(data) == 0 || len(data) > mdsSessionsJSONLimit || !json.Valid(data) {
		return nil, errors.New("MDS sessions native listing exceeds its bound or is invalid")
	}
	var native []struct {
		ID       *uint64 `json:"id"`
		State    string  `json:"state"`
		Caps     *int64  `json:"num_caps"`
		Leases   *int64  `json:"num_leases"`
		AuthName struct {
			Type string `json:"type_str"`
			ID   string `json:"id"`
		} `json:"auth_name"`
		Entity struct {
			Name struct {
				Type string  `json:"type"`
				Num  *uint64 `json:"num"`
			} `json:"name"`
			Addr struct {
				Type  string  `json:"type"`
				Addr  string  `json:"addr"`
				Nonce *uint32 `json:"nonce"`
			} `json:"addr"`
		} `json:"entity"`
		Metadata map[string]json.RawMessage `json:"client_metadata"`
	}
	if err := json.Unmarshal(data, &native); err != nil || native == nil || len(native) > 4096 {
		return nil, errors.New("MDS sessions native listing shape differs")
	}
	result := make([]mdsSessionsNative, 0, len(native))
	seen := map[uint64]bool{}
	for _, n := range native {
		if n.ID == nil || *n.ID == 0 || seen[*n.ID] || n.State == "" || n.Caps == nil || n.Leases == nil || n.Entity.Name.Num == nil || *n.Entity.Name.Num != *n.ID || n.Entity.Name.Type != "client" || n.Entity.Addr.Nonce == nil || n.Metadata == nil || len(n.Metadata) > 1026 {
			return nil, errors.New("MDS sessions native entry identity differs")
		}
		seen[*n.ID] = true
		address, err := mdsSessionsAddress(n.Entity.Addr.Addr, *n.Entity.Addr.Nonce)
		if err != nil {
			return nil, err
		}
		entry := mdsSessionsNative{ID: *n.ID, State: n.State, AuthType: n.AuthName.Type, AuthID: n.AuthName.ID, EntityType: n.Entity.Name.Type, AddressType: n.Entity.Addr.Type, Address: address, Caps: *n.Caps, Leases: *n.Leases, Metadata: map[string]string{}}
		for key, raw := range n.Metadata {
			switch key {
			case "client_features":
				var features struct {
					Bits *string `json:"feature_bits"`
				}
				if json.Unmarshal(raw, &features) != nil || features.Bits == nil {
					return nil, errors.New("MDS sessions native client feature bitset differs")
				}
				entry.ClientFeatures = *features.Bits
			case "metric_spec":
				var spec struct {
					Flags struct {
						Bits *string `json:"feature_bits"`
					} `json:"metric_flags"`
				}
				if json.Unmarshal(raw, &spec) != nil || spec.Flags.Bits == nil {
					return nil, errors.New("MDS sessions native metric specification differs")
				}
				entry.MetricFlags = *spec.Flags.Bits
			default:
				var value string
				if json.Unmarshal(raw, &value) != nil {
					return nil, errors.New("MDS sessions native client metadata value is not a string")
				}
				entry.Metadata[key] = value
			}
		}
		if entry.ClientFeatures == "" || entry.MetricFlags == "" {
			return nil, errors.New("MDS sessions native client feature fields missing")
		}
		result = append(result, entry)
	}
	return result, nil
}

func parseMDSSessionsDaemonEpoch(data []byte) (uint32, error) {
	var status struct {
		Epoch *uint32 `json:"mdsmap_epoch"`
		State string  `json:"state"`
	}
	if len(data) == 0 || len(data) > 64<<10 || json.Unmarshal(data, &status) != nil || status.Epoch == nil || *status.Epoch == 0 || status.State != "up:active" {
		return 0, errors.New("MDS sessions native daemon status differs")
	}
	return *status.Epoch, nil
}

func parseMDSSessionsDaemonConfig(data []byte, name string) (string, error) {
	var values map[string]string
	if len(data) == 0 || len(data) > 4096 || json.Unmarshal(data, &values) != nil || len(values) != 1 {
		return "", errors.New("MDS sessions native daemon configuration differs")
	}
	value, ok := values[name]
	if !ok || value != "true" && value != "false" {
		return "", errors.New("MDS sessions native daemon configuration value differs")
	}
	return value, nil
}

type mdsSessionsAction struct {
	name string
	gids []uint64
}

// The request is <id> <ownership token> <closed action> [GlobalIDs]. Only a
// fixed action set is admitted; GlobalIDs are canonical positive decimals.
func parseMDSSessionsRequest(fields []string, token string, previousID int64) (int64, mdsSessionsAction, error) {
	var action mdsSessionsAction
	if len(fields) < 3 || len(token) != 32 || fields[1] != token {
		return 0, action, errors.New("invalid owned MDS sessions request scope")
	}
	if _, err := hex.DecodeString(token); err != nil {
		return 0, action, errors.New("invalid owned MDS sessions request token")
	}
	id, err := strconv.ParseInt(fields[0], 10, 64)
	if err != nil || id <= 0 || id <= previousID || strconv.FormatInt(id, 10) != fields[0] {
		return 0, action, errors.New("invalid MDS sessions request identity")
	}
	action.name = fields[2]
	args := fields[3:]
	switch action.name {
	case "observe":
		if len(args) == 0 || len(args) > mdsSessionsMaxGlobalIDs {
			return 0, mdsSessionsAction{}, errors.New("MDS sessions observation requires one to eight GlobalIDs")
		}
	case "evict":
		if len(args) != 1 {
			return 0, mdsSessionsAction{}, errors.New("MDS sessions eviction requires exactly one GlobalID")
		}
	case "refuse-on", "refuse-off", "require-on", "require-off", "require-reclaim-on", "require-multi-on", "require-reclaim-off", "require-multi-off", "stale-on", "stale-off", "autoclose-on":
		if len(args) != 0 {
			return 0, mdsSessionsAction{}, errors.New("MDS sessions mutation takes no caller arguments")
		}
	default:
		return 0, mdsSessionsAction{}, errors.New("MDS sessions action is not admitted")
	}
	for _, arg := range args {
		gid, err := strconv.ParseUint(arg, 10, 64)
		if err != nil || gid == 0 || strconv.FormatUint(gid, 10) != arg || slices.Contains(action.gids, gid) {
			return 0, mdsSessionsAction{}, errors.New("invalid MDS sessions GlobalID")
		}
		action.gids = append(action.gids, gid)
	}
	return id, action, nil
}

type mdsSessionsResult struct {
	Source            string               `json:"source_commit"`
	Token             string               `json:"token"`
	RequestID         int64                `json:"request_id"`
	Action            string               `json:"action"`
	GlobalIDs         []uint64             `json:"global_ids,omitempty"`
	FSID              string               `json:"fsid"`
	FSCID             int64                `json:"fscid"`
	FSName            string               `json:"fs_name"`
	MDSGID            uint64               `json:"mds_gid"`
	MDSEndpoint       string               `json:"mds_endpoint"`
	MDSNonce          uint32               `json:"mds_nonce"`
	MDSIncarnation    int32                `json:"mds_incarnation"`
	Rank              int32                `json:"rank"`
	IdentityChecked   bool                 `json:"identity_checked"`
	Map               *mdsSessionsMap      `json:"map,omitempty"`
	DaemonEpoch       uint32               `json:"daemon_mdsmap_epoch,omitempty"`
	DaemonDeferStale  string               `json:"daemon_mds_defer_session_stale,omitempty"`
	Sessions          []mdsSessionsNative  `json:"sessions"`
	RawSessions       json.RawMessage      `json:"raw_sessions,omitempty"`
	Blocklist         []mdsSessionsBlock   `json:"blocklist"`
	Mutation          *mdsSessionsMutation `json:"mutation,omitempty"`
	Complete          bool                 `json:"complete"`
	Error             string               `json:"error,omitempty"`
	ElapsedMillis     int64                `json:"elapsed_ms"`
	ObservedUTC       string               `json:"observed_utc"`
	BaselineBlocklist []mdsSessionsBlock   `json:"baseline_blocklist"`
}

func (c *fixtureCluster) mdsSessionsJournal() error {
	s, x := c.mdsFresh, c.mdsSessions
	identity := ""
	if x.identityCreateIntent {
		identity = "client.mds-sessions-foreign-" + s.token
	}
	return writeMDSFreshJSON(x.out, "mds-sessions-owned.json", map[string]any{
		"source_commit": mdsFreshSource, "token": s.token, "fsid": c.profile.fsid, "fs_name": s.name, "fscid": s.oracle.FSCID,
		"foreign_identity": identity, "foreign_fs_name": x.foreignFS, "baseline": x.baseline, "mutations": x.mutations,
		"owned_session_addresses": x.owned,
	})
}

func (c *fixtureCluster) mdsSessionsDaemon(ctx context.Context, args ...string) ([]byte, error) {
	return c.cluster.Ceph(ctx, append([]string{"tell", "mds." + c.mdsFresh.name + ":0"}, args...)...)
}

func (c *fixtureCluster) mdsSessionsDaemonConfig(ctx context.Context, name string) (string, error) {
	data, err := c.mdsSessionsDaemon(ctx, "config", "get", name, "--format", "json")
	if err != nil {
		return "", err
	}
	return parseMDSSessionsDaemonConfig(data, name)
}

func (c *fixtureCluster) mdsSessionsReadMap(ctx context.Context) (mdsSessionsMap, error) {
	s := c.mdsFresh
	data, err := c.cluster.Ceph(ctx, "fs", "get", s.name, "--format", "json")
	if err != nil {
		return mdsSessionsMap{}, err
	}
	return parseMDSSessionsMap(data, s.oracle.FSCID, s.name)
}

func (c *fixtureCluster) mdsSessionsBlocklist(ctx context.Context) ([]mdsSessionsBlock, error) {
	entries, err := c.cluster.BlocklistEntries(ctx)
	if err != nil {
		return nil, err
	}
	if len(entries) > 4096 {
		return nil, errors.New("MDS sessions native blocklist exceeds its bound")
	}
	result := make([]mdsSessionsBlock, 0, len(entries))
	for _, entry := range entries {
		result = append(result, mdsSessionsBlock{Address: entry.Address, Range: entry.Range, Until: entry.Until.UTC().Format(time.RFC3339Nano)})
	}
	return result, nil
}

func mdsSessionsFindBlock(entries []mdsSessionsBlock, address string) *mdsSessionsBlock {
	for i := range entries {
		if !entries[i].Range && entries[i].Address == address {
			return &entries[i]
		}
	}
	return nil
}

func (c *fixtureCluster) mdsSessionsList(ctx context.Context) ([]mdsSessionsNative, json.RawMessage, error) {
	data, err := c.mdsSessionsDaemon(ctx, "session", "ls", "--format", "json")
	if err != nil {
		return nil, nil, err
	}
	sessions, err := parseMDSSessionsList(data)
	if err != nil {
		return nil, nil, err
	}
	return sessions, json.RawMessage(data), nil
}

// mdsSessionsSettle waits until the native map satisfies check and the active
// daemon has applied at least that MDSMap epoch. MON command completion alone
// does not prove that the MDS evaluates the next session request with it.
func (c *fixtureCluster) mdsSessionsSettle(ctx context.Context, check func(mdsSessionsMap) bool) (mdsSessionsMap, uint32, error) {
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for {
		current, err := c.mdsSessionsReadMap(ctx)
		if err != nil {
			return current, 0, err
		}
		if check(current) {
			data, err := c.mdsSessionsDaemon(ctx, "status", "--format", "json")
			if err != nil {
				return current, 0, err
			}
			epoch, err := parseMDSSessionsDaemonEpoch(data)
			if err != nil {
				return current, 0, err
			}
			if epoch >= current.Epoch {
				return current, epoch, nil
			}
		}
		select {
		case <-ctx.Done():
			return current, 0, fmt.Errorf("MDS sessions native map did not settle: %w", ctx.Err())
		case <-tick.C:
		}
	}
}

func (c *fixtureCluster) mdsSessionsWaitDaemonConfig(ctx context.Context, name, want string) (string, error) {
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for {
		value, err := c.mdsSessionsDaemonConfig(ctx, name)
		if err != nil || value == want {
			return value, err
		}
		select {
		case <-ctx.Done():
			return value, fmt.Errorf("MDS daemon %s did not reach %s: %w", name, want, ctx.Err())
		case <-tick.C:
		}
	}
}

func (c *fixtureCluster) prepareMDSSessions(ctx context.Context, out string) error {
	p := c.profile
	if p.mdsSessions != "1" || p.mdsFresh != "1" || p.mdsCommands == "1" || p.osdProfile != "legacy-v6" || c.mdsFresh == nil || c.mdsFresh.fs == nil || c.mdsSessions != nil {
		return errors.New("invalid MDS sessions preparation scope")
	}
	s := c.mdsFresh
	x := &mdsSessionsScope{out: out, foreignFS: "ceph-msgr-foreign-" + s.token, owned: map[string]uint64{}, required: map[string]string{}}
	c.mdsSessions = x
	ctx, stop := context.WithTimeout(ctx, fixtureStartupTimeout)
	defer stop()
	var err error
	if x.baseline.Map, err = c.mdsSessionsReadMap(ctx); err != nil {
		return err
	}
	timeouts, err := s.fs.SessionTimeouts(ctx)
	if err != nil {
		return err
	}
	if x.baseline.Map.Refuse || len(x.baseline.Map.Required) != 0 || time.Duration(x.baseline.Map.Timeout)*time.Second != timeouts.Timeout || time.Duration(x.baseline.Map.Autoclose)*time.Second != timeouts.Autoclose || timeouts == mdsSessionsStaleWindow {
		return errors.New("MDS sessions acceptance requires an independently observed baseline without refusal, required features or the stale window")
	}
	entries, err := c.cluster.Configuration(ctx)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Name == mdsSessionsDeferOption {
			x.baseline.CentralDeferEntries++
		}
	}
	if x.baseline.CentralDeferEntries != 0 {
		return errors.New("MDS sessions acceptance refuses to adopt an existing mds_defer_session_stale entry")
	}
	for name, target := range map[string]*string{mdsSessionsDeferOption: &x.baseline.DeferStale, "mds_session_blocklist_on_evict": &x.baseline.BlocklistOnEvict, "mds_session_blocklist_on_timeout": &x.baseline.BlocklistOnTimeout} {
		if *target, err = c.mdsSessionsDaemonConfig(ctx, name); err != nil {
			return err
		}
	}
	if x.baseline.DeferStale != "true" || x.baseline.BlocklistOnEvict != "true" {
		return errors.New("MDS sessions acceptance requires the native defer-stale and evict-blocklist defaults")
	}
	if x.baseline.Blocklist, err = c.mdsSessionsBlocklist(ctx); err != nil {
		return err
	}
	x.baseline.BlocklistObservedUTC = time.Now().UTC().Format(time.RFC3339Nano)
	x.baselineReady = true
	// Record the identity name before the provider mutation creates it.
	x.identityCreateIntent = true
	if err := c.mdsSessionsJournal(); err != nil {
		return err
	}
	name := "mds-sessions-foreign-" + s.token
	x.foreign, err = c.cluster.CreateClient(ctx, name, mdsSessionsForeignCaps(x.foreignFS))
	if err != nil {
		return fmt.Errorf("create dedicated MDS sessions foreign identity: %w", err)
	}
	_, keyring, err := x.foreign.ConnectionConfig()
	if err != nil {
		return err
	}
	key, err := mdsSessionsKeyringSecret(keyring)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(out, "mds-sessions-foreign.key"), []byte(key+"\n"), 0600); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(out, "mds-sessions-foreign.keyring"), keyring, 0600); err != nil {
		return err
	}
	c.profile.mdsSessionsForeignIdentity = "client." + name
	if err := c.mdsSessionsJournal(); err != nil {
		return err
	}
	return writeMDSFreshJSON(out, "mds-sessions-oracle.json", map[string]any{
		"source_commit": mdsFreshSource, "token": s.token, "fsid": s.oracle.FSID, "fscid": s.oracle.FSCID, "fs_name": s.name,
		"mds_gid": s.oracle.MDS.GID, "client_identity": s.oracle.ClientIdentity, "foreign_identity": c.profile.mdsSessionsForeignIdentity,
		"foreign_fs_name": x.foreignFS, "baseline": x.baseline,
		"stale_window": map[string]int64{"session_timeout": int64(mdsSessionsStaleWindow.Timeout / time.Second), "session_autoclose": int64(mdsSessionsStaleWindow.Autoclose / time.Second)},
	})
}

func (c *fixtureCluster) mdsSessionsRecord(m *mdsSessionsMutation, state string, output []byte, err error) error {
	m.State = state
	if len(output) != 0 {
		m.Output = strings.TrimSpace(string(output[:min(len(output), 4096)]))
	}
	if err != nil {
		m.Error = err.Error()
	}
	return c.mdsSessionsJournal()
}

func (c *fixtureCluster) mdsSessionsControl(ctx context.Context, out string, fields []string) (resultErr error) {
	s, x := c.mdsFresh, c.mdsSessions
	if c.profile.mdsSessions != "1" || c.profile.mdsFresh != "1" || s == nil || s.fs == nil || x == nil || s.out != out || x.out != out {
		return errors.New("invalid owned MDS sessions control scope")
	}
	ctx, stop := context.WithTimeout(ctx, 90*time.Second)
	defer stop()
	if err := lockMDSFresh(ctx, &s.mu); err != nil {
		return err
	}
	defer s.mu.Unlock()
	if s.cleanupAttempted {
		return errors.New("MDS sessions control requested after cleanup began")
	}
	id, action, err := parseMDSSessionsRequest(fields, s.token, x.lastRequestID)
	if err != nil {
		return err
	}
	x.lastRequestID = id
	started := time.Now()
	result := mdsSessionsResult{Source: mdsFreshSource, Token: s.token, RequestID: id, Action: action.name, GlobalIDs: action.gids, FSID: s.oracle.FSID, FSCID: s.oracle.FSCID, FSName: s.name,
		MDSGID: s.oracle.MDS.GID, MDSEndpoint: s.oracle.MDS.Endpoint, MDSNonce: s.oracle.MDS.Nonce, MDSIncarnation: s.oracle.MDS.Incarnation, Sessions: []mdsSessionsNative{}, Blocklist: []mdsSessionsBlock{}, BaselineBlocklist: x.baseline.Blocklist}
	defer func() {
		result.ElapsedMillis = time.Since(started).Milliseconds()
		result.ObservedUTC = time.Now().UTC().Format(time.RFC3339Nano)
		if resultErr != nil {
			result.Error = resultErr.Error()
		}
		resultErr = errors.Join(resultErr, writeMDSFreshJSON(out, "mds-sessions-result.json", result))
	}()
	checkIdentity := func() error {
		current, err := c.readMDSFreshOracle(ctx)
		if err != nil {
			return err
		}
		if current.FSID != s.oracle.FSID || current.FSCID != s.oracle.FSCID || current.MDS.GID != s.oracle.MDS.GID || current.MDS.Endpoint != s.oracle.MDS.Endpoint || current.MDS.Nonce != s.oracle.MDS.Nonce || current.MDS.Incarnation != s.oracle.MDS.Incarnation || current.MDS.Rank != 0 || current.MetadataPool != s.oracle.MetadataPool || !reflect.DeepEqual(current.DataPools, s.oracle.DataPools) {
			return errors.New("MDS sessions native target changed during the owned action")
		}
		return nil
	}
	if action.name != "observe" {
		// Mutations first prove the owned target before changing it.
		if err := checkIdentity(); err != nil {
			return err
		}
	}
	if err := c.mdsSessionsAct(ctx, id, action, &result); err != nil {
		return err
	}
	if err := checkIdentity(); err != nil {
		return err
	}
	result.IdentityChecked = true
	current, err := c.mdsSessionsReadMap(ctx)
	if err != nil {
		return err
	}
	result.Map = &current
	data, err := c.mdsSessionsDaemon(ctx, "status", "--format", "json")
	if err != nil {
		return err
	}
	if result.DaemonEpoch, err = parseMDSSessionsDaemonEpoch(data); err != nil {
		return err
	}
	if result.DaemonDeferStale, err = c.mdsSessionsDaemonConfig(ctx, mdsSessionsDeferOption); err != nil {
		return err
	}
	if result.Sessions, result.RawSessions, err = c.mdsSessionsList(ctx); err != nil {
		return err
	}
	for _, session := range result.Sessions {
		if slices.Contains(action.gids, session.ID) {
			// The test supplied this GlobalID; its address is owned evidence
			// for cleanup of any blocklist entry it may later receive.
			x.owned[session.Address] = session.ID
		}
	}
	if result.Blocklist, err = c.mdsSessionsBlocklist(ctx); err != nil {
		return err
	}
	if err := c.mdsSessionsJournal(); err != nil {
		return err
	}
	result.Complete = true
	return nil
}

func (c *fixtureCluster) mdsSessionsAct(ctx context.Context, id int64, action mdsSessionsAction, result *mdsSessionsResult) error {
	s, x := c.mdsFresh, c.mdsSessions
	record := func(kind, target string) *mdsSessionsMutation {
		m := &mdsSessionsMutation{Kind: kind, RequestID: id, Target: target, State: "intended"}
		x.mutations = append(x.mutations, m)
		result.Mutation = m
		return m
	}
	findMutation := func(kind string) *mdsSessionsMutation {
		for _, m := range slices.Backward(x.mutations) {
			if m.Kind == kind {
				result.Mutation = m
				return m
			}
		}
		return nil
	}
	switch action.name {
	case "observe":
		return nil
	case "refuse-on", "require-on", "require-reclaim-on", "require-multi-on":
		kind, state := "refuse_client_session", x.refuse
		args := []string{"fs", "set", s.name, "refuse_client_session", "true"}
		check := func(m mdsSessionsMap) bool { return m.Refuse }
		requirement, requires := mdsSessionsRequirementFor(action.name)
		if requires {
			kind, state = requirement.kind, x.required[requirement.kind]
			args = []string{"fs", "required_client_features", s.name, "add", requirement.feature}
			check = func(m mdsSessionsMap) bool {
				return m.Required[requirement.key] == requirement.feature
			}
		}
		setState := func(value string) {
			if requires {
				x.required[kind] = value
			} else {
				x.refuse = value
			}
		}
		current, err := c.mdsSessionsReadMap(ctx)
		if err != nil {
			return err
		}
		if state != "" || current.Refuse != x.baseline.Map.Refuse || !maps.Equal(current.Required, x.baseline.Map.Required) {
			return fmt.Errorf("MDS sessions %s is not at its owned baseline", kind)
		}
		if requires {
			// update_required_client_features evicts and blocklists every open
			// session lacking the feature. Refuse rather than cause evictions.
			sessions, _, err := c.mdsSessionsList(ctx)
			if err != nil {
				return err
			}
			if len(sessions) != 0 {
				return errors.New("MDS sessions required-feature mutation refused while native sessions exist")
			}
		}
		m := record(kind, args[len(args)-1])
		if err := c.mdsSessionsJournal(); err != nil {
			return err
		}
		output, err := c.cluster.Ceph(ctx, args...)
		if err != nil {
			// Never replayed: cleanup reconciles from native state.
			setState("uncertain")
			return errors.Join(err, c.mdsSessionsRecord(m, "uncertain", output, err))
		}
		setState("applied")
		if err := c.mdsSessionsRecord(m, "applied", output, nil); err != nil {
			return err
		}
		_, _, err = c.mdsSessionsSettle(ctx, check)
		return err
	case "refuse-off", "require-off", "require-reclaim-off", "require-multi-off":
		kind, state := "refuse_client_session", x.refuse
		args := []string{"fs", "set", s.name, "refuse_client_session", "false"}
		requirement, requires := mdsSessionsRequirementFor(action.name)
		if requires {
			kind, state = requirement.kind, x.required[requirement.kind]
			args = []string{"fs", "required_client_features", s.name, "rm", requirement.feature}
		}
		setState := func(value string) {
			if requires {
				x.required[kind] = value
			} else {
				x.refuse = value
			}
		}
		m := findMutation(kind)
		if state != "applied" || m == nil || m.State != "applied" {
			return fmt.Errorf("MDS sessions %s restore requested without an applied owned mutation", kind)
		}
		m.State = "restore-intended"
		if err := c.mdsSessionsJournal(); err != nil {
			return err
		}
		output, err := c.cluster.Ceph(ctx, args...)
		if err != nil {
			setState("restore-uncertain")
			return errors.Join(err, c.mdsSessionsRecord(m, "restore-uncertain", output, err))
		}
		setState("restored")
		if err := c.mdsSessionsRecord(m, "restored", output, nil); err != nil {
			return err
		}
		baseline := x.baseline.Map
		_, _, err = c.mdsSessionsSettle(ctx, func(m mdsSessionsMap) bool {
			return m.Refuse == baseline.Refuse && maps.Equal(m.Required, baseline.Required)
		})
		return err
	case "stale-on", "autoclose-on":
		window, kind := mdsSessionsStaleWindow, "stale_window"
		if action.name == "autoclose-on" {
			window, kind = mdsSessionsAutocloseWindow, "autoclose_window"
		}
		// A window is applied from the baseline or after the previous one
		// was restored, never on top of an applied or uncertain one.
		if x.stale != "" && x.stale != "restored" || x.stale == "" && (x.timeouts != nil || x.deferStale != nil) {
			return errors.New("MDS sessions stale window is not at its owned baseline")
		}
		x.windowKind = kind
		m := record(kind, fmt.Sprintf("session_timeout=%d session_autoclose=%d %s=false", window.Timeout/time.Second, window.Autoclose/time.Second, mdsSessionsDeferOption))
		if err := c.mdsSessionsJournal(); err != nil {
			return err
		}
		var err error
		// Both providers retain a handle on uncertain replies for Restore.
		x.timeouts, err = s.fs.TemporarySessionTimeouts(ctx, window)
		if err == nil {
			x.deferStale, err = c.cluster.TemporaryConfig(ctx, ceph.ConfigSetting{Section: "mds", Name: mdsSessionsDeferOption, Value: "false"})
		}
		if err != nil {
			x.stale = "uncertain"
			return errors.Join(err, c.mdsSessionsRecord(m, "uncertain", nil, err))
		}
		x.stale = "applied"
		if err := c.mdsSessionsRecord(m, "applied", nil, nil); err != nil {
			return err
		}
		want := window
		if _, _, err := c.mdsSessionsSettle(ctx, func(m mdsSessionsMap) bool {
			return time.Duration(m.Timeout)*time.Second == want.Timeout && time.Duration(m.Autoclose)*time.Second == want.Autoclose
		}); err != nil {
			return err
		}
		_, err = c.mdsSessionsWaitDaemonConfig(ctx, mdsSessionsDeferOption, "false")
		return err
	case "stale-off":
		m := findMutation(x.windowKind)
		if x.stale != "applied" || m == nil || m.State != "applied" || x.timeouts == nil || x.deferStale == nil {
			return errors.New("MDS sessions stale window restore requested without an applied owned mutation")
		}
		m.State = "restore-intended"
		if err := c.mdsSessionsJournal(); err != nil {
			return err
		}
		err := errors.Join(x.deferStale.Restore(ctx), x.timeouts.Restore(ctx))
		if err != nil {
			x.stale = "restore-uncertain"
			return errors.Join(err, c.mdsSessionsRecord(m, "restore-uncertain", nil, err))
		}
		x.stale = "restored"
		if err := c.mdsSessionsRecord(m, "restored", nil, nil); err != nil {
			return err
		}
		baseline := x.baseline.Map
		if _, _, err := c.mdsSessionsSettle(ctx, func(m mdsSessionsMap) bool {
			return m.Timeout == baseline.Timeout && m.Autoclose == baseline.Autoclose
		}); err != nil {
			return err
		}
		_, err = c.mdsSessionsWaitDaemonConfig(ctx, mdsSessionsDeferOption, x.baseline.DeferStale)
		return err
	case "evict":
		gid := action.gids[0]
		sessions, _, err := c.mdsSessionsList(ctx)
		if err != nil {
			return err
		}
		var victim *mdsSessionsNative
		for i := range sessions {
			if sessions[i].ID == gid {
				victim = &sessions[i]
			}
		}
		if victim == nil || victim.State != "open" {
			return errors.New("MDS sessions eviction target is not an open native session")
		}
		before, err := c.mdsSessionsBlocklist(ctx)
		if err != nil {
			return err
		}
		if mdsSessionsFindBlock(before, victim.Address) != nil || mdsSessionsFindBlock(x.baseline.Blocklist, victim.Address) != nil {
			return errors.New("MDS sessions eviction target address is already blocklisted")
		}
		x.owned[victim.Address] = gid
		m := record("eviction", strconv.FormatUint(gid, 10))
		m.GlobalID, m.Address = gid, victim.Address
		if err := c.mdsSessionsJournal(); err != nil {
			return err
		}
		output, err := c.mdsSessionsDaemon(ctx, "session", "evict", "id="+strconv.FormatUint(gid, 10))
		if err != nil {
			return errors.Join(err, c.mdsSessionsRecord(m, "uncertain", output, err))
		}
		if err := c.mdsSessionsRecord(m, "requested", output, nil); err != nil {
			return err
		}
		tick := time.NewTicker(250 * time.Millisecond)
		defer tick.Stop()
		for {
			sessions, _, err := c.mdsSessionsList(ctx)
			if err != nil {
				return err
			}
			present := slices.ContainsFunc(sessions, func(n mdsSessionsNative) bool { return n.ID == gid })
			blocks, err := c.mdsSessionsBlocklist(ctx)
			if err != nil {
				return err
			}
			if entry := mdsSessionsFindBlock(blocks, victim.Address); entry != nil && !present {
				m.Until = entry.Until
				return c.mdsSessionsRecord(m, "evicted", nil, nil)
			}
			select {
			case <-ctx.Done():
				err := fmt.Errorf("MDS sessions eviction did not remove the session and blocklist its address: %w", ctx.Err())
				return errors.Join(err, c.mdsSessionsRecord(m, "uncertain", nil, err))
			case <-tick.C:
			}
		}
	}
	return errors.New("MDS sessions action is not admitted")
}

type mdsSessionsCleanup struct {
	Source             string             `json:"source_commit"`
	Token              string             `json:"token"`
	FSName             string             `json:"fs_name"`
	Attempted          bool               `json:"attempted"`
	BaselineMap        mdsSessionsMap     `json:"baseline_map"`
	FinalMap           *mdsSessionsMap    `json:"final_map,omitempty"`
	RefuseReconciled   bool               `json:"refuse_client_session_reconciled"`
	RefuseRestored     bool               `json:"refuse_client_session_restored"`
	RequiredReconciled bool               `json:"required_client_features_reconciled"`
	RequiredRestored   bool               `json:"required_client_features_restored"`
	StaleRestored      bool               `json:"stale_window_restored"`
	DaemonDeferStale   string             `json:"daemon_mds_defer_session_stale,omitempty"`
	CentralDeferAbsent bool               `json:"central_mds_defer_session_stale_absent"`
	RemovedBlocklist   []mdsSessionsBlock `json:"removed_blocklist"`
	UnownedBlocklist   []mdsSessionsBlock `json:"unowned_new_blocklist"`
	BlocklistRestored  bool               `json:"blocklist_restored"`
	FinalBlocklist     []mdsSessionsBlock `json:"final_blocklist"`
	IdentityRemoved    bool               `json:"foreign_identity_removed"`
	Completed          bool               `json:"completed"`
	Errors             []string           `json:"errors,omitempty"`
}

// finishMDSSessions runs once, inside the cached MDS fresh cleanup and before
// the owned filesystem is removed. It never replays an uncertain mutation; it
// compares native state with the recorded baseline and reverts only owned
// differences, then verifies each restoration with an independent readback.
func (c *fixtureCluster) finishMDSSessions(ctx context.Context) error {
	s, x := c.mdsFresh, c.mdsSessions
	if s == nil || x == nil {
		return nil
	}
	ctx, stop := context.WithTimeout(ctx, 45*time.Second)
	defer stop()
	receipt := mdsSessionsCleanup{Source: mdsFreshSource, Token: s.token, FSName: s.name, Attempted: true, BaselineMap: x.baseline.Map, RemovedBlocklist: []mdsSessionsBlock{}, UnownedBlocklist: []mdsSessionsBlock{}}
	if err := writeMDSFreshJSON(x.out, "mds-sessions-cleanup.json", receipt); err != nil {
		return err
	}
	var errs []error
	fail := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}
	if x.deferStale != nil || x.timeouts != nil {
		// The provider handles reconcile an interrupted apply or restore and
		// refuse outside edits; a confirmed restore is a no-op.
		var err error
		if x.deferStale != nil {
			err = x.deferStale.Restore(ctx)
		}
		if x.timeouts != nil {
			err = errors.Join(err, x.timeouts.Restore(ctx))
		}
		fail(err)
	}
	if x.baselineReady {
		c.mdsSessionsReconcile(ctx, &receipt, fail)
	} else {
		// Preparation failed before any session mutation was possible.
		receipt.RefuseRestored, receipt.RequiredRestored, receipt.StaleRestored, receipt.BlocklistRestored = true, true, true, true
	}
	if x.foreign != nil {
		err := c.cluster.DeleteClient(ctx, x.foreign)
		fail(err)
		receipt.IdentityRemoved = err == nil
	} else {
		receipt.IdentityRemoved = !x.identityCreateIntent
		if x.identityCreateIntent {
			fail(errors.New("MDS sessions foreign identity creation outcome is unknown"))
		}
	}
	for _, err := range errs {
		receipt.Errors = append(receipt.Errors, err.Error())
	}
	receipt.Completed = len(errs) == 0 && receipt.RefuseRestored && receipt.RequiredRestored && receipt.StaleRestored && receipt.BlocklistRestored && receipt.IdentityRemoved
	if !receipt.Completed && len(errs) == 0 {
		errs = append(errs, errors.New("MDS sessions cleanup did not verify every restoration"))
	}
	return errors.Join(errors.Join(errs...), writeMDSFreshJSON(x.out, "mds-sessions-cleanup.json", receipt))
}

func (c *fixtureCluster) mdsSessionsReconcile(ctx context.Context, receipt *mdsSessionsCleanup, fail func(error)) {
	s, x := c.mdsFresh, c.mdsSessions
	touched := func(kind string) bool {
		return slices.ContainsFunc(x.mutations, func(m *mdsSessionsMutation) bool { return m.Kind == kind })
	}
	baseline := x.baseline.Map
	current, err := c.mdsSessionsReadMap(ctx)
	fail(err)
	if err == nil {
		if current.Refuse != baseline.Refuse && touched("refuse_client_session") {
			_, err := c.cluster.Ceph(ctx, "fs", "set", s.name, "refuse_client_session", strconv.FormatBool(baseline.Refuse))
			fail(err)
			receipt.RefuseReconciled = err == nil
		}
		removed, removeFailed := false, false
		for _, name := range slices.Sorted(maps.Keys(mdsSessionsRequirements)) {
			r := mdsSessionsRequirements[name]
			if touched(r.kind) && current.Required[r.key] != "" && baseline.Required[r.key] == "" {
				_, err := c.cluster.Ceph(ctx, "fs", "required_client_features", s.name, "rm", r.feature)
				fail(err)
				removed, removeFailed = true, removeFailed || err != nil
			}
		}
		receipt.RequiredReconciled = removed && !removeFailed
		final, _, err := c.mdsSessionsSettle(ctx, func(m mdsSessionsMap) bool {
			return m.Refuse == baseline.Refuse && maps.Equal(m.Required, baseline.Required) && m.Timeout == baseline.Timeout && m.Autoclose == baseline.Autoclose
		})
		fail(err)
		receipt.FinalMap = &final
		receipt.RefuseRestored = err == nil && final.Refuse == baseline.Refuse
		receipt.RequiredRestored = err == nil && maps.Equal(final.Required, baseline.Required)
		receipt.StaleRestored = err == nil && final.Timeout == baseline.Timeout && final.Autoclose == baseline.Autoclose
	}
	entries, err := c.cluster.Configuration(ctx)
	fail(err)
	receipt.CentralDeferAbsent = err == nil && !slices.ContainsFunc(entries, func(e ceph.ConfigEntry) bool { return e.Name == mdsSessionsDeferOption })
	if err == nil && !receipt.CentralDeferAbsent {
		fail(errors.New("mds_defer_session_stale central entry remained after restoration"))
	}
	receipt.DaemonDeferStale, err = c.mdsSessionsWaitDaemonConfig(ctx, mdsSessionsDeferOption, x.baseline.DeferStale)
	fail(err)
	receipt.StaleRestored = receipt.StaleRestored && receipt.CentralDeferAbsent && err == nil
	blocks, err := c.mdsSessionsBlocklist(ctx)
	fail(err)
	if err != nil {
		return
	}
	receipt.BlocklistRestored = true
	for _, entry := range blocks {
		if entry.Range || mdsSessionsFindBlock(x.baseline.Blocklist, entry.Address) != nil {
			continue
		}
		if _, ok := x.owned[entry.Address]; !ok {
			// New, but not attributable to an owned session address: report
			// it and leave it untouched.
			receipt.UnownedBlocklist = append(receipt.UnownedBlocklist, entry)
			continue
		}
		var recorded *mdsSessionsMutation
		for _, m := range x.mutations {
			if m.Kind == "eviction" && m.Address == entry.Address && m.Until != "" {
				recorded = m
			}
		}
		if recorded != nil && recorded.Until != entry.Until {
			receipt.BlocklistRestored = false
			fail(errors.New("owned eviction blocklist entry changed outside the fixture; refusing removal"))
			continue
		}
		if _, err := c.cluster.Ceph(ctx, "osd", "blocklist", "rm", entry.Address); err != nil {
			receipt.BlocklistRestored = false
			fail(err)
			continue
		}
		receipt.RemovedBlocklist = append(receipt.RemovedBlocklist, entry)
	}
	final, err := c.mdsSessionsBlocklist(ctx)
	fail(err)
	receipt.FinalBlocklist = final
	if err != nil {
		receipt.BlocklistRestored = false
		return
	}
	for _, entry := range receipt.RemovedBlocklist {
		if mdsSessionsFindBlock(final, entry.Address) != nil {
			receipt.BlocklistRestored = false
			fail(errors.New("owned blocklist entry remained after removal"))
		}
	}
}
