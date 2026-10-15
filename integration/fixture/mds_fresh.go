package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
)

const mdsFreshSource = "7f793731f1b39eb4f465e960113d2363c311b964"
const mdsFreshJSONLimit = 256 << 10

type mdsFreshAddress struct {
	Type  string `json:"type"`
	Addr  string `json:"addr"`
	Nonce uint32 `json:"nonce"`
}
type mdsFreshDaemon struct {
	Name        string            `json:"name"`
	GID         uint64            `json:"gid"`
	Rank        int32             `json:"rank"`
	State       string            `json:"state"`
	Incarnation int32             `json:"incarnation"`
	Addresses   []mdsFreshAddress `json:"addresses"`
	Endpoint    string            `json:"endpoint"`
	Nonce       uint32            `json:"nonce"`
}
type mdsFreshOracle struct {
	Source         string           `json:"source_commit"`
	Token          string           `json:"token"`
	FSID           string           `json:"fsid"`
	FSCID          int64            `json:"fscid"`
	FSName         string           `json:"fs_name"`
	FSMapEpoch     uint32           `json:"fsmap_epoch"`
	MDSMapEpoch    uint32           `json:"mdsmap_epoch"`
	MetadataPool   int64            `json:"metadata_pool"`
	DataPools      []int64          `json:"data_pools"`
	ClientIdentity string           `json:"client_identity"`
	MDS            mdsFreshDaemon   `json:"mds"`
	BeforeSessions []cephfs.Session `json:"before_sessions"`
	Filesystem     json.RawMessage  `json:"filesystem"`
	FSDump         json.RawMessage  `json:"fs_dump"`
}
type mdsFreshScope struct {
	mu               sync.Mutex
	fs               *cephfs.Filesystem
	client           *ceph.ClientConfig
	out, token, name string
	oracle           mdsFreshOracle
	globalID         uint64
	otherGlobalID    uint64
	lastRequestID    int64
	cleanupAttempted bool
	cleanupErr       error
	removeFilesystem func(context.Context) error
}
type mdsFreshNativeFS struct {
	ID  int64 `json:"id"`
	Map struct {
		Epoch        uint32            `json:"epoch"`
		Name         string            `json:"fs_name"`
		Enabled      bool              `json:"enabled"`
		MaxMDS       int               `json:"max_mds"`
		MetadataPool int64             `json:"metadata_pool"`
		DataPools    []int64           `json:"data_pools"`
		In           []int             `json:"in"`
		Up           map[string]uint64 `json:"up"`
		Failed       []int             `json:"failed"`
		Stopped      []int             `json:"stopped"`
		Damaged      []int             `json:"damaged"`
		Info         map[string]struct {
			mdsFreshDaemon
			Addrs struct {
				Vector []mdsFreshAddress `json:"addrvec"`
			} `json:"addrs"`
			JoinFSCID int64 `json:"join_fscid"`
		} `json:"info"`
	} `json:"mdsmap"`
}

// Native map data stays consumer-only. A complete identity must agree with the
// provider-owned filesystem, daemon descriptor and independent native OSDMap.
func parseMDSFreshOracle(p profile, token, name, identity string, owned map[string]bool, status *cephfs.FilesystemStatus, get, dump, osd []byte) (mdsFreshOracle, error) {
	var result mdsFreshOracle
	if len(token) != 32 || name != "ceph-msgr-fresh-"+token || identity != "client.mds-fresh-"+token || status == nil || status.FilesystemName != name || len(owned) != 1 {
		return result, errors.New("MDS fresh owned scope differs")
	}
	if _, err := hex.DecodeString(token); err != nil {
		return result, errors.New("MDS fresh token differs")
	}
	fsidHex := strings.ReplaceAll(p.fsid, "-", "")
	if len(p.fsid) != 36 || p.fsid[8] != '-' || p.fsid[13] != '-' || p.fsid[18] != '-' || p.fsid[23] != '-' || len(fsidHex) != 32 {
		return result, errors.New("MDS fresh fixture FSID is invalid")
	}
	if _, err := hex.DecodeString(fsidHex); err != nil {
		return result, errors.New("MDS fresh fixture FSID is invalid")
	}
	for _, data := range [][]byte{get, dump, osd} {
		if len(data) == 0 || len(data) > mdsFreshJSONLimit || !json.Valid(data) {
			return result, errors.New("MDS fresh native JSON exceeds its bound or is invalid")
		}
	}
	var fs mdsFreshNativeFS
	var directory struct {
		Epoch       uint32            `json:"epoch"`
		Filesystems []json.RawMessage `json:"filesystems"`
		Standbys    []json.RawMessage `json:"standbys"`
	}
	var pools struct {
		FSID  string `json:"fsid"`
		Pools []struct {
			ID   int64  `json:"pool"`
			Name string `json:"pool_name"`
		} `json:"pools"`
	}
	if json.Unmarshal(get, &fs) != nil || json.Unmarshal(dump, &directory) != nil || json.Unmarshal(osd, &pools) != nil || p.fsid == "" || pools.FSID != p.fsid {
		return result, errors.New("MDS fresh native cluster identity differs")
	}
	if directory.Epoch == 0 || len(directory.Filesystems) != 1 || directory.Standbys == nil || len(directory.Standbys) != 0 || fs.ID < 0 || fs.ID > math.MaxInt32 || fs.ID != status.FilesystemID || status.MaxMDS != 1 || len(status.Active) != 1 || len(status.Standby)+len(status.StandbyReplay) != 0 {
		return result, errors.New("MDS fresh filesystem or rank topology differs")
	}
	var fromGet, fromDump any
	if json.Unmarshal(get, &fromGet) != nil || json.Unmarshal(directory.Filesystems[0], &fromDump) != nil || !reflect.DeepEqual(fromGet, fromDump) {
		return result, errors.New("MDS fresh native fs get and fs dump disagree")
	}
	m := fs.Map
	if m.Name != name || m.Epoch == 0 || !m.Enabled || m.MaxMDS != 1 || !reflect.DeepEqual(m.In, []int{0}) || len(m.Up) != 1 || len(m.Info) != 1 || len(m.Failed)+len(m.Stopped)+len(m.Damaged) != 0 || m.MetadataPool < 0 || len(m.DataPools) != 1 || m.DataPools[0] < 0 || m.DataPools[0] == m.MetadataPool {
		return result, errors.New("MDS fresh native map metadata differs")
	}
	a := status.Active[0]
	info, ok := m.Info["gid_"+strconv.FormatUint(a.GID, 10)]
	if !ok || !a.Owned || !owned[a.Name] || a.Rank != 0 || a.GID == 0 || a.State != "up:active" || info.Name != a.Name || info.GID != a.GID || info.Rank != 0 || info.State != a.State || info.JoinFSCID != fs.ID || info.Incarnation < 0 || m.Up["mds_0"] != a.GID {
		return result, errors.New("MDS fresh active native daemon identity differs")
	}
	wantedPools := map[string]int64{name + "-meta": m.MetadataPool, name + "-data": m.DataPools[0]}
	seen := map[string]bool{}
	for _, pool := range pools.Pools {
		if want, exists := wantedPools[pool.Name]; exists {
			if seen[pool.Name] || want != pool.ID {
				return result, errors.New("MDS fresh native owned pool identity differs")
			}
			seen[pool.Name] = true
		}
	}
	if len(seen) != 2 || len(info.Addrs.Vector) == 0 || len(info.Addrs.Vector) > 16 {
		return result, errors.New("MDS fresh owned pools or address vector missing")
	}
	selected := 0
	for _, address := range info.Addrs.Vector {
		endpoint, err := netip.ParseAddrPort(address.Addr)
		if err != nil || endpoint.Port() == 0 || endpoint.Addr().IsUnspecified() || endpoint.Addr().IsMulticast() || endpoint.Addr().Is4In6() || endpoint.Addr().Zone() != "" || address.Type != "v1" && address.Type != "v2" {
			return result, errors.New("MDS fresh native address is invalid")
		}
		family := "6"
		if endpoint.Addr().Is4() {
			family = "4"
		}
		if address.Type == "v2" && family == p.ipFamily {
			if address.Nonce == 0 {
				return result, errors.New("MDS fresh selected daemon nonce is zero")
			}
			selected++
			info.Endpoint, info.Nonce = address.Addr, address.Nonce
		}
	}
	if selected != 1 {
		return result, errors.New("MDS fresh requires exactly one advertised v2 address of the selected family")
	}
	info.Addresses = info.Addrs.Vector
	return mdsFreshOracle{Source: mdsFreshSource, Token: token, FSID: p.fsid, FSCID: fs.ID, FSName: name,
		FSMapEpoch: directory.Epoch, MDSMapEpoch: m.Epoch, MetadataPool: m.MetadataPool, DataPools: m.DataPools,
		ClientIdentity: identity, MDS: info.mdsFreshDaemon, Filesystem: bytes.Clone(get), FSDump: bytes.Clone(dump)}, nil
}

func writeMDSFreshJSON(out, name string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil || len(data) > 1<<20 {
		return errors.Join(err, errors.New("MDS fresh public evidence cannot be encoded within its bound"))
	}
	f, err := os.CreateTemp(out, name+"-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(append(data, '\n')); err == nil {
		err = f.Chmod(0644)
	}
	err = errors.Join(err, f.Close())
	if err != nil {
		return err
	}
	return os.Rename(f.Name(), filepath.Join(out, name))
}

func (c *fixtureCluster) mdsFreshOwnership() map[string]string {
	if c == nil || c.cluster == nil || c.profile.mdsFresh != "1" {
		return nil
	}
	result := map[string]string{}
	for _, fs := range cephfs.Filesystems(c.cluster) {
		if fs == nil {
			continue
		}
		for _, daemon := range fs.MDSs() {
			if daemon != nil && daemon.Container != nil && daemon.GetContainerID() != "" {
				result["mds."+daemon.ID] = daemon.GetContainerID()
			}
		}
	}
	return result
}

func (c *fixtureCluster) readMDSFreshOracle(ctx context.Context) (mdsFreshOracle, error) {
	s := c.mdsFresh
	var zero mdsFreshOracle
	for attempt := 0; attempt < 3; attempt++ {
		get, err := c.cluster.Ceph(ctx, "fs", "get", s.name, "--format", "json")
		if err != nil {
			return zero, err
		}
		dump, err := c.cluster.Ceph(ctx, "fs", "dump", "--format", "json")
		if err != nil {
			return zero, err
		}
		osd, err := c.cluster.Ceph(ctx, "osd", "dump", "--format", "json")
		if err != nil {
			return zero, err
		}
		status, err := s.fs.MDSStatus(ctx)
		if err != nil {
			return zero, err
		}
		owned := map[string]bool{}
		for _, daemon := range s.fs.MDSs() {
			if daemon != nil && daemon.Container != nil && daemon.GetContainerID() != "" {
				owned[daemon.ID] = true
			}
		}
		result, err := parseMDSFreshOracle(c.profile, s.token, s.name, "client.mds-fresh-"+s.token, owned, status, get, dump, osd)
		if err == nil {
			return result, nil
		}
		if attempt == 2 {
			return zero, err
		}
		// Only read-only map observations are repeated. No mutating command
		// is replayed when a map is changing or startup becomes uncertain.
	}
	return zero, errors.New("MDS fresh native snapshot unavailable")
}

func (c *fixtureCluster) prepareMDSFresh(ctx context.Context, out string) error {
	if c.profile.mdsFresh != "1" || c.profile.osd != "1" || !c.profile.mdsFreshOSDAllowed() || c.profile.osdReplicas != "1" || c.mdsFresh != nil {
		return errors.New("invalid MDS fresh fixture preparation scope")
	}
	ctx, stop := context.WithTimeout(ctx, fixtureStartupTimeout)
	defer stop()
	initial, err := c.cluster.Ceph(ctx, "fs", "dump", "--format", "json")
	var directory struct {
		Filesystems []json.RawMessage `json:"filesystems"`
		Standbys    []json.RawMessage `json:"standbys"`
	}
	if err != nil || len(initial) > mdsFreshJSONLimit || json.Unmarshal(initial, &directory) != nil || directory.Filesystems == nil || directory.Standbys == nil || len(directory.Filesystems)+len(directory.Standbys) != 0 {
		return errors.New("MDS fresh preparation requires an independently observed empty filesystem directory")
	}
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return err
	}
	s := &mdsFreshScope{out: out, token: hex.EncodeToString(token[:])}
	s.name = "ceph-msgr-fresh-" + s.token
	s.removeFilesystem = func(call context.Context) error { return cephfs.Remove(call, c.cluster, s.name) }
	c.mdsFresh = s
	// Record names before the first mutation, including partial Start errors.
	if err := writeMDSFreshJSON(out, "mds-fresh-owned.json", map[string]any{"source_commit": mdsFreshSource, "token": s.token, "fsid": c.profile.fsid, "fs_name": s.name}); err != nil {
		return err
	}
	s.fs, err = cephfs.Start(ctx, c.cluster, mdsFreshFilesystemConfig(c.profile, s.name))
	if err != nil {
		return fmt.Errorf("prepare owned MDS fresh filesystem: %w", err)
	}
	if err := s.fs.WaitReady(ctx); err != nil {
		return err
	}
	s.oracle, err = c.readMDSFreshOracle(ctx)
	if err != nil {
		return err
	}
	clientName := "mds-fresh-" + s.token
	// AUTH/MGR/MDS are requested, but native KeyServerData::get_caps returns
	// an empty capability ticket for an existing identity's omitted service.
	// Requesting a MGR ticket does not require granting MGR permissions.
	s.client, err = c.cluster.CreateClient(ctx, clientName, mdsFreshCaps(s.name))
	if err != nil {
		return fmt.Errorf("create dedicated MDS fresh identity: %w", err)
	}
	_, keyring, err := s.client.ConnectionConfig()
	if err != nil {
		return err
	}
	var key string
	for _, line := range strings.Split(string(keyring), "\n") {
		k, v, ok := strings.Cut(line, "=")
		if ok && strings.TrimSpace(k) == "key" {
			if key != "" {
				return errors.New("dedicated MDS fresh keyring has multiple keys")
			}
			key = strings.TrimSpace(v)
		}
	}
	if key == "" {
		return errors.New("dedicated MDS fresh key missing")
	}
	if err := os.WriteFile(filepath.Join(out, "mds-fresh.key"), []byte(key+"\n"), 0600); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(out, "mds-fresh.keyring"), keyring, 0600); err != nil {
		return err
	}
	s.oracle.BeforeSessions, err = s.fs.Sessions(ctx)
	if err != nil || len(s.oracle.BeforeSessions) > 4096 {
		return errors.Join(err, errors.New("native MDS fresh baseline sessions unavailable within bound"))
	}
	var peers map[string]string
	if json.Unmarshal([]byte(c.profile.peers), &peers) != nil || peers == nil {
		return errors.New("MDS fresh relay peer inventory missing")
	}
	peers["mds."+s.oracle.MDS.Name] = s.oracle.MDS.Endpoint
	encoded, _ := json.Marshal(peers)
	c.profile.peers, c.profile.mdsFreshIdentity = string(encoded), s.oracle.ClientIdentity
	if err := os.WriteFile(filepath.Join(out, "peers.json"), encoded, 0600); err != nil {
		return err
	}
	return writeMDSFreshJSON(out, "mds-fresh-oracle.json", s.oracle)
}

func mdsFreshCaps(name string) ceph.ClientCaps {
	return ceph.ClientCaps{Mon: "allow r", MDS: "allow r fsname=" + name}
}

func mdsFreshFilesystemConfig(p profile, name string) cephfs.Config {
	return cephfs.Config{Name: name, ActiveMDS: 1,
		MetadataPool: ceph.PoolConfig{Name: name + "-meta", PGNum: 16, Replicas: 1, MinSize: 1, NoHashPSPool: p.mdsCommands == "1"},
		DataPool:     ceph.PoolConfig{Name: name + "-data", PGNum: 1, Replicas: 1, MinSize: 1, NoHashPSPool: p.mdsCommands == "1"}}
}

func parseMDSFreshObservation(fields []string, token string, previousID int64, previousGlobalID uint64) (int64, uint64, error) {
	if len(fields) != 3 || len(token) != 32 || fields[1] != token {
		return 0, 0, errors.New("invalid owned MDS fresh observation scope")
	}
	if _, err := hex.DecodeString(token); err != nil {
		return 0, 0, errors.New("invalid owned MDS fresh observation token")
	}
	id, err := strconv.ParseInt(fields[0], 10, 64)
	gid, gidErr := strconv.ParseUint(fields[2], 10, 64)
	if err != nil || gidErr != nil || id <= previousID || id <= 0 || gid == 0 || previousGlobalID != 0 && gid != previousGlobalID || strconv.FormatInt(id, 10) != fields[0] || strconv.FormatUint(gid, 10) != fields[2] {
		return 0, 0, errors.New("invalid MDS fresh observation identity")
	}
	return id, gid, nil
}

func lockMDSFresh(ctx context.Context, mu *sync.Mutex) error {
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if mu.TryLock() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

func (c *fixtureCluster) observeMDSFresh(ctx context.Context, out string, fields []string) error {
	s := c.mdsFresh
	if c.profile.mdsFresh != "1" || s == nil || s.fs == nil || s.out != out {
		return errors.New("invalid owned MDS fresh observation scope")
	}
	ctx, stop := context.WithTimeout(ctx, 15*time.Second)
	defer stop()
	if err := lockMDSFresh(ctx, &s.mu); err != nil {
		return err
	}
	defer s.mu.Unlock()
	if s.cleanupAttempted {
		return errors.New("MDS fresh observation requested after cleanup began")
	}
	var id int64
	var gid uint64
	var gids []uint64
	var err error
	if c.profile.mdsCommands == "1" {
		id, gids, err = parseMDSCommandsObservation(fields, s.token, s.lastRequestID, s.globalID, s.otherGlobalID)
		if err == nil {
			gid = gids[0]
		}
	} else {
		id, gid, err = parseMDSFreshObservation(fields, s.token, s.lastRequestID, s.globalID)
	}
	if err != nil {
		return err
	}
	current, err := c.readMDSFreshOracle(ctx)
	if err != nil {
		return err
	}
	if current.FSID != s.oracle.FSID || current.FSCID != s.oracle.FSCID || current.MDS.GID != s.oracle.MDS.GID || current.MDS.Endpoint != s.oracle.MDS.Endpoint || current.MDS.Nonce != s.oracle.MDS.Nonce || current.MDS.Incarnation != s.oracle.MDS.Incarnation || current.MetadataPool != s.oracle.MetadataPool || !reflect.DeepEqual(current.DataPools, s.oracle.DataPools) {
		return errors.New("MDS fresh native target changed during the owned observation")
	}
	sessions, err := s.fs.Sessions(ctx)
	if err != nil || len(sessions) > 4096 {
		return errors.Join(err, errors.New("MDS fresh native sessions unavailable within bound"))
	}
	present := false
	var presentIDs []uint64
	for _, session := range sessions {
		if session.Rank != 0 {
			return errors.New("MDS fresh session observation includes another rank")
		}
		present = present || session.ID == gid
		for _, expected := range gids {
			if session.ID == expected {
				present, presentIDs = true, append(presentIDs, expected)
			}
		}
	}
	result := struct {
		Source     string           `json:"source_commit"`
		Token      string           `json:"token"`
		RequestID  int64            `json:"request_id"`
		FSID       string           `json:"fsid"`
		FSCID      int64            `json:"fscid"`
		FSName     string           `json:"fs_name"`
		MDSGID     uint64           `json:"mds_gid"`
		Rank       int32            `json:"rank"`
		GlobalID   uint64           `json:"expected_global_id"`
		GlobalIDs  []uint64         `json:"expected_global_ids,omitempty"`
		PresentIDs []uint64         `json:"present_global_ids,omitempty"`
		Present    bool             `json:"expected_global_id_present"`
		Sessions   []cephfs.Session `json:"sessions"`
		Filesystem json.RawMessage  `json:"filesystem"`
		FSDump     json.RawMessage  `json:"fs_dump"`
	}{mdsFreshSource, s.token, id, current.FSID, current.FSCID, current.FSName, current.MDS.GID, 0, gid, gids, presentIDs, present, sessions, current.Filesystem, current.FSDump}
	if err := writeMDSFreshJSON(out, "mds-fresh-session-result.json", result); err != nil {
		return err
	}
	s.lastRequestID, s.globalID = id, gid
	if len(gids) == 2 {
		s.otherGlobalID = gids[1]
	}
	if present {
		return errors.New("fresh Messenger client's GlobalID unexpectedly owns an open native MDS session")
	}
	return nil
}

// One attempt is cached before calling a mutating provider API. Unknown Remove
// or identity-revocation results are never replayed by the Terminate fallback.
func (c *fixtureCluster) finishMDSFresh(ctx context.Context) error {
	if c == nil || c.mdsFresh == nil {
		return nil
	}
	s := c.mdsFresh
	ctx, stop := context.WithTimeout(ctx, 45*time.Second)
	defer stop()
	if err := lockMDSFresh(ctx, &s.mu); err != nil {
		return err
	}
	defer s.mu.Unlock()
	if s.cleanupAttempted {
		return s.cleanupErr
	}
	s.cleanupAttempted = true
	receipt := struct {
		Source                 string          `json:"source_commit"`
		Token                  string          `json:"token"`
		FSID                   string          `json:"fsid"`
		FSName                 string          `json:"fs_name"`
		FSCID                  int64           `json:"fscid"`
		MetadataPool           int64           `json:"metadata_pool"`
		DataPools              []int64         `json:"data_pools"`
		Attempted              bool            `json:"attempted"`
		RemovePending          bool            `json:"remove_pending"`
		FilesystemRemoved      bool            `json:"filesystem_removed"`
		PoolsRemoved           bool            `json:"pools_removed"`
		IdentityRemoved        bool            `json:"identity_removed"`
		CommandIdentityRemoved bool            `json:"command_identity_removed,omitempty"`
		Completed              bool            `json:"completed"`
		Error                  string          `json:"error,omitempty"`
		FSDump                 json.RawMessage `json:"fs_dump,omitempty"`
		OSDDump                json.RawMessage `json:"osd_dump,omitempty"`
	}{Source: mdsFreshSource, Token: s.token, FSID: c.profile.fsid, FSName: s.name, FSCID: s.oracle.FSCID, MetadataPool: s.oracle.MetadataPool, DataPools: s.oracle.DataPools, Attempted: true, RemovePending: true}
	if err := writeMDSFreshJSON(s.out, "mds-fresh-cleanup.json", receipt); err != nil {
		s.cleanupErr = err
		return err
	}
	var removeErr error
	if s.removeFilesystem == nil {
		removeErr = errors.New("owned MDS fresh removal operation unavailable")
	} else {
		removeErr = s.removeFilesystem(ctx)
	}
	if removeErr == nil {
		receipt.RemovePending = false
		receipt.FSDump, removeErr = c.cluster.Ceph(ctx, "fs", "dump", "--format", "json")
		var fs struct {
			Filesystems []struct {
				ID  int64 `json:"id"`
				Map struct {
					Name string `json:"fs_name"`
				} `json:"mdsmap"`
			} `json:"filesystems"`
		}
		if removeErr == nil && (len(receipt.FSDump) > mdsFreshJSONLimit || json.Unmarshal(receipt.FSDump, &fs) != nil || fs.Filesystems == nil) {
			removeErr = errors.New("MDS fresh cleanup native FSMap unavailable")
		}
		if removeErr == nil {
			receipt.FilesystemRemoved = true
			for _, f := range fs.Filesystems {
				if f.Map.Name == s.name || s.oracle.FSCID > 0 && f.ID == s.oracle.FSCID {
					receipt.FilesystemRemoved = false
					removeErr = errors.New("owned MDS fresh filesystem remained after Remove")
				}
			}
		}
		if removeErr == nil {
			receipt.OSDDump, removeErr = c.cluster.Ceph(ctx, "osd", "dump", "--format", "json")
			var pools struct {
				FSID  string `json:"fsid"`
				Pools []struct {
					Name string `json:"pool_name"`
				} `json:"pools"`
			}
			if removeErr == nil && (len(receipt.OSDDump) > mdsFreshJSONLimit || json.Unmarshal(receipt.OSDDump, &pools) != nil || pools.FSID != c.profile.fsid || pools.Pools == nil) {
				removeErr = errors.New("MDS fresh cleanup native OSDMap unavailable")
			}
			if removeErr == nil {
				receipt.PoolsRemoved = true
				for _, p := range pools.Pools {
					if p.Name == s.name+"-meta" || p.Name == s.name+"-data" {
						receipt.PoolsRemoved = false
						removeErr = errors.New("owned MDS fresh pools remained after Remove")
					}
				}
			}
		}
	}
	if removeErr == nil && c.mdsCommands != nil && c.mdsCommands.client != nil {
		removeErr = c.cluster.DeleteClient(ctx, c.mdsCommands.client)
		receipt.CommandIdentityRemoved = removeErr == nil
	}
	if removeErr == nil && s.client != nil {
		removeErr = c.cluster.DeleteClient(ctx, s.client)
		receipt.IdentityRemoved = removeErr == nil
	}
	receipt.Completed = removeErr == nil
	if removeErr != nil {
		receipt.Error = removeErr.Error()
	}
	s.cleanupErr = errors.Join(removeErr, writeMDSFreshJSON(s.out, "mds-fresh-cleanup.json", receipt))
	return s.cleanupErr
}
