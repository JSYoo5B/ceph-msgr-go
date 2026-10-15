package integration_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jsyoo5b/ceph-msgr-go/cephmsgr"
	"github.com/jsyoo5b/ceph-msgr-go/internal/testcluster"
)

const mdsMapsSource = "7f793731f1b39eb4f465e960113d2363c311b964"

type nativeMDSMaps struct {
	Source       string          `json:"source_commit"`
	Token        string          `json:"token"`
	FSID         string          `json:"fsid"`
	FSName       string          `json:"fs_name"`
	FSCID        int32           `json:"fscid"`
	FSMapEpoch   uint32          `json:"fsmap_epoch"`
	BeforeEpoch  uint32          `json:"before_fsmap_epoch"`
	DefaultFSCID int32           `json:"default_fscid"`
	MDSMapEpoch  uint32          `json:"mdsmap_epoch"`
	MetadataPool int64           `json:"metadata_pool"`
	DataPools    []int64         `json:"data_pools"`
	Filesystem   json.RawMessage `json:"filesystem"`
	FSDump       json.RawMessage `json:"fs_dump"`
	Finished     bool            `json:"finished"`
	Cleanup      struct {
		Deleted  []mdsMapCleanupItem `json:"deleted"`
		Retained []mdsMapCleanupItem `json:"retained_for_fixture_teardown"`
	} `json:"cleanup"`
	UnresolvedMutation *string `json:"unresolved_mutation"`
}

type mdsMapCleanupItem struct {
	Kind   string `json:"kind"`
	Name   string `json:"name"`
	ID     int64  `json:"id"`
	Reason string `json:"reason,omitempty"`
}

// Only the disposable native controller constructs and interprets commands.
// Its complete fs dump/fs get JSON is retained alongside parsed wire maps.
func mdsMapsControl(ctx context.Context, dir, action string) (nativeMDSMaps, error) {
	var result nativeMDSMaps
	if err := testcluster.WritePublicEvidence(filepath.Join(dir, "mds-maps-input.json"), []byte("{}\n")); err != nil {
		return result, err
	}
	path := filepath.Join(dir, "mds-maps-result.json")
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return result, err
	}
	if err := testcluster.ControlDaemon(ctx, dir, "verify", "mds-maps", action); err != nil {
		return result, err
	}
	f, err := os.Open(path)
	if err != nil {
		return result, err
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if err != nil {
		return result, err
	}
	if len(data) > 1<<20 {
		return result, errors.New("native MDS map evidence exceeds limit")
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return result, err
	}
	if result.Source != mdsMapsSource || len(result.Token) != 32 || result.FSID != os.Getenv("CEPH_MSGR_FSID") {
		return result, errors.New("native MDS map evidence provenance differs")
	}
	if _, err := hex.DecodeString(result.Token); err != nil {
		return result, errors.New("native MDS map ownership token is invalid")
	}
	return result, nil
}

func mdsMapsRawFS(t *testing.T, m cephmsgr.FSMapUserUpdate) {
	t.Helper()
	if m.SourceGeneration == 0 || len(m.RawFront) < 10 || !bytes.Equal(m.RawFront[4:], m.RawMap) || binary.LittleEndian.Uint32(m.RawFront) != m.Epoch {
		t.Fatal("FSMapUser raw envelope or local delivery provenance differs")
	}
}

func mdsMapsCompare(t *testing.T, native nativeMDSMaps, fs cephmsgr.FSMapUserUpdate, m cephmsgr.MDSMapUpdate) {
	t.Helper()
	mdsMapsRawFS(t, fs)
	if fs.Epoch != native.FSMapEpoch || fs.LegacyClientFSCID != native.DefaultFSCID || len(fs.Filesystems) != 1 || fs.Filesystems[0].ID != native.FSCID || fs.Filesystems[0].Name != native.FSName {
		t.Fatal("FSMapUser differs from independent native filesystem directory", fs, native)
	}
	var f struct {
		ID  int32 `json:"id"`
		Map struct {
			Epoch        uint32                     `json:"epoch"`
			Name         string                     `json:"fs_name"`
			MetadataPool int64                      `json:"metadata_pool"`
			DataPools    []int64                    `json:"data_pools"`
			Enabled      bool                       `json:"enabled"`
			Info         map[string]json.RawMessage `json:"info"`
			Up           map[string]json.RawMessage `json:"up"`
			In           []int32                    `json:"in"`
			Failed       []int32                    `json:"failed"`
			Stopped      []int32                    `json:"stopped"`
			Damaged      []int32                    `json:"damaged"`
		} `json:"mdsmap"`
	}
	var dump struct {
		Epoch        uint32            `json:"epoch"`
		DefaultFSCID int32             `json:"default_fscid"`
		Filesystems  []json.RawMessage `json:"filesystems"`
		Standbys     []json.RawMessage `json:"standbys"`
	}
	if err := json.Unmarshal(native.Filesystem, &f); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(native.FSDump, &dump); err != nil {
		t.Fatal(err)
	}
	if dump.Epoch != fs.Epoch || dump.DefaultFSCID != fs.LegacyClientFSCID || len(dump.Filesystems) != 1 || len(dump.Standbys) != 0 || f.ID != native.FSCID || f.Map.Name != native.FSName || f.Map.Epoch != native.MDSMapEpoch || f.Map.MetadataPool != native.MetadataPool || !reflect.DeepEqual(f.Map.DataPools, native.DataPools) || !f.Map.Enabled {
		t.Fatal("native full JSON and independently recorded filesystem identity differ")
	}
	w := m.Map
	if m.FSID != native.FSID || m.FilesystemID != native.FSCID || m.Epoch != f.Map.Epoch || w.Epoch != m.Epoch || w.Name != f.Map.Name || w.MetadataPool != f.Map.MetadataPool || !reflect.DeepEqual(w.DataPools, f.Map.DataPools) || w.Enabled != f.Map.Enabled || m.SourceGeneration == 0 {
		t.Fatal("received MDSMap identity, epoch, pools or enabled state differs from native fs get", m, native)
	}
	if len(w.Daemons)+len(w.Up)+len(w.In)+len(w.Failed)+len(w.Stopped)+len(w.Damaged) != 0 || len(f.Map.Info)+len(f.Map.Up)+len(f.Map.In)+len(f.Map.Failed)+len(f.Map.Stopped)+len(f.Map.Damaged) != 0 {
		t.Fatal("empty native filesystem unexpectedly has daemon/rank state")
	}
	// These are envelope/retention checks, not native CLI blob equality. The
	// pinned MON exposes fs dump/fs get JSON, without an fs/mds getmap command.
	uuid, err := hex.DecodeString(strings.ReplaceAll(native.FSID, "-", ""))
	if err != nil || len(uuid) != 16 || len(m.RawFront) < 28 || !bytes.Equal(m.RawFront[:16], uuid) || binary.LittleEndian.Uint32(m.RawFront[16:]) != m.Epoch {
		t.Fatal("MDSMap raw cluster/epoch envelope differs")
	}
	length := uint64(binary.LittleEndian.Uint32(m.RawFront[20:]))
	if length != uint64(len(m.RawMap)) || length+28 > uint64(len(m.RawFront)) || !bytes.Equal(m.RawFront[24:24+int(length)], m.RawMap) {
		t.Fatal("MDSMap raw nested map retention differs")
	}
	nameOffset := 24 + int(length)
	nameLength := uint64(binary.LittleEndian.Uint32(m.RawFront[nameOffset:]))
	if uint64(nameOffset)+4+nameLength != uint64(len(m.RawFront)) || string(m.RawFront[nameOffset+4:]) != m.MapFSName {
		t.Fatal("MDSMap raw trailing filesystem name differs")
	}
}

func TestCephMDSMapsIntegration(t *testing.T) {
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if dir == "" || os.Getenv("CEPH_MSGR_TEST_OSD") != "1" || os.Getenv("CEPH_MSGR_TEST_OSD_PROFILE") != "replicated-pg-v8" || os.Getenv("CEPH_MSGR_TEST_OSD_REPLICAS") != "1" {
		t.Skip("MDS map observation requires the disposable single-OSD fixture with no MDS daemon")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	options := integrationOptions(t)
	options.EnableMDSMaps = true
	// This fixture's hashpspool profile independently requires replicated-PG
	// admission. EnableOSD requests an OSD ticket, but this test never calls
	// OpenOSD. MDS-map opt-in itself neither requests an MDS ticket nor opens
	// an MDS connection. The fresh unknown-FSCID client uses these same options.
	options.EnableOSD = true
	options.OSDProfile = cephmsgr.OSDProfileReplicatedPG
	var osdDials atomic.Uint32
	dial := options.DialContext
	options.DialContext = func(call context.Context, network, endpoint string) (net.Conn, error) {
		_, role, _ := testcluster.SplitPeer(endpoint)
		if strings.HasPrefix(role, "osd.") {
			osdDials.Add(1)
		}
		return dial(call, network, endpoint)
	}
	c, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	initial := c.Snapshot()
	evidence := struct {
		NativeStart, NativeObserve, NativeFinish nativeMDSMaps
		InitialFS, CreatedFS, RemovedFS          cephmsgr.FSMapUserUpdate
		MDSMap                                   cephmsgr.MDSMapUpdate
		UnknownFSCID                             int32
		UnknownFSCIDWaitError                    string
		UnknownMONCommandPreserved               bool
		LocalNextCanceled, LifetimeCanceled      bool
		SameMONMDSBindingRefused, MONPreserved   bool
		FixtureOSDTicketObtained                 bool
		InitialOSDTicket, FinalOSDTicket         cephmsgr.TicketState
		InitialMONEndpoint, FinalMONEndpoint     string
		OSDDialAttempts                          uint32
		RenewedSourceMDSMaps                     []cephmsgr.MDSMapUpdate
		RenewedMONBindingAdmitted                bool
		CleanupErrors                            []string
	}{}
	evidence.FixtureOSDTicketObtained = !initial.OSDTicket.Expires.IsZero()
	evidence.InitialOSDTicket = initial.OSDTicket
	evidence.InitialMONEndpoint = initial.Monitor.Endpoint
	started, finished := false, false
	defer func() {
		evidence.OSDDialAttempts = osdDials.Load()
		evidence.FinalOSDTicket = c.Snapshot().OSDTicket
		if started && !finished {
			cleanup, stop := context.WithTimeout(context.Background(), 60*time.Second)
			defer stop()
			result, err := mdsMapsControl(cleanup, dir, "finish")
			evidence.NativeFinish = result
			if err != nil {
				evidence.CleanupErrors = append(evidence.CleanupErrors, err.Error())
				t.Error("native MDS map cleanup", err)
			}
		}
		data, err := json.MarshalIndent(evidence, "", "  ")
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(dir, "mds-maps-go.json"), append(data, '\n'))
		}
		if err != nil {
			t.Error("publish MDS map evidence", err)
		}
	}()
	fs, err := c.WatchFSMapUser(ctx, cephmsgr.FSMapUserOptions{MaxBufferedBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	local, stopLocal := context.WithCancel(ctx)
	stopLocal()
	if _, err := fs.Next(local); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled Next consumed or waited for a map", err)
	}
	evidence.LocalNextCanceled = true
	evidence.InitialFS, err = fs.Next(ctx)
	if err != nil || len(evidence.InitialFS.Filesystems) != 0 {
		t.Fatal("native initial filesystem directory is not empty", err)
	}
	mdsMapsRawFS(t, evidence.InitialFS)
	started = true // Cleanup must inspect the ownership journal even if start times out.
	evidence.NativeStart, err = mdsMapsControl(ctx, dir, "start")
	if err != nil {
		t.Fatal("create disposable native filesystem", err)
	}
	native := evidence.NativeStart
	if native.FSCID < 0 || native.FSCID == math.MaxInt32 || native.FSMapEpoch <= evidence.InitialFS.Epoch || native.BeforeEpoch != evidence.InitialFS.Epoch || native.FSName == "" || len(native.DataPools) != 1 || native.MDSMapEpoch == 0 {
		t.Fatal("invalid native filesystem creation receipt", native)
	}
	for {
		evidence.CreatedFS, err = fs.Next(ctx)
		if err != nil {
			t.Fatal("wait created filesystem directory", err)
		}
		if evidence.CreatedFS.Epoch >= native.FSMapEpoch {
			break
		}
	}
	mdsLife, stopMDS := context.WithCancel(ctx)
	defer stopMDS()
	mds, err := c.WatchMDSMaps(mdsLife, native.FSCID, cephmsgr.MDSMapOptions{MaxBufferedBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer mds.Close()
	evidence.MDSMap, err = mds.Next(ctx)
	if err != nil {
		t.Fatal("receive explicitly selected native MDSMap", err)
	}
	evidence.NativeObserve, err = mdsMapsControl(ctx, dir, "observe")
	if err != nil || evidence.NativeObserve.Token != native.Token {
		t.Fatal("independent native filesystem observation", err)
	}
	mdsMapsCompare(t, evidence.NativeObserve, evidence.CreatedFS, evidence.MDSMap)
	stopMDS()
	// Already accepted maps drain before the stable lifetime terminal.
	for n := 0; ; n++ {
		if n > 64 {
			t.Fatal("canceled MDS map queue did not terminate within its bound")
		}
		_, err := mds.Next(ctx)
		if errors.Is(err, context.Canceled) {
			break
		}
		if err != nil {
			t.Fatal("MDS map lifetime cancellation", err)
		}
	}
	evidence.LifetimeCanceled = true
	lastSource := evidence.MDSMap.SourceGeneration
	for attempts := 0; ; attempts++ {
		if attempts == 3 {
			t.Fatal("MON source changed repeatedly before its MDS binding fence could be observed")
		}
		probe, err := c.WatchMDSMaps(ctx, native.FSCID, cephmsgr.MDSMapOptions{})
		if errors.Is(err, cephmsgr.ErrMDSMapSourceBound) {
			break
		}
		if err != nil {
			t.Fatal("MDS binding fence setup", err)
		}
		// Authentication renewal may replace the physical MON source between
		// the accepted map and this call. A fresh source may bind once; require
		// a different local source and then check that source's own fence.
		wait, stop := context.WithTimeout(ctx, 3*time.Second)
		update, receiveErr := probe.Next(wait)
		stop()
		probe.Close()
		if receiveErr != nil || update.SourceGeneration <= lastSource || update.FSID != native.FSID || update.FilesystemID != native.FSCID || update.Map.Name != native.FSName {
			t.Fatalf("new MDS watch lacks a confirmed new MON source: error=%v previous_source=%d new_source=%d fsid=%s fscid=%d name=%s", receiveErr, lastSource, update.SourceGeneration, update.FSID, update.FilesystemID, update.Map.Name)
		}
		evidence.RenewedSourceMDSMaps = append(evidence.RenewedSourceMDSMaps, update)
		evidence.RenewedMONBindingAdmitted = true
		lastSource = update.SourceGeneration
	}
	evidence.SameMONMDSBindingRefused = true
	// Unknown positive FSCID is a silent native subscription miss, not a
	// remote authorization result. Use a fresh source, without implicit fallback.
	negative, err := cephmsgr.Dial(ctx, options)
	if err != nil {
		t.Fatal(err)
	}
	defer negative.Close()
	evidence.UnknownFSCID = native.FSCID + 1
	missing, err := negative.WatchMDSMaps(ctx, evidence.UnknownFSCID, cephmsgr.MDSMapOptions{})
	if err != nil {
		t.Fatal("unknown FSCID subscription setup", err)
	}
	defer missing.Close()
	short, stopShort := context.WithTimeout(ctx, time.Second)
	_, err = missing.Next(short)
	stopShort()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("unknown FSCID unexpectedly supplied a map", err)
	}
	evidence.UnknownFSCIDWaitError = err.Error()
	if _, err := negative.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"status","format":"json"}`)}); err != nil {
		t.Fatal("ordinary MON command after unknown FSCID wait cancellation", err)
	}
	evidence.UnknownMONCommandPreserved = true
	if _, err := c.MonCommand(ctx, cephmsgr.Command{JSON: []byte(`{"prefix":"status","format":"json"}`)}); err != nil {
		t.Fatal("ordinary MON command after map cancellation", err)
	}
	state := c.Snapshot()
	evidence.FinalMONEndpoint = state.Monitor.Endpoint
	if state.GlobalID != initial.GlobalID || state.FSID != initial.FSID || !state.Monitor.Ready || state.Manager.Ready || initial.OSDTicket.Expires.IsZero() || state.OSDTicket.Expires.IsZero() || osdDials.Load() != 0 {
		t.Fatalf("map observation identity/service boundary: global_id=%d/%d fsid=%s/%s mon_ready=%t mgr_ready=%t initial_osd_ticket=%t current_osd_ticket=%t osd_dials=%d", initial.GlobalID, state.GlobalID, initial.FSID, state.FSID, state.Monitor.Ready, state.Manager.Ready, !initial.OSDTicket.Expires.IsZero(), !state.OSDTicket.Expires.IsZero(), osdDials.Load())
	}
	evidence.MONPreserved = true
	evidence.NativeFinish, err = mdsMapsControl(ctx, dir, "finish")
	if err != nil {
		t.Fatal("remove disposable native filesystem", err)
	}
	finish := evidence.NativeFinish
	if !finish.Finished || finish.Token != native.Token || finish.UnresolvedMutation != nil || finish.FSMapEpoch <= native.FSMapEpoch {
		t.Fatal("native filesystem cleanup was not confirmed", finish)
	}
	var removed struct {
		Filesystems []json.RawMessage `json:"filesystems"`
	}
	if err := json.Unmarshal(finish.FSDump, &removed); err != nil || len(removed.Filesystems) != 0 {
		t.Fatal("native cleanup retained a filesystem", err)
	}
	deletedFS, pools := false, map[int64]bool{}
	for _, item := range finish.Cleanup.Deleted {
		if item.Kind == "filesystem" && item.ID == int64(native.FSCID) && item.Name == native.FSName {
			deletedFS = true
		} else if item.Kind == "pool" {
			pools[item.ID] = true
		}
	}
	for _, item := range finish.Cleanup.Retained {
		if item.Kind != "pool" || item.Reason != "pool deletion disabled; disposable fixture teardown owns final removal" {
			t.Fatal("native cleanup retained unexpected ownership", item)
		}
		pools[item.ID] = true
	}
	if !deletedFS || len(pools) != 2 || !pools[native.MetadataPool] || !pools[native.DataPools[0]] {
		t.Fatal("native cleanup did not account for the owned filesystem and both pools", finish)
	}
	finished = true
	for {
		evidence.RemovedFS, err = fs.Next(ctx)
		if err != nil {
			t.Fatal("wait native filesystem removal map", err)
		}
		if evidence.RemovedFS.Epoch >= finish.FSMapEpoch {
			break
		}
	}
	mdsMapsRawFS(t, evidence.RemovedFS)
	if len(evidence.RemovedFS.Filesystems) != 0 || evidence.RemovedFS.LegacyClientFSCID != finish.DefaultFSCID {
		t.Fatal("filesystem removal map differs from native fs dump")
	}
	t.Logf("native MON FSMapUser/MDSMap observed FSID=%s FSCID=%d name=%s epochs=%d/%d; canceled source fenced, MON preserved, filesystem removed and both pools accounted for; no MDS daemon/session or live CLI blob-equality claim", native.FSID, native.FSCID, native.FSName, evidence.CreatedFS.Epoch, evidence.MDSMap.Epoch)
}
