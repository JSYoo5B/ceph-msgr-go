package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// rbdMirrorPayload restates the fixture's deterministic image content, so
// that the test checks the native export digests against bytes it derives
// itself.
func rbdMirrorPayload(size, seed int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte((i*37+seed)%251 + 1)
	}
	return data
}

type rbdMirrorProgress struct {
	State             string `json:"state"`
	Remote            int64  `json:"remote_snapshot_timestamp"`
	Local             int64  `json:"local_snapshot_timestamp"`
	LastSnapshotBytes int64  `json:"last_snapshot_bytes"`
}

type rbdMirrorSession struct {
	PeerType  string `json:"peer_type"`
	GlobalID  uint64 `json:"global_id"`
	Address   string `json:"address"`
	State     string `json:"state"`
	Connected bool   `json:"connected"`
}

type rbdMirrorMonSession struct {
	Entity   string `json:"entity_name"`
	GlobalID uint64 `json:"global_id"`
}

type rbdMirrorReceipt struct {
	Schema       int    `json:"schema_version"`
	Selector     string `json:"selector"`
	Phase        string `json:"phase"`
	Error        string `json:"error"`
	CephImage    string `json:"ceph_image"`
	NetemImage   string `json:"netem_image"`
	NetemImageID string `json:"netem_image_id"`
	Mode         string `json:"mirror_mode"`
	Source       struct {
		FSID    string `json:"fsid"`
		Network string `json:"network"`
		OSD     int    `json:"osd"`
	} `json:"source"`
	Destination struct {
		FSID     string `json:"fsid"`
		Network  string `json:"network"`
		Monitors int    `json:"monitors"`
		OSDs     int    `json:"osds"`
	} `json:"destination"`
	Mirror struct {
		GlobalID           string `json:"global_image_id"`
		SourceImageID      string `json:"source_image_id"`
		DestinationImageID string `json:"destination_image_id"`
		Daemon             string `json:"daemon"`
		DaemonClient       string `json:"daemon_client"`
		SourcePeerClient   string `json:"source_peer_client"`
		InstanceID         string `json:"instance_id"`
		DaemonAddress      string `json:"daemon_source_address"`
	} `json:"mirror"`
	Initial struct {
		Bytes             int                   `json:"bytes"`
		SourceSHA256      string                `json:"source_sha256"`
		DestinationSHA256 string                `json:"destination_sha256"`
		Progress          rbdMirrorProgress     `json:"progress"`
		Sessions          []rbdMirrorSession    `json:"source_osd_sessions"`
		MonSessions       []rbdMirrorMonSession `json:"source_mon_sessions"`
	} `json:"initial"`
	Cut struct {
		Peer              string             `json:"peer"`
		HelperContainers  int                `json:"helper_containers"`
		Applied           bool               `json:"applied"`
		Loss              float64            `json:"loss"`
		Drops             uint64             `json:"drops"`
		PatchOffset       int                `json:"patch_offset"`
		PatchBytes        int                `json:"patch_bytes"`
		Samples           int                `json:"samples"`
		SampleSeconds     float64            `json:"sample_seconds"`
		Progress          rbdMirrorProgress  `json:"progress"`
		SourceSHA256      string             `json:"source_sha256"`
		DestinationSHA256 string             `json:"destination_sha256"`
		Sessions          []rbdMirrorSession `json:"source_osd_sessions"`
	} `json:"cut"`
	Restore struct {
		DropsBeforeRestore uint64             `json:"drops_before_restore"`
		Restored           bool               `json:"restored"`
		StatusAfter        string             `json:"lease_status_after_restore"`
		HelperContainers   int                `json:"helper_containers_after_restore"`
		CatchUpSeconds     float64            `json:"catch_up_seconds"`
		DestinationSHA256  string             `json:"destination_sha256"`
		Progress           rbdMirrorProgress  `json:"progress"`
		Sessions           []rbdMirrorSession `json:"source_osd_sessions"`
	} `json:"restore"`
	Cleanup struct {
		Attempted  bool `json:"attempted"`
		Containers int  `json:"containers"`
		Confirmed  bool `json:"confirmed"`
		Network    bool `json:"destination_network_removed"`
	} `json:"cleanup"`
}

const rbdMirrorTestSelector = "^TestCephRBDMirrorPathRecoveryIntegration$"

// rbdMirrorSourcePeer is the CephX user that "rbd mirror pool peer bootstrap
// create" makes on the source and that the daemon uses to reach it
// (src/librbd/api/Mirror.cc at v20.2.4). The daemon's own client name is a
// destination-side identity.
const rbdMirrorSourcePeer = "client.rbd-mirror-peer"

// rbdMirrorSessionFor returns the source OSD sessions from address, and
// those with globalID.
func rbdMirrorSessionFor(sessions []rbdMirrorSession, address string, globalID uint64) (fromAddress, withID []rbdMirrorSession) {
	for _, session := range sessions {
		if session.PeerType != "client" {
			continue
		}
		if address != "" && session.Address == address {
			fromAddress = append(fromAddress, session)
		}
		if globalID != 0 && session.GlobalID == globalID {
			withID = append(withID, session)
		}
	}
	return fromAddress, withID
}

// rbdMirrorPeerWitness returns the source OSD sessions from the daemon's
// address whose nonzero GlobalID a source MON session of the source peer
// identity carries: the address alone, or a MON session of another entity
// from that address, does not attribute a session to the daemon.
func rbdMirrorPeerWitness(osd []rbdMirrorSession, mons []rbdMirrorMonSession, address string) []rbdMirrorSession {
	if address == "" {
		return nil
	}
	peerIDs := map[uint64]bool{}
	for _, session := range mons {
		if session.Entity == rbdMirrorSourcePeer && session.GlobalID != 0 {
			peerIDs[session.GlobalID] = true
		}
	}
	var witness []rbdMirrorSession
	fromAddress, _ := rbdMirrorSessionFor(osd, address, 0)
	for _, session := range fromAddress {
		if session.GlobalID != 0 && peerIDs[session.GlobalID] {
			witness = append(witness, session)
		}
	}
	return witness
}

// TestCephRBDMirrorPathRecoveryIntegration is the local RBD snapshot-mirror
// checkpoint. The fixture's single-BlueStore cluster is the mirror source; the
// fixture starts an owned destination cluster and one native rbd-mirror
// daemon. One directed fault drops only the packets that the source OSD sends
// to the daemon's source-network address. While it holds, the daemon cannot
// copy a new mirror snapshot, and this Go client's own session to the same OSD
// still reads the native oracle object. After the restore the distinguishable
// patch arrives at the destination. The product implements no mirroring: the
// Go session is the control that attributes the fault to one peer path.
func TestCephRBDMirrorPathRecoveryIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_NATIVE_RBD_MIRROR") != "1" {
		t.Skip("requires the fixture-selected exclusive RBD mirror marker")
	}
	c, _, oracle, data := osdFixture(t)
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	defer cancel()
	var r rbdMirrorReceipt
	read := func() {
		t.Helper()
		raw, err := os.ReadFile(filepath.Join(dir, "rbd-mirror-receipt.json"))
		if err != nil || len(raw) > 1<<20 {
			t.Fatal("bounded RBD mirror receipt", err)
		}
		r = rbdMirrorReceipt{}
		if err := json.Unmarshal(raw, &r); err != nil {
			t.Fatal("decode RBD mirror receipt", err)
		}
	}
	step := func(action string, budget time.Duration) {
		t.Helper()
		call, stop := context.WithTimeout(ctx, budget)
		err := testcluster.RequestControl(call, dir, "rbd-mirror", "rbd-mirror-done", action)
		stop()
		if err != nil {
			t.Fatal("RBD mirror", action, err)
		}
		read()
		if r.Error != "" || r.Phase != action {
			t.Fatalf("RBD mirror %s: phase %q error %q", action, r.Phase, r.Error)
		}
	}
	summary := map[string]any{"selector": rbdMirrorTestSelector}
	started := false
	t.Cleanup(func() {
		if started && !r.Cleanup.Attempted {
			// A fresh bound: cancellation of the subject wait does not skip
			// the owned destination and daemon cleanup.
			cleanup, stop := context.WithTimeout(context.Background(), 3*time.Minute)
			err := testcluster.RequestControl(cleanup, dir, "rbd-mirror", "rbd-mirror-done", "finish")
			stop()
			if err != nil {
				t.Error("RBD mirror cleanup", err)
			}
		}
		raw, err := json.MarshalIndent(summary, "", "  ")
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(dir, "rbd-mirror-results.json"), append(raw, '\n'))
		}
		if err != nil {
			t.Error("RBD mirror results", err)
		}
	})

	before := rbdMirrorPayload(4<<20, 17)
	patch := rbdMirrorPayload(1<<20, 99)
	after := append([]byte{}, before...)
	copy(after[2<<20:], patch)
	beforeDigest, afterDigest := fmt.Sprintf("%x", sha256.Sum256(before)), fmt.Sprintf("%x", sha256.Sum256(after))

	started = true
	step("start", 12*time.Minute)
	if r.Schema != 1 || r.Selector != rbdMirrorTestSelector || r.Mode != "snapshot" || r.CephImage == "" || r.NetemImage == "" {
		t.Fatalf("RBD mirror receipt provenance: %+v", r)
	}
	if r.Source.FSID != c.Snapshot().FSID || r.Destination.FSID == "" || r.Destination.FSID == r.Source.FSID ||
		r.Source.Network == "" || r.Destination.Network == r.Source.Network || r.Destination.Monitors != 1 || r.Destination.OSDs != 1 || r.Source.OSD != int(oracle.ID) {
		t.Fatalf("source and destination identities: source %+v destination %+v, Go FSID %s osd.%d", r.Source, r.Destination, c.Snapshot().FSID, oracle.ID)
	}
	if r.Mirror.GlobalID == "" || r.Mirror.SourceImageID == "" || r.Mirror.DestinationImageID == "" || r.Mirror.Daemon == "" || r.Mirror.InstanceID == "" || r.Mirror.DaemonAddress == "" {
		t.Fatalf("mirror identities: %+v", r.Mirror)
	}
	if r.Initial.Bytes != len(before) || r.Initial.SourceSHA256 != beforeDigest || r.Initial.DestinationSHA256 != beforeDigest ||
		r.Initial.Progress.Remote == 0 || r.Initial.Progress.Local != r.Initial.Progress.Remote {
		t.Fatalf("initial data at the destination: %+v", r.Initial)
	}
	if r.Mirror.SourcePeerClient != rbdMirrorSourcePeer || r.Mirror.DaemonClient == "" || r.Mirror.DaemonClient == rbdMirrorSourcePeer {
		t.Fatalf("mirror CephX identities: source peer %q, daemon %q", r.Mirror.SourcePeerClient, r.Mirror.DaemonClient)
	}
	if witness := rbdMirrorPeerWitness(r.Initial.Sessions, r.Initial.MonSessions, r.Mirror.DaemonAddress); len(witness) == 0 {
		t.Fatalf("no source OSD session from %s has the GlobalID of a %s MON session: OSD %+v MON %+v", r.Mirror.DaemonAddress, rbdMirrorSourcePeer, r.Initial.Sessions, r.Initial.MonSessions)
	}

	// The Go session to the same source OSD exists before the fault.
	o, err := c.OpenOSD(ctx, cephmsgr.OSDTarget{ID: oracle.ID, Address: oracle.Address})
	if err != nil {
		t.Fatal("source OSD session", err)
	}
	readOracle := func(label string) cephmsgr.OSDReply {
		t.Helper()
		call, stop := context.WithTimeout(ctx, 20*time.Second)
		defer stop()
		reply, err := o.Request(call, osdObjectRequest(oracle, cephmsgr.OSDOperation{Code: cephmsgr.OSDStat}, cephmsgr.OSDOperation{Code: cephmsgr.OSDRead, Length: oracle.Size}))
		if err != nil {
			t.Fatal(label, "Go read", err)
		}
		checkOSDMetadata(t, reply, oracle)
		if len(reply.Operations) != 2 || reply.Operations[1].Result != 0 || !bytes.Equal(reply.Operations[1].Data, data) {
			t.Fatal(label, "Go read differs from the native oracle bytes", reply.Operations)
		}
		return reply
	}
	baseline := readOracle("before the fault")
	goID := baseline.Submission.GlobalID
	summary["go_global_id_nonzero"] = goID != 0

	step("cut", 4*time.Minute)
	if !r.Cut.Applied || r.Cut.Loss != 100 || r.Cut.Drops == 0 || r.Cut.HelperContainers == 0 || r.Cut.Peer != r.Mirror.DaemonAddress || r.Cut.PatchOffset != 2<<20 || r.Cut.PatchBytes != len(patch) {
		t.Fatalf("directed fault: %+v", r.Cut)
	}
	if r.Cut.Samples < 4 || r.Cut.SampleSeconds < 30 || r.Cut.Progress.State != "up+replaying" || r.Cut.Progress.Remote != r.Initial.Progress.Remote || r.Cut.Progress.Local != r.Initial.Progress.Local {
		t.Fatalf("mirror progress during the cut: %+v, before %+v", r.Cut, r.Initial.Progress)
	}
	if r.Cut.SourceSHA256 != afterDigest || r.Cut.DestinationSHA256 != beforeDigest {
		t.Fatalf("bytes during the cut: source %s destination %s", r.Cut.SourceSHA256, r.Cut.DestinationSHA256)
	}
	fromDaemon, _ := rbdMirrorSessionFor(r.Cut.Sessions, r.Cut.Peer, 0)
	_, goSessions := rbdMirrorSessionFor(r.Cut.Sessions, "", goID)
	if len(fromDaemon) == 0 || len(goSessions) == 0 {
		t.Fatalf("source OSD sessions during the cut: daemon %+v Go %d in %+v", fromDaemon, goID, r.Cut.Sessions)
	}
	for _, session := range goSessions {
		if session.Address == r.Cut.Peer || session.State != "READY" || !session.Connected {
			t.Fatalf("Go session to the source OSD during the cut: %+v, peer %s", session, r.Cut.Peer)
		}
	}
	// The same OSD keeps serving this Go session while it drops every
	// packet it sends to the daemon.
	during := readOracle("during the fault")
	if during.Submission.GlobalID != goID || during.Transaction <= baseline.Transaction {
		t.Fatal("Go read during the fault used another session", during.Submission, baseline.Submission)
	}

	step("restore", 7*time.Minute)
	if r.Restore.DropsBeforeRestore < r.Cut.Drops || !r.Restore.Restored || r.Restore.StatusAfter == "" || r.Restore.HelperContainers != 0 {
		t.Fatalf("fault restoration: %+v", r.Restore)
	}
	if r.Restore.DestinationSHA256 != afterDigest || r.Restore.Progress.Remote <= r.Initial.Progress.Remote ||
		r.Restore.Progress.Local != r.Restore.Progress.Remote || r.Restore.Progress.LastSnapshotBytes != int64(len(patch)) {
		t.Fatalf("catch-up after the restore: %+v, before %+v", r.Restore, r.Initial.Progress)
	}
	if fromDaemon, _ := rbdMirrorSessionFor(r.Restore.Sessions, r.Cut.Peer, 0); len(fromDaemon) == 0 {
		t.Fatalf("no source OSD session from the daemon after the restore: %+v", r.Restore.Sessions)
	}

	step("finish", 4*time.Minute)
	if !r.Cleanup.Attempted || !r.Cleanup.Confirmed || !r.Cleanup.Network || r.Cleanup.Containers == 0 {
		t.Fatalf("RBD mirror cleanup: %+v", r.Cleanup)
	}
	summary["phase"], summary["netem_image_id"], summary["drops"] = r.Phase, r.NetemImageID, r.Restore.DropsBeforeRestore
	summary["cut_samples"], summary["catch_up_seconds"], summary["owned_containers_removed"] = r.Cut.Samples, r.Restore.CatchUpSeconds, r.Cleanup.Containers
	t.Logf("RBD_MIRROR_PATH_RECOVERY source=%s destination=%s osd.%d dropped %d packets to rbd-mirror %s at %s; for %d samples over %.0fs the daemon stayed %s at snapshot %d while the source changed and this Go session read osd.%d; %.0fs after the restore snapshot %d arrived with %d bytes; %d owned containers and the destination network removed",
		r.Source.FSID, r.Destination.FSID, r.Source.OSD, r.Restore.DropsBeforeRestore, r.Mirror.Daemon, r.Cut.Peer, r.Cut.Samples, r.Cut.SampleSeconds, r.Cut.Progress.State, r.Initial.Progress.Remote,
		oracle.ID, r.Restore.CatchUpSeconds, r.Restore.Progress.Remote, r.Restore.Progress.LastSnapshotBytes, r.Cleanup.Containers)
}

func TestRBDMirrorPeerWitnessRequiresSourceIdentity(t *testing.T) {
	const daemon = "172.18.0.9"
	osd := []rbdMirrorSession{
		{PeerType: "client", GlobalID: 4410, Address: daemon, State: "READY", Connected: true},
		{PeerType: "client", GlobalID: 4207, Address: "172.18.0.4", State: "READY", Connected: true},
	}
	peer := []rbdMirrorMonSession{{Entity: rbdMirrorSourcePeer, GlobalID: 4410}}
	if witness := rbdMirrorPeerWitness(osd, peer, daemon); len(witness) != 1 || witness[0].GlobalID != 4410 {
		t.Fatal("source peer session not attributed", witness)
	}
	for name, tc := range map[string]struct {
		osd     []rbdMirrorSession
		mons    []rbdMirrorMonSession
		address string
	}{
		// The correct address and GlobalID under another CephX identity.
		"other entity":       {osd, []rbdMirrorMonSession{{Entity: "client.admin", GlobalID: 4410}}, daemon},
		"destination entity": {osd, []rbdMirrorMonSession{{Entity: "client.rbd-mirror.a", GlobalID: 4410}}, daemon},
		"zero GlobalID":      {[]rbdMirrorSession{{PeerType: "client", Address: daemon}}, []rbdMirrorMonSession{{Entity: rbdMirrorSourcePeer}}, daemon},
		"other GlobalID":     {osd, []rbdMirrorMonSession{{Entity: rbdMirrorSourcePeer, GlobalID: 4207}}, daemon},
		"other address":      {osd, peer, "172.18.0.5"},
		"no address":         {osd, peer, ""},
		"no MON session":     {osd, nil, daemon},
		"not a client":       {[]rbdMirrorSession{{PeerType: "osd", GlobalID: 4410, Address: daemon}}, peer, daemon},
	} {
		if witness := rbdMirrorPeerWitness(tc.osd, tc.mons, tc.address); len(witness) != 0 {
			t.Fatal(name, "attributed a session to the source peer", witness)
		}
	}
}

// A broad native suite must skip this checkpoint before reading any fixture
// input. The empty private directory deliberately cannot satisfy osdFixture.
func TestRBDMirrorRequiresExclusiveMarker(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("CEPH_MSGR_NATIVE_RBD_MIRROR", "")
	t.Setenv("CEPH_MSGR_TEST_OSD", "1")
	t.Setenv("CEPH_MSGR_CONTROL_DIR", dir)
	if !t.Run("unselected", TestCephRBDMirrorPathRecoveryIntegration) {
		t.Fatal("unselected checkpoint accessed the fixture")
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatal("unselected checkpoint published a fixture control", err)
	}
}
