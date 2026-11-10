package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/fault"
	"github.com/JSYoo5B/ceph-testcontainers-go/rbd"
	rbdmirror "github.com/JSYoo5B/ceph-testcontainers-go/rbd/mirror"
	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
)

// The RBD mirror path-recovery checkpoint owns a second disposable
// destination cluster and one rbd-mirror daemon for the fixture's own
// single-BlueStore cluster, which is the mirror source. It is admitted only
// by its exact selector; broad suites never start it.
const rbdMirrorSelector = "^TestCephRBDMirrorPathRecoveryIntegration$"
const rbdMirrorMarker = "CEPH_MSGR_NATIVE_RBD_MIRROR"

// rbdMirrorNetemImage is the provider's netem helper image
// (fault.NetemHelperImage) pinned to the digest of its extension-netem-v1 tag.
const rbdMirrorNetemImage = "ghcr.io/jsyoo5b/ceph-testcontainers-images:extension-netem-v1@sha256:63d17894d30de34fbf6c956b38e2576002aed58748b779b04c5e4632fc9dfdc3"

const (
	rbdMirrorPool       = "msgr-mirror"
	rbdMirrorImage      = "path"
	rbdMirrorImageSize  = 4 << 20
	rbdMirrorPatchAt    = 2 << 20
	rbdMirrorPatchSize  = 1 << 20
	rbdMirrorSeedBefore = 17
	rbdMirrorSeedPatch  = 99
	rbdMirrorReceipt    = "rbd-mirror-receipt.json"
	// rbdMirrorCutWindow is how long the cut daemon is sampled for absent
	// progress, rbdMirrorSampleEvery the sampling interval.
	rbdMirrorCutWindow   = 30 * time.Second
	rbdMirrorSampleEvery = 5 * time.Second
	// rbdMirrorSyncBound bounds each wait for destination bytes.
	rbdMirrorSyncBound = 4 * time.Minute
)

func (p profile) rbdMirrorFixture() bool {
	return p.osd == "1" && p.osdStore == "bluestore" && p.osdReplicas == "1" && p.osdProfile == "legacy-v6" && p.ipFamily == "4" &&
		p.mappedIPv6 == "0" && p.rbdBase == "0" && p.mdsFresh == "0" && p.osdCachePools == "0" && p.mgrService == "0" &&
		p.osdSoak == "" && p.monFreeze == "" && p.sparseReadOracle == "" && p.mapExtOracle == "" && p.u64XattrOracle == "" && p.writeSameOracle == ""
}

func (p profile) rbdMirrorSelected() bool {
	return p.testRun == rbdMirrorSelector && p.rbdMirrorFixture()
}

// rbdMirrorPayload is the deterministic image content; the product test
// restates the same formula independently.
func rbdMirrorPayload(size, seed int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte((i*37+seed)%251 + 1)
	}
	return data
}

func rbdMirrorDigest(data []byte) string { return fmt.Sprintf("%x", sha256.Sum256(data)) }

// rbdMirrorProgress is the snapshot progress that the destination image's
// native mirror status description carries after "replaying, ".
type rbdMirrorProgress struct {
	State             string `json:"state"`
	Remote            int64  `json:"remote_snapshot_timestamp"`
	Local             int64  `json:"local_snapshot_timestamp"`
	LastSnapshotBytes int64  `json:"last_snapshot_bytes"`
	ReplayState       string `json:"replay_state"`
}

// rbdMirrorSession is one connection of a source OSD messenger dump.
type rbdMirrorSession struct {
	Messenger string `json:"messenger"`
	PeerType  string `json:"peer_type"`
	PeerID    int64  `json:"peer_id"`
	GlobalID  uint64 `json:"global_id"`
	Address   string `json:"address"`
	State     string `json:"state"`
	Connected bool   `json:"connected"`
}

// rbdMirrorMonSession is one native MON session whose address is the
// daemon's source-network address.
type rbdMirrorMonSession struct {
	Monitor  string `json:"monitor"`
	Entity   string `json:"entity_name"`
	GlobalID uint64 `json:"global_id"`
}

type rbdMirrorFixtureReceipt struct {
	Schema       int    `json:"schema_version"`
	Selector     string `json:"selector"`
	Phase        string `json:"phase"`
	CephImage    string `json:"ceph_image"`
	NetemImage   string `json:"netem_image"`
	NetemImageID string `json:"netem_image_id"`
	Pool         string `json:"pool"`
	Image        string `json:"image"`
	Mode         string `json:"mirror_mode"`
	Source       struct {
		FSID        string `json:"fsid"`
		Version     string `json:"ceph_version"`
		Network     string `json:"network"`
		OSD         int    `json:"osd"`
		OSDAddress  string `json:"osd_address"`
		Monitors    int    `json:"monitors"`
		Managers    int    `json:"managers"`
		PoolPGs     int    `json:"pool_pg_num"`
		PoolSize    int    `json:"pool_size"`
		KeyType     string `json:"key_type"`
		Messenger   string `json:"connection_mode"`
		AddressKind string `json:"address_family"`
	} `json:"source"`
	Destination struct {
		FSID     string `json:"fsid"`
		Version  string `json:"ceph_version"`
		Network  string `json:"network"`
		Monitors int    `json:"monitors"`
		Managers int    `json:"managers"`
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
		SourceNetworks     int    `json:"daemon_networks"`
	} `json:"mirror"`
	Initial struct {
		Bytes             int               `json:"bytes"`
		SourceSHA256      string            `json:"source_sha256"`
		DestinationSHA256 string            `json:"destination_sha256"`
		SyncSeconds       float64           `json:"sync_seconds"`
		Progress          rbdMirrorProgress `json:"progress"`
		// Sessions and MonSessions are the source OSD's and MONs' sessions
		// before the fault: the daemon's peer path is attributed here.
		Sessions    []rbdMirrorSession    `json:"source_osd_sessions"`
		MonSessions []rbdMirrorMonSession `json:"source_mon_sessions"`
	} `json:"initial"`
	Cut struct {
		Peer              string             `json:"peer"`
		Interface         string             `json:"interface"`
		HelperContainers  int                `json:"helper_containers"`
		Applied           bool               `json:"applied"`
		Loss              float64            `json:"loss"`
		Packets           uint64             `json:"packets"`
		Drops             uint64             `json:"drops"`
		PatchOffset       int                `json:"patch_offset"`
		PatchBytes        int                `json:"patch_bytes"`
		Samples           int                `json:"samples"`
		SampleSeconds     float64            `json:"sample_seconds"`
		States            []string           `json:"states"`
		Progress          rbdMirrorProgress  `json:"progress"`
		SourceSHA256      string             `json:"source_sha256"`
		DestinationSHA256 string             `json:"destination_sha256"`
		Sessions          []rbdMirrorSession `json:"source_osd_sessions"`
	} `json:"cut"`
	Restore struct {
		DropsBeforeRestore uint64             `json:"drops_before_restore"`
		AppliedBefore      bool               `json:"applied_before_restore"`
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
	Error string `json:"error,omitempty"`
}

// rbdMirrorScope owns the destination cluster, the mirror and the fault.
// Every mutation is issued once; nothing is retried automatically.
type rbdMirrorScope struct {
	out         string
	destination *ceph.Container
	mirror      *rbdmirror.Container
	daemon      *rbdmirror.Daemon
	osd         *ceph.OSDContainer
	lease       *fault.NetworkProfileLease
	peer        netip.Addr
	baseline    rbdMirrorProgress
	receipt     rbdMirrorFixtureReceipt
	// issued names the teardown phases already issued, owned the Docker
	// containers the teardown must remove, and inventoried whether the
	// destination network's members are among them.
	issued      map[string]bool
	owned       map[string]bool
	inventoried bool
	// finished is set once every owned teardown phase has been issued.
	finished bool
}

func (s *rbdMirrorScope) save() error {
	raw, err := json.MarshalIndent(s.receipt, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(s.out, rbdMirrorReceipt), append(raw, '\n'), 0o644)
}

// controlRBDMirrorRequest serves "rbd-mirror <start|cut|restore|finish>",
// strictly in that order and once each.
func (c *fixtureCluster) controlRBDMirrorRequest(ctx context.Context, out string) error {
	fields, err := c.request(out, "rbd-mirror")
	if err != nil || fields == nil {
		return err
	}
	if len(fields) != 2 || !c.profile.rbdMirrorSelected() {
		return errors.New("RBD mirror checkpoint requires its exclusive single-BlueStore IPv4 selector")
	}
	phase := ""
	if c.rbdMirror != nil {
		phase = c.rbdMirror.receipt.Phase
	}
	// finish is admitted once after any earlier phase, so that a failed
	// checkpoint still releases its owned resources.
	var admitted bool
	switch fields[1] {
	case "start":
		admitted = c.rbdMirror == nil
	case "cut":
		admitted = c.rbdMirror != nil && phase == "start" && c.rbdMirror.receipt.Error == ""
	case "restore":
		admitted = c.rbdMirror != nil && phase == "cut" && c.rbdMirror.receipt.Error == ""
	case "finish":
		admitted = c.rbdMirror != nil && !c.rbdMirror.finished
	default:
		return errors.New("invalid RBD mirror checkpoint action")
	}
	if !admitted {
		return fmt.Errorf("RBD mirror checkpoint action %s is not admitted after phase %q", fields[1], phase)
	}
	if c.rbdMirror == nil {
		c.rbdMirror = &rbdMirrorScope{out: out}
	}
	s := c.rbdMirror
	bounds := map[string]time.Duration{"start": 10 * time.Minute, "cut": 3 * time.Minute, "restore": 6 * time.Minute, "finish": 3 * time.Minute}
	actionCtx, stop := context.WithTimeout(ctx, bounds[fields[1]])
	defer stop()
	switch fields[1] {
	case "start":
		err = c.rbdMirrorStart(actionCtx, s)
	case "cut":
		err = c.rbdMirrorCut(actionCtx, s)
	case "restore":
		err = c.rbdMirrorRestore(actionCtx, s)
	case "finish":
		err = s.finish(actionCtx)
	}
	if err == nil {
		s.receipt.Phase = fields[1]
	} else if s.receipt.Error == "" {
		s.receipt.Error = fields[1] + ": " + err.Error()
	}
	if saveErr := s.save(); saveErr != nil {
		return errors.Join(err, saveErr)
	}
	if err != nil && fields[1] == "finish" {
		return fmt.Errorf("RBD mirror checkpoint cleanup: %w", err)
	}
	// A failed start, cut or restore is acknowledged with its error in the
	// receipt, so that the controller stays available for the test's finish
	// request; the test fails on the recorded error.
	return publishAck(out, "rbd-mirror-done."+fields[0])
}

func (c *fixtureCluster) rbdMirrorStart(ctx context.Context, s *rbdMirrorScope) error {
	p := c.profile
	r := &s.receipt
	r.Schema, r.Selector, r.CephImage, r.NetemImage = 1, rbdMirrorSelector, p.image, rbdMirrorNetemImage
	r.Pool, r.Image, r.Mode = rbdMirrorPool, rbdMirrorImage, string(rbdmirror.ModeSnapshot)
	r.Source.KeyType, r.Source.Messenger, r.Source.AddressKind = p.keyType, p.connectionMode, "ipv4"
	osds, err := c.cluster.OSDs(ctx)
	if err != nil {
		return err
	}
	if len(osds) != 1 {
		return errors.New("RBD mirror checkpoint requires exactly one source OSD")
	}
	s.osd = osds[0]
	r.Source.OSD = s.osd.ID
	sourceFacts, err := ceph.FactsOf(c.cluster)
	if err != nil {
		return err
	}
	r.Source.Network, r.Source.Version = sourceFacts.NetworkName, sourceFacts.CephVersion
	status, err := observe(c.cluster).Status(ctx)
	if err != nil {
		return err
	}
	r.Source.FSID = status.FSID
	if r.Source.OSDAddress, err = osdPublicAddress(ctx, c.cluster, s.osd.ID); err != nil {
		return err
	}
	monitors, err := c.cluster.Monitors(ctx)
	if err != nil {
		return err
	}
	managers, err := c.cluster.Managers(ctx)
	if err != nil {
		return err
	}
	r.Source.Monitors, r.Source.Managers = len(monitors), len(managers)

	// The destination mirrors the source's authentication and Messenger
	// policy with one MON, MGR and BlueStore OSD.
	mode := ceph.MessengerSecure
	if p.connectionMode == "crc" {
		mode = ceph.MessengerCRC
	}
	policy := ceph.MessengerPolicy{Monitors: []ceph.MessengerConnectionMode{mode}, Managers: []ceph.MessengerConnectionMode{mode}, OSDs: []ceph.MessengerConnectionMode{mode}}
	s.destination, err = ceph.Run(ctx, p.image, ceph.WithStartupTimeout(fixtureStartupTimeout),
		ceph.WithAuth(ceph.AuthConfig{KeyType: ceph.CephxCipher(p.keyType), ServiceCipher: ceph.CephxCipher(p.serviceCipher)}),
		ceph.WithClusterConfig(ceph.ClusterConfig{}.WithMonitorCount(1).WithManagerCount(1).WithOSDCount(1).WithPoolDefaults(1, 1)),
		ceph.WithOSDStorage(ceph.OSDStorage{}.WithObjectStore(ceph.OSDBlueStore)),
		ceph.WithNetworkConfig(ceph.NetworkConfig{}.WithMessengerMode(ceph.MessengerV2Only).WithMessengerPolicy(policy)))
	if err != nil {
		return fmt.Errorf("start destination cluster: %w", err)
	}
	destinationFacts, err := ceph.FactsOf(s.destination)
	if err != nil {
		return err
	}
	r.Destination.Network, r.Destination.Version = destinationFacts.NetworkName, destinationFacts.CephVersion
	destinationStatus, err := observe(s.destination).Status(ctx)
	if err != nil {
		return err
	}
	r.Destination.FSID = destinationStatus.FSID
	if r.Destination.FSID == "" || r.Destination.FSID == r.Source.FSID || r.Destination.Network == r.Source.Network {
		return errors.New("destination cluster is not independent of the source")
	}
	for _, count := range []struct {
		n   *int
		get func(context.Context) (int, error)
	}{
		{&r.Destination.Monitors, func(ctx context.Context) (int, error) { m, err := s.destination.Monitors(ctx); return len(m), err }},
		{&r.Destination.Managers, func(ctx context.Context) (int, error) { m, err := s.destination.Managers(ctx); return len(m), err }},
		{&r.Destination.OSDs, func(ctx context.Context) (int, error) { m, err := s.destination.OSDs(ctx); return len(m), err }},
	} {
		if *count.n, err = count.get(ctx); err != nil {
			return err
		}
	}
	for _, cluster := range []*ceph.Container{c.cluster, s.destination} {
		if _, err := cluster.CreatePool(ctx, ceph.PoolConfig{Name: rbdMirrorPool, Application: "rbd", PGNum: 1}); err != nil {
			return fmt.Errorf("create mirror pool: %w", err)
		}
		if err := rbd.InitPool(ctx, cluster, rbdMirrorPool); err != nil {
			return fmt.Errorf("initialize mirror pool: %w", err)
		}
		if err := observe(cluster).WaitForClean(ctx); err != nil {
			return fmt.Errorf("mirror pool clean: %w", err)
		}
	}
	pool, err := c.cluster.Pool(ctx, rbdMirrorPool)
	if err != nil {
		return err
	}
	state, err := pool.Status(ctx)
	if err != nil {
		return err
	}
	r.Source.PoolPGs, r.Source.PoolSize = state.PGNum, state.Size

	s.mirror, err = rbdmirror.Run(ctx, destinationFacts.Images.RBDAux, rbdmirror.WithConfig(rbdmirror.Config{Source: c.cluster, Destination: s.destination, Pool: rbdMirrorPool}))
	if err != nil {
		return fmt.Errorf("start RBD mirror: %w", err)
	}
	daemons := s.mirror.Daemons()
	if len(daemons) != 1 {
		return fmt.Errorf("RBD mirror owns %d daemons, want 1", len(daemons))
	}
	s.daemon = daemons[0]
	r.Mirror.Daemon, r.Mirror.DaemonClient = s.daemon.DaemonName, s.daemon.ClientName
	inspected, err := s.daemon.Inspect(ctx)
	if err != nil {
		return err
	}
	if inspected.NetworkSettings == nil {
		return errors.New("RBD mirror daemon has no network settings")
	}
	r.Mirror.SourceNetworks = len(inspected.NetworkSettings.Networks)
	endpoint := inspected.NetworkSettings.Networks[r.Source.Network]
	if endpoint == nil || !endpoint.IPAddress.IsValid() || !endpoint.IPAddress.Is4() {
		return errors.New("RBD mirror daemon has no IPv4 endpoint on the source network")
	}
	s.peer = endpoint.IPAddress
	r.Mirror.DaemonAddress = s.peer.String()

	before := rbdMirrorPayload(rbdMirrorImageSize, rbdMirrorSeedBefore)
	r.Initial.Bytes = len(before)
	if err := c.Container.CopyToContainer(ctx, before, "/tmp/rbd-mirror-seed", 0o600); err != nil {
		return err
	}
	spec := rbdMirrorPool + "/" + rbdMirrorImage
	if _, err := c.rbdMirrorSourceShell(ctx, "rbd import /tmp/rbd-mirror-seed "+spec+" --object-size 1M --image-feature layering,exclusive-lock --no-progress && rm -f /tmp/rbd-mirror-seed"); err != nil {
		return fmt.Errorf("native seed import: %w", err)
	}
	if r.Initial.SourceSHA256, err = c.rbdMirrorSourceDigest(ctx); err != nil {
		return err
	}
	if r.Initial.SourceSHA256 != rbdMirrorDigest(before) {
		return errors.New("native source export differs from the seed")
	}
	if err := s.mirror.EnableImage(ctx, rbdMirrorImage); err != nil {
		return fmt.Errorf("enable snapshot mirroring: %w", err)
	}
	started := time.Now()
	if r.Initial.DestinationSHA256, err = s.waitDestination(ctx, rbdMirrorDigest(before)); err != nil {
		return fmt.Errorf("initial destination data: %w", err)
	}
	r.Initial.SyncSeconds = time.Since(started).Seconds()
	// The status reports the local snapshot a little after its bytes arrive.
	if r.Initial.Progress, err = s.waitProgress(ctx, func(p rbdMirrorProgress) bool { return p.Remote != 0 && p.Local == p.Remote }); err != nil {
		return fmt.Errorf("initial snapshot in sync: %w", err)
	}
	s.baseline = r.Initial.Progress
	image, err := s.mirror.ImageStatus(ctx, rbdMirrorImage)
	if err != nil {
		return err
	}
	if !image.ReplayReady || image.GlobalID == "" || image.SourceImageID == "" || image.DestinationImageID == "" || !image.SourcePrimary || image.DestinationPrimary || image.DaemonName != s.daemon.DaemonName {
		return fmt.Errorf("mirror image identities incomplete: %+v", image)
	}
	r.Mirror.GlobalID, r.Mirror.SourceImageID, r.Mirror.DestinationImageID, r.Mirror.InstanceID = image.GlobalID, image.SourceImageID, image.DestinationImageID, image.InstanceID
	// The daemon reaches the source as the CephX user of the destination
	// pool's receiving peer, not as its own destination-side client.
	info, err := s.mirror.DestinationRBD(ctx, "mirror", "pool", "info", rbdMirrorPool, "--format", "json")
	if err != nil {
		return fmt.Errorf("destination mirror pool info: %w", err)
	}
	if r.Mirror.SourcePeerClient, err = parseRBDMirrorPeerClient(info); err != nil {
		return err
	}
	if r.Initial.Sessions, err = c.rbdMirrorSessions(ctx, s); err != nil {
		return err
	}
	if r.Initial.MonSessions, err = c.rbdMirrorMonSessions(ctx, s.peer); err != nil {
		return err
	}
	return nil
}

func (c *fixtureCluster) rbdMirrorCut(ctx context.Context, s *rbdMirrorScope) error {
	r := &s.receipt
	r.Cut.Peer = s.peer.String()
	// The source OSD drops every packet it sends to the daemon's address on
	// the source network, and only those.
	var err error
	s.lease, err = fault.ApplyNetworkProfile(ctx, c.cluster, s.osd.Container, fault.NetworkProfile{HelperImage: rbdMirrorNetemImage, Plane: ceph.PublicNetworkPlane, Peer: s.peer, Loss: 100})
	if s.lease != nil {
		r.NetemImageID, r.Cut.Interface = s.lease.HelperImageID, s.lease.Interface
	}
	if err != nil {
		return fmt.Errorf("apply directed source OSD fault: %w", err)
	}
	// The held lease keeps its helper in the source OSD's namespace: the
	// same scoped count must reach zero after the restore.
	if s.lease.ContainerID != s.osd.GetContainerID() {
		return fmt.Errorf("netem lease targets %s, not the source OSD container", s.lease.ContainerID)
	}
	if r.Cut.HelperContainers, err = rbdMirrorHelperContainers(ctx, s.lease.HelperImageID, s.lease.ContainerID); err != nil {
		return err
	}
	if r.Cut.HelperContainers == 0 {
		return errors.New("no netem helper shares the source OSD's network namespace while the fault holds")
	}
	patch := rbdMirrorPayload(rbdMirrorPatchSize, rbdMirrorSeedPatch)
	after := rbdMirrorPayload(rbdMirrorImageSize, rbdMirrorSeedBefore)
	copy(after[rbdMirrorPatchAt:], patch)
	r.Cut.PatchOffset, r.Cut.PatchBytes = rbdMirrorPatchAt, len(patch)
	if err := c.Container.CopyToContainer(ctx, rbdMirrorDiff(rbdMirrorImageSize, rbdMirrorPatchAt, patch), "/tmp/rbd-mirror-patch.diff", 0o600); err != nil {
		return err
	}
	spec := rbdMirrorPool + "/" + rbdMirrorImage
	// The source client writes through the same OSD while the cut holds.
	if _, err := c.rbdMirrorSourceShell(ctx, "rbd import-diff /tmp/rbd-mirror-patch.diff "+spec+" --no-progress && rm -f /tmp/rbd-mirror-patch.diff"); err != nil {
		return fmt.Errorf("native patch write: %w", err)
	}
	if _, err := s.mirror.SourceRBD(ctx, "mirror", "image", "snapshot", spec); err != nil {
		return fmt.Errorf("native mirror snapshot: %w", err)
	}
	// Sample at least every rbdMirrorSampleEvery, and once more after the
	// whole window has elapsed.
	started := time.Now()
	for {
		progress, err := s.progress(ctx)
		if err != nil {
			return err
		}
		r.Cut.States = append(r.Cut.States, progress.State)
		r.Cut.Samples++
		if progress.State != "up+replaying" || progress.Remote != s.baseline.Remote || progress.Local != s.baseline.Local {
			return fmt.Errorf("the cut daemon advanced: %+v, before %+v", progress, s.baseline)
		}
		r.Cut.Progress = progress
		remaining := rbdMirrorCutWindow - time.Since(started)
		if remaining <= 0 {
			break
		}
		if err := sleepContext(ctx, min(remaining, rbdMirrorSampleEvery)); err != nil {
			return err
		}
	}
	r.Cut.SampleSeconds = time.Since(started).Seconds()
	if r.Cut.DestinationSHA256, err = s.destinationDigest(ctx); err != nil {
		return err
	}
	if r.Cut.SourceSHA256, err = c.rbdMirrorSourceDigest(ctx); err != nil {
		return err
	}
	if r.Cut.DestinationSHA256 != rbdMirrorDigest(rbdMirrorPayload(rbdMirrorImageSize, rbdMirrorSeedBefore)) || r.Cut.SourceSHA256 != rbdMirrorDigest(after) {
		return errors.New("source or destination bytes differ during the cut")
	}
	cut, err := s.lease.Status(ctx)
	if err != nil {
		return err
	}
	r.Cut.Applied, r.Cut.Loss, r.Cut.Packets, r.Cut.Drops = cut.Applied, cut.Loss, cut.Packets, cut.Drops
	if !cut.Applied || cut.Drops == 0 {
		return fmt.Errorf("netem status during the cut: applied=%t drops=%d", cut.Applied, cut.Drops)
	}
	if r.Cut.Sessions, err = c.rbdMirrorSessions(ctx, s); err != nil {
		return err
	}
	return nil
}

func (c *fixtureCluster) rbdMirrorRestore(ctx context.Context, s *rbdMirrorScope) error {
	r := &s.receipt
	held, err := s.lease.Status(ctx)
	if err != nil {
		return err
	}
	r.Restore.DropsBeforeRestore, r.Restore.AppliedBefore = held.Drops, held.Applied
	if !held.Applied || held.Drops < r.Cut.Drops {
		return fmt.Errorf("netem status before the restore: applied=%t drops=%d", held.Applied, held.Drops)
	}
	if err := s.lease.Restore(ctx); err != nil {
		return fmt.Errorf("restore directed source OSD fault: %w", err)
	}
	r.Restore.Restored = true
	// A restored lease refuses further status reads; record that refusal.
	// The absent helper containers and the catch-up below are the
	// independent evidence that the profile no longer holds.
	if _, statusErr := s.lease.Status(ctx); statusErr == nil {
		return errors.New("netem lease still reports status after the restore")
	} else {
		r.Restore.StatusAfter = statusErr.Error()
	}
	if r.Restore.HelperContainers, err = rbdMirrorHelperContainers(ctx, s.lease.HelperImageID, s.lease.ContainerID); err != nil {
		return err
	}
	if r.Restore.HelperContainers != 0 {
		return fmt.Errorf("%d netem helper containers remain after the restore", r.Restore.HelperContainers)
	}
	after := rbdMirrorPayload(rbdMirrorImageSize, rbdMirrorSeedBefore)
	copy(after[rbdMirrorPatchAt:], rbdMirrorPayload(rbdMirrorPatchSize, rbdMirrorSeedPatch))
	started := time.Now()
	if r.Restore.DestinationSHA256, err = s.waitDestination(ctx, rbdMirrorDigest(after)); err != nil {
		return fmt.Errorf("destination catch-up: %w", err)
	}
	r.Restore.CatchUpSeconds = time.Since(started).Seconds()
	if r.Restore.Progress, err = s.waitProgress(ctx, func(p rbdMirrorProgress) bool {
		return p.Remote > s.baseline.Remote && p.Local == p.Remote
	}); err != nil {
		return fmt.Errorf("new snapshot in sync: %w", err)
	}
	if r.Restore.Progress.LastSnapshotBytes != rbdMirrorPatchSize {
		return fmt.Errorf("last snapshot copied %d bytes, want %d", r.Restore.Progress.LastSnapshotBytes, rbdMirrorPatchSize)
	}
	if r.Restore.Sessions, err = c.rbdMirrorSessions(ctx, s); err != nil {
		return err
	}
	return nil
}

// rbdMirrorEngine is the Docker inventory through which the cleanup confirms
// removal independently of the owned handles.
type rbdMirrorEngine interface {
	NetworkInspect(context.Context, string, client.NetworkInspectOptions) (client.NetworkInspectResult, error)
	ContainerInspect(context.Context, string, client.ContainerInspectOptions) (client.ContainerInspectResult, error)
	Close() error
}

func rbdMirrorDockerEngine(ctx context.Context) (rbdMirrorEngine, error) {
	return testcontainers.NewDockerClientWithOpts(ctx)
}

// rbdMirrorPhase is one owned teardown call.
type rbdMirrorPhase struct {
	name string
	run  func(context.Context) error
}

// finish restores a held fault, terminates the mirror and then the
// destination cluster, and confirms their removal independently through the
// Docker engine. The source pool, its mirroring policy and peer user stay in
// the disposable source cluster, which the fixture removes as a whole.
func (s *rbdMirrorScope) finish(ctx context.Context) error {
	var phases []rbdMirrorPhase
	if s.lease != nil && !s.receipt.Restore.Restored {
		phases = append(phases, rbdMirrorPhase{"directed fault restore", s.lease.Restore})
	}
	var daemons []string
	if s.mirror != nil {
		for _, daemon := range s.mirror.Daemons() {
			if daemon != nil && daemon.Container != nil && daemon.GetContainerID() != "" {
				daemons = append(daemons, daemon.GetContainerID())
			}
		}
		phases = append(phases, rbdMirrorPhase{"RBD mirror termination", func(ctx context.Context) error { return s.mirror.Terminate(ctx) }})
	}
	if s.destination != nil {
		phases = append(phases, rbdMirrorPhase{"destination cluster termination", func(ctx context.Context) error { return s.destination.Terminate(ctx) }})
	}
	return s.teardown(ctx, phases, daemons, rbdMirrorDockerEngine)
}

// teardown issues every owned phase once, in order, whether or not the
// Docker inventory is available: an inventory failure only withholds the
// independent confirmation. A phase not reached before ctx ends stays for the
// fixture's fallback cleanup, and the scope is finished only once every phase
// has been issued.
func (s *rbdMirrorScope) teardown(ctx context.Context, phases []rbdMirrorPhase, daemons []string, open func(context.Context) (rbdMirrorEngine, error)) error {
	if s.finished {
		return nil
	}
	r := &s.receipt
	r.Cleanup.Attempted = true
	if s.issued == nil {
		s.issued = map[string]bool{}
	}
	if s.owned == nil {
		s.owned = map[string]bool{}
	}
	var errs []error
	engine, err := open(ctx)
	if err != nil {
		errs = append(errs, fmt.Errorf("open the Docker inventory: %w", err))
		engine = nil
	} else {
		defer func() { _ = engine.Close() }()
	}
	network := r.Destination.Network
	if engine != nil && network != "" && !s.inventoried {
		inspected, err := engine.NetworkInspect(ctx, network, client.NetworkInspectOptions{})
		if err != nil {
			errs = append(errs, fmt.Errorf("inspect destination network before cleanup: %w", err))
		} else {
			s.inventoried = true
			for id := range inspected.Network.Containers {
				s.owned[id] = true
			}
		}
	}
	for _, id := range daemons {
		s.owned[id] = true
	}
	pending := 0
	for _, phase := range phases {
		if s.issued[phase.name] {
			continue
		}
		if err := ctx.Err(); err != nil {
			pending++
			errs = append(errs, fmt.Errorf("%s not reached: %w", phase.name, err))
			continue
		}
		s.issued[phase.name] = true
		if err := phase.run(ctx); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", phase.name, err))
		}
	}
	s.finished = pending == 0
	if engine == nil {
		return errors.Join(append(errs, errors.New("owned container removal cannot be independently confirmed"))...)
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	r.Cleanup.Containers = len(s.owned)
	for id := range s.owned {
		if _, err := engine.ContainerInspect(ctx, id, client.ContainerInspectOptions{}); !errdefs.IsNotFound(err) {
			errs = append(errs, fmt.Errorf("owned container removal not independently confirmed: %v", err))
		}
	}
	if network != "" {
		if _, err := engine.NetworkInspect(ctx, network, client.NetworkInspectOptions{}); !errdefs.IsNotFound(err) {
			errs = append(errs, fmt.Errorf("destination network removal not independently confirmed: %v", err))
		} else {
			r.Cleanup.Network = true
		}
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	r.Cleanup.Confirmed = len(s.owned) != 0 && r.Cleanup.Network
	if !r.Cleanup.Confirmed {
		return errors.New("RBD mirror cleanup inventory is empty")
	}
	return nil
}

// rbdMirrorSourceShell runs one native shell recipe in the fixture's source
// client container.
func (c *fixtureCluster) rbdMirrorSourceShell(ctx context.Context, script string) ([]byte, error) {
	result, err := c.cluster.RunNativeCommand(ctx, c.Container, ceph.NativeCommand{Argv: []string{"sh", "-c", script}, Timeout: 2 * time.Minute, MaxStdout: 64 << 10, MaxStderr: 64 << 10, Label: "RBD mirror source recipe"})
	if err != nil {
		return nil, err
	}
	if !result.Complete() || result.ExitCode != 0 {
		return nil, fmt.Errorf("RBD mirror source recipe exit=%d complete=%t: %s", result.ExitCode, result.Complete(), bytes.TrimSpace(result.Stderr))
	}
	return result.Stdout, nil
}

const rbdMirrorExportDigest = "rbd export " + rbdMirrorPool + "/" + rbdMirrorImage + " - --no-progress | sha256sum"

func (c *fixtureCluster) rbdMirrorSourceDigest(ctx context.Context) (string, error) {
	out, err := c.rbdMirrorSourceShell(ctx, "set -o pipefail 2>/dev/null; "+rbdMirrorExportDigest)
	if err != nil {
		return "", err
	}
	return rbdMirrorParseDigest(out)
}

func rbdMirrorParseDigest(out []byte) (string, error) {
	fields := strings.Fields(string(out))
	if len(fields) == 0 || len(fields[0]) != 64 {
		return "", fmt.Errorf("native export digest is malformed: %q", out)
	}
	return fields[0], nil
}

// destinationDigest exports the destination image through the destination's
// own client container.
func (s *rbdMirrorScope) destinationDigest(ctx context.Context) (string, error) {
	target, err := s.destination.ClientContainer(ctx)
	if err != nil {
		return "", err
	}
	result, err := s.destination.RunNativeCommand(ctx, target, ceph.NativeCommand{Argv: []string{"sh", "-c", "set -o pipefail 2>/dev/null; " + rbdMirrorExportDigest}, Timeout: time.Minute, MaxStdout: 4 << 10, MaxStderr: 16 << 10, Label: "RBD mirror destination export"})
	if err != nil {
		return "", err
	}
	if !result.Complete() || result.ExitCode != 0 {
		return "", fmt.Errorf("destination export exit=%d complete=%t: %s", result.ExitCode, result.Complete(), bytes.TrimSpace(result.Stderr))
	}
	return rbdMirrorParseDigest(result.Stdout)
}

// waitDestination polls the destination's bytes until they match want.
func (s *rbdMirrorScope) waitDestination(ctx context.Context, want string) (string, error) {
	ctx, stop := context.WithTimeout(ctx, rbdMirrorSyncBound)
	defer stop()
	var last error
	for {
		got, err := s.destinationDigest(ctx)
		if err == nil && got == want {
			return got, nil
		}
		if err == nil {
			err = fmt.Errorf("destination digest %s, want %s", got, want)
		}
		last = err
		if err := sleepContext(ctx, 2*time.Second); err != nil {
			return "", errors.Join(err, last)
		}
	}
}

func (s *rbdMirrorScope) progress(ctx context.Context) (rbdMirrorProgress, error) {
	status, err := s.mirror.ImageStatus(ctx, rbdMirrorImage)
	if err != nil {
		return rbdMirrorProgress{}, fmt.Errorf("mirror status: %w", err)
	}
	return parseRBDMirrorProgress(status.State, status.Description)
}

func parseRBDMirrorProgress(state, description string) (rbdMirrorProgress, error) {
	_, detail, ok := strings.Cut(description, ", ")
	var progress rbdMirrorProgress
	if !ok || json.Unmarshal([]byte(detail), &progress) != nil {
		return rbdMirrorProgress{}, fmt.Errorf("mirror status has no snapshot progress: %q", description)
	}
	progress.State = state
	return progress, nil
}

func (s *rbdMirrorScope) waitProgress(ctx context.Context, done func(rbdMirrorProgress) bool) (rbdMirrorProgress, error) {
	ctx, stop := context.WithTimeout(ctx, time.Minute)
	defer stop()
	var last rbdMirrorProgress
	var lastErr error
	for {
		progress, err := s.progress(ctx)
		if err == nil {
			last = progress
			if done(progress) {
				return progress, nil
			}
		} else {
			lastErr = err
		}
		if err := sleepContext(ctx, time.Second); err != nil {
			return last, errors.Join(err, lastErr)
		}
	}
}

// rbdMirrorSessions reads the source OSD's public ("client") messenger
// sessions from its admin socket, which the fault does not affect.
func (c *fixtureCluster) rbdMirrorSessions(ctx context.Context, s *rbdMirrorScope) ([]rbdMirrorSession, error) {
	raw, err := s.osd.AdminSocket(ctx, "messenger", "dump", "client")
	if err != nil {
		return nil, fmt.Errorf("source OSD messenger dump: %w", err)
	}
	sessions, err := parseRBDMirrorSessions(raw)
	if err != nil {
		return nil, err
	}
	if !rbdMirrorPeerSession(sessions, s.peer.String()) {
		return sessions, fmt.Errorf("source osd.%d has no session with the mirror daemon at %s", s.osd.ID, s.peer)
	}
	return sessions, nil
}

func rbdMirrorPeerSession(sessions []rbdMirrorSession, peer string) bool {
	for _, session := range sessions {
		if session.PeerType == "client" && session.Address == peer && session.GlobalID != 0 {
			return true
		}
	}
	return false
}

// parseRBDMirrorSessions projects "messenger dump client" onto the peer's
// type, ID, GlobalID, IP address and state, sorted for stable receipts.
func parseRBDMirrorSessions(raw []byte) ([]rbdMirrorSession, error) {
	type connection struct {
		State  string `json:"state"`
		Status struct {
			Connected bool `json:"connected"`
		} `json:"status"`
		Peer struct {
			Type     string `json:"type"`
			ID       int64  `json:"id"`
			GlobalID uint64 `json:"global_id"`
			Addr     struct {
				Addrvec []struct {
					Addr string `json:"addr"`
				} `json:"addrvec"`
			} `json:"addr"`
		} `json:"peer"`
		Protocol struct {
			V2 *struct {
				State string `json:"state"`
			} `json:"v2"`
		} `json:"protocol"`
	}
	type messenger struct {
		Connections []struct {
			Async connection `json:"async_connection"`
		} `json:"connections"`
		AnonConns []connection `json:"anon_conns"`
	}
	var dump struct {
		Name      string     `json:"name"`
		Messenger *messenger `json:"messenger"`
		Status    *struct {
			Name      string     `json:"name"`
			Messenger *messenger `json:"messenger"`
		} `json:"status"`
	}
	if err := json.Unmarshal(raw, &dump); err != nil {
		return nil, fmt.Errorf("decode messenger dump: %w", err)
	}
	name, body := dump.Name, dump.Messenger
	if dump.Status != nil {
		name, body = dump.Status.Name, dump.Status.Messenger
	}
	if body == nil {
		return nil, errors.New("messenger dump has no messenger")
	}
	all := make([]connection, 0, len(body.Connections)+len(body.AnonConns))
	for _, c := range body.Connections {
		all = append(all, c.Async)
	}
	all = append(all, body.AnonConns...)
	sessions := make([]rbdMirrorSession, 0, len(all))
	for _, c := range all {
		session := rbdMirrorSession{Messenger: name, PeerType: c.Peer.Type, PeerID: c.Peer.ID, GlobalID: c.Peer.GlobalID, State: c.State, Connected: c.Status.Connected}
		if c.Protocol.V2 != nil && c.Protocol.V2.State != "" {
			session.State = c.Protocol.V2.State
		}
		for _, addr := range c.Peer.Addr.Addrvec {
			if parsed, err := netip.ParseAddrPort(addr.Addr); err == nil && parsed.Addr().IsValid() && !parsed.Addr().IsUnspecified() {
				session.Address = parsed.Addr().Unmap().String()
				break
			}
		}
		sessions = append(sessions, session)
	}
	sort.Slice(sessions, func(i, j int) bool {
		a, b := sessions[i], sessions[j]
		if a.Address != b.Address {
			return a.Address < b.Address
		}
		return a.GlobalID < b.GlobalID
	})
	return sessions, nil
}

// parseRBDMirrorPeerClient returns the source-side CephX user of the one
// receiving peer in the destination's "rbd mirror pool info" JSON.
func parseRBDMirrorPeerClient(raw []byte) (string, error) {
	var info struct {
		Peers []struct {
			Direction  string `json:"direction"`
			ClientName string `json:"client_name"`
		} `json:"peers"`
	}
	if err := json.Unmarshal(raw, &info); err != nil {
		return "", fmt.Errorf("decode destination mirror pool info: %w", err)
	}
	var clients []string
	for _, peer := range info.Peers {
		if peer.Direction == "rx-only" || peer.Direction == "rx-tx" {
			clients = append(clients, peer.ClientName)
		}
	}
	if len(clients) != 1 || !strings.HasPrefix(clients[0], "client.") || len(clients[0]) == len("client.") {
		return "", fmt.Errorf("destination mirror pool has receiving peer clients %q, want one", clients)
	}
	return clients[0], nil
}

// rbdMirrorMonSessions names the source MON sessions that come from the
// daemon's address: its source-side peer identity.
func (c *fixtureCluster) rbdMirrorMonSessions(ctx context.Context, peer netip.Addr) ([]rbdMirrorMonSession, error) {
	monitors, err := c.cluster.Monitors(ctx)
	if err != nil {
		return nil, err
	}
	var found []rbdMirrorMonSession
	for _, mon := range monitors {
		raw, err := mon.AdminSocket(ctx, "sessions")
		if err != nil {
			return nil, fmt.Errorf("mon.%s sessions: %w", mon.DaemonName, err)
		}
		sessions, err := parseRBDMirrorMonSessions(mon.DaemonName, raw, peer)
		if err != nil {
			return nil, err
		}
		found = append(found, sessions...)
	}
	if len(found) == 0 {
		return nil, fmt.Errorf("no source MON session comes from the mirror daemon at %s", peer)
	}
	return found, nil
}

func parseRBDMirrorMonSessions(monitor string, raw []byte, peer netip.Addr) ([]rbdMirrorMonSession, error) {
	var entries []json.RawMessage
	if err := json.Unmarshal(raw, &entries); err != nil {
		return nil, fmt.Errorf("decode mon.%s sessions: %w", monitor, err)
	}
	var found []rbdMirrorMonSession
	for _, entry := range entries {
		var session struct {
			Entity   string `json:"entity_name"`
			GlobalID uint64 `json:"global_id"`
			Addrs    struct {
				Addrvec []struct {
					Addr string `json:"addr"`
				} `json:"addrvec"`
			} `json:"addrs"`
			Socket struct {
				Addr string `json:"addr"`
			} `json:"socket_addr"`
		}
		if json.Unmarshal(entry, &session) != nil {
			continue
		}
		addrs := []string{session.Socket.Addr}
		for _, addr := range session.Addrs.Addrvec {
			addrs = append(addrs, addr.Addr)
		}
		for _, addr := range addrs {
			if parsed, err := netip.ParseAddrPort(addr); err == nil && parsed.Addr().Unmap() == peer {
				found = append(found, rbdMirrorMonSession{Monitor: monitor, Entity: session.Entity, GlobalID: session.GlobalID})
				break
			}
		}
	}
	return found, nil
}

// rbdMirrorHelperContainers counts the netem helper containers, running or
// not, that share the network namespace of target: the lease's own helpers.
// Helpers of the same image for other targets are not this lease's.
func rbdMirrorHelperContainers(ctx context.Context, imageID, target string) (int, error) {
	if imageID == "" || target == "" {
		return 0, errors.New("netem helper image or target is unknown")
	}
	docker, err := testcontainers.NewDockerClientWithOpts(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = docker.Close() }()
	listed, err := docker.ContainerList(ctx, client.ContainerListOptions{All: true, Filters: client.Filters{}.Add("ancestor", imageID)})
	if err != nil {
		return 0, err
	}
	return rbdMirrorScopedHelpers(listed.Items, imageID, target), nil
}

// rbdMirrorScopedHelpers counts the listed containers of imageID that run in
// the network namespace of the target container ID.
func rbdMirrorScopedHelpers(items []container.Summary, imageID, target string) int {
	count := 0
	for _, item := range items {
		if item.ImageID == imageID && item.HostConfig.NetworkMode == "container:"+target {
			count++
		}
	}
	return count
}

// osdPublicAddress returns the OSD's v2 public address from the native map.
func osdPublicAddress(ctx context.Context, cluster *ceph.Container, id int) (string, error) {
	addresses, err := observe(cluster).DaemonAddresses(ctx)
	if err != nil {
		return "", err
	}
	for _, osd := range addresses.OSDs {
		if osd.ID != id {
			continue
		}
		for _, addr := range osd.Public {
			if addr.Type == "v2" {
				return addr.Addr, nil
			}
		}
	}
	return "", fmt.Errorf("osd.%d has no v2 public address", id)
}

// rbdMirrorDiff is one "rbd diff v1" stream that writes data at offset of an
// image of size bytes; rbd import-diff applies it natively.
func rbdMirrorDiff(size, offset int, data []byte) []byte {
	diff := []byte("rbd diff v1\n")
	diff = append(diff, 's')
	diff = binary.LittleEndian.AppendUint64(diff, uint64(size))
	diff = append(diff, 'w')
	diff = binary.LittleEndian.AppendUint64(diff, uint64(offset))
	diff = binary.LittleEndian.AppendUint64(diff, uint64(len(data)))
	diff = append(diff, data...)
	return append(diff, 'e')
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
