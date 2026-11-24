package integration_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/JSYoo5B/ceph-msgr-go/cephmsgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/testcluster"
)

// nativeMDSCephFSCharmapEntry is one planned entry of the charmap fixture
// (integration/fixture/mds_cephfs_charmap.go): its original spelling and
// another spelling a stock client resolves to the same entry.
type nativeMDSCephFSCharmapEntry struct {
	Name  string `json:"name"`
	Probe string `json:"probe"`
}

type nativeMDSCephFSCharmapPlan struct {
	Dir           string                        `json:"dir"`
	Normalization string                        `json:"normalization"`
	Entries       []nativeMDSCephFSCharmapEntry `json:"entries"`
	Native        nativeMDSCephFSCharmapEntry   `json:"native"`
}

type nativeMDSCephFSCharmapText struct {
	CaseSensitive string `json:"casesensitive"`
	Normalization string `json:"normalization"`
	Encoding      string `json:"encoding"`
}

type nativeMDSCephFSCharmapListed struct {
	Name      string                      `json:"name"`
	DIno      uint64                      `json:"d_ino"`
	Stat      nativeMDSCephFSStat         `json:"stat"`
	Charmap   *nativeMDSCephFSCharmapText `json:"charmap"`
	TargetHex string                      `json:"target_hex"`
}

type nativeMDSCephFSCharmapProbe struct {
	Name string              `json:"name"`
	Stat nativeMDSCephFSStat `json:"stat"`
}

type nativeMDSCephFSCharmapOutput struct {
	Action         string                         `json:"action"`
	Path           string                         `json:"path"`
	NativeGlobalID uint64                         `json:"native_global_id"`
	Stat           *nativeMDSCephFSStat           `json:"stat"`
	Charmap        *nativeMDSCephFSCharmapText    `json:"charmap"`
	Entries        []nativeMDSCephFSCharmapListed `json:"entries"`
	Probes         []nativeMDSCephFSCharmapProbe  `json:"probes"`
}

// charmapWireNames is this test's consumer name policy for the plan's
// originals in a case-insensitive NFKC/UTF-8 directory: the name a stock
// client sends (Client::_wrap_name: NFKC, then full case folding), written
// out because the library applies no policy of its own. The native probes
// check each against the stock client's own wrapping.
var charmapWireNames = map[string]string{
	"\uff33\uff55\uff42-\uff24\uff49\uff52":                                     "sub-dir",
	"Cafe\u0301-File":                                                           "caf\u00e9-file",
	"\uff2e\uff4f\uff44\uff45":                                                  "node",
	"\uff33\uff59\uff4d-\uff2c\uff49\uff4e\uff4b":                               "sym-link",
	"\uff28\uff41\uff52\uff44-\uff2c\uff49\uff4e\uff4b":                         "hard-link",
	"\uff32\uff45\uff4e\uff41\uff4d\uff45\uff44":                                "renamed",
	"\uff21\uff46\uff54\uff45\uff52-\uff32\uff45\uff53\uff54\uff41\uff52\uff54": "after-restart",
	"\uff2e\uff41\uff54\uff49\uff56\uff45-\uff25\uff4e\uff54\uff52\uff59":       "native-entry",
}

// charmapPlanOrder is the operation that creates each planned entry, in the
// plan's order; the native oracle creates plan.Native.
var charmapPlanOrder = []string{"mkdir", "create", "mknod", "symlink", "link", "rename", "mkdir after restart"}

// TestCephFSCharmapWireNamesDifferFromTheirOriginals keeps the consumer
// policy table meaningful: every original is valid UTF-8 and differs from the
// name it is sent as, so a session that sent originals would not create the
// entries the stock client finds, and the wire names are distinct.
func TestCephFSCharmapWireNamesDifferFromTheirOriginals(t *testing.T) {
	seen := map[string]bool{}
	for original, wire := range charmapWireNames {
		if !utf8.ValidString(original) || !utf8.ValidString(wire) || original == wire || wire != strings.ToLower(wire) || seen[wire] || strings.ContainsAny(wire, "/\x00") {
			t.Fatalf("charmap policy entry %q -> %q is not a transformation the acceptance can observe", original, wire)
		}
		seen[wire] = true
	}
	if len(charmapWireNames) != len(charmapPlanOrder)+1 {
		t.Fatal("charmap policy table does not cover the planned entries", len(charmapWireNames))
	}
}

func cephfsMknodArgs(mode, rdev uint32) (a [cephfsArgsBytes]byte) {
	binary.LittleEndian.PutUint32(a[0:], mode)
	binary.LittleEndian.PutUint32(a[4:], rdev)
	return a
}

// charmapReply is one Go request and its first reply as received.
type charmapReply struct {
	Name          string
	Session       string
	Op            uint32
	Transaction   uint64
	Result        int32
	Safe          bool
	DentryName    string `json:",omitempty"`
	AlternateName string `json:",omitempty"`
	TargetIno     uint64 `json:",omitempty"`
	TargetMode    uint32 `json:",omitempty"`
	TargetNLink   uint32 `json:",omitempty"`
	Error         string `json:",omitempty"`
}

// TestCephMDSCephFSCharmapIntegration accepts the CHARMAP opt-in
// (Options.EnableCephFSCharmap) against a live Tentacle MDS, with the stock
// client as the independent namespace oracle. The native oracle marks an
// empty Go-created directory case-insensitive with NFKC normalization. A
// default CephFS session is refused (-EPERM, Server::can_handle_charmap) for
// mkdir, create, symlink, link and rename destination in it, with no entry
// left; its mknod is admitted, because the pinned handle_client_mknod has no
// such check, and removed at once. The opt-in session then
// performs each of the six with this test's consumer policy (the NFKC, folded
// name as the path and the original spelling as the alternate name), and
// every reply carries the wire name and the alternate name in its dentry
// lease. A new subdirectory inherits the charmap (Go InodeStat and native
// getxattr). The stock client lists each entry by its original spelling,
// resolves the original and another spelling to the Go inode, and reads the
// symlink target unchanged; Go finds the entry the stock client created by
// its wrapped name with the native original as the alternate name, and not
// by that original. The MDS transforms nothing: a lookup by an original name
// is ENOENT. The owned filesystem then requires CHARMAP (a journaled fixture
// toggle) across a restart of the owned MDS. The new incarnation admits the
// reconnect while the bit is required: it checks the requirement against the
// features its session map kept, because reconnect sends no feature bits. The
// reconnected session keeps CHARMAP in the native session record, the new
// incarnation refuses a default CephFS session with the native
// missing-feature reason, and after the requirement is removed the session
// creates one more entry, which the stock client resolves.
//
// Not exercised: a case-sensitive charmap, the nfd, nfc and nfkd
// normalizations, an encoding other than utf8, invalid UTF-8 names, a charmap
// on a nonempty directory, a rename inside or out of the charmap directory,
// a rename onto an existing entry, alternate names longer than the MDS
// limit, multiple ranks, and a reconnect refused because a session lacking
// the bit was held across a restart that added the requirement (the order of
// the new MDSMap and the reconnect phase is not owned by this fixture).
//
//scenario:row mds-cephfs-charmap.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-mdscephfscharmap key=aes service=aes mode=crc family=4 stress= runtime=host osd=1 osd_store=bluestore osd_replicas=1 osd_profile=legacy-v6 mds_fresh=1 mds_cephfs_completion=1 capabilities=mds-charmap
//scenario:row mds-cephfs-charmap.aes256k.ip6.container.osd-store-memstore.osd-replicas-1.osd-profile-legacy-v6.t-mdscephfscharmap key=aes256k service=aes256k family=6 stress= runtime=container osd=1 osd_store=memstore osd_replicas=1 osd_profile=legacy-v6 mds_fresh=1 mds_cephfs_completion=1 capabilities=mds-charmap
//scenario:covers CLIENT to MDS/CephFS
//fixture:isolated restarts the owned MDS via mds fail; a failed step leaves the charmap directory and its entries
func TestCephMDSCephFSCharmapIntegration(t *testing.T) {
	if os.Getenv("CEPH_MSGR_TEST_MDS_CEPHFS_COMPLETION") != "1" {
		t.Skip("set CEPH_MSGR_TEST_MDS_CEPHFS_COMPLETION=1 for owned CephFS charmap acceptance")
	}
	dir := os.Getenv("CEPH_MSGR_CONTROL_DIR")
	if dir == "" || os.Getenv("CEPH_MSGR_TEST_MDS_FRESH") != "1" || os.Getenv("CEPH_MSGR_TEST_OSD_PROFILE") != "legacy-v6" {
		t.Fatal("charmap acceptance requires the owned completion fixture")
	}
	ctx, stop := context.WithTimeout(context.Background(), 6*time.Minute)
	t.Cleanup(stop)
	var oracle nativeMDSCephFSCompletionOracle
	var fresh nativeMDSFresh
	var current nativeMDSCephFSTarget
	for _, read := range []struct {
		name   string
		target any
	}{{"mds-cephfs-completion-oracle.json", &oracle}, {"mds-fresh-oracle.json", &fresh}, {"mds-cephfs-completion-target.json", &current}} {
		if err := readMDSFreshJSON(filepath.Join(dir, read.name), read.target); err != nil {
			t.Fatal(err)
		}
	}
	identity, key := os.Getenv("CEPH_MSGR_MDS_CEPHFS_COMPLETION_IDENTITY"), os.Getenv("CEPH_MSGR_MDS_CEPHFS_COMPLETION_KEY_FILE")
	plan := oracle.Plan.Charmap
	planned := true
	for i, e := range plan.Entries {
		planned = planned && charmapWireNames[e.Name] != "" && e.Probe != "" && e.Probe != e.Name && i < len(charmapPlanOrder)
	}
	if oracle.Source != mdsFreshSource || oracle.Token != fresh.Token || len(oracle.Token) != 32 || oracle.FSID != fresh.FSID || oracle.FSID != os.Getenv("CEPH_MSGR_FSID") ||
		oracle.FSCID != fresh.FSCID || oracle.FSName != fresh.FSName || identity != "client.mds-cephfs-completion-"+oracle.Token || oracle.ClientIdentity != identity ||
		oracle.ClientCaps["mds"] != "allow rw fsname="+fresh.FSName || oracle.NativeCaps["mds"] != "allow rwps fsname="+fresh.FSName || oracle.Plan.Directory != "ceph-msgr-completion-"+oracle.Token ||
		plan.Dir != "charmap" || plan.Normalization != "nfkc" || len(plan.Entries) != len(charmapPlanOrder) || !planned || charmapWireNames[plan.Native.Name] == "" || plan.Native.Probe == "" ||
		current.Source != mdsFreshSource || current.Token != oracle.Token || current.Adopted != 0 || current.MDS.GID != fresh.MDS.GID || current.MDS.Nonce != fresh.MDS.Nonce {
		t.Fatal("charmap oracle, native caps or closed plan differs")
	}
	control := &mdsCephFSCompletionControl{dir: dir, token: oracle.Token, fresh: fresh, mds: current.MDS, goGIDs: map[uint64]bool{}}
	target := func(d nativeMDSCephFSDaemon) cephmsgr.MDSTarget {
		return cephmsgr.MDSTarget{FilesystemID: fresh.FSCID, Rank: 0, Address: fmt.Sprintf("v2:%s/%d", d.Endpoint, d.Nonce)}
	}
	dials := newRecoveryDials(false, current.MDS.Endpoint)
	client := recoveryClient(t, ctx, identity, key, dials, func(o *cephmsgr.Options) { o.EnableMDS, o.EnableCephFS, o.EnableCephFSCharmap = true, true, true })
	snap := client.Snapshot()
	control.goGIDs[snap.GlobalID] = true
	if !bytes.Equal(client.MDSClientFeatures(), cephmsgr.MDSCephFSCharmapClientFeatures()) {
		t.Fatal("the opt-in client does not declare the charmap bitset", client.MDSClientFeatures())
	}
	metadata := func(role string) map[string]string {
		return map[string]string{"entity_id": strings.TrimPrefix(identity, "client."), "hostname": "ceph-msgr-go-" + oracle.Token[:8] + "-" + role, "root": "/"}
	}
	options := cephmsgr.MDSSessionOptions{Metadata: metadata("charmap"), RenewInterval: 2 * time.Second, MaxEvents: 1024}
	var connection *cephmsgr.MDSConnection
	var session *cephmsgr.MDSSession
	var agent *cephfsCapAgent
	var epoch uint32
	var dirIno, cIno, linkIno, renameIno uint64
	inodes := map[string]uint64{}
	realms := map[uint64]cephmsgr.MDSSnapRealm{}
	facts := map[string]any{"global_id": snap.GlobalID, "shared_nonce": snap.CephFSNonce, "declared_hex": hex.EncodeToString(client.MDSClientFeatures()), "wire_names": charmapWireNames}
	var replies []charmapReply
	caseTimes := map[string]int64{}
	t.Cleanup(func() {
		facts["mds_dials"], facts["osd_dials"] = dials.attempts(), dials.OSD.Load()
		if agent != nil {
			facts["agent"] = agent.snapshot()
		}
		data, err := json.Marshal(map[string]any{"source_commit": mdsFreshSource, "plan": plan, "facts": facts, "replies": replies, "native": control.receipts, "case_ms": caseTimes})
		if err == nil && len(data) > 8<<20 {
			err = errors.New("charmap evidence exceeds bound")
		}
		if err == nil {
			err = testcluster.WritePublicEvidence(filepath.Join(dir, "mds-cephfs-charmap-results.json"), data)
		}
		if err != nil {
			t.Error("preserve charmap evidence", err)
		}
	})
	req := func(t *testing.T, action string, args ...string) nativeMDSCephFSCompletionReceipt {
		t.Helper()
		r, err := control.request(ctx, action, args...)
		if err != nil {
			t.Fatal("charmap control", action, err)
		}
		return r
	}
	observe := func(t *testing.T, gid uint64) *nativeMDSSession {
		t.Helper()
		r := req(t, "observe", strconv.FormatUint(gid, 10))
		epoch = r.Map.Epoch
		return r.session(gid)
	}
	// submit sends one request on s and waits for its first reply; a
	// successful unsafe reply is then made safe. It never fails on the
	// result: the caller checks it.
	submit := func(t *testing.T, label string, s *cephmsgr.MDSSession, a *cephfsCapAgent, name string, r cephmsgr.MDSRequest) (cephmsgr.MDSReply, cephmsgr.MDSTrace) {
		t.Helper()
		r.MDSMapEpoch = epoch
		e := charmapReply{Name: name, Session: label, Op: r.Op}
		call, err := s.Submit(ctx, r)
		if err != nil {
			e.Error = err.Error()
			replies = append(replies, e)
			t.Fatal("submit", name, err)
		}
		e.Transaction = call.Transaction()
		wait, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		first, err := call.Wait(wait)
		e.Result, e.Safe = first.Result, first.Safe
		if err != nil {
			e.Error = err.Error()
			replies = append(replies, e)
			t.Fatal("first reply", name, err)
		}
		trace, _, err := a.applyReply(name, first)
		if err == nil {
			var rs []cephmsgr.MDSSnapRealm
			if rs, err = first.DecodeSnapTrace(); err == nil {
				for _, realm := range rs {
					realms[realm.Ino] = realm
				}
			}
		}
		e.DentryName = trace.DentryName
		if trace.Lease != nil {
			e.AlternateName = string(trace.Lease.AlternateName)
		}
		if trace.Target != nil {
			e.TargetIno, e.TargetMode, e.TargetNLink = trace.Target.Ino, trace.Target.Mode, trace.Target.NLink
		}
		replies = append(replies, e)
		if err != nil {
			t.Fatal("decode reply", name, err)
		}
		if first.Result == 0 && !first.Safe {
			if err := s.FlushJournal(wait); err != nil {
				t.Fatal(err)
			}
			if safe, err := call.WaitSafe(wait); err != nil || safe.Result != 0 || !safe.Safe {
				t.Fatal("safe reply", name, safe.Result, err)
			}
		}
		return first, trace
	}
	getattr := func(t *testing.T, name string, ino uint64) cephmsgr.MDSOptionalMetadata {
		t.Helper()
		first, trace := submit(t, "charmap", session, agent, name, cephmsgr.MDSRequest{Op: cephmsgr.MDSOpGetattr, Args: cephfsGetattrArgs(cephStatAll), Path: cephmsgr.MDSFilePath{Ino: ino}})
		if first.Result != 0 || trace.Target == nil || trace.Target.Ino != ino {
			t.Fatal("getattr differs", name, first.Result)
		}
		opt, err := trace.Target.DecodeOptionalMetadata()
		if err != nil {
			t.Fatalf("%s optional metadata %x: %v", name, trace.Target.OptionalMetadata, err)
		}
		return opt
	}
	wantCharmap := cephmsgr.MDSCharmap{CaseSensitive: false, Normalization: plan.Normalization, Encoding: "utf8"}
	nativeCharmap := nativeMDSCephFSCharmapText{CaseSensitive: "0", Normalization: plan.Normalization, Encoding: "utf8"}
	hasCharmap := func(opt cephmsgr.MDSOptionalMetadata) bool {
		return opt.Charmap != nil && *opt.Charmap == wantCharmap && len(opt.Options) == 1 && opt.Options[0].Kind == cephmsgr.MDSOptionalMetadataCharmap
	}
	// created checks a successful entry-creating reply: the dentry is the
	// wire name in the charmap directory and its lease carries the original.
	created := func(t *testing.T, name string, first cephmsgr.MDSReply, trace cephmsgr.MDSTrace, wire, original string, mode uint32) uint64 {
		t.Helper()
		if first.Result != 0 || trace.Target == nil || trace.DirInode == nil || trace.DirInode.Ino != cIno || trace.DentryName != wire || trace.Lease == nil ||
			string(trace.Lease.AlternateName) != original || trace.Target.Mode&0o170000 != mode {
			t.Fatalf("%s reply differs: result %d dentry %q lease %+v", name, first.Result, trace.DentryName, trace.Lease)
		}
		return trace.Target.Ino
	}
	// names reads the charmap directory with the stock client.
	names := func(t *testing.T) *nativeMDSCephFSCharmapOutput {
		t.Helper()
		out := req(t, "charmap-names").Charmap
		if out.Charmap == nil || *out.Charmap != nativeCharmap || out.Stat == nil || out.Stat.Ino != cIno {
			t.Fatal("native charmap directory readback differs", out.Charmap, out.Stat)
		}
		return out
	}
	probe := func(out *nativeMDSCephFSCharmapOutput, name string) nativeMDSCephFSStat {
		for _, p := range out.Probes {
			if p.Name == name {
				return p.Stat
			}
		}
		return nativeMDSCephFSStat{}
	}
	leaf := func(name string, run func(*testing.T)) bool {
		at := time.Now()
		ok := t.Run(name, run)
		caseTimes[name] = time.Since(at).Milliseconds()
		if agent != nil {
			if s := agent.snapshot(); len(s.Errors) != 0 {
				t.Error("charmap cap agent", s.Errors)
				ok = false
			}
		}
		return ok
	}

	if !leaf("opt_in_session_and_native_charmap", func(t *testing.T) {
		var err error
		if connection, err = client.OpenMDS(ctx, target(current.MDS)); err != nil {
			t.Fatal(err)
		}
		if session, err = connection.OpenSession(ctx, options); err != nil {
			t.Fatal(err)
		}
		agent = startCephFSCapAgent(ctx, session)
		if err := checkNativeCephFSCharmapSession(observe(t, snap.GlobalID), identity, options.Metadata); err != nil {
			t.Fatal(err)
		}
		first, trace := submit(t, "charmap", session, agent, "mkdir owned directory", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpMkdir, Flags: cephmsgr.MDSRequestFlagWantDentry,
			Args: cephfsMkdirArgs(0o40755), Path: cephmsgr.MDSFilePath{Ino: 1, Path: oracle.Plan.Directory}})
		if first.Result != 0 || trace.Target == nil {
			t.Fatal("owned directory mkdir differs", first.Result)
		}
		dirIno = trace.Target.Ino
		first, trace = submit(t, "charmap", session, agent, "mkdir charmap directory", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpMkdir, Flags: cephmsgr.MDSRequestFlagWantDentry,
			Args: cephfsMkdirArgs(0o40755), Path: cephmsgr.MDSFilePath{Ino: dirIno, Path: plan.Dir}})
		if first.Result != 0 || trace.Target == nil || trace.DentryName != plan.Dir {
			t.Fatal("charmap directory mkdir differs", first.Result)
		}
		cIno = trace.Target.Ino
		if opt := getattr(t, "getattr charmap directory before", cIno); opt.Options != nil || opt.Charmap != nil {
			t.Fatal("the charmap directory has options before the native change", opt)
		}
		set := req(t, "charmap-set").Charmap
		if set.Charmap == nil || *set.Charmap != nativeCharmap || set.Stat == nil || set.Stat.Ino != cIno {
			t.Fatal("native charmap set readback differs", set.Charmap, set.Stat)
		}
		if opt := getattr(t, "getattr charmap directory", cIno); !hasCharmap(opt) {
			t.Fatal("Go charmap differs from the native readback", opt)
		}
		// The link and rename sources live in the owned directory, which has
		// no charmap.
		for _, source := range []struct {
			name string
			ino  *uint64
		}{{"link-source", &linkIno}, {"rename-source", &renameIno}} {
			first, trace := submit(t, "charmap", session, agent, "create "+source.name, cephmsgr.MDSRequest{Op: cephmsgr.MDSOpCreate, Flags: cephmsgr.MDSRequestFlagWantDentry,
				Args: cephfsOpenArgs(cephOpenCreate|cephOpenExclusive|cephOpenReadWrite, 0o644, 0), Path: cephmsgr.MDSFilePath{Ino: dirIno, Path: source.name}})
			if first.Result != 0 || trace.Target == nil || trace.DentryName != source.name || !cephfsCreated(first.Extra, trace.Target.Ino) {
				t.Fatal("source create differs", source.name, first.Result)
			}
			*source.ino = trace.Target.Ino
			agent.open(trace.Target.Ino, cephFileModeRDWR)
			if _, _, err := agent.close(trace.Target.Ino, cephFileModeRDWR); err != nil {
				t.Fatal(err)
			}
		}
		facts["owned_dir_ino"], facts["charmap_ino"], facts["link_source_ino"], facts["rename_source_ino"] = dirIno, cIno, linkIno, renameIno
	}) {
		return
	}

	// requests builds the six entry-creating requests of one name in the
	// charmap directory.
	requests := func(wire string, alternate []byte, symlinkTarget string) map[string]cephmsgr.MDSRequest {
		at := cephmsgr.MDSFilePath{Ino: cIno, Path: wire}
		flags := uint32(cephmsgr.MDSRequestFlagWantDentry)
		return map[string]cephmsgr.MDSRequest{
			"mkdir":  {Op: cephmsgr.MDSOpMkdir, Flags: flags, Args: cephfsMkdirArgs(0o40755), Path: at, AlternateName: alternate},
			"create": {Op: cephmsgr.MDSOpCreate, Flags: flags, Args: cephfsOpenArgs(cephOpenCreate|cephOpenExclusive|cephOpenReadWrite, 0o644, 0), Path: at, AlternateName: alternate},
			"mknod":  {Op: cephmsgr.MDSOpMknod, Flags: flags, Args: cephfsMknodArgs(0o100644, 0), Path: at, AlternateName: alternate},
			// The target is the symlink's data: the MDS stores it unchanged.
			"symlink": {Op: cephmsgr.MDSOpSymlink, Flags: flags, Path: at, Path2: cephmsgr.MDSFilePath{Path: symlinkTarget}, AlternateName: alternate},
			"link":    {Op: cephmsgr.MDSOpLink, Flags: flags, Path: at, Path2: cephmsgr.MDSFilePath{Ino: dirIno, Path: "link-source"}, AlternateName: alternate},
			"rename":  {Op: cephmsgr.MDSOpRename, Flags: flags, Path: at, Path2: cephmsgr.MDSFilePath{Ino: dirIno, Path: "rename-source"}, AlternateName: alternate},
		}
	}
	symlinkTarget := plan.Entries[1].Name

	if !leaf("default_profile_refused_in_charmap_directory", func(t *testing.T) {
		plain, _ := mdsCommandClient(t, ctx, identity, key, current.MDS.Endpoint, true, func(o *cephmsgr.Options) { o.EnableCephFS = true })
		gid := plain.Snapshot().GlobalID
		control.goGIDs[gid] = true
		conn, err := plain.OpenMDS(ctx, target(current.MDS))
		if err != nil {
			t.Fatal(err)
		}
		meta := metadata("plain")
		s, err := conn.OpenSession(ctx, cephmsgr.MDSSessionOptions{Metadata: meta, RenewInterval: 2 * time.Second, MaxEvents: 1024})
		if err != nil {
			t.Fatal(err)
		}
		a := startCephFSCapAgent(ctx, s)
		if err := checkNativeCephFSSession(observe(t, gid), identity, meta); err != nil {
			t.Fatal(err)
		}
		// The names a consumer without the opt-in would send are irrelevant:
		// the MDS refuses before it reads them.
		ops := requests("refused", nil, symlinkTarget)
		for _, op := range []string{"mkdir", "create", "symlink", "link", "rename"} {
			first, _ := submit(t, "plain", s, a, "plain "+op+" in charmap directory", ops[op])
			if first.Result != -1 || first.IsTarget {
				t.Fatalf("default CephFS %s in the charmap directory was not refused with -EPERM: %d", op, first.Result)
			}
		}
		// handle_client_mknod never calls can_handle_charmap: the pinned MDS
		// creates the node, without any name policy. It is removed at once,
		// before a stock client could list an entry with no alternate name.
		first, trace := submit(t, "plain", s, a, "plain mknod in charmap directory", ops["mknod"])
		if first.Result != 0 || trace.Target == nil || trace.DentryName != "refused" || trace.Lease == nil || len(trace.Lease.AlternateName) != 0 {
			t.Fatal("default CephFS mknod in the charmap directory differs from the ungated native handler", first.Result)
		}
		facts["plain_mknod_ino"] = trace.Target.Ino
		if first, _ := submit(t, "plain", s, a, "plain unlink mknod", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpUnlink, Path: cephmsgr.MDSFilePath{Ino: cIno, Path: "refused"}}); first.Result != 0 {
			t.Fatal("remove the default CephFS mknod entry", first.Result)
		}
		listed := names(t)
		if len(listed.Entries) != 0 {
			t.Fatal("a refused request left an entry in the charmap directory", listed.Entries)
		}
		first, trace = submit(t, "plain", s, a, "plain lookup rename source", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpLookup, Flags: cephmsgr.MDSRequestFlagWantDentry,
			Args: cephfsGetattrArgs(cephStatAll), Path: cephmsgr.MDSFilePath{Ino: dirIno, Path: "rename-source"}})
		if first.Result != 0 || trace.Target == nil || trace.Target.Ino != renameIno {
			t.Fatal("the refused rename moved its source", first.Result)
		}
		items, barrier := a.releaseAll()
		if len(items) != 0 {
			if err := s.ReleaseCaps(ctx, items, barrier); err != nil {
				t.Fatal(err)
			}
		}
		closing, cancel := context.WithTimeout(ctx, 15*time.Second)
		err = s.Close(closing)
		cancel()
		if err != nil || s.Status().State != cephmsgr.MDSSessionClosed {
			t.Fatal("default CephFS session close unconfirmed", err)
		}
		if observe(t, gid) != nil {
			t.Fatal("default CephFS session remains in the native map")
		}
		facts["plain_global_id"] = gid
	}) {
		return
	}

	if !leaf("opt_in_creates_wrapped_entries", func(t *testing.T) {
		modes := map[string]uint32{"mkdir": 0o040000, "create": 0o100000, "mknod": 0o100000, "symlink": 0o120000, "link": 0o100000, "rename": 0o100000}
		for i, op := range charmapPlanOrder[:6] {
			original := plan.Entries[i].Name
			wire := charmapWireNames[original]
			first, trace := submit(t, "charmap", session, agent, op+" "+wire, requests(wire, []byte(original), symlinkTarget)[op])
			ino := created(t, op+" "+wire, first, trace, wire, original, modes[op])
			inodes[original] = ino
			switch op {
			case "create":
				if !cephfsCreated(first.Extra, ino) {
					t.Fatal("create extra does not name the created inode", hex.EncodeToString(first.Extra))
				}
				agent.open(ino, cephFileModeRDWR)
				if _, _, err := agent.close(ino, cephFileModeRDWR); err != nil {
					t.Fatal(err)
				}
			case "link":
				if ino != linkIno || trace.Target.NLink != 2 {
					t.Fatal("link did not add a second name to the source inode", ino, trace.Target.NLink)
				}
			case "rename":
				if ino != renameIno {
					t.Fatal("rename did not move the source inode", ino)
				}
			}
		}
		// The new subdirectory inherits the charmap (handle_client_mkdir).
		if opt := getattr(t, "getattr new subdirectory", inodes[plan.Entries[0].Name]); !hasCharmap(opt) {
			t.Fatal("the new subdirectory did not inherit the charmap", opt)
		}
		// The MDS resolves only the wire name; the original is ENOENT.
		for _, e := range plan.Entries[:6] {
			wire := charmapWireNames[e.Name]
			first, trace := submit(t, "charmap", session, agent, "lookup "+wire, cephmsgr.MDSRequest{Op: cephmsgr.MDSOpLookup, Flags: cephmsgr.MDSRequestFlagWantDentry,
				Args: cephfsGetattrArgs(cephStatAll), Path: cephmsgr.MDSFilePath{Ino: cIno, Path: wire}})
			if first.Result != 0 || trace.Target == nil || trace.Target.Ino != inodes[e.Name] || trace.Lease == nil || string(trace.Lease.AlternateName) != e.Name {
				t.Fatal("lookup by the wire name differs", wire, first.Result)
			}
			if first, _ := submit(t, "charmap", session, agent, "lookup original "+wire, cephmsgr.MDSRequest{Op: cephmsgr.MDSOpLookup, Flags: cephmsgr.MDSRequestFlagWantDentry,
				Args: cephfsGetattrArgs(cephStatAll), Path: cephmsgr.MDSFilePath{Ino: cIno, Path: e.Name}}); first.Result != -2 {
				t.Fatal("the MDS resolved an original spelling", e.Name, first.Result)
			}
		}
		facts["inodes"] = inodes
	}) {
		return
	}

	// checkNative compares the stock client's view with the Go entries
	// created so far (the first count planned entries) and the native entry.
	checkNative := func(t *testing.T, count int, nativeIno uint64) *nativeMDSCephFSCharmapOutput {
		t.Helper()
		out := names(t)
		want := map[string]uint64{plan.Native.Name: nativeIno}
		for _, e := range plan.Entries[:count] {
			want[e.Name] = inodes[e.Name]
		}
		if len(out.Entries) != len(want) {
			t.Fatal("native listing of the charmap directory differs", out.Entries)
		}
		for _, e := range out.Entries {
			if want[e.Name] == 0 || e.DIno != want[e.Name] || e.Stat.Ino != want[e.Name] {
				t.Fatalf("native listing names %q (inode %d) instead of an original spelling with its Go inode", e.Name, e.DIno)
			}
			switch {
			case e.Name == plan.Entries[0].Name || count > 6 && e.Name == plan.Entries[6].Name:
				if e.Charmap == nil || *e.Charmap != nativeCharmap {
					t.Fatal("native readback of an inherited charmap differs", e.Name, e.Charmap)
				}
			case e.Name == plan.Entries[3].Name:
				if e.TargetHex != hex.EncodeToString([]byte(symlinkTarget)) {
					t.Fatal("native symlink target differs", e.TargetHex)
				}
			}
		}
		for i, e := range append(slices.Clone(plan.Entries), plan.Native) {
			ino, exists := want[e.Name], true
			if i < len(plan.Entries) && i >= count {
				ino, exists = 0, false
			}
			for _, spelling := range []string{e.Name, e.Probe} {
				if s := probe(out, spelling); s.Exists != exists || s.Ino != ino || !exists && s.Errno != 2 {
					t.Fatalf("native stat of %q resolves to %+v, want inode %d", spelling, s, ino)
				}
			}
		}
		return out
	}
	var nativeIno uint64
	if !leaf("native_namespace_matches_wrapped_names", func(t *testing.T) {
		made := req(t, "charmap-create").Charmap
		if made.Stat == nil || !made.Stat.Exists || made.Stat.Ino == 0 {
			t.Fatal("native charmap entry create differs", made.Stat)
		}
		nativeIno = made.Stat.Ino
		facts["native_before_restart"] = checkNative(t, 6, nativeIno)
		wire := charmapWireNames[plan.Native.Name]
		first, trace := submit(t, "charmap", session, agent, "lookup native entry", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpLookup, Flags: cephmsgr.MDSRequestFlagWantDentry,
			Args: cephfsGetattrArgs(cephStatAll), Path: cephmsgr.MDSFilePath{Ino: cIno, Path: wire}})
		if first.Result != 0 || trace.Target == nil || trace.Target.Ino != nativeIno || trace.DentryName != wire || trace.Lease == nil || string(trace.Lease.AlternateName) != plan.Native.Name {
			t.Fatal("Go lookup of the native entry by its wire name differs", first.Result, trace.Lease)
		}
		if first, _ := submit(t, "charmap", session, agent, "lookup native original", cephmsgr.MDSRequest{Op: cephmsgr.MDSOpLookup, Flags: cephmsgr.MDSRequestFlagWantDentry,
			Args: cephfsGetattrArgs(cephStatAll), Path: cephmsgr.MDSFilePath{Ino: cIno, Path: plan.Native.Name}}); first.Result != -2 {
			t.Fatal("the MDS resolved the native original spelling", first.Result)
		}
		inodes[plan.Native.Name] = nativeIno
	}) {
		return
	}

	if !leaf("restart_reconnect_keeps_the_declaration", func(t *testing.T) {
		// The owned filesystem requires CHARMAP across the restart. Applying
		// the requirement evicts nothing, because the open opt-in session
		// declares the bit.
		required := req(t, "charmap-require-on")
		if required.Map == nil || len(required.Map.Required) != 1 || required.Map.Required["feature_22"] != "charmap" || required.Mutation == nil || required.Mutation.State != "applied" {
			t.Fatal("the charmap requirement was not applied", required.Map, required.Mutation)
		}
		if err := checkNativeCephFSCharmapSession(observe(t, snap.GlobalID), identity, options.Metadata); err != nil || session.Status().State != cephmsgr.MDSSessionOpen {
			t.Fatal("the charmap requirement disturbed the opt-in session", err, session.Status())
		}
		facts["required_before_restart"] = required.Map
		armed := req(t, "restart-arm").Restart
		facts["armed"] = armed
		req(t, "restart-fire")
		select {
		case <-session.Done():
		case <-time.After(45 * time.Second):
			t.Fatal("charmap session did not end after the restart")
		}
		lost := session
		if lost.Status().State != cephmsgr.MDSSessionLost || !describeRecoveryError(lost.Err()).SessionReset {
			t.Fatal("charmap session did not become Lost with RESET", lost.Status())
		}
		select {
		case <-agent.done:
		case <-time.After(10 * time.Second):
			t.Fatal("old charmap cap agent did not stop")
		}
		caps, rs, err := agent.reconnectState(realms)
		if err != nil {
			t.Fatal(err)
		}
		// The renamed inode's short path is its new dentry: the consumer
		// sends the wire name wherever a name travels again.
		renamed := charmapWireNames[plan.Entries[5].Name]
		for i := range caps {
			if caps[i].Ino == renameIno {
				caps[i].PathBase, caps[i].Path = cIno, renamed
			}
		}
		// The MDS issued caps with mkdir, create and regular-file mknod (not
		// symlink); the renamed file keeps its own. The linked inode keeps
		// its first dentry, link-source.
		for _, i := range []int{0, 1, 2, 5} {
			ino, wire := inodes[plan.Entries[i].Name], charmapWireNames[plan.Entries[i].Name]
			if !slices.ContainsFunc(caps, func(c cephmsgr.MDSCapReconnect) bool { return c.Ino == ino && c.PathBase == cIno && c.Path == wire }) {
				t.Fatal("reconnect state lacks the wire path of a held cap", wire, ino)
			}
		}
		facts["reconnect_caps"], facts["reconnect_realms"], facts["lost"] = caps, rs, lost.Status()
		var next *nativeMDSCephFSDaemon
		until := time.Now().Add(60 * time.Second)
		for next == nil {
			status := req(t, "restart-status").RankStatus
			if status.Rank0 != nil && status.Rank0.GID != armed.Armed.GID && status.Rank0.Name == armed.Armed.Name && status.Rank0.State != "up:replay" {
				next = status.Rank0
				break
			}
			if time.Now().After(until) {
				t.Fatal("charmap replacement never reached reconnect", status)
			}
			time.Sleep(100 * time.Millisecond)
		}
		if next.State != "up:reconnect" || next.Nonce == armed.Armed.Nonce || next.Incarnation == armed.Armed.Incarnation {
			t.Fatal("charmap replacement incarnation differs", next)
		}
		facts["replacement"] = next
		if err := connection.Close(); err != nil {
			t.Fatal(err)
		}
		dials.allowMDS(next.Endpoint)
		if connection, err = client.OpenMDS(ctx, target(*next)); err != nil {
			t.Fatal(err)
		}
		opening, cancel := context.WithTimeout(ctx, 30*time.Second)
		reopened, calls, err := connection.ReconnectSession(opening, lost, cephmsgr.MDSReconnectState{Caps: caps, Realms: rs}, options)
		cancel()
		if err != nil || len(calls) != 0 || reopened == nil || reopened.Status().State != cephmsgr.MDSSessionOpen || reopened.Status().ReconnectedFrom != lost.Status().Generation ||
			reopened.Status().GlobalID != snap.GlobalID || !cephmsgr.MDSFeatureBitsetHas(reopened.Status().Open.SupportedFeatures, 22) {
			t.Fatal("charmap reconnect did not OPEN", err)
		}
		session = reopened
		agent = startCephFSCapAgentFrom(ctx, reopened, agent)
		facts["reconnected"] = session.Status()
		until = time.Now().Add(60 * time.Second)
		for {
			status := req(t, "restart-status").RankStatus
			if status.Rank0 != nil && status.Rank0.GID == next.GID && status.Rank0.State == "up:active" {
				break
			}
			if time.Now().After(until) {
				t.Fatal("charmap replacement never became active", status)
			}
			time.Sleep(100 * time.Millisecond)
		}
		req(t, "restart-adopt")
		native := observe(t, snap.GlobalID)
		if err := checkNativeCephFSCharmapSession(native, identity, options.Metadata); err != nil {
			t.Fatal("the new incarnation does not keep the CHARMAP declaration", err)
		}
		if !strings.HasSuffix(native.Address, ":0/"+strconv.FormatUint(uint64(snap.CephFSNonce), 10)) || native.Caps < int64(len(caps)) {
			t.Fatal("charmap reconnect changed native identity or lost held caps", native)
		}
		// The new incarnation read the requirement from its first MDSMap and
		// admitted the reconnect against the features its session map kept.
		// It refuses a default CephFS session with the native reason, which
		// prints the missing bit 22 as one feature_bitset_t block, and keeps
		// no session for it.
		plain, _ := mdsCommandClient(t, ctx, identity, key, next.Endpoint, true, func(o *cephmsgr.Options) { o.EnableCephFS = true })
		plainGID := plain.Snapshot().GlobalID
		control.goGIDs[plainGID] = true
		plainConn, err := plain.OpenMDS(ctx, target(*next))
		if err != nil {
			t.Fatal(err)
		}
		refused, err := plainConn.OpenSession(ctx, cephmsgr.MDSSessionOptions{Metadata: metadata("required"), RenewInterval: 2 * time.Second})
		var rejected *cephmsgr.MDSSessionRejectedError
		if refused != nil {
			closing, cancel := context.WithTimeout(ctx, 15*time.Second)
			refused.Close(closing)
			cancel()
		}
		if refused != nil || !errors.As(err, &rejected) || rejected.Reason != "missing required features '0x0000000000400000'" || rejected.Blocklisted || rejected.Flags != 0 ||
			rejected.Metadata["error_string"] != rejected.Reason {
			t.Fatal("the new incarnation did not refuse a default CephFS session while CHARMAP is required", err)
		}
		if observe(t, plainGID) != nil {
			t.Fatal("the refused default CephFS session is in the native session map")
		}
		facts["required_reject_reason"] = rejected.Reason
		restored := req(t, "charmap-require-off")
		if restored.Map == nil || len(restored.Map.Required) != 0 || restored.Mutation == nil || restored.Mutation.State != "restored" {
			t.Fatal("the charmap requirement was not restored", restored.Map, restored.Mutation)
		}
		// The new incarnation honors the kept declaration: one more entry.
		original := plan.Entries[6].Name
		wire := charmapWireNames[original]
		first, trace := submit(t, "charmap", session, agent, "mkdir "+wire+" after restart", requests(wire, []byte(original), symlinkTarget)["mkdir"])
		inodes[original] = created(t, "mkdir after restart", first, trace, wire, original, 0o040000)
		if opt := getattr(t, "getattr subdirectory after restart", inodes[original]); !hasCharmap(opt) {
			t.Fatal("the subdirectory created after the restart did not inherit the charmap", opt)
		}
		facts["native_after_restart"] = checkNative(t, 7, nativeIno)
		if dials.OSD.Load() != 0 || client.Snapshot().GlobalID != snap.GlobalID || client.Snapshot().CephFSNonce != snap.CephFSNonce {
			t.Fatal("charmap reconnect issued object I/O or changed client identity")
		}
	}) {
		return
	}

	leaf("cleanup_unlink_release_close", func(t *testing.T) {
		for _, e := range append(slices.Clone(plan.Entries), plan.Native) {
			op := cephmsgr.MDSOpUnlink
			if e.Name == plan.Entries[0].Name || e.Name == plan.Entries[6].Name {
				op = cephmsgr.MDSOpRmdir
			}
			wire := charmapWireNames[e.Name]
			if first, _ := submit(t, "charmap", session, agent, "remove "+wire, cephmsgr.MDSRequest{Op: op, Path: cephmsgr.MDSFilePath{Ino: cIno, Path: wire}}); first.Result != 0 {
				t.Fatal("remove charmap entry", wire, first.Result)
			}
		}
		for _, step := range []struct {
			op   uint32
			base uint64
			name string
		}{{cephmsgr.MDSOpUnlink, dirIno, "link-source"}, {cephmsgr.MDSOpRmdir, dirIno, plan.Dir}, {cephmsgr.MDSOpRmdir, 1, oracle.Plan.Directory}} {
			if first, _ := submit(t, "charmap", session, agent, "remove "+step.name, cephmsgr.MDSRequest{Op: step.op, Path: cephmsgr.MDSFilePath{Ino: step.base, Path: step.name}}); first.Result != 0 {
				t.Fatal("remove", step.name, first.Result)
			}
		}
		native, err := control.stat(ctx, "dir")
		if err != nil || native.Exists || native.Errno != 2 {
			t.Fatal("native owned directory remained", native, err)
		}
		items, barrier := agent.releaseAll()
		if len(items) != 0 {
			if err := session.ReleaseCaps(ctx, items, barrier); err != nil {
				t.Fatal(err)
			}
		}
		closing, cancel := context.WithTimeout(ctx, 15*time.Second)
		err = session.Close(closing)
		cancel()
		if err != nil || session.Status().State != cephmsgr.MDSSessionClosed {
			t.Fatal("charmap close unconfirmed", err)
		}
		if observe(t, snap.GlobalID) != nil {
			t.Fatal("charmap session remains in the native map")
		}
	})
}
