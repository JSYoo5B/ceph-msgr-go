package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/cephfs"
)

func freshProfileEnv() map[string]string {
	return map[string]string{"CEPH_MSGR_TEST_MDS_FRESH": "1", "CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_OSD_PROFILE": "replicated-pg-reply-v8"}
}

func TestMDSFreshClosedProfile(t *testing.T) {
	for _, mode := range []string{"replicated-pg-v8", "replicated-pg-reply-v8"} {
		for _, runtimeMode := range []string{"host", "container"} {
			env := freshProfileEnv()
			env["CEPH_MSGR_TEST_OSD_PROFILE"], env["CEPH_MSGR_TEST_RUNTIME"] = mode, runtimeMode
			p, err := loadProfile(func(k string) string { return env[k] })
			if err != nil || p.mdsFresh != "1" || p.testRun != "^TestCephMDSFreshIntegration$" {
				t.Fatal("MDS fresh profile was not exclusively selected", p, err)
			}
			p.mdsFreshIdentity = "client.fresh-test"
			for _, dir := range []string{"/out", filepath.Join(t.TempDir(), "private output")} {
				x := p.testEnvironment(dir, "")
				if x["CEPH_MSGR_KEY_FILE"] != filepath.Join(dir, "key") || x["CEPH_MSGR_IDENTITY"] != "client.test" || x["CEPH_MSGR_MDS_FRESH_KEY_FILE"] != filepath.Join(dir, "mds-fresh.key") || x["CEPH_MSGR_MDS_FRESH_IDENTITY"] != p.mdsFreshIdentity || x["CEPH_MSGR_TEST_MDS_FRESH"] != "1" || p.containerEnvironment()["CEPH_MSGR_TEST_MDS_FRESH"] != "1" {
					t.Fatal("dedicated identity altered original oracle identity or lost runtime paths", x)
				}
			}
		}
	}
	for _, change := range []map[string]string{
		{"CEPH_MSGR_TEST_MDS_FRESH": "true"}, {"CEPH_MSGR_TEST_MDS_FRESH": " 1"},
		{"CEPH_MSGR_TEST_OSD": "0"}, {"CEPH_MSGR_TEST_OSD_PROFILE": "legacy-v6"},
		{"CEPH_MSGR_TEST_OSD_REPLICAS": "3", "CEPH_MSGR_TEST_OSD_STORE": "bluestore"},
		{"CEPH_MSGR_TEST_RUN": "^TestCeph(MDSFresh|MDSMaps)Integration$"},
		{"CEPH_MSGR_TEST_RUN": ".*"}, {"CEPH_MSGR_TEST_SPARSE_READ_ORACLE": "on"},
		{"CEPH_MSGR_TEST_IDLE_SESSIONS": "1"}, {"CEPH_MSGR_TEST_MGR_COUNT": "0"},
	} {
		env := freshProfileEnv()
		for k, v := range change {
			env[k] = v
		}
		if p, err := loadProfile(func(k string) string { return env[k] }); err == nil || p != (profile{}) {
			t.Fatal("unsafe MDS fresh profile admitted", change, p, err)
		}
	}
}

func TestMDSFreshBootstrapScope(t *testing.T) {
	for _, mode := range []string{"crc", "secure"} {
		p := profile{mdsFresh: "1", osd: "1", osdProfile: "replicated-pg-reply-v8", connectionMode: mode}
		sections := bootstrapSections(t, fixtureBootstrapConfig(p))
		if sections["mds"] != nil || sections["global"]["mon_allow_pool_delete"] != "true" || sections["global"]["osd_pool_default_flag_hashpspool"] != "true" {
			t.Fatal("fresh fixture overrode provider MDS modes or lost owned-removal policy", sections)
		}
		p.mdsFresh = "0"
		normal := fixtureBootstrapConfig(p)
		if strings.Contains(normal, "[mds]") || strings.Contains(normal, "mon_allow_pool_delete") {
			t.Fatal("fresh-only policy leaked into an existing OSD fixture", normal)
		}
	}
}

// Option is a public function type with an opaque options argument. Type
// inference lets this test evaluate its actual file validator without running
// a cluster, depending on its private fields, or contacting Docker.
func validateMDSFreshConfigOption[T any](option func(*T) error) error {
	var state T
	return option(&state)
}

func TestMDSFreshBootstrapUsesAllowedPublicConfig(t *testing.T) {
	for _, mode := range []string{"crc", "secure"} {
		p := profile{mdsFresh: "1", osd: "1", osdProfile: "replicated-pg-reply-v8", connectionMode: mode}
		path := filepath.Join(t.TempDir(), "bootstrap.conf")
		config := fixtureBootstrapConfig(p)
		if err := os.WriteFile(path, []byte(config), 0600); err != nil {
			t.Fatal(err)
		}
		if err := validateMDSFreshConfigOption(ceph.WithConfigFile(path)); err != nil {
			t.Fatal("public config preflight rejected fresh bootstrap, including owned pool deletion policy", err)
		}
		if err := os.WriteFile(path, []byte(config+"[mds]\nms_service_mode = "+mode+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := validateMDSFreshConfigOption(ceph.WithConfigFile(path)); err == nil || !strings.Contains(err.Error(), "managed by the fixture") {
			t.Fatal("first native preflight failure no longer reproduced by actual provider validator", err)
		}
	}
}

func TestMDSFreshDedicatedCapsDoNotGrantMgrOrOSD(t *testing.T) {
	name := "ceph-msgr-fresh-" + strings.Repeat("a1", 16)
	caps := mdsFreshCaps(name)
	if caps.Mon != "allow r" || caps.MDS != "allow r fsname="+name || caps.MGR != "" || caps.OSD != "" {
		t.Fatal("dedicated handshake identity gained unrelated service permissions")
	}
}

type freshNativeTest struct {
	p         profile
	token     string
	name      string
	owned     map[string]bool
	status    *cephfs.FilesystemStatus
	fs, osd   map[string]any
	info, mds map[string]any
	dumpEdit  func(map[string]any)
}

func freshNativeFixture() freshNativeTest {
	token := strings.Repeat("a1", 16)
	name := "ceph-msgr-fresh-" + token
	info := map[string]any{"name": name + ".a", "gid": 51, "rank": 0, "state": "up:active", "incarnation": 1, "join_fscid": 1, "addrs": map[string]any{"addrvec": []any{map[string]any{"type": "v2", "addr": "172.18.0.9:6800", "nonce": 17}}}}
	mds := map[string]any{"epoch": 4, "fs_name": name, "enabled": true, "max_mds": 1, "metadata_pool": 3, "data_pools": []int{4}, "in": []int{0}, "up": map[string]int{"mds_0": 51}, "info": map[string]any{"gid_51": info}, "failed": []int{}, "stopped": []int{}, "damaged": []int{}}
	return freshNativeTest{
		p:     profile{mdsFresh: "1", fsid: "00112233-4455-6677-8899-aabbccddeeff", ipFamily: "4"},
		token: token, name: name, owned: map[string]bool{name + ".a": true}, info: info, mds: mds,
		status: &cephfs.FilesystemStatus{FilesystemName: name, FilesystemID: 1, MaxMDS: 1, Active: []cephfs.MDSStatus{{Name: name + ".a", GID: 51, Rank: 0, State: "up:active", Owned: true}}},
		fs:     map[string]any{"id": 1, "mdsmap": mds},
		osd:    map[string]any{"fsid": "00112233-4455-6677-8899-aabbccddeeff", "pools": []any{map[string]any{"pool": 3, "pool_name": name + "-meta"}, map[string]any{"pool": 4, "pool_name": name + "-data"}}},
	}
}

func (x freshNativeTest) parse() (mdsFreshOracle, error) {
	dump := map[string]any{"epoch": 4, "filesystems": []any{x.fs}, "standbys": []int{}}
	if x.dumpEdit != nil {
		x.dumpEdit(dump)
	}
	get, _ := json.Marshal(x.fs)
	directory, _ := json.Marshal(dump)
	osd, _ := json.Marshal(x.osd)
	return parseMDSFreshOracle(x.p, x.token, x.name, "client.mds-fresh-"+x.token, x.owned, x.status, get, directory, osd)
}

func TestMDSFreshNativeOwnedMap(t *testing.T) {
	for _, ipv6 := range []bool{false, true} {
		x := freshNativeFixture()
		address := "172.18.0.9:6800"
		if ipv6 {
			x.p.ipFamily, address = "6", "[fd62:93fe::9]:6800"
			x.info["addrs"] = map[string]any{"addrvec": []any{map[string]any{"type": "v2", "addr": address, "nonce": 17}}}
		}
		got, err := x.parse()
		if err != nil || got.FSID != x.p.fsid || got.FSCID != 1 || got.FSMapEpoch != 4 || got.MDSMapEpoch != 4 || got.MDS.Name != x.name+".a" || got.MDS.GID != 51 || got.MDS.Rank != 0 || got.MDS.Endpoint != address || got.MDS.Nonce != 17 || got.MDS.Addresses[0].Type != "v2" || got.MetadataPool != 3 || !reflect.DeepEqual(got.DataPools, []int64{4}) {
			t.Fatal("owned native MDS metadata or numeric address lost", got, err)
		}
	}
}

func TestMDSFreshNativeMapRejectsForeignOrAmbiguousIdentity(t *testing.T) {
	for name, change := range map[string]func(*freshNativeTest){
		"cluster FSID":    func(x *freshNativeTest) { x.osd["fsid"] = "foreign" },
		"invalid FSID":    func(x *freshNativeTest) { x.p.fsid = "not-a-uuid"; x.osd["fsid"] = x.p.fsid },
		"foreign FS":      func(x *freshNativeTest) { x.mds["fs_name"] = "foreign" },
		"foreign FSCID":   func(x *freshNativeTest) { x.fs["id"] = 8 },
		"unbounded FSCID": func(x *freshNativeTest) { x.fs["id"] = uint64(1) << 32 },
		"foreign join":    func(x *freshNativeTest) { x.info["join_fscid"] = 8 },
		"foreign GID":     func(x *freshNativeTest) { x.info["gid"] = 52 },
		"foreign name":    func(x *freshNativeTest) { x.info["name"] = "foreign" },
		"unowned daemon":  func(x *freshNativeTest) { x.status.Active[0].Owned = false },
		"external daemon": func(x *freshNativeTest) { x.owned = map[string]bool{"foreign": true} },
		"different rank":  func(x *freshNativeTest) { x.info["rank"] = 1 },
		"standby state":   func(x *freshNativeTest) { x.info["state"] = "up:standby" },
		"disabled FS":     func(x *freshNativeTest) { x.mds["enabled"] = false },
		"bad up identity": func(x *freshNativeTest) { x.mds["up"] = map[string]int{"mds_0": 52} },
		"wrong data pool": func(x *freshNativeTest) { x.mds["data_pools"] = []int{9} },
		"duplicate pools": func(x *freshNativeTest) { x.mds["data_pools"] = []int{3} },
		"extra FS":        func(x *freshNativeTest) { x.dumpEdit = func(d map[string]any) { d["filesystems"] = []any{x.fs, x.fs} } },
		"get/dump disagree": func(x *freshNativeTest) {
			x.dumpEdit = func(d map[string]any) { d["filesystems"] = []any{map[string]any{"id": 999}} }
		},
		"standby present": func(x *freshNativeTest) { x.dumpEdit = func(d map[string]any) { d["standbys"] = []int{1} } },
		"wrong family":    func(x *freshNativeTest) { x.p.ipFamily = "6" },
		"no address":      func(x *freshNativeTest) { x.info["addrs"] = map[string]any{"addrvec": []any{}} },
		"unknown type": func(x *freshNativeTest) {
			x.info["addrs"] = map[string]any{"addrvec": []any{map[string]any{"type": "any", "addr": "172.18.0.9:6800"}}}
		},
		"v1 only": func(x *freshNativeTest) {
			x.info["addrs"] = map[string]any{"addrvec": []any{map[string]any{"type": "v1", "addr": "172.18.0.9:6800"}}}
		},
		"unspecified addr": func(x *freshNativeTest) {
			x.info["addrs"] = map[string]any{"addrvec": []any{map[string]any{"type": "v2", "addr": "0.0.0.0:6800"}}}
		},
		"zero port": func(x *freshNativeTest) {
			x.info["addrs"] = map[string]any{"addrvec": []any{map[string]any{"type": "v2", "addr": "172.18.0.9:0"}}}
		},
		"unbounded nonce": func(x *freshNativeTest) {
			x.info["addrs"] = map[string]any{"addrvec": []any{map[string]any{"type": "v2", "addr": "172.18.0.9:6800", "nonce": uint64(1) << 32}}}
		},
		"mapped IPv6": func(x *freshNativeTest) {
			x.info["addrs"] = map[string]any{"addrvec": []any{map[string]any{"type": "v2", "addr": "[::ffff:172.18.0.9]:6800", "nonce": 17}}}
		},
		"IPv4 multicast": func(x *freshNativeTest) {
			x.info["addrs"] = map[string]any{"addrvec": []any{map[string]any{"type": "v2", "addr": "224.0.0.9:6800", "nonce": 17}}}
		},
		"IPv6 multicast": func(x *freshNativeTest) {
			x.p.ipFamily = "6"
			x.info["addrs"] = map[string]any{"addrvec": []any{map[string]any{"type": "v2", "addr": "[ff02::9]:6800", "nonce": 17}}}
		},
		"zero nonce": func(x *freshNativeTest) {
			x.info["addrs"] = map[string]any{"addrvec": []any{map[string]any{"type": "v2", "addr": "172.18.0.9:6800", "nonce": 0}}}
		},
		"ambiguous v2": func(x *freshNativeTest) {
			a := map[string]any{"type": "v2", "addr": "172.18.0.9:6800"}
			x.info["addrs"] = map[string]any{"addrvec": []any{a, a}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			x := freshNativeFixture()
			change(&x)
			if _, err := x.parse(); err == nil {
				t.Fatal("unsafe native MDS target was accepted")
			}
		})
	}
}

func TestMDSFreshObservationScope(t *testing.T) {
	token := strings.Repeat("a1", 16)
	for _, fields := range [][]string{
		{"10", token}, {"10", "foreign", "42"}, {"10", token, "0"}, {"0", token, "42"},
		{"-1", token, "42"}, {"10", token, "-1"}, {"10", token, "18446744073709551616"},
		{"10", token, "42", "outside"}, {"10", token, "+42"}, {"010", token, "42"},
	} {
		if _, _, err := parseMDSFreshObservation(fields, token, 0, 0); err == nil {
			t.Fatal("unbounded or unowned observer request accepted", fields)
		}
	}
	if id, gid, err := parseMDSFreshObservation([]string{"10", token, "42"}, token, 9, 42); err != nil || id != 10 || gid != 42 {
		t.Fatal("scoped observer request lost identity", id, gid, err)
	}
	for _, fields := range [][]string{{"10", token, "42"}, {"11", token, "43"}} {
		if _, _, err := parseMDSFreshObservation(fields, token, 10, 42); err == nil {
			t.Fatal("reused request or different client binding accepted", fields)
		}
	}
}

func TestMDSFreshControlRejectsBeforeNativeWork(t *testing.T) {
	for _, enabled := range []string{"0", "1"} {
		out := t.TempDir()
		if err := os.WriteFile(filepath.Join(out, "mds-fresh-observe"), []byte("1 unowned 42\n"), 0600); err != nil {
			t.Fatal(err)
		}
		c := &fixtureCluster{profile: profile{mdsFresh: enabled}}
		if err := c.controls(context.Background(), out); err == nil || !strings.Contains(err.Error(), "invalid owned MDS fresh") {
			t.Fatal("unowned observation reached native work", err)
		}
		entries, _ := os.ReadDir(out)
		if len(entries) != 0 {
			t.Fatal("unowned observation produced an acknowledgement", entries)
		}
	}
}

func TestMDSFreshCleanupCachesUnknownAndLocksAreBounded(t *testing.T) {
	unknown := errors.New("uncertain native Remove")
	calls := 0
	c := &fixtureCluster{mdsFresh: &mdsFreshScope{out: t.TempDir(), token: strings.Repeat("a1", 16), name: "owned", removeFilesystem: func(ctx context.Context) error {
		calls++
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) > 45*time.Second {
			t.Fatal("cleanup native mutation has no bounded deadline")
		}
		return unknown
	}}}
	if err := c.finishMDSFresh(context.Background()); !errors.Is(err, unknown) || calls != 1 || !c.mdsFresh.cleanupAttempted {
		t.Fatal("first uncertain removal lost its pending identity", err, calls)
	}
	if err := c.finishMDSFresh(context.Background()); !errors.Is(err, unknown) {
		t.Fatal("cached uncertain cleanup was replayed or replaced", err)
	}
	if calls != 1 {
		t.Fatal("uncertain native Remove was replayed", calls)
	}
	var mu sync.Mutex
	mu.Lock()
	ctx, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stop()
	if err := lockMDSFresh(ctx, &mu); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("native cleanup/observation lock ignored its deadline", err)
	}
	mu.Unlock()
	if err := lockMDSFresh(context.Background(), &mu); err != nil {
		t.Fatal(err)
	}
	mu.Unlock()
}

func TestMDSFreshEvidencePublicNoSecrets(t *testing.T) {
	out := t.TempDir()
	data := map[string]any{"source_commit": mdsFreshSource, "token": strings.Repeat("a1", 16), "client_identity": "client.mds-fresh-owned"}
	if err := writeMDSFreshJSON(out, "mds-fresh-oracle.json", data); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(out, "mds-fresh-oracle.json"))
	if err != nil || runtime.GOOS != "windows" && info.Mode().Perm() != 0644 {
		t.Fatal("public metadata cannot be collected by host", info, err)
	}
	if err := writeMDSFreshJSON(out, "too-large.json", strings.Repeat("x", 1<<20)); err == nil {
		t.Fatal("unbounded public metadata accepted")
	}
}
