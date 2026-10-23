package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
)

func TestProfileDefaults(t *testing.T) {
	// Getenv exposes both an unset variable and an explicitly empty one as "".
	p, err := loadProfile(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	want := profile{
		mdsFresh:             "0",
		mdsCommands:          "0",
		mdsSessions:          "0",
		mdsCephFS:            "0",
		mdsCephFSRanks:       "0",
		mdsCephFSRanksReplay: "0",
		mdsCephFSCompletion:  "0",
		mdsCephFSData:        "0",
		rbdBase:              "0",
		image:                "quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9",
		keyType:              "aes256k",
		serviceCipher:        "aes256k",
		ipFamily:             "4",
		mgrCount:             "2",
		runtime:              "container",
		race:                 "0",
		expireTickets:        "0",
		idleSessions:         "0",
		authEpoch:            "0",
		authEpochCycles:      "2",
		shortTickets:         "0",
		nativeMgrPreparation: "0",
		monPing:              "0",
		osdCachePools:        "0", mgrService: "0",
		connectionMode: "secure",
		mappedIPv6:     "0",
		osd:            "0",
		osdStore:       "memstore",
		osdReplicas:    "1",
		osdProfile:     "legacy-v6",
		testRun:        "^TestCeph.*Integration$",
		testTimeout:    "10m",
	}
	if p != want {
		t.Fatalf("profile = %#v, want %#v", p, want)
	}
}

// Exercise the real parser with the row forwarded by the trusted workflow.
// This binds selection to an admitted runtime and pool, rather than accepting
// a syntactically valid planner row whose parent would skip.
func TestCompressionOfferCatalogProfile(t *testing.T) {
	const id = "osd.aes.ip4.crc.host.osd-store-bluestore.osd-replicas-1.osd-profile-legacy-v6.t-msgrcompressionoffer"
	data, err := os.ReadFile(filepath.Join("..", "..", ".github", "scenarios", "catalog.json"))
	if err != nil {
		t.Fatal(err)
	}
	var catalog struct {
		Scenarios []struct {
			ID     string            `json:"id"`
			Params map[string]string `json:"params"`
		} `json:"scenarios"`
	}
	if err := json.Unmarshal(data, &catalog); err != nil {
		t.Fatal(err)
	}
	var params map[string]string
	for _, row := range catalog.Scenarios {
		if row.ID == id {
			if params != nil {
				t.Fatal("duplicate compression-offer scenario")
			}
			params = row.Params
		}
	}
	if params == nil {
		t.Fatal("compression-offer scenario missing")
	}
	forwarded := map[string]string{
		"CEPH_MSGR_TEST_KEY_TYPE": "key", "CEPH_MSGR_TEST_SERVICE_CIPHER": "service",
		"CEPH_MSGR_TEST_CONNECTION_MODE": "mode", "CEPH_MSGR_TEST_IP_FAMILY": "family",
		"CEPH_MSGR_STRESS_DURATION": "stress", "CEPH_MSGR_TEST_RUNTIME": "runtime",
		"CEPH_MSGR_TEST_OSD": "osd", "CEPH_MSGR_TEST_OSD_STORE": "osd_store",
		"CEPH_MSGR_TEST_OSD_REPLICAS": "osd_replicas", "CEPH_MSGR_TEST_OSD_PROFILE": "osd_profile",
		"CEPH_MSGR_TEST_RUN": "test_run", "CEPH_MSGR_TEST_IMAGE": "image",
	}
	p, err := loadProfile(func(name string) string { return params[forwarded[name]] })
	if err != nil {
		t.Fatal(err)
	}
	if p.testRun != "^TestCephMsgrCompressionOfferIntegration$" ||
		p.connectionMode != "crc" || p.osd != "1" || p.keyType != "aes" || p.serviceCipher != "aes" ||
		p.ipFamily != "4" || p.runtime != "host" || p.osdStore != "bluestore" ||
		p.osdProfile != "legacy-v6" || p.osdReplicas != "1" || p.image != profileDefaultImage {
		t.Fatalf("compression-offer profile = %#v", p)
	}
	if got := p.osdTopology(); got != (osdTopology{daemons: 1, poolSize: 1, poolMinSize: 1}) {
		t.Fatalf("compression-offer topology = %#v", got)
	}
	if !regexp.MustCompile(p.testRun).MatchString("TestCephMsgrCompressionOfferIntegration") {
		t.Fatal("compression-offer parent is not selected")
	}
}

func TestProfileSelection(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		want string
	}{
		{"normal", nil, "^TestCeph.*Integration$"},
		{"no-manager", map[string]string{"CEPH_MSGR_TEST_MGR_COUNT": "0"}, "^TestCephManagerAvailabilityIntegration$"},
		{"expired", map[string]string{"CEPH_MSGR_TEST_EXPIRE_TICKETS": "1"}, "^TestCeph(ExpiredTicketRecovery|DiscardedTicketProof)Integration$"},
		{"idle", map[string]string{"CEPH_MSGR_TEST_IDLE_SESSIONS": "1"}, "^TestCephIdleSessionsIntegration$"},
		{"auth-epoch", map[string]string{"CEPH_MSGR_TEST_AUTH_EPOCH": "1"}, "^TestCephServiceKeyEpochIntegration$"},
		{"fractional", map[string]string{"CEPH_MSGR_TEST_SHORT_TICKETS": "1"}, "^TestCephFractionalTicketRenewalIntegration$"},
		{"native-mgr-preparation", map[string]string{"CEPH_MSGR_TEST_NATIVE_MGR_PREPARATION": "1"}, "^TestCephNativeManagerPreparationIntegration$"},
		{"reject-mon", map[string]string{"CEPH_MSGR_TEST_MODE_REJECTION": "mon"}, "^TestCephConnectionModeRejectionIntegration$"},
		{"reject-mgr", map[string]string{"CEPH_MSGR_TEST_MODE_REJECTION": "mgr"}, "^TestCephConnectionModeRejectionIntegration$"},
		{"mapped", map[string]string{"CEPH_MSGR_TEST_MAPPED_IPV6": "1", "CEPH_MSGR_TEST_IP_FAMILY": "6"}, "^TestCephMappedAddressIntegration$"},
		{"replicated", map[string]string{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_OSD_STORE": "bluestore", "CEPH_MSGR_TEST_OSD_REPLICAS": "3"}, "^TestCeph(OSD|MonOSDMaps|MonPoolOp|MonStatfs|MonMapVersion|MonServiceMap).*Integration$"},
		{"bluestore", map[string]string{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_OSD_STORE": "bluestore"}, "^TestCeph(OSD|MonOSDMaps|MonPoolOp|MonStatfs|MonMapVersion|MonServiceMap).*Integration$"},
		{"pg-v8", map[string]string{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_OSD_PROFILE": "replicated-pg-v8"}, "^TestCeph(OSD|MonOSDMaps|MonPoolOp|MonStatfs|MonMapVersion|MonServiceMap).*Integration$"},
		{"pg-reply-v8", map[string]string{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_OSD_PROFILE": "replicated-pg-reply-v8"}, "^TestCeph(OSD|MonOSDMaps|MonPoolOp|MonStatfs|MonMapVersion|MonServiceMap).*Integration$"},
		{"pg-request-v9", map[string]string{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_OSD_PROFILE": "replicated-pg-request-v9-reply-v8"}, "^TestCeph(OSD|MonOSDMaps|MonPoolOp|MonStatfs|MonMapVersion|MonServiceMap).*Integration$"},
		{"osd", map[string]string{"CEPH_MSGR_TEST_OSD": "1"}, "^TestCeph(OSD|MonOSDMaps|MonPoolOp|MonStatfs|MonMapVersion|MonServiceMap).*Integration$"},
		{"osd-soak", map[string]string{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_OSD_STORE": "bluestore", "CEPH_MSGR_TEST_OSD_REPLICAS": "3", "CEPH_MSGR_TEST_OSD_SOAK": "4m"}, "^TestCephOSDSoak(Failover)?Integration$"},
		{"ipv6-osd", map[string]string{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_IP_FAMILY": "6"}, "^TestCeph(OSD|MonOSDMaps|MonPoolOp|MonStatfs|MonMapVersion|MonServiceMap).*Integration$"},
		{"ipv6-replicated-osd", map[string]string{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_IP_FAMILY": "6", "CEPH_MSGR_TEST_OSD_STORE": "bluestore", "CEPH_MSGR_TEST_OSD_REPLICAS": "3"}, "^TestCeph(OSD|MonOSDMaps|MonPoolOp|MonStatfs|MonMapVersion|MonServiceMap).*Integration$"},
		{"crc-host-osd", map[string]string{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_CONNECTION_MODE": "crc", "CEPH_MSGR_TEST_RUNTIME": "host", "CEPH_MSGR_TEST_RACE": "1"}, "^TestCeph(OSD|MonOSDMaps|MonPoolOp|MonStatfs|MonMapVersion|MonServiceMap).*Integration$"},
		{"crc-aes-host-race-replicated-osd", map[string]string{"CEPH_MSGR_TEST_KEY_TYPE": "aes", "CEPH_MSGR_TEST_SERVICE_CIPHER": "aes", "CEPH_MSGR_TEST_IP_FAMILY": "4", "CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_OSD_STORE": "bluestore", "CEPH_MSGR_TEST_OSD_REPLICAS": "3", "CEPH_MSGR_TEST_CONNECTION_MODE": "crc", "CEPH_MSGR_TEST_RUNTIME": "host", "CEPH_MSGR_TEST_RACE": "1"}, "^TestCeph(OSD|MonOSDMaps|MonPoolOp|MonStatfs|MonMapVersion|MonServiceMap).*Integration$"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, override := range []string{"", "^CallerChosen[Integration$"} {
				env := copyProfileEnv(tc.env)
				env["CEPH_MSGR_TEST_RUN"] = override
				p, err := loadProfile(func(name string) string { return env[name] })
				if env["CEPH_MSGR_TEST_OSD_SOAK"] != "" && override != "" {
					// Soak receipt admission resolves the selected parents before
					// fixture startup, including invalid caller regex refusal.
					if err == nil {
						t.Fatal("invalid soak selection reached fixture startup")
					}
					continue
				}
				if err != nil {
					t.Fatal(err)
				}
				if want := map[bool]string{true: "30m", false: "10m"}[env["CEPH_MSGR_TEST_OSD"] == "1"]; p.testTimeout != want {
					t.Fatalf("default test timeout %q, want %q", p.testTimeout, want)
				}
				want := tc.want
				if override != "" {
					want = override // The runner passes regex validation to go test.
				}
				if p.testRun != want {
					t.Fatalf("test selector = %q, want %q", p.testRun, want)
				}
				containerEnv, testEnv := p.containerEnvironment(), p.testEnvironment("/out", "")
				for name, want := range tc.env {
					if containerEnv[name] != want {
						t.Fatalf("container %s = %q, want %q", name, containerEnv[name], want)
					}
					switch name {
					case "CEPH_MSGR_TEST_IP_FAMILY", "CEPH_MSGR_TEST_RUNTIME", "CEPH_MSGR_TEST_RACE":
						// These select fixture addressing/build behavior, not test behavior.
					default:
						if testEnv[name] != want {
							t.Fatalf("test %s = %q, want %q", name, testEnv[name], want)
						}
					}
				}
			}
		})
	}
}

func TestProfileCipherAndRawValues(t *testing.T) {
	for _, tc := range []struct {
		key, service, want string
	}{
		{"aes", "", "aes"},
		{"aes256k", "", "aes256k"},
		{"aes", "aes256k", "aes256k"},
		{"aes256k", "aes", "aes"},
	} {
		env := map[string]string{
			"CEPH_MSGR_TEST_KEY_TYPE":       tc.key,
			"CEPH_MSGR_TEST_SERVICE_CIPHER": tc.service,
			"CEPH_MSGR_TEST_IMAGE":          " own image ",
			"CEPH_MSGR_STRESS_DURATION":     " 3m ",
			"CEPH_MSGR_TEST_TIMEOUT":        " invalid-timeout ",
			"CEPH_MSGR_TEST_DIAGNOSTICS":    " /output path ",
		}
		p, err := loadProfile(func(name string) string { return env[name] })
		if err != nil {
			t.Fatal(err)
		}
		if p.keyType != tc.key || p.serviceCipher != tc.want || p.image != " own image " || p.stressDuration != " 3m " || p.testTimeout != " invalid-timeout " || p.diagnostics != " /output path " {
			t.Fatalf("caller values changed: %#v", p)
		}
	}
}

func TestProfileInvalidChoices(t *testing.T) {
	for _, name := range []string{
		"CEPH_MSGR_TEST_KEY_TYPE", "CEPH_MSGR_TEST_SERVICE_CIPHER", "CEPH_MSGR_TEST_IP_FAMILY",
		"CEPH_MSGR_TEST_MGR_COUNT", "CEPH_MSGR_TEST_RUNTIME", "CEPH_MSGR_TEST_RACE",
		"CEPH_MSGR_TEST_EXPIRE_TICKETS", "CEPH_MSGR_TEST_IDLE_SESSIONS", "CEPH_MSGR_TEST_AUTH_EPOCH",
		"CEPH_MSGR_TEST_SHORT_TICKETS", "CEPH_MSGR_TEST_MODE_REJECTION", "CEPH_MSGR_TEST_CONNECTION_MODE",
		"CEPH_MSGR_TEST_NATIVE_MGR_PREPARATION",
		"CEPH_MSGR_TEST_MDS_FRESH",
		"CEPH_MSGR_TEST_MDS_COMMANDS",
		"CEPH_MSGR_TEST_MDS_SESSIONS",
		"CEPH_MSGR_TEST_MDS_CEPHFS",
		"CEPH_MSGR_TEST_MDS_CEPHFS_RANKS",
		"CEPH_MSGR_TEST_MDS_CEPHFS_RANKS_REPLAY",
		"CEPH_MSGR_TEST_MDS_CEPHFS_COMPLETION",
		"CEPH_MSGR_TEST_MDS_CEPHFS_DATA",
		"CEPH_MSGR_TEST_RBD",
		"CEPH_MSGR_TEST_MAPPED_IPV6", "CEPH_MSGR_TEST_OSD",
	} {
		t.Run(name, func(t *testing.T) {
			p, err := loadProfile(func(key string) string {
				if key == name {
					return " unsupported "
				}
				return ""
			})
			if err == nil || !strings.Contains(err.Error(), name) || p != (profile{}) {
				t.Fatalf("invalid choice returned %#v, %v", p, err)
			}
		})
	}
}

func TestProfileInvalidCombinations(t *testing.T) {
	auth := []string{"CEPH_MSGR_TEST_EXPIRE_TICKETS", "CEPH_MSGR_TEST_IDLE_SESSIONS", "CEPH_MSGR_TEST_AUTH_EPOCH", "CEPH_MSGR_TEST_SHORT_TICKETS"}
	cases := []map[string]string{
		{"CEPH_MSGR_TEST_OSD_REPLICAS": "3"},
		{"CEPH_MSGR_TEST_OSD_REPLICAS": "3", "CEPH_MSGR_TEST_OSD": "1"},
		{"CEPH_MSGR_TEST_OSD_STORE": "bluestore"},
		{"CEPH_MSGR_TEST_OSD_STORE": "filestore", "CEPH_MSGR_TEST_OSD": "1"},
		{"CEPH_MSGR_TEST_RACE": "1"},
		{"CEPH_MSGR_TEST_CONNECTION_MODE": "crc", "CEPH_MSGR_TEST_MODE_REJECTION": "mon"},
		{"CEPH_MSGR_TEST_CONNECTION_MODE": "crc", "CEPH_MSGR_TEST_MODE_REJECTION": "mgr"},
		{"CEPH_MSGR_TEST_MODE_REJECTION": "mgr", "CEPH_MSGR_TEST_MGR_COUNT": "0"},
		{"CEPH_MSGR_TEST_MAPPED_IPV6": "1"},
		{"CEPH_MSGR_TEST_MAPPED_IPV6": "1", "CEPH_MSGR_TEST_IP_FAMILY": "6", "CEPH_MSGR_TEST_RUNTIME": "host"},
		{"CEPH_MSGR_TEST_MAPPED_IPV6": "1", "CEPH_MSGR_TEST_IP_FAMILY": "6", "CEPH_MSGR_TEST_MGR_COUNT": "0"},
		{"CEPH_MSGR_TEST_MAPPED_IPV6": "1", "CEPH_MSGR_TEST_IP_FAMILY": "6", "CEPH_MSGR_STRESS_DURATION": "0"},
		{"CEPH_MSGR_TEST_MAPPED_IPV6": "1", "CEPH_MSGR_TEST_IP_FAMILY": "6", "CEPH_MSGR_TEST_MODE_REJECTION": "mon"},
		{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_MGR_COUNT": "0"},
		{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_STRESS_DURATION": "0"},
		{"CEPH_MSGR_TEST_OSD_SOAK": "4m"},
		{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_OSD_SOAK": "10s"},
		{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_OSD_SOAK": "forever"},
		{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_MODE_REJECTION": "mgr"},
		{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_MAPPED_IPV6": "1", "CEPH_MSGR_TEST_IP_FAMILY": "6"},
	}
	for i, key := range auth {
		cases = append(cases,
			map[string]string{key: "1", "CEPH_MSGR_TEST_NATIVE_MGR_PREPARATION": "1"},
			map[string]string{key: "1", "CEPH_MSGR_TEST_MGR_COUNT": "0"},
			map[string]string{key: "1", "CEPH_MSGR_TEST_MODE_REJECTION": "mon"},
			map[string]string{key: "1", "CEPH_MSGR_TEST_MAPPED_IPV6": "1", "CEPH_MSGR_TEST_IP_FAMILY": "6"},
			map[string]string{key: "1", "CEPH_MSGR_TEST_OSD": "1"},
		)
		for _, other := range auth[i+1:] {
			cases = append(cases, map[string]string{key: "1", other: "1"})
		}
	}
	cases = append(cases,
		map[string]string{"CEPH_MSGR_TEST_NATIVE_MGR_PREPARATION": "1", "CEPH_MSGR_TEST_MGR_COUNT": "0"},
		map[string]string{"CEPH_MSGR_TEST_NATIVE_MGR_PREPARATION": "1", "CEPH_MSGR_TEST_OSD": "1"},
		map[string]string{"CEPH_MSGR_TEST_NATIVE_MGR_PREPARATION": "1", "CEPH_MSGR_TEST_MODE_REJECTION": "mgr"},
		map[string]string{"CEPH_MSGR_TEST_NATIVE_MGR_PREPARATION": "1", "CEPH_MSGR_STRESS_DURATION": "3m"},
	)
	for i, env := range cases {
		t.Run(fmt.Sprintf("case-%02d", i), func(t *testing.T) {
			env["CEPH_MSGR_TEST_RUN"] = "^Custom$"
			p, err := loadProfile(func(name string) string { return env[name] })
			if err == nil || p != (profile{}) {
				t.Fatalf("invalid combination %v returned %#v, %v", env, p, err)
			}
		})
	}
}

func TestProfileCRCAndStressDoNotOverrestrictAuthentication(t *testing.T) {
	for _, selector := range []string{"", "CEPH_MSGR_TEST_EXPIRE_TICKETS", "CEPH_MSGR_TEST_IDLE_SESSIONS", "CEPH_MSGR_TEST_AUTH_EPOCH", "CEPH_MSGR_TEST_SHORT_TICKETS"} {
		env := map[string]string{"CEPH_MSGR_TEST_CONNECTION_MODE": "crc", "CEPH_MSGR_STRESS_DURATION": "3m"}
		if selector != "" {
			env[selector] = "1"
		} else {
			env["CEPH_MSGR_TEST_MGR_COUNT"] = "0"
		}
		if _, err := loadProfile(func(name string) string { return env[name] }); err != nil {
			t.Fatalf("valid CRC/stress profile %v: %v", env, err)
		}
	}
}

func TestProfileAuthEpochCycles(t *testing.T) {
	for _, tc := range []struct {
		epoch, cycles, want string
		ok                  bool
	}{
		{"1", "", "2", true}, {"0", "", "2", true}, {"1", "2", "2", true}, {"0", "2", "2", true},
		{"1", "1", "1", true}, {"1", "150", "150", true}, {"1", "1000", "1000", true},
		{"0", "150", "", false}, {"1", "0", "", false}, {"1", "1001", "", false},
		{"1", "-3", "", false}, {"1", "+5", "", false}, {"1", "05", "", false}, {"1", " 5", "", false}, {"1", "x", "", false},
	} {
		env := map[string]string{"CEPH_MSGR_TEST_AUTH_EPOCH": tc.epoch, "CEPH_MSGR_TEST_AUTH_EPOCH_CYCLES": tc.cycles}
		p, err := loadProfile(func(name string) string { return env[name] })
		if tc.ok != (err == nil) {
			t.Fatalf("%v: %#v, %v", env, p, err)
		}
		if !tc.ok {
			if !strings.Contains(err.Error(), "CEPH_MSGR_TEST_AUTH_EPOCH_CYCLES") || p != (profile{}) {
				t.Fatalf("%v: %#v, %v", env, p, err)
			}
			continue
		}
		if got := p.testEnvironment("/out", "")["CEPH_MSGR_TEST_AUTH_EPOCH_CYCLES"]; got != tc.want {
			t.Fatalf("%v: cycles %q, want %q", env, got, tc.want)
		}
		if _, leaked := p.containerEnvironment()["CEPH_MSGR_TEST_AUTH_EPOCH_CYCLES"]; leaked {
			t.Fatal("cycle count is a test input, not a daemon input")
		}
	}
}

func TestProfileMonitors(t *testing.T) {
	p, err := loadProfile(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	if p.monitors() != "" || p.fsid != "" {
		t.Fatal("profile fabricated topology before native bootstrap")
	}
	p.monAddresses = "172.22.0.2:3300,[fd00::3]:3300"
	if p.monitors() != p.monAddresses {
		t.Fatal("native addresses were not retained")
	}
}

func TestProfileEnvironmentContracts(t *testing.T) {
	p, err := loadProfile(func(name string) string {
		return map[string]string{
			"CEPH_MSGR_TEST_KEY_TYPE":        "aes",
			"CEPH_MSGR_TEST_SERVICE_CIPHER":  "aes256k",
			"CEPH_MSGR_TEST_RUNTIME":         "host",
			"CEPH_MSGR_TEST_RACE":            "1",
			"CEPH_MSGR_TEST_CONNECTION_MODE": "crc",
			"CEPH_MSGR_STRESS_DURATION":      "3m",
		}[name]
	})
	if err != nil {
		t.Fatal(err)
	}
	p.monAddresses = "172.22.0.2:3300,172.22.0.3:3300,172.22.0.4:3300"
	p.fsid = "94b09669-3ee8-4a10-af82-346cb674debb"
	p.peers = `{"mon.a":"172.22.0.2:3300"}`
	container := p.containerEnvironment()
	wantContainer := map[string]string{
		"CEPH_MSGR_TEST_MDS_FRESH":               "0",
		"CEPH_MSGR_TEST_MDS_COMMANDS":            "0",
		"CEPH_MSGR_TEST_MDS_SESSIONS":            "0",
		"CEPH_MSGR_TEST_MDS_CEPHFS":              "0",
		"CEPH_MSGR_TEST_MDS_CEPHFS_RANKS":        "0",
		"CEPH_MSGR_TEST_MDS_CEPHFS_RANKS_REPLAY": "0",
		"CEPH_MSGR_TEST_MDS_CEPHFS_COMPLETION":   "0",
		"CEPH_MSGR_TEST_MDS_CEPHFS_DATA":         "0",
		"CEPH_MSGR_TEST_RBD":                     "0",
		"CEPH_MSGR_TEST_KEY_TYPE":                "aes", "CEPH_MSGR_TEST_SERVICE_CIPHER": "aes256k",
		"CEPH_MSGR_TEST_IP_FAMILY": "4", "CEPH_MSGR_TEST_MGR_COUNT": "2",
		"CEPH_MSGR_TEST_RUNTIME": "host", "CEPH_MSGR_TEST_RACE": "1",
		"CEPH_MSGR_TEST_EXPIRE_TICKETS": "0", "CEPH_MSGR_TEST_IDLE_SESSIONS": "0",
		"CEPH_MSGR_TEST_AUTH_EPOCH": "0", "CEPH_MSGR_TEST_SHORT_TICKETS": "0",
		"CEPH_MSGR_TEST_NATIVE_MGR_PREPARATION": "0",
		"CEPH_MSGR_TEST_MON_PING":               "0",
		"CEPH_MSGR_TEST_OSD_CACHE_POOLS":        "0",
		"CEPH_MSGR_TEST_MGR_SERVICE":            "0",
		"CEPH_MSGR_TEST_MODE_REJECTION":         "", "CEPH_MSGR_TEST_CONNECTION_MODE": "crc",
		"CEPH_MSGR_TEST_MAPPED_IPV6": "0", "CEPH_MSGR_TEST_OSD": "0",
		"CEPH_MSGR_TEST_OSD_STORE": "memstore", "CEPH_MSGR_TEST_OSD_REPLICAS": "1",
		"CEPH_MSGR_TEST_OSD_PROFILE": "legacy-v6",
		"CEPH_MSGR_STRESS_DURATION":  "3m", "CEPH_MSGR_TEST_OSD_SOAK": "", "CEPH_MSGR_TEST_MON_FREEZE": "", "CEPH_MSGR_TEST_CONTROL_DIAGNOSTICS": "1",
	}
	if !reflect.DeepEqual(container, wantContainer) {
		t.Fatalf("container environment = %v, want %v", container, wantContainer)
	}
	for _, controlDir := range []string{"/out", filepath.Join(t.TempDir(), "control directory")} {
		for _, proxy := range []string{"", "127.0.0.1:40123"} {
			got := p.testEnvironment(controlDir, proxy)
			want := map[string]string{
				"CEPH_MSGR_TEST_MDS_FRESH":               "0",
				"CEPH_MSGR_TEST_MDS_COMMANDS":            "0",
				"CEPH_MSGR_TEST_MDS_SESSIONS":            "0",
				"CEPH_MSGR_TEST_MDS_CEPHFS":              "0",
				"CEPH_MSGR_TEST_MDS_CEPHFS_RANKS":        "0",
				"CEPH_MSGR_TEST_MDS_CEPHFS_RANKS_REPLAY": "0",
				"CEPH_MSGR_TEST_MDS_CEPHFS_COMPLETION":   "0",
				"CEPH_MSGR_TEST_MDS_CEPHFS_DATA":         "0",
				"CEPH_MSGR_TEST_RBD":                     "0",
				"CEPH_MSGR_MONITORS":                     p.monAddresses,
				"CEPH_MSGR_NATIVE_U64_XATTR_ORACLE":      "",
				"CEPH_MSGR_NATIVE_WRITESAME_ORACLE":      "",
				"CEPH_MSGR_NATIVE_SPARSE_READ_ORACLE":    "",
				"CEPH_MSGR_NATIVE_MAPEXT_ORACLE":         "",
				copyFromLateErrorMarker:                  "",
				"CEPH_MSGR_KEY_FILE":                     filepath.Join(controlDir, "key"), "CEPH_MSGR_CONTROL_DIR": controlDir,
				"CEPH_MSGR_TEST_PEERS": p.peers, "CEPH_MSGR_IDENTITY": "client.test", "CEPH_MSGR_FSID": p.fsid,
				"CEPH_MSGR_TEST_KEY_TYPE":       "aes",
				"CEPH_MSGR_TEST_SERVICE_CIPHER": "aes256k", "CEPH_MSGR_TEST_MGR_COUNT": "2",
				"CEPH_MSGR_TEST_EXPIRE_TICKETS": "0", "CEPH_MSGR_TEST_IDLE_SESSIONS": "0",
				"CEPH_MSGR_TEST_AUTH_EPOCH": "0", "CEPH_MSGR_TEST_AUTH_EPOCH_CYCLES": "2", "CEPH_MSGR_TEST_SHORT_TICKETS": "0",
				"CEPH_MSGR_TEST_NATIVE_MGR_PREPARATION": "0",
				"CEPH_MSGR_TEST_MON_PING":               "0",
				"CEPH_MSGR_TEST_OSD_CACHE_POOLS":        "0",
				"CEPH_MSGR_TEST_MGR_SERVICE":            "0",
				"CEPH_MSGR_TEST_MODE_REJECTION":         "", "CEPH_MSGR_TEST_CONNECTION_MODE": "crc",
				"CEPH_MSGR_TEST_MAPPED_IPV6": "0", "CEPH_MSGR_TEST_OSD": "0",
				"CEPH_MSGR_TEST_OSD_STORE": "memstore", "CEPH_MSGR_TEST_OSD_REPLICAS": "1",
				"CEPH_MSGR_TEST_OSD_PROFILE": "legacy-v6",
				"CEPH_MSGR_STRESS_DURATION":  "3m", "CEPH_MSGR_TEST_OSD_SOAK": "", "CEPH_MSGR_TEST_MON_FREEZE": "", "CEPH_MSGR_TEST_PROXY": proxy,
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("test environment = %v, want %v", got, want)
			}
			got["CEPH_MSGR_TEST_CONNECTION_MODE"] = "changed"
			if p.testEnvironment(controlDir, proxy)["CEPH_MSGR_TEST_CONNECTION_MODE"] != "crc" {
				t.Fatal("test environment mutation changed the profile")
			}
		}
	}
	container["CEPH_MSGR_TEST_KEY_TYPE"] = "changed"
	if p.containerEnvironment()["CEPH_MSGR_TEST_KEY_TYPE"] != "aes" {
		t.Fatal("container environment mutation changed the profile")
	}
}

func copyProfileEnv(input map[string]string) map[string]string {
	result := make(map[string]string, len(input))
	for name, value := range input {
		result[name] = value
	}
	return result
}

func TestOSDPGV8ProfileRequiresOSDAndRejectsUnknownValues(t *testing.T) {
	for _, env := range []map[string]string{
		{"CEPH_MSGR_TEST_OSD_PROFILE": "replicated-pg-v8"},
		{"CEPH_MSGR_TEST_OSD_PROFILE": "replicated-pg-reply-v8"},
		{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_OSD_PROFILE": "modern"},
	} {
		if p, err := loadProfile(func(name string) string { return env[name] }); err == nil || p != (profile{}) {
			t.Fatal("invalid explicit PG profile accepted", p, err)
		}
	}
}

func TestOSDPGProfileGateKeepsBothExplicitV8ModesAndRejectsAliases(t *testing.T) {
	for _, mode := range []string{"replicated-pg-v8", "replicated-pg-reply-v8", "replicated-pg-request-v9-reply-v8"} {
		p, err := loadProfile(func(name string) string {
			if name == "CEPH_MSGR_TEST_OSD" {
				return "1"
			}
			if name == "CEPH_MSGR_TEST_OSD_PROFILE" {
				return mode
			}
			return ""
		})
		if err != nil || !p.osdPGV8() || p.osdProfile != mode {
			t.Fatal(mode, p, err)
		}
		if p.containerEnvironment()["CEPH_MSGR_TEST_OSD_PROFILE"] != mode || p.testEnvironment("/out", "")["CEPH_MSGR_TEST_OSD_PROFILE"] != mode {
			t.Fatal("selected profile identity lost", mode)
		}
	}
	for _, mode := range []string{"replicated-pg-reply-v8 ", "reply-v8", "replicated-pg-v9", "replicated-pg-request-v9", "replicated-pg-request-v9-reply-v8 "} {
		if p, err := loadProfile(func(name string) string {
			if name == "CEPH_MSGR_TEST_OSD" {
				return "1"
			}
			if name == "CEPH_MSGR_TEST_OSD_PROFILE" {
				return mode
			}
			return ""
		}); err == nil || p != (profile{}) {
			t.Fatal("profile alias admitted", mode, p, err)
		}
	}
	if (profile{osdProfile: "legacy-v6"}).osdPGV8() {
		t.Fatal("legacy profile changed routing gate")
	}
}
