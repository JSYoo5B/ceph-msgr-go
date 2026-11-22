package main

import (
	"maps"
	"slices"
	"strings"
	"testing"
)

// The epoch-state controls of issue #9 came after the capability migration,
// so the legacy admission proof does not cover them; these are their own
// proofs against the current admission.
var osdEpochStateSelectors = []string{
	"^TestCephOSDErasureOptimizedK2M2WatchFailoverIntegration$",
	"^TestCephOSDErasureOptimizedK2M2SnapshotFailoverIntegration$",
	"^TestCephOSDErasureOptimizedK2M2PGBackoffIntegration$",
}

// Each epoch-state control has catalog rows that declare osd-erasure-width
// alone on the four-OSD topology. Over every row topology combined with each
// control's selector, with that declaration and without one, the fixture
// admits the control exactly where osd-erasure-width is declared on the
// four-OSD erasure-capacity topology, and then only for the exact test
// selected; the declaration on any other topology is refused, and so is the
// selector without the declaration on the four-OSD topology.
func TestOSDEpochStateControlsAdmissionOverTheCatalog(t *testing.T) {
	rows := ciRepositoryCatalog(t)
	for _, selector := range osdEpochStateSelectors {
		declaring := 0
		for _, row := range rows {
			if row.Params["test_run"] != selector {
				continue
			}
			if row.Params["capabilities"] != capOSDErasureWidth.name || row.Params["osd_replicas"] != "4" {
				t.Errorf("%s declares [%s] on topology %q for %s", row.ID, row.Params["capabilities"], row.Params["osd_replicas"], selector)
			}
			declaring++
		}
		if declaring == 0 {
			t.Fatal("no catalog row selects", selector)
		}
	}
	admitted, refused := 0, 0
	for _, row := range rows {
		for _, selector := range osdEpochStateSelectors {
			for _, capabilities := range []string{capOSDErasureWidth.name, ""} {
				params := maps.Clone(row.Params)
				params["test_run"], params["capabilities"] = selector, capabilities
				env := ciRowEnvironment(t, params)
				p, err := loadProfile(func(k string) string { return env[k] })
				// The row's own topology admits osd-erasure-width when the row
				// itself loads on the four-OSD erasure-capacity topology.
				own := ciRowEnvironment(t, row.Params)
				rowProfile, rowErr := loadProfile(func(k string) string { return own[k] })
				width := rowErr == nil && rowProfile.osdTopology().erasureCapacity == fixtureOSDErasureCapacity
				declared := capabilities != ""
				switch {
				case declared && width && (err != nil || p.testRun != selector || !p.admits(capOSDErasureWidth)):
					t.Errorf("%s with %s [%s]: the control was not admitted alone: %v", row.ID, selector, capabilities, err)
				case declared && !width && err == nil:
					t.Errorf("%s with %s [%s]: osd-erasure-width was admitted off the four-OSD topology", row.ID, selector, capabilities)
				case !declared && err == nil && p.osdTopology().erasureCapacity != "":
					t.Errorf("%s with %s: the four-OSD topology admitted the control without osd-erasure-width", row.ID, selector)
				}
				if declared && err == nil {
					admitted++
				} else if declared {
					refused++
				}
			}
		}
	}
	if admitted == 0 || refused == 0 {
		t.Fatal("the catalog no longer exercises both the admission and the refusal", admitted, refused)
	}
	t.Logf("%d topology and selector combinations admit the epoch-state controls, %d declarations refused", admitted, refused)
}

// osd-erasure-width admits one exact test. A joined, unanchored or prefix
// selection of the epoch-state controls is refused with the declaration, as
// is a control selected with a different capability.
func TestOSDEpochStateControlsRefuseNonExactSelections(t *testing.T) {
	base := map[string]string{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_OSD_STORE": "bluestore", "CEPH_MSGR_TEST_OSD_REPLICAS": "4", "CEPH_MSGR_TEST_OSD_PROFILE": "replicated-pg-v8"}
	load := func(selector, capabilities string) (profile, error) {
		env := copyProfileEnv(base)
		env["CEPH_MSGR_TEST_RUN"], env["CEPH_MSGR_TEST_CAPABILITIES"] = selector, capabilities
		return loadProfile(func(k string) string { return env[k] })
	}
	for _, selector := range osdEpochStateSelectors {
		if p, err := load(selector, capOSDErasureWidth.name); err != nil || p.testRun != selector {
			t.Fatal("the control was not admitted with its declaration", selector, err)
		}
	}
	for name, tc := range map[string]struct{ selector, capabilities string }{
		"joined":         {"^TestCephOSDErasureOptimizedK2M2(WatchFailover|SnapshotFailover)Integration$", capOSDErasureWidth.name},
		"joined backoff": {"^TestCephOSDErasureOptimizedK2M2(PGBackoff|Failover)Integration$", capOSDErasureWidth.name},
		"unanchored":     {strings.TrimSuffix(osdEpochStateSelectors[0], "$"), capOSDErasureWidth.name},
		"prefix":         {"^TestCephOSDErasureOptimizedK2M2.*FailoverIntegration$", capOSDErasureWidth.name},
		"undeclared":     {osdEpochStateSelectors[2], ""},
		"other":          {osdEpochStateSelectors[1], capOSDPGInterval.name},
	} {
		if p, err := load(tc.selector, tc.capabilities); err == nil {
			t.Fatalf("%s: selection admitted: %+v", name, p)
		}
	}
}

// The native oracle gained the pgstate and watchers modes for these controls.
// Like every ordinary owned-pool mode they are admitted on an OSD fixture and
// not on the MON-only cold bootstrap. The legacy admission proof compares only
// the modes of e2d4b01, all of which the oracle still has. This test names only
// its own two modes, so a mode another change adds needs no edit here.
func TestOSDEpochStateOracleModesAdmission(t *testing.T) {
	for _, mode := range legacyOwnedPoolModes {
		if !oracleModes[mode] {
			t.Fatal("a mode of the legacy admission proof is gone from the oracle", mode)
		}
	}
	if !slices.IsSorted(legacyOwnedPoolModes) {
		t.Fatal("legacy pool modes are not sorted")
	}
	added := []string{"pgstate", "watchers"}
	for _, mode := range added {
		if !oracleModes[mode] || slices.Contains(legacyOwnedPoolModes, mode) {
			t.Fatal("the epoch-state oracle mode is not a mode added after the migration", mode)
		}
	}
	cold := map[string]string{"CEPH_MSGR_TEST_MGR_SERVICE": "1", "CEPH_MSGR_TEST_MGR_COUNT": "0", "CEPH_MSGR_TEST_RUN": "^TestCephMgrServiceColdBootstrapIntegration$"}
	coldProfile, err := loadProfile(func(k string) string { return cold[k] })
	if err != nil {
		t.Fatal(err)
	}
	width := map[string]string{"CEPH_MSGR_TEST_OSD": "1", "CEPH_MSGR_TEST_OSD_STORE": "bluestore", "CEPH_MSGR_TEST_OSD_REPLICAS": "4",
		"CEPH_MSGR_TEST_OSD_PROFILE": "replicated-pg-v8", "CEPH_MSGR_TEST_RUN": osdEpochStateSelectors[0], "CEPH_MSGR_TEST_CAPABILITIES": capOSDErasureWidth.name}
	widthProfile, err := loadProfile(func(k string) string { return width[k] })
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range added {
		if !nativePoolModeAllowed(widthProfile, oracleModes, mode) {
			t.Fatal("the epoch-state control was refused its oracle mode", mode)
		}
		if nativePoolModeAllowed(coldProfile, oracleModes, mode) || nativePoolModeAllowed(profile{osd: "0"}, oracleModes, mode) {
			t.Fatal("a fixture without OSDs was admitted the oracle mode", mode)
		}
	}
}
