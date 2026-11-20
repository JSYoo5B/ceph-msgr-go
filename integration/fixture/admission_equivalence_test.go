package main

import (
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// This file is the migration proof of the capability admission (issue #175).
// It keeps the selector-keyed admission that the fixture used before, copied
// from integration/fixture at e2d4b01 (loadProfile, fixtureTicketTTLs and the
// admission predicates), and requires the capability admission to admit the
// same modes. The copy compares a run argument with the old named selectors;
// it is test-only and is not used by the fixture.
//
// The proof is historical. It covers the catalog rows of the migration
// (legacyHistoricalRows), their topologies and their selectors and
// declarations, the only cases the old admission decided. A row, selector,
// capability or mode added after the migration has no old admission: it is
// not taught to this copy, and its own tests prove where it is admitted and
// refused. The copy stays as of e2d4b01.

const (
	legacyCopyFromLateErrorSelector                = "^TestCephOSDCopyFromLateErrorIntegration$"
	legacyCopyFromMultiChunkSelector               = "^TestCephOSDCopyFromMultiChunkLateErrorIntegration$"
	legacyDaemonCommandMDSSelector                 = "^TestCephAppliedMDSCommandIsNotReplayedIntegration$"
	legacyMonForcedKillSelector                    = "^TestCephMonMapVersion(ConnectedKill|UnprocessedKill)Integration$"
	legacyMdsCephFSSharedFlockSelector             = "^TestCephMDSCephFSSharedFlockRecoveryIntegration$"
	legacyMdsCephFSHeldFlockSplitSelector          = "^TestCephMDSCephFSHeldFlockSplitReconnectIntegration$"
	legacyMdsCephFSHeldFlockSplitFragmentsSelector = "^TestCephMDSCephFSHeldFlockSplitFragmentsIntegration$"
	legacyMdsCephFSWritebackSelector               = "^TestCephMDSCephFSWritebackFenceIntegration$"
	legacyCacheOpGoSelector                        = "^TestCephOSDCacheOpGoIntegration$"
	legacyCacheTierOpsSelector                     = "^TestCephOSDCacheTierOpsIntegration$"
	legacyMgrServiceColdBootstrapSelector          = "^TestCephMgrServiceColdBootstrapIntegration$"
	legacyRbdJournalLostAppendSelector             = "^TestCephRBDJournalLostAppendReplyIntegration$"
	legacyRbdJournalPrimaryChangeSelector          = "^TestCephRBDJournalPrimaryChangeIntegration$"
	legacyRbdLockOwnerLossSelector                 = "^TestCephRBDLockOwnerLossIntegration$"
	legacyRbdLockOwnerLossPendingSelector          = "^TestCephRBDLockOwnerLossPendingIntegration$"
	legacyRbdMirrorSelector                        = "^TestCephRBDMirrorPathRecoveryIntegration$"
	legacyOSDSoakSelector                          = "^TestCephOSDSoak(Failover)?Integration$"
)

// legacyOwnedPoolModes are the owned-pool modes of the native oracle at
// e2d4b01, in sorted order. The proof compares these modes only: a mode the
// oracle gained later had no old admission, and its own tests prove where it
// is admitted (osd_epoch_state_admission_test.go).
var legacyOwnedPoolModes = []string{"cleanup", "clusterstat", "create", "fullratios", "inconsistent", "mgrstate", "observe", "opsinflight", "osdflags", "pghistory",
	"pginterval", "place", "poolinfo", "quota", "seed", "selfmanaged", "service", "servicecompare", "servicemetadata", "servicewatch", "shardhistory", "snapshot", "versions"}

var legacyOsdErasureWidthSelectors = []string{
	"^TestCephOSDErasureK2M2Integration$",
	"^TestCephOSDErasureOptimizedK2M2(Failover)?Integration$",
	"^TestCephOSDErasureOptimizedK2M2(InFlightMutation|SnapshotAndDegradedWrite)Integration$",
	"^TestCephOSDErasureOptimizedK2M2AppliedResubmitIntegration$",
	"^TestCephOSDErasureOptimizedK2M2PrimaryHistoryCalibrationIntegration$",
	"^TestCephOSDErasureOptimizedK2M2CopyFromHistoryIntegration$",
	"^TestCephOSDErasureOptimizedK2M2PartialWriteShardHistoryIntegration$",
	"^TestCephOSDErasureOptimizedK2M2PartialWriteLaterWriteShardHistoryIntegration$",
	"^TestCephOSDErasureOptimizedK2M2PartialWriteNewIntervalShardHistoryIntegration$",
	"^TestCephOSDErasureOptimizedK2M2PartialWriteShardReturnShardHistoryIntegration$",
	"^TestCephOSDErasureOptimizedK2M2PartialWriteDegradedWriteShardHistoryIntegration$",
	"^TestCephOSDErasureOptimizedK2M2PartialWritePrimaryReplacementShardHistoryIntegration$",
	"^TestCephOSDErasureOptimizedK2M2RequestHeldAtDyingPrimaryIntegration$",
}

var legacyShardHistorySelectors = legacyOsdErasureWidthSelectors[5:12]

var legacyPGIntervalSelectors = legacyOsdErasureWidthSelectors[7:12]

// legacySplitSelectors are the three selectors of two tests each that the
// capability rows split into one row per test; the legacy admission is
// evaluated with the selector the new single-test row was split from.
var legacySplitSelectors = map[string]string{
	"^TestCephMonMapVersionConnectedKillIntegration$":                      legacyMonForcedKillSelector,
	"^TestCephMonMapVersionUnprocessedKillIntegration$":                    legacyMonForcedKillSelector,
	"^TestCephOSDErasureOptimizedK2M2Integration$":                         legacyOsdErasureWidthSelectors[1],
	"^TestCephOSDErasureOptimizedK2M2FailoverIntegration$":                 legacyOsdErasureWidthSelectors[1],
	"^TestCephOSDErasureOptimizedK2M2InFlightMutationIntegration$":         legacyOsdErasureWidthSelectors[2],
	"^TestCephOSDErasureOptimizedK2M2SnapshotAndDegradedWriteIntegration$": legacyOsdErasureWidthSelectors[2],
}

func legacyRun(p profile) string { return p.testRun }

func legacyRBDReplicas(run string) string {
	if run == legacyRbdJournalPrimaryChangeSelector {
		return "3"
	}
	return "1"
}

func legacyValidateIgnoreCacheProbeProfile(p profile, run string) error {
	if p.osdCachePools != "0" || p.image != profileDefaultImage || p.osd != "1" || p.osdReplicas != "1" || p.osdStore != "bluestore" || p.osdProfile != "legacy-v6" ||
		p.mdsFresh != "0" || p.mdsCommands != "0" || p.mdsSessions != "0" || p.mdsCephFS != "0" || p.mdsCephFSRanks != "0" || p.mdsCephFSCompletion != "0" || p.mdsCephFSData != "0" || p.rbdBase != "0" || p.monPing != "0" || p.monFreeze != "" || run != "^TestCeph(OSD|MonOSDMaps|MonPoolOp|MonStatfs|MonMapVersion|MonServiceMap).*Integration$" || p.osdSoak != "" || p.stressDuration != "" ||
		p.expireTickets != "0" || p.idleSessions != "0" || p.authEpoch != "0" || p.shortTickets != "0" || p.nativeMgrPreparation != "0" || p.modeRejection != "" ||
		p.sparseReadOracle != "" || p.mapExtOracle != "" || p.u64XattrOracle != "" || p.writeSameOracle != "" || p.race != "0" || p.mappedIPv6 != "0" {
		return errors.New("native IgnoreCache probe requires pinned isolated legacy-v6 single BlueStore OSD, no MDS/RBD or other test selector/fault profile")
	}
	return nil
}

func legacyMonForcedKill(p profile) bool {
	return legacyRun(p) == legacyMonForcedKillSelector && p.osd == "1" && p.osdStore == "bluestore" && p.osdReplicas == "1" && p.osdProfile == "legacy-v6"
}

func legacyShardHistory(p profile) bool {
	return slices.Contains(legacyShardHistorySelectors, legacyRun(p)) && p.osd == "1" && p.osdStore == "bluestore" && p.osdReplicas == "4"
}

func legacyMgrColdBootstrap(p profile) bool {
	return p.mgrService == "1" && p.mgrCount == "0" && p.osd == "0" && legacyRun(p) == legacyMgrServiceColdBootstrapSelector
}

func legacyRBDMirror(p profile) bool {
	return legacyRun(p) == legacyRbdMirrorSelector && p.rbdMirrorFixture()
}

func legacyPoolModeAllowed(p profile, mode string) bool {
	if mode == "pginterval" && !(legacyShardHistory(p) && slices.Contains(legacyPGIntervalSelectors, legacyRun(p))) {
		return false
	}
	return slices.Contains(legacyOwnedPoolModes, mode) && (p.osd == "1" || legacyMgrColdBootstrap(p) && (mode == "mgrstate" || mode == "servicewatch"))
}

func legacyJournalActions(run string) []string {
	switch run {
	case legacyRbdJournalLostAppendSelector:
		return rbdJournalLostAppendActions
	case legacyRbdJournalPrimaryChangeSelector:
		return rbdJournalPrimaryChangeActions
	}
	return rbdJournalActions
}

func legacyLockActions(run string) []string {
	switch run {
	case legacyRbdLockOwnerLossSelector:
		return rbdLockOwnerLossActions
	case legacyRbdLockOwnerLossPendingSelector:
		return rbdLockOwnerLossPendingActions
	}
	return rbdLockActions
}

func legacyHeldFlockSplit(action, run string) bool {
	if slices.Contains(mdsCephFSHeldFlockSplitActions, action) && run != legacyMdsCephFSHeldFlockSplitSelector && run != legacyMdsCephFSHeldFlockSplitFragmentsSelector {
		return false
	}
	return !slices.Contains(mdsCephFSHeldFlockSplitFragmentsActions, action) || run == legacyMdsCephFSHeldFlockSplitFragmentsSelector
}

func legacyCleanupBudget(p profile) time.Duration {
	switch {
	case p.mdsCephFSRanksReplay == "1":
		return 4 * time.Minute
	case p.mdsCephFSRanks == "1" || p.mdsCephFSCompletion == "1" || legacyRBDMirror(p):
		return 3 * time.Minute
	case p.mdsSessions == "1" || p.mdsCephFS == "1" || p.mdsCephFSData == "1" || p.rbdBase == "1":
		return 2 * time.Minute
	}
	return time.Minute
}

func legacyLoadProfile(getenv func(string) string) (profile, error) {
	run := getenv("CEPH_MSGR_TEST_RUN")
	value := func(name, fallback string) string {
		if result := getenv(name); result != "" {
			return result
		}
		return fallback
	}
	p := profile{
		mdsFresh:             value("CEPH_MSGR_TEST_MDS_FRESH", "0"),
		mdsCommands:          value("CEPH_MSGR_TEST_MDS_COMMANDS", "0"),
		mdsSessions:          value("CEPH_MSGR_TEST_MDS_SESSIONS", "0"),
		mdsCephFS:            value("CEPH_MSGR_TEST_MDS_CEPHFS", "0"),
		mdsCephFSRanks:       value("CEPH_MSGR_TEST_MDS_CEPHFS_RANKS", "0"),
		mdsCephFSRanksReplay: value("CEPH_MSGR_TEST_MDS_CEPHFS_RANKS_REPLAY", "0"),
		mdsCephFSCompletion:  value("CEPH_MSGR_TEST_MDS_CEPHFS_COMPLETION", "0"),
		mdsCephFSData:        value("CEPH_MSGR_TEST_MDS_CEPHFS_DATA", "0"),
		rbdBase:              value("CEPH_MSGR_TEST_RBD", "0"),
		osdProfile:           value("CEPH_MSGR_TEST_OSD_PROFILE", "legacy-v6"),
		sparseReadOracle:     getenv("CEPH_MSGR_TEST_SPARSE_READ_ORACLE"),
		mapExtOracle:         getenv("CEPH_MSGR_TEST_MAPEXT_ORACLE"),
		u64XattrOracle:       getenv("CEPH_MSGR_TEST_U64_XATTR_ORACLE"),
		writeSameOracle:      getenv("CEPH_MSGR_TEST_WRITESAME_ORACLE"),
		image:                value("CEPH_MSGR_TEST_IMAGE", profileDefaultImage),
		keyType:              value("CEPH_MSGR_TEST_KEY_TYPE", "aes256k"),
		ipFamily:             value("CEPH_MSGR_TEST_IP_FAMILY", "4"),
		mgrCount:             value("CEPH_MSGR_TEST_MGR_COUNT", "2"),
		runtime:              value("CEPH_MSGR_TEST_RUNTIME", "container"),
		race:                 value("CEPH_MSGR_TEST_RACE", "0"),
		expireTickets:        value("CEPH_MSGR_TEST_EXPIRE_TICKETS", "0"),
		idleSessions:         value("CEPH_MSGR_TEST_IDLE_SESSIONS", "0"),
		authEpoch:            value("CEPH_MSGR_TEST_AUTH_EPOCH", "0"),
		authEpochCycles:      value("CEPH_MSGR_TEST_AUTH_EPOCH_CYCLES", authEpochDefaultCycles),
		shortTickets:         value("CEPH_MSGR_TEST_SHORT_TICKETS", "0"),
		nativeMgrPreparation: value("CEPH_MSGR_TEST_NATIVE_MGR_PREPARATION", "0"),
		monPing:              value("CEPH_MSGR_TEST_MON_PING", "0"),
		osdCachePools:        value("CEPH_MSGR_TEST_OSD_CACHE_POOLS", "0"),
		mgrService:           value("CEPH_MSGR_TEST_MGR_SERVICE", "0"),
		modeRejection:        getenv("CEPH_MSGR_TEST_MODE_REJECTION"),
		connectionMode:       value("CEPH_MSGR_TEST_CONNECTION_MODE", "secure"),
		mappedIPv6:           value("CEPH_MSGR_TEST_MAPPED_IPV6", "0"),
		osd:                  value("CEPH_MSGR_TEST_OSD", "0"),
		osdStore:             value("CEPH_MSGR_TEST_OSD_STORE", "memstore"),
		osdReplicas:          value("CEPH_MSGR_TEST_OSD_REPLICAS", "1"),
		stressDuration:       getenv("CEPH_MSGR_STRESS_DURATION"),
		osdSoak:              getenv("CEPH_MSGR_TEST_OSD_SOAK"),
		sourceSHA:            getenv("CEPH_MSGR_TEST_SOURCE_SHA"),
		monFreeze:            getenv("CEPH_MSGR_TEST_MON_FREEZE"),
		testTimeout:          value("CEPH_MSGR_TEST_TIMEOUT", "10m"),
		diagnostics:          getenv("CEPH_MSGR_TEST_DIAGNOSTICS"),
	}
	p.serviceCipher = value("CEPH_MSGR_TEST_SERVICE_CIPHER", p.keyType)
	for _, choice := range []struct {
		name, value string
		allowed     []string
	}{
		{"CEPH_MSGR_TEST_KEY_TYPE", p.keyType, []string{"aes", "aes256k"}},
		{"CEPH_MSGR_TEST_MDS_FRESH", p.mdsFresh, []string{"0", "1"}},
		{"CEPH_MSGR_TEST_MDS_COMMANDS", p.mdsCommands, []string{"0", "1"}},
		{"CEPH_MSGR_TEST_MDS_SESSIONS", p.mdsSessions, []string{"0", "1"}},
		{"CEPH_MSGR_TEST_MDS_CEPHFS", p.mdsCephFS, []string{"0", "1"}},
		{"CEPH_MSGR_TEST_MDS_CEPHFS_RANKS", p.mdsCephFSRanks, []string{"0", "1"}},
		{"CEPH_MSGR_TEST_MDS_CEPHFS_RANKS_REPLAY", p.mdsCephFSRanksReplay, []string{"0", "1"}},
		{"CEPH_MSGR_TEST_MDS_CEPHFS_COMPLETION", p.mdsCephFSCompletion, []string{"0", "1"}},
		{"CEPH_MSGR_TEST_MDS_CEPHFS_DATA", p.mdsCephFSData, []string{"0", "1"}},
		{"CEPH_MSGR_TEST_RBD", p.rbdBase, []string{"0", "1"}},
		{"CEPH_MSGR_TEST_SERVICE_CIPHER", p.serviceCipher, []string{"aes", "aes256k"}},
		{"CEPH_MSGR_TEST_RUNTIME", p.runtime, []string{"container", "host"}},
		{"CEPH_MSGR_TEST_RACE", p.race, []string{"0", "1"}},
		{"CEPH_MSGR_TEST_EXPIRE_TICKETS", p.expireTickets, []string{"0", "1"}},
		{"CEPH_MSGR_TEST_IDLE_SESSIONS", p.idleSessions, []string{"0", "1"}},
		{"CEPH_MSGR_TEST_AUTH_EPOCH", p.authEpoch, []string{"0", "1"}},
		{"CEPH_MSGR_TEST_SHORT_TICKETS", p.shortTickets, []string{"0", "1"}},
		{"CEPH_MSGR_TEST_NATIVE_MGR_PREPARATION", p.nativeMgrPreparation, []string{"0", "1"}},
		{"CEPH_MSGR_TEST_MON_PING", p.monPing, []string{"0", "1"}},
		{"CEPH_MSGR_TEST_OSD_CACHE_POOLS", p.osdCachePools, []string{"0", "1"}},
		{"CEPH_MSGR_TEST_MGR_SERVICE", p.mgrService, []string{"0", "1"}},
		{"CEPH_MSGR_TEST_MODE_REJECTION", p.modeRejection, []string{"", "mon", "mgr"}},
		{"CEPH_MSGR_TEST_CONNECTION_MODE", p.connectionMode, []string{"secure", "crc"}},
		{"CEPH_MSGR_TEST_MAPPED_IPV6", p.mappedIPv6, []string{"0", "1"}},
		{"CEPH_MSGR_TEST_OSD", p.osd, []string{"0", "1"}},
		{"CEPH_MSGR_TEST_OSD_PROFILE", p.osdProfile, []string{"legacy-v6", "replicated-pg-v8", "replicated-pg-reply-v8", "replicated-pg-request-v9-reply-v8"}},
		{"CEPH_MSGR_TEST_OSD_STORE", p.osdStore, []string{"memstore", "bluestore"}},
		{"CEPH_MSGR_TEST_OSD_REPLICAS", p.osdReplicas, osdTopologyNames()},
		{"CEPH_MSGR_TEST_MGR_COUNT", p.mgrCount, []string{"0", "2"}},
		{"CEPH_MSGR_TEST_IP_FAMILY", p.ipFamily, []string{"4", "6"}},
	} {
		if !slices.Contains(choice.allowed, choice.value) {
			return profile{}, fmt.Errorf("%s must be one of %s", choice.name, strings.Join(choice.allowed, ", "))
		}
	}
	if p.osdProfile == "replicated-pg-request-v9-reply-v8" && p.osdReplicas != "1" {
		return profile{}, fmt.Errorf("request-v9 fixture requires exactly one OSD for its verified acceptance recipes")
	}
	if p.osdProfile == "replicated-pg-request-v9-reply-v8" && (p.mdsFresh != "0" || p.rbdBase != "0") {
		return profile{}, fmt.Errorf("request-v9 fixture requires isolated OSD acceptance, without MDS or RBD workflows")
	}
	if p.osdPGV8() && p.osd != "1" {
		return profile{}, fmt.Errorf("%s requires CEPH_MSGR_TEST_OSD=1", p.osdProfile)
	}
	if p.writeSameOracle != "" && p.osd != "1" {
		return profile{}, fmt.Errorf("native WRITESAME oracle requires OSD fixture")
	}
	if p.u64XattrOracle != "" && p.osd != "1" {
		return profile{}, fmt.Errorf("native U64 xattr oracle requires OSD fixture")
	}
	if (p.sparseReadOracle != "" || p.mapExtOracle != "") && p.osd != "1" {
		return profile{}, fmt.Errorf("native read oracle requires OSD fixture")
	}
	if p.osdReplicas != "1" && p.osd != "1" {
		return profile{}, fmt.Errorf("replicated fixture requires CEPH_MSGR_TEST_OSD=1")
	}
	if p.osdTopology().daemons > 1 && p.osdStore != "bluestore" {
		return profile{}, fmt.Errorf("multi-OSD replicated and erasure-capacity profiles require BlueStore")
	}
	if p.osdStore != "memstore" && p.osd != "1" {
		return profile{}, fmt.Errorf("BlueStore fixture requires CEPH_MSGR_TEST_OSD=1")
	}
	if p.race == "1" && p.runtime != "host" {
		return profile{}, fmt.Errorf("development race instrumentation requires CEPH_MSGR_TEST_RUNTIME=host")
	}
	if p.connectionMode == "crc" && p.modeRejection != "" {
		return profile{}, fmt.Errorf("CRC mode cannot be combined with the secure-client mode-rejection fixture")
	}
	selected := "^TestCeph.*Integration$"
	if p.mgrCount == "0" {
		selected = "^TestCephManagerAvailabilityIntegration$"
	}
	if p.expireTickets == "1" {
		if p.mgrCount != "2" {
			return profile{}, fmt.Errorf("expired-ticket fixture requires two managers")
		}
		selected = "^TestCeph(ExpiredTicketRecovery|DiscardedTicketProof)Integration$"
	}
	if p.idleSessions == "1" {
		if p.mgrCount != "2" || p.expireTickets != "0" {
			return profile{}, fmt.Errorf("idle-session fixture requires two managers and no expired-ticket fixture")
		}
		selected = "^TestCephIdleSessionsIntegration$"
	}
	if p.authEpochCycles != authEpochDefaultCycles {
		// Local soak runs only. CI keeps the default two cycles.
		cycles, err := strconv.Atoi(p.authEpochCycles)
		if err != nil || cycles < 1 || cycles > authEpochMaxCycles || strconv.Itoa(cycles) != p.authEpochCycles {
			return profile{}, fmt.Errorf("CEPH_MSGR_TEST_AUTH_EPOCH_CYCLES must be an integer from 1 to %d", authEpochMaxCycles)
		}
		if p.authEpoch != "1" {
			return profile{}, fmt.Errorf("CEPH_MSGR_TEST_AUTH_EPOCH_CYCLES requires CEPH_MSGR_TEST_AUTH_EPOCH=1")
		}
	}
	if p.authEpoch == "1" {
		if p.mgrCount != "2" || p.expireTickets != "0" || p.idleSessions != "0" {
			return profile{}, fmt.Errorf("service-key epoch fixture requires two managers and no expired-ticket or idle-session fixture")
		}
		selected = "^TestCephServiceKeyEpochIntegration$"
	}
	if p.shortTickets == "1" {
		if p.mgrCount != "2" || p.expireTickets != "0" || p.idleSessions != "0" || p.authEpoch != "0" {
			return profile{}, fmt.Errorf("fractional-ticket fixture requires two managers and no other authentication fixture")
		}
		selected = "^TestCephFractionalTicketRenewalIntegration$"
	}
	if p.modeRejection != "" {
		if p.mgrCount != "2" || p.expireTickets != "0" || p.idleSessions != "0" || p.authEpoch != "0" || p.shortTickets != "0" {
			return profile{}, fmt.Errorf("mode-rejection fixture requires two managers and no authentication fixture")
		}
		selected = "^TestCephConnectionModeRejectionIntegration$"
	}
	if p.mappedIPv6 == "1" {
		if p.ipFamily != "6" || p.runtime != "container" || p.mgrCount != "2" || p.expireTickets != "0" || p.idleSessions != "0" || p.authEpoch != "0" || p.shortTickets != "0" || p.modeRejection != "" || p.stressDuration != "" {
			return profile{}, fmt.Errorf("mapped-IPv6 fixture requires container IPv6, two managers, and no authentication, mode-rejection, or stress fixture")
		}
		selected = "^TestCephMappedAddressIntegration$"
	}
	if p.osd == "1" {
		if p.mgrCount != "2" || p.mappedIPv6 != "0" || p.expireTickets != "0" || p.idleSessions != "0" || p.authEpoch != "0" || p.shortTickets != "0" || p.modeRejection != "" || p.stressDuration != "" {
			return profile{}, fmt.Errorf("OSD fixture requires two managers and no mapped-IPv6, authentication, mode-rejection, or stress fixture")
		}
		selected = "^TestCeph(OSD|MonOSDMaps|MonPoolOp|MonStatfs|MonMapVersion|MonServiceMap).*Integration$"
	}
	// The default OSD selection on three BlueStore replicas runs past ten
	// minutes; an explicit CEPH_MSGR_TEST_TIMEOUT still wins.
	if p.osd == "1" && getenv("CEPH_MSGR_TEST_TIMEOUT") == "" {
		p.testTimeout = "30m"
	}
	if p.osdSoak != "" {
		if duration, err := time.ParseDuration(p.osdSoak); err != nil || duration < 30*time.Second || p.osd != "1" {
			return profile{}, fmt.Errorf("OSD soak requires CEPH_MSGR_TEST_OSD=1 and a duration of at least 30s")
		}
		selected = "^TestCephOSDSoak(Failover)?Integration$"
	}
	if p.nativeMgrPreparation == "1" {
		if p.mgrCount != "2" || p.expireTickets != "0" || p.idleSessions != "0" || p.authEpoch != "0" || p.shortTickets != "0" || p.modeRejection != "" || p.mappedIPv6 != "0" || p.osd != "0" || p.stressDuration != "" {
			return profile{}, fmt.Errorf("native MGR preparation acceptance requires an isolated ordinary two-MGR fixture")
		}
		selected = "^TestCephNativeManagerPreparationIntegration$"
	}
	if p.mdsCommands == "1" && p.mdsFresh != "1" {
		return profile{}, fmt.Errorf("MDS commands require CEPH_MSGR_TEST_MDS_FRESH=1")
	}
	if p.mdsSessions == "1" && (p.mdsFresh != "1" || p.mdsCommands == "1") {
		return profile{}, fmt.Errorf("MDS sessions require CEPH_MSGR_TEST_MDS_FRESH=1 and exclude CEPH_MSGR_TEST_MDS_COMMANDS")
	}
	if p.mdsCephFS == "1" && (p.mdsFresh != "1" || p.mdsCommands == "1" || p.mdsSessions == "1") {
		return profile{}, fmt.Errorf("MDS CephFS metadata acceptance requires CEPH_MSGR_TEST_MDS_FRESH=1 and excludes CEPH_MSGR_TEST_MDS_COMMANDS and CEPH_MSGR_TEST_MDS_SESSIONS")
	}
	if p.mdsCephFSRanks == "1" && (p.mdsFresh != "1" || p.mdsCommands == "1" || p.mdsSessions == "1" || p.mdsCephFS == "1") {
		return profile{}, fmt.Errorf("MDS CephFS multi-rank acceptance requires CEPH_MSGR_TEST_MDS_FRESH=1 and excludes CEPH_MSGR_TEST_MDS_COMMANDS, CEPH_MSGR_TEST_MDS_SESSIONS and CEPH_MSGR_TEST_MDS_CEPHFS")
	}
	if p.mdsCephFSRanksReplay == "1" && p.mdsCephFSRanks != "1" {
		return profile{}, fmt.Errorf("MDS CephFS standby-replay acceptance extends CEPH_MSGR_TEST_MDS_CEPHFS_RANKS=1")
	}
	if p.mdsCephFSCompletion == "1" && (p.mdsFresh != "1" || p.mdsCommands == "1" || p.mdsSessions == "1" || p.mdsCephFS == "1" || p.mdsCephFSRanks == "1") {
		return profile{}, fmt.Errorf("MDS CephFS completion acceptance requires CEPH_MSGR_TEST_MDS_FRESH=1 and excludes CEPH_MSGR_TEST_MDS_COMMANDS, CEPH_MSGR_TEST_MDS_SESSIONS, CEPH_MSGR_TEST_MDS_CEPHFS and CEPH_MSGR_TEST_MDS_CEPHFS_RANKS")
	}
	if p.mdsCephFSData == "1" && (p.mdsFresh != "1" || p.mdsCommands == "1" || p.mdsSessions == "1" || p.mdsCephFS == "1" || p.mdsCephFSRanks == "1" || p.mdsCephFSCompletion == "1") {
		return profile{}, fmt.Errorf("MDS CephFS data acceptance requires CEPH_MSGR_TEST_MDS_FRESH=1 and excludes CEPH_MSGR_TEST_MDS_COMMANDS, CEPH_MSGR_TEST_MDS_SESSIONS, CEPH_MSGR_TEST_MDS_CEPHFS, CEPH_MSGR_TEST_MDS_CEPHFS_RANKS and CEPH_MSGR_TEST_MDS_CEPHFS_COMPLETION")
	}
	if p.mdsFresh == "1" {
		selected = "^TestCephMDSFreshIntegration$"
		if p.mdsCommands == "1" {
			selected = "^TestCephMDSCommandsIntegration$"
			// The applied-command outcome cell shares the owned command
			// fixture and is admitted only when explicitly selected.
			if run == legacyDaemonCommandMDSSelector {
				selected = run
			}
		}
		if p.mdsSessions == "1" {
			selected = "^TestCephMDSSessionsIntegration$"
		}
		if p.mdsCephFS == "1" {
			selected = "^TestCephMDSCephFS(Push|Recovery)?Integration$"
			// The shared held-lock variant of the recovery acceptance owns a
			// fresh metadata fixture and is admitted only when explicitly selected.
			if run == legacyMdsCephFSSharedFlockSelector {
				selected = run
			}
		}
		if p.mdsCephFSRanks == "1" {
			selected = "^TestCephMDSCephFSRanksIntegration$"
			if p.mdsCephFSRanksReplay == "1" {
				selected = "^TestCephMDSCephFSRanksReplayIntegration$"
			}
		}
		if p.mdsCephFSCompletion == "1" {
			selected = "^TestCephMDSCephFSCompletionIntegration$"
			// The split reconnect and its held-lock variants each own a fresh
			// completion fixture and are admitted only when explicitly selected.
			if run == "^TestCephMDSCephFSMultiReconnectIntegration$" || run == legacyMdsCephFSHeldFlockSplitSelector || run == legacyMdsCephFSHeldFlockSplitFragmentsSelector {
				selected = run
			}
		}
		if p.mdsCephFSData == "1" {
			selected = "^TestCephMDSCephFSDataIntegration$"
			// The cap-send recovery and writeback-fence variants share the
			// owned data fixture and are admitted only when explicitly selected.
			if run == "^TestCephMDSCephFSCapSendRecoveryIntegration$" || run == legacyMdsCephFSWritebackSelector {
				selected = run
			}
		}
		if p.osd != "1" || !p.mdsFreshOSDAllowed() || p.osdReplicas != "1" || (p.sparseReadOracle != "" || p.mapExtOracle != "") ||
			run != "" && run != selected {
			return profile{}, fmt.Errorf("MDS fixture requires its exclusive selector %s and one admitted OSD profile", selected)
		}
	}
	if p.rbdBase == "1" {
		// The RBD base, clone, journaling, write and exclusive-lock acceptances own
		// a pool, two identities and six native images on the single-OSD
		// legacy-v6 fixture; their Go client admits no HASHPSPOOL bit, so
		// every pool must keep the legacy placement seed.
		selected = "^TestCephRBD(Base|Write|ExclusiveLock|Clone|Journal)Integration$"
		// The lost-append-reply acceptance needs a journal no other test has
		// advanced and is admitted only when explicitly selected.
		if run == legacyRbdJournalLostAppendSelector {
			selected = run
		}
		// The lock-owner-loss acceptance blocklists two client addresses and
		// kills a native lock owner; it is admitted only when explicitly
		// selected, on a fixture no other RBD test shares.
		if run == legacyRbdLockOwnerLossSelector || run == legacyRbdLockOwnerLossPendingSelector {
			selected = run
		}
		// The primary-change acceptance is the lost-append-reply sequence on
		// three replicated BlueStore OSDs. No other RBD selection is admitted
		// on that topology, and this one on no other.
		if run == legacyRbdJournalPrimaryChangeSelector {
			selected = run
		}
		if p.osd != "1" || p.osdProfile != "legacy-v6" || p.osdReplicas != legacyRBDReplicas(run) || p.osdReplicas == "3" && p.osdStore != "bluestore" || p.mdsFresh != "0" ||
			(p.sparseReadOracle != "" || p.mapExtOracle != "") || run != "" && run != selected {
			return profile{}, fmt.Errorf("RBD acceptance requires its exclusive selector %s, the single-OSD legacy-v6 fixture (three BlueStore OSDs for %s alone) and no MDS profile", selected, legacyRbdJournalPrimaryChangeSelector)
		}
	}
	if p.monPing == "1" {
		selected = "^TestCephMonPingIntegration$"
		if p.mgrCount != "2" || p.osd != "0" || p.mdsFresh != "0" || p.rbdBase != "0" || p.expireTickets != "0" || p.idleSessions != "0" || p.authEpoch != "0" || p.shortTickets != "0" || p.nativeMgrPreparation != "0" || p.modeRejection != "" || p.mappedIPv6 != "0" || p.stressDuration != "" || p.monFreeze != "" || run != "" && run != selected {
			return profile{}, fmt.Errorf("MON ping acceptance requires its exclusive selector %s and the ordinary three-MON/two-MGR fixture", selected)
		}
	}
	if p.osdCachePools == "1" {
		selected = "^TestCephOSDIgnoreCacheIntegration$"
		if p.mgrCount != "2" || run != "" && run != selected && run != legacyCacheTierOpsSelector && run != legacyCacheOpGoSelector {
			return profile{}, fmt.Errorf("cache-pool acceptance requires two MGRs and an exclusive selector %s, %s or %s", selected, legacyCacheTierOpsSelector, legacyCacheOpGoSelector)
		}
		checked := p
		checked.osdCachePools = "0"
		if err := legacyValidateIgnoreCacheProbeProfile(checked, "^TestCeph(OSD|MonOSDMaps|MonPoolOp|MonStatfs|MonMapVersion|MonServiceMap).*Integration$"); err != nil {
			return profile{}, err
		}
	}
	if p.mgrService == "1" {
		// Ordinary service acceptance uses the one-OSD oracle fixture. Cold
		// bootstrap instead admits only a closed MON-only observation recipe.
		selected = "^TestCephMgrServiceRegistrationIntegration$"
		topologyAllowed := p.osd == "1" && p.mgrCount == "2"
		if p.mgrCount == "0" {
			selected = legacyMgrServiceColdBootstrapSelector
			topologyAllowed = p.osd == "0" && p.runtime == "container"
		}
		if !topologyAllowed || p.osdProfile != "legacy-v6" || p.osdReplicas != "1" || p.mdsFresh != "0" || p.rbdBase != "0" ||
			p.monPing != "0" || p.osdCachePools != "0" || p.expireTickets != "0" || p.idleSessions != "0" || p.authEpoch != "0" || p.shortTickets != "0" ||
			p.nativeMgrPreparation != "0" || p.modeRejection != "" || p.mappedIPv6 != "0" || p.stressDuration != "" || p.osdSoak != "" || p.monFreeze != "" ||
			p.sparseReadOracle != "" || p.mapExtOracle != "" || run != "" && run != selected {
			return profile{}, fmt.Errorf("MGR service acceptance requires its exclusive selector %s and either two MGRs with one legacy-v6 OSD or zero initial MGRs without OSDs", selected)
		}
	}
	if p.osdTopology().erasureCapacity {
		// The four-daemon topology exists only for the bounded k=2,m=2
		// erasure-width scenarios. The fixture's ordinary pools keep the
		// three-replica policy; broad OSD selections stay on topology "3".
		selected = legacyOsdErasureWidthSelectors[0]
		if p.osd != "1" || (p.osdProfile != "legacy-v6" && p.osdProfile != "replicated-pg-v8") || p.osdSoak != "" || p.monFreeze != "" ||
			p.osdCachePools != "0" || p.mgrService != "0" || p.mdsFresh != "0" || p.rbdBase != "0" ||
			p.sparseReadOracle != "" || p.mapExtOracle != "" || p.u64XattrOracle != "" || p.writeSameOracle != "" ||
			run != "" && !slices.Contains(legacyOsdErasureWidthSelectors, run) {
			return profile{}, fmt.Errorf("four-OSD erasure-capacity topology requires one of its exclusive selectors %v, BlueStore and the legacy-v6 or replicated-pg-v8 profile", legacyOsdErasureWidthSelectors)
		}
	}
	if run == "" {
		run = selected
	}
	p.testRun = run
	if _, err := soakExpectedTests(p); err != nil {
		return profile{}, err
	}
	return p, nil
}

func legacyFixtureTicketTTLs(p profile) (auth, service time.Duration) {
	service = 12 * time.Second
	if p.idleSessions == "1" || p.authEpoch == "1" {
		service = 120 * time.Second
	}
	auth = service
	if p.monFreeze == "1" {
		// The opt-in freeze can outlast the ordinary 12 s AUTH proof.
		// A native OSD whose MON hunt then fails authentication may stop
		// reconnecting altogether. Keep its reclaim proof valid across the
		// bounded pause; service tickets still expire on the ordinary clock.
		// Dedicated short-ticket authentication below retains its own TTL.
		auth = 120 * time.Second
	}
	if p.osd == "1" && p.osdProfile == "replicated-pg-v8" && p.osdReplicas == "3" &&
		p.osdStore == "bluestore" && p.mdsFresh != "1" && p.shortTickets != "1" && legacyRun(p) == "^TestCephOSDUpmapIntegration$" {
		// This placement-only recipe can spend tens of seconds in native
		// controls. A 12 s ticket leaves only 3 s at renewal, less than one
		// 10 s MON setup attempt. Give both reclaim and OSD service proofs
		// the same window already used by other long-control profiles.
		// Dedicated short/fractional authentication profiles stay unchanged.
		auth = 120 * time.Second
		service = 120 * time.Second
	}
	if legacyMonForcedKill(p) {
		// The unprocessed-request cell keeps every MON out of quorum for
		// tens of seconds. Routine 12 s renewal would replace the subject
		// session during that window and move the pending request onto a
		// retired connection, and native daemons would present reclaim
		// proofs older than their validity once the MONs return.
		auth = 120 * time.Second
		service = 120 * time.Second
	}
	if p.shortTickets == "1" {
		auth = shortTicketAuthTTL
	}
	return auth, service
}

// fixtureMarkers are the test-environment markers the admission decides.
var fixtureMarkers = []string{copyFromLateErrorMarker, copyFromMultiChunkMarker, rbdMirrorMarker, monForcedKillMarker, shardHistoryMarker}

// admissionModes is every fixture mode the admission decides for a profile.
type admissionModes struct {
	Loaded                                          bool
	TestRun, TestTimeout                            string
	Kill, Daemon, PoolModes, Journal, Lock, Markers string
	CopyLate, CopyMulti, Mirror, Shard, Interval    bool
	HeldFlock, Writeback, ColdBootstrap, Probe      string
	Auth, Service, Cleanup                          time.Duration
	BlockSize                                       int64
	RBDReplicas, Manifest, Environment              string
}

func flags(values ...bool) string {
	var b strings.Builder
	for _, v := range values {
		b.WriteString(strconv.FormatBool(v)[:1])
	}
	return b.String()
}

func heldFlockActions() []string {
	return append(slices.Clone(mdsCephFSHeldFlockSplitActions), append(slices.Clone(mdsCephFSHeldFlockSplitFragmentsActions), "list", "restart-arm")...)
}

func daemonActions() (kinds, actions []string) {
	return []string{"mon", "mgr", "osd", "mds", "rgw"}, []string{"pause", "resume", "stop", "start", "kill", ""}
}

// environmentWithoutMarkers is the rest of the profile's daemon and test
// environment, which no admission decides.
func environmentWithoutMarkers(p profile) string {
	test := p.testEnvironment("/out", "")
	for _, marker := range fixtureMarkers {
		delete(test, marker)
	}
	return fmt.Sprint(p.containerEnvironment(), test, fixtureBootstrapConfig(profile{osd: p.osd, osdStore: p.osdStore, osdReplicas: p.osdReplicas, osdProfile: p.osdProfile}))
}

func capabilityModes(p profile, err error) admissionModes {
	if err != nil {
		return admissionModes{}
	}
	m := admissionModes{Loaded: true, TestRun: p.testRun, TestTimeout: p.testTimeout}
	m.Kill = flags(p.forcedKillAdmitted("mon"), p.forcedKillAdmitted("osd"), p.forcedKillAdmitted("mgr"), p.forcedKillAdmitted("mds"))
	kinds, actions := daemonActions()
	var daemon, pool, held []bool
	for _, kind := range kinds {
		for _, action := range actions {
			daemon = append(daemon, p.daemonControlAdmitted(kind, action))
		}
	}
	for _, mode := range legacyOwnedPoolModes {
		pool = append(pool, nativePoolModeAllowed(p, oracleModes, mode))
	}
	for _, action := range heldFlockActions() {
		held = append(held, admitMDSCephFSHeldFlockSplit(action, p) == nil)
	}
	m.Daemon, m.PoolModes, m.HeldFlock = flags(daemon...), flags(pool...), flags(held...)
	m.Journal, m.Lock = strings.Join(rbdJournalActionsFor(p), ","), strings.Join(rbdLockActionsFor(p), ",")
	var markers []string
	for _, marker := range fixtureMarkers {
		markers = append(markers, p.testEnvironment("/out", "")[marker])
	}
	m.Markers = strings.Join(markers, ",")
	m.CopyLate, m.CopyMulti, m.Mirror = p.copyFromLateErrorSelected(), p.copyFromMultiChunkSelected(), p.rbdMirrorSelected()
	m.Shard, m.Interval = p.shardHistorySelected(), p.pgIntervalSelected()
	m.Writeback = flags(p.admits(capMDSWritebackFence))
	m.ColdBootstrap = flags(p.mgrServiceColdBootstrap())
	m.Probe = flags(validateIgnoreCacheProbeProfile(p) == nil)
	m.Auth, m.Service = fixtureTicketTTLs(p)
	m.Cleanup, m.BlockSize, m.RBDReplicas = p.cleanupBudget(), fixtureOSDBlockSize(p), p.rbdReplicas()
	m.Manifest = flags(p.admits(capRBDJournalPrimaryChange), p.admits(capRBDLockPendingRequests), p.admits(capRBDLockOwnerLoss))
	m.Environment = environmentWithoutMarkers(p)
	return m
}

func legacyModes(p profile, err error) admissionModes {
	if err != nil {
		return admissionModes{}
	}
	run := legacyRun(p)
	m := admissionModes{Loaded: true, TestRun: run, TestTimeout: p.testTimeout}
	m.Kill = flags(false, true, false, false)
	if legacyMonForcedKill(p) {
		m.Kill = flags(true, true, false, false)
	}
	kinds, actions := daemonActions()
	var daemon, pool, held []bool
	for _, kind := range kinds {
		for _, action := range actions {
			if kind != "mds" {
				daemon = append(daemon, kind == "mon" || kind == "mgr" || kind == "osd")
			} else {
				daemon = append(daemon, action == "stop" && p.mdsCommands == "1" && p.mdsFresh == "1" && run != legacyDaemonCommandMDSSelector)
			}
		}
	}
	for _, mode := range legacyOwnedPoolModes {
		pool = append(pool, legacyPoolModeAllowed(p, mode))
	}
	for _, action := range heldFlockActions() {
		held = append(held, legacyHeldFlockSplit(action, run))
	}
	m.Daemon, m.PoolModes, m.HeldFlock = flags(daemon...), flags(pool...), flags(held...)
	m.Journal, m.Lock = strings.Join(legacyJournalActions(run), ","), strings.Join(legacyLockActions(run), ",")
	m.CopyLate = run == legacyCopyFromLateErrorSelector && p.copyFromErrorFixture()
	m.CopyMulti = run == legacyCopyFromMultiChunkSelector && p.copyFromErrorFixture()
	m.Mirror = legacyRBDMirror(p)
	m.Shard = legacyShardHistory(p)
	m.Interval = m.Shard && slices.Contains(legacyPGIntervalSelectors, run)
	marker := func(on bool) string {
		if on {
			return "1"
		}
		return ""
	}
	m.Markers = strings.Join([]string{marker(m.CopyLate), marker(m.CopyMulti), marker(m.Mirror), marker(legacyMonForcedKill(p)), marker(m.Shard)}, ",")
	m.Writeback = flags(run == legacyMdsCephFSWritebackSelector)
	m.ColdBootstrap = flags(legacyMgrColdBootstrap(p))
	m.Probe = flags(legacyValidateIgnoreCacheProbeProfile(p, run) == nil)
	m.Auth, m.Service = legacyFixtureTicketTTLs(p)
	m.Cleanup = legacyCleanupBudget(p)
	if p.osd == "1" && p.osdStore == "bluestore" && p.osdReplicas == "3" && p.osdSoak != "" && run == legacyOSDSoakSelector {
		m.BlockSize = 4 << 30
	}
	m.RBDReplicas = legacyRBDReplicas(run)
	m.Manifest = flags(run == legacyRbdJournalPrimaryChangeSelector, run == legacyRbdLockOwnerLossPendingSelector,
		run == legacyRbdLockOwnerLossSelector || run == legacyRbdLockOwnerLossPendingSelector)
	m.Environment = environmentWithoutMarkers(p)
	return m
}

func loadBoth(env map[string]string) (current, legacy admissionModes) {
	p, err := loadProfile(func(name string) string { return env[name] })
	current = capabilityModes(p, err)
	legacyEnv := maps.Clone(env)
	delete(legacyEnv, "CEPH_MSGR_TEST_CAPABILITIES")
	if joined, ok := legacySplitSelectors[env["CEPH_MSGR_TEST_RUN"]]; ok {
		legacyEnv["CEPH_MSGR_TEST_RUN"] = joined
	}
	lp, lerr := legacyLoadProfile(func(name string) string { return legacyEnv[name] })
	legacy = legacyModes(lp, lerr)
	if joined, ok := legacySplitSelectors[env["CEPH_MSGR_TEST_RUN"]]; ok && current.Loaded && legacy.Loaded {
		// A split row runs one of the two tests the old row ran.
		name := strings.TrimSuffix(strings.TrimPrefix(current.TestRun, "^"), "$")
		if !regexp.MustCompile(joined).MatchString(name) {
			panic("split selector outside its legacy selection")
		}
		legacy.TestRun = current.TestRun
	}
	return current, legacy
}

// historicalCatalogRows are the current catalog's rows of the migration, in
// catalog order. A migration row that is gone or renamed fails the test: the
// proof would otherwise shrink without notice.
func historicalCatalogRows(t *testing.T) []ciScenario {
	t.Helper()
	historical := map[string]bool{}
	for _, id := range legacyHistoricalRows {
		historical[id] = true
	}
	var rows []ciScenario
	for _, row := range ciRepositoryCatalog(t) {
		if historical[row.ID] {
			rows = append(rows, row)
			delete(historical, row.ID)
		}
	}
	if len(historical) != 0 || len(rows) != len(legacyHistoricalRows) {
		t.Fatalf("%d migration rows in the catalog, missing %v", len(rows), slices.Sorted(maps.Keys(historical)))
	}
	return rows
}

// For every catalog row of the migration the capability admission admits
// exactly the modes the selector-keyed admission admitted, the split rows each
// the modes of the row they come from. Over every such row topology combined
// with every such row's selector and declaration it never admits a profile or
// mode the old admission did not: a selector without a declaration behaves
// exactly as before, and a declared capability is admitted only where the old
// admission admitted its test, or refused.
func TestCapabilityAdmissionMatchesTheLegacySelectorAdmission(t *testing.T) {
	rows := historicalCatalogRows(t)
	declarations := map[string]string{"": ""}
	for _, row := range rows {
		declarations[row.Params["test_run"]] = row.Params["capabilities"]
	}
	for selector := range legacySplitSelectors {
		if _, ok := declarations[selector]; !ok {
			t.Fatal("split selector has no catalog row", selector)
		}
	}
	// Selectors no row uses, none with a declaration: broad suites, the soak
	// cells and a test outside the four-OSD rows. The three joined legacy
	// selections are not among them: they no longer admit their modes, which
	// moved to the split rows (TestJoinedLegacySelectionsNoLongerAdmitModes).
	for _, selector := range []string{"^TestCeph.*Integration$", "^TestCephOSD.*Integration$", "^TestCephMDSCephFS.*Integration$", "^TestCephRBD.*Integration$",
		"^TestCephOSDSoakIntegration$", "^TestCephOSDSoakFailoverIntegration$", legacyOSDSoakSelector, "^TestCephOSDErasureIntegration$"} {
		if _, ok := declarations[selector]; !ok {
			declarations[selector] = ""
		}
	}
	for _, row := range rows {
		current, legacy := loadBoth(ciRowEnvironment(t, row.Params))
		if !current.Loaded || current != legacy {
			t.Errorf("%s: capability admission\n%+v\nlegacy admission\n%+v", row.ID, current, legacy)
		}
	}
	compared, refused := 0, 0
	for _, row := range rows {
		for selector, capabilities := range declarations {
			params := maps.Clone(row.Params)
			params["test_run"], params["capabilities"] = selector, capabilities
			current, legacy := loadBoth(ciRowEnvironment(t, params))
			switch {
			case current.Loaded && current != legacy:
				t.Errorf("%s with %s [%s]: capability admission\n%+v\nlegacy admission\n%+v", row.ID, selector, capabilities, current, legacy)
			case !current.Loaded && legacy.Loaded && capabilities == "":
				t.Errorf("%s with %s: refused without a declaration, admitted before", row.ID, selector)
			case !current.Loaded && legacy.Loaded:
				refused++
			}
			compared++
		}
	}
	t.Logf("%d rows; %d topology and selector combinations, %d declared capabilities refused on a topology that does not support them", len(rows), compared, refused)
}

// The three legacy selections of two tests each keep no mode of their own:
// with or without a declaration, MON SIGKILL and the four-OSD topology are
// refused to them, and their tests run from the split rows instead.
func TestJoinedLegacySelectionsNoLongerAdmitModes(t *testing.T) {
	for _, row := range ciRepositoryCatalog(t) {
		joined, ok := legacySplitSelectors[row.Params["test_run"]]
		if !ok {
			continue
		}
		for _, capabilities := range []string{"", row.Params["capabilities"]} {
			params := maps.Clone(row.Params)
			params["test_run"], params["capabilities"] = joined, capabilities
			env := ciRowEnvironment(t, params)
			p, err := loadProfile(func(name string) string { return env[name] })
			if err == nil && (p.forcedKillAdmitted("mon") || p.osdTopology().erasureCapacity) {
				t.Fatalf("%s: joined selection %s [%s] admitted a mode", row.ID, joined, capabilities)
			}
		}
	}
}
