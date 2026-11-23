package main

import (
	"cmp"
	"fmt"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
)

// authEpochDefaultCycles is the CI cycle count of the service-key epoch
// acceptance test. CEPH_MSGR_TEST_AUTH_EPOCH_CYCLES raises it for local soak
// runs; each cycle wipes the rotating service keys once and makes one new cold
// client's first MGR handshake.
const (
	authEpochDefaultCycles = "2"
	authEpochMaxCycles     = 1000
)

const profileDefaultImage = "quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9"

// profile describes a development fixture, not product connection options.
type profile struct {
	monAddresses, fsid, peers, mdsFreshIdentity, mdsCommandsIdentity      string
	mdsSessionsForeignIdentity, mdsCephFSIdentity                         string
	mdsCephFSRecoveryIdentity, mdsCephFSRanksIdentity                     string
	mdsCephFSCompletionIdentity, mdsCephFSCompletion                      string
	mdsCephFSDataIdentity, mdsCephFSData                                  string
	mdsFresh, mdsCommands, mdsSessions, mdsCephFS, mdsCephFSRanks         string
	mdsCephFSRanksReplay                                                  string
	rbdBase, rbdBaseIdentity                                              string
	image, keyType, serviceCipher, ipFamily, mgrCount, runtime, race      string
	expireTickets, idleSessions, authEpoch, shortTickets                  string
	authEpochCycles                                                       string
	sparseReadOracle, mapExtOracle                                        string
	u64XattrOracle                                                        string
	writeSameOracle                                                       string
	osdProfile                                                            string
	nativeMgrPreparation, monPing, osdCachePools, mgrService              string
	modeRejection, connectionMode, mappedIPv6, osd, osdStore, osdReplicas string
	stressDuration, testRun, testTimeout, diagnostics                     string
	// capabilities are the declared fixture capabilities in canonical
	// (sorted, comma-separated) form; capability.go admits their modes.
	capabilities string
	// defaultRun is the selection of the fixture this profile starts, which
	// testRun equals unless a capability admits one exact test instead.
	defaultRun string
	// osdSoak is the opt-in OSD soak duration; it selects admitted soak parents.
	osdSoak string
	// sourceSHA is the controller's tested revision for workload receipts.
	sourceSHA string
	// sequence holds the space-separated selectors of a fixture sequence
	// (sequence.go), whose union testRun is; empty for one row.
	sequence string
	// rgwNative routes the row to one dedicated RGW checkpoint of this
	// module (rgw_native_route.go) instead of the controller's fixture.
	rgwNative string
	// monFreeze opts in to the connected-MON freeze tests, which can leave the
	// OSD on an old OSDMap (an unexplained fixture failure; see
	// integration/mon_connected_stall_test.go).
	monFreeze string
	// inputs holds the inputs declared with defineInput (input.go), the one
	// field for all of them. The fields above are frozen: a new input is
	// declared beside the code that reads it and becomes no field.
	inputs string
}

// profileInputPrefix begins the name of every fixture input a catalog row
// sets: the trusted native workflow forwards a row parameter p as
// CEPH_MSGR_TEST_<P> (tools/native_env.py).
const profileInputPrefix = "CEPH_MSGR_TEST_"

// loadEnvironmentProfile loads the profile from environ ("NAME=value"
// entries, the first of a repeated name winning as in os.Getenv) and refuses
// a CEPH_MSGR_TEST_ name that loadProfile does not read. The names loadProfile
// reads are the fixture's input vocabulary, and there is no other list of
// them: a new input is admitted by the code that reads it, and a row
// parameter or a variable that no input reads is refused instead of ignored.
// tools/scenario_plan.py validate passes every catalog row through this
// admission (TestCatalogRowsAreAdmittedByTheFixture).
func loadEnvironmentProfile(environ []string) (profile, error) {
	values := map[string]string{}
	for _, entry := range environ {
		name, value, _ := strings.Cut(entry, "=")
		if _, seen := values[name]; !seen {
			values[name] = value
		}
	}
	read := map[string]bool{}
	p, err := loadProfile(func(name string) string {
		read[name] = true
		return values[name]
	})
	if err != nil {
		return profile{}, err
	}
	var unread []string
	for name := range values {
		if strings.HasPrefix(name, profileInputPrefix) && !read[name] {
			unread = append(unread, name)
		}
	}
	if len(unread) != 0 {
		slices.Sort(unread)
		return profile{}, fmt.Errorf("no fixture input reads %s", strings.Join(unread, ", "))
	}
	return p, nil
}

// loadProfile reads one row's fixture profile, or a sequence of rows that
// share it (CEPH_MSGR_TEST_SEQUENCE, sequence.go).
func loadProfile(getenv func(string) string) (profile, error) {
	if sequence := getenv(sequenceEnv); sequence != "" {
		return loadSequenceProfile(getenv, sequence)
	}
	return loadSingleProfile(getenv)
}

func loadSingleProfile(getenv func(string) string) (profile, error) {
	p := loadPinnedFields(getenv)
	p.inputs = readDeclaredInputs(getenv)
	return admitSingleProfile(p, getenv)
}

// loadPinnedFields reads the frozen inputs that are fields of profile. A new
// input is declared with defineInput instead and adds no line here.
func loadPinnedFields(getenv func(string) string) profile {
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
		rgwNative:            value(rgwNativeEnv, "0"),
		testRun:              getenv("CEPH_MSGR_TEST_RUN"),
		capabilities:         getenv("CEPH_MSGR_TEST_CAPABILITIES"),
		testTimeout:          value("CEPH_MSGR_TEST_TIMEOUT", "10m"),
		diagnostics:          getenv("CEPH_MSGR_TEST_DIAGNOSTICS"),
	}
	p.serviceCipher = value("CEPH_MSGR_TEST_SERVICE_CIPHER", p.keyType)
	return p
}

func admitSingleProfile(p profile, getenv func(string) string) (profile, error) {
	capabilities, err := parseCapabilities(p.capabilities, p.testRun)
	if err != nil {
		return profile{}, err
	}
	p.capabilities = capabilities
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
		{rgwNativeEnv, p.rgwNative, []string{"0", "1"}},
	} {
		if !slices.Contains(choice.allowed, choice.value) {
			return profile{}, fmt.Errorf("%s must be one of %s", choice.name, strings.Join(choice.allowed, ", "))
		}
	}
	if p.rgwNative == "1" {
		// The routed checkpoint starts its own cluster; nothing below
		// applies to it, and rgwNativeProfileProblem refuses every input
		// that would select another fixture mode.
		if err := rgwNativeProfileProblem(p); err != nil {
			return profile{}, err
		}
		p.defaultRun = p.testRun
		if err := suitePartProblem(p); err != nil {
			return profile{}, err
		}
		return p, nil
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
	// minutes; an explicit CEPH_MSGR_TEST_TIMEOUT still wins. This static
	// value bounds a run without a job deadline and each test of a fixture
	// sequence; under a native CI job's deadline the default selection is
	// bounded by the job instead (job_budget.go).
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
		admitted := false
		if p.mdsCommands == "1" {
			selected = "^TestCephMDSCommandsIntegration$"
			// The applied-command outcome cell shares the owned command
			// fixture and is admitted only by its capability.
			admitted = p.admitsExclusive(fixtureMDSCommands)
		}
		if p.mdsSessions == "1" {
			selected = "^TestCephMDSSessionsIntegration$"
		}
		if p.mdsCephFS == "1" {
			selected = "^TestCephMDSCephFS(Push|Recovery)?Integration$"
			// The shared and the FCNTL held-lock variants of the recovery
			// acceptance own a fresh metadata fixture and are admitted only by
			// a capability.
			admitted = p.admitsExclusive(fixtureMDSCephFS)
		}
		if p.mdsCephFSRanks == "1" {
			selected = "^TestCephMDSCephFSRanksIntegration$"
			// The held-lock standby takeover owns a fresh two-rank fixture
			// and is admitted only by its capability.
			admitted = p.admitsExclusive(fixtureMDSCephFSRanks)
			if p.mdsCephFSRanksReplay == "1" {
				selected = "^TestCephMDSCephFSRanksReplayIntegration$"
				admitted = false
			}
		}
		if p.mdsCephFSCompletion == "1" {
			selected = "^TestCephMDSCephFSCompletionIntegration$"
			// The split reconnect and its held-lock variants each own a fresh
			// completion fixture and are admitted only by a capability.
			admitted = p.admitsExclusive(fixtureMDSCephFSCompletion)
		}
		if p.mdsCephFSData == "1" {
			selected = "^TestCephMDSCephFSDataIntegration$"
			// The cap-send recovery and writeback-fence variants share the
			// owned data fixture and are admitted only by a capability.
			admitted = p.admitsExclusive(fixtureMDSCephFSData)
		}
		if p.osd != "1" || !p.mdsFreshOSDAllowed() || p.osdReplicas != "1" || (p.sparseReadOracle != "" || p.mapExtOracle != "") ||
			!ownSelection(p.testRun, selected, admitted) {
			return profile{}, fmt.Errorf("MDS fixture requires its exclusive selector %s and one admitted OSD profile", selected)
		}
	}
	if p.rbdBase == "1" {
		// The RBD base, clone, journaling, write and exclusive-lock acceptances own
		// a pool, two identities and six native images on the single-OSD
		// legacy-v6 fixture; their Go client admits no HASHPSPOOL bit, so
		// every pool must keep the legacy placement seed. The replicated-PG
		// profiles are refused: their explicit-PG requests and HASHPSPOOL
		// pools are not verified with RBD.
		selected = "^TestCephRBD(Base|Write|ExclusiveLock|Clone|Journal)Integration$"
		// The lost-append-reply acceptance needs a journal no other test has
		// advanced. The lock-owner-loss acceptances blocklist two client
		// addresses and kill a native lock owner. The primary-change
		// acceptance is the lost-append-reply sequence on three replicated
		// BlueStore OSDs; no other RBD selection is admitted on that topology,
		// and that one on no other. Each runs alone on a fresh fixture and is
		// admitted only by its capability.
		admitted := p.admitsExclusive(fixtureRBD)
		if p.osd != "1" || p.osdProfile != "legacy-v6" || p.osdReplicas != p.rbdReplicas() || p.osdReplicas == "3" && p.osdStore != "bluestore" || p.mdsFresh != "0" ||
			(p.sparseReadOracle != "" || p.mapExtOracle != "") || !ownSelection(p.testRun, selected, admitted) {
			return profile{}, fmt.Errorf("RBD acceptance requires its exclusive selector %s or one capability-admitted test, the single-OSD legacy-v6 fixture (three BlueStore OSDs for capability %s alone) and no MDS profile", selected, capRBDJournalPrimaryChange.name)
		}
	}
	if p.monPing == "1" {
		selected = "^TestCephMonPingIntegration$"
		if p.mgrCount != "2" || p.osd != "0" || p.mdsFresh != "0" || p.rbdBase != "0" || p.expireTickets != "0" || p.idleSessions != "0" || p.authEpoch != "0" || p.shortTickets != "0" || p.nativeMgrPreparation != "0" || p.modeRejection != "" || p.mappedIPv6 != "0" || p.stressDuration != "" || p.monFreeze != "" || !ownSelection(p.testRun, selected, false) {
			return profile{}, fmt.Errorf("MON ping acceptance requires its exclusive selector %s and the ordinary three-MON/two-MGR fixture", selected)
		}
	}
	if p.osdCachePools == "1" {
		selected = "^TestCephOSDIgnoreCacheIntegration$"
		// The Go cache-tier and cache-operation acceptances reuse the live
		// cache-pool fixture alone and are admitted only by its capability.
		if p.mgrCount != "2" || !ownSelection(p.testRun, selected, p.admitsExclusive(fixtureOSDCachePools)) {
			return profile{}, fmt.Errorf("cache-pool acceptance requires two MGRs and its exclusive selector %s or one test admitted by an exclusive capability of this fixture, such as %s", selected, capExclusiveOSDCachePools.name)
		}
		checked := p
		checked.osdCachePools = "0"
		if err := validateIgnoreCacheProbeTopology(checked); err != nil {
			return profile{}, err
		}
	}
	if p.mgrService == "1" {
		// Ordinary service acceptance uses the one-OSD oracle fixture. Cold
		// bootstrap instead admits only a closed MON-only observation recipe.
		selected = "^TestCephMgrServiceRegistrationIntegration$"
		topologyAllowed := p.osd == "1" && p.mgrCount == "2"
		if p.mgrCount == "0" {
			selected = "^TestCephMgrServiceColdBootstrapIntegration$"
			topologyAllowed = p.osd == "0" && p.runtime == "container"
		}
		if !topologyAllowed || p.osdProfile != "legacy-v6" || p.osdReplicas != "1" || p.mdsFresh != "0" || p.rbdBase != "0" ||
			p.monPing != "0" || p.osdCachePools != "0" || p.expireTickets != "0" || p.idleSessions != "0" || p.authEpoch != "0" || p.shortTickets != "0" ||
			p.nativeMgrPreparation != "0" || p.modeRejection != "" || p.mappedIPv6 != "0" || p.stressDuration != "" || p.osdSoak != "" || p.monFreeze != "" ||
			p.sparseReadOracle != "" || p.mapExtOracle != "" || !ownSelection(p.testRun, selected, false) {
			return profile{}, fmt.Errorf("MGR service acceptance requires its exclusive selector %s and either two MGRs with one legacy-v6 OSD or zero initial MGRs without OSDs", selected)
		}
	}
	if topology := p.osdTopology(); topology.erasureCapacity != "" {
		// The four- and six-daemon topologies exist only for the bounded
		// erasure-width scenarios. The fixture's ordinary pools keep the
		// three-replica policy; broad OSD selections stay on topology "3".
		// Each has one default baseline; every other selection is one exact
		// test admitted by the topology's own erasure-width capability.
		selected = topology.erasureDefault
		if p.osd != "1" || (p.osdProfile != "legacy-v6" && p.osdProfile != "replicated-pg-v8") || p.osdSoak != "" || p.monFreeze != "" ||
			p.osdCachePools != "0" || p.mgrService != "0" || p.mdsFresh != "0" || p.rbdBase != "0" ||
			p.sparseReadOracle != "" || p.mapExtOracle != "" || p.u64XattrOracle != "" || p.writeSameOracle != "" ||
			!ownSelection(p.testRun, selected, p.admitsExclusive(topology.erasureCapacity)) {
			return profile{}, fmt.Errorf("%s topology requires its default selector %s or one test admitted by an exclusive capability of this topology, such as %s or %s, BlueStore and the legacy-v6 or replicated-pg-v8 profile", topology.erasureCapacity, selected, capOSDErasureWidth.name, capOSDErasureWidthSix.name)
		}
	}
	p.testRun = cmp.Or(p.testRun, selected)
	p.defaultRun = selected
	if err := capabilityTopologyProblem(p); err != nil {
		return profile{}, err
	}
	if _, err := soakExpectedTests(p); err != nil {
		return profile{}, err
	}
	if err := suitePartProblem(p); err != nil {
		return profile{}, err
	}
	return p, nil
}

// rbdReplicas is the OSD topology the selected RBD acceptance runs on: one
// OSD, or three for the primary-change sequence.
func (p profile) rbdReplicas() string {
	if p.declares(capRBDJournalPrimaryChange) {
		return "3"
	}
	return "1"
}

// osdTopology is one closed OSD fixture shape. Daemon capacity and the
// replication policy of the fixture's own replicated pools are separate
// facts: a wider erasure-code profile needs more daemons, not a different
// replicated bootstrap pool size.
type osdTopology struct {
	// daemons is the number of owned OSD daemons (IDs 0..daemons-1).
	daemons int
	// poolSize and poolMinSize are the replicated bootstrap defaults and
	// the fixture data pool's size and min_size.
	poolSize, poolMinSize int
	// erasureCapacity names the fixture of a topology that exists only to
	// host a wider erasure-coded pool owned by its exclusive test; empty for
	// the ordinary topologies. erasureDefault is that fixture's default
	// selection.
	erasureCapacity ownFixture
	erasureDefault  string
}

// capOSDErasureWidth admits one exact test other than the default k=2,m=2
// baseline on the four-OSD erasure-capacity topology.
var capOSDErasureWidth = defineExclusiveCapability(fixtureOSDErasureCapacity, "osd-erasure-width",
	"run one test other than the default k=2,m=2 baseline on the four-OSD erasure-capacity topology",
	"OSD=1 OSD_REPLICAS=4", nil)

// capOSDErasureWidthSix admits one exact test other than the default
// optimized k=4,m=2 baseline on the six-OSD erasure-capacity topology.
var capOSDErasureWidthSix = defineExclusiveCapability(fixtureOSDErasureSix, "osd-erasure-width-six",
	"run one test other than the default optimized k=4,m=2 baseline on the six-OSD erasure-capacity topology",
	"OSD=1 OSD_REPLICAS=6", nil)

// osdTopologies is the closed CEPH_MSGR_TEST_OSD_REPLICAS vocabulary. The
// variable keeps its historical name because the trusted native workflow
// already forwards it; its value selects a topology, not an arbitrary count.
// "1" and "3" keep their original meaning. "4" adds a fourth BlueStore
// daemon for the k=2,m=2 and k=3,m=1 pools, and "6" has six for a k=4,m=2
// pool; on both the replicated pools stay size 3/min_size 2.
var osdTopologies = map[string]osdTopology{
	"1": {daemons: 1, poolSize: 1, poolMinSize: 1},
	"3": {daemons: 3, poolSize: 3, poolMinSize: 2},
	"4": {daemons: 4, poolSize: 3, poolMinSize: 2, erasureCapacity: fixtureOSDErasureCapacity, erasureDefault: "^TestCephOSDErasureK2M2Integration$"},
	"6": {daemons: 6, poolSize: 3, poolMinSize: 2, erasureCapacity: fixtureOSDErasureSix, erasureDefault: "^TestCephOSDErasureOptimizedK4M2Integration$"},
}

func osdTopologyNames() []string {
	names := make([]string, 0, len(osdTopologies))
	for name := range osdTopologies {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// osdTopology returns the profile's validated topology. An unvalidated
// value yields the zero topology, which has no daemons.
func (p profile) osdTopology() osdTopology { return osdTopologies[p.osdReplicas] }

func (p profile) mdsFreshOSDAllowed() bool {
	if p.mdsPublicClient() {
		return p.osdProfile == "legacy-v6"
	}
	return p.osdPGV8()
}

// mdsPublicClient reports the closed MDS profiles whose public Go clients
// enable only MDS. They use the legacy/no-HASHPSPOOL fixture so MON admission
// needs neither EnableOSD nor the modern OSD profile's extra feature bits.
func (p profile) mdsPublicClient() bool {
	return p.mdsCommands == "1" || p.mdsSessions == "1" || p.mdsCephFS == "1" || p.mdsCephFSRanks == "1" || p.mdsCephFSCompletion == "1" || p.mdsCephFSData == "1"
}

// mdsFreshTopology is the owned filesystem's MDS shape: one active rank, two
// active ranks and one standby for the multi-rank acceptance, or three active
// ranks and one standby (later a standby-replay follower) for its
// standby-replay extension.
func (p profile) mdsFreshTopology() (active, standby int) {
	if p.mdsCephFSRanks == "1" && p.mdsCephFSRanksReplay == "1" {
		return 3, 1
	}
	if p.mdsCephFSRanks == "1" {
		return 2, 1
	}
	return 1, 0
}

// cleanupBudget bounds fixture termination. Session and CephFS metadata
// acceptance restore or verify owned filesystem, configuration, blocklist and
// identity state before removal.
func (p profile) cleanupBudget() time.Duration {
	if p.mdsCephFSRanksReplay == "1" {
		// Four owned daemons, a runtime journal pause to verify discarded or
		// restored, nested pins and the follower's return after the takeover.
		return 4 * time.Minute
	}
	if p.mdsCephFSRanks == "1" || p.mdsCephFSCompletion == "1" {
		// Pin restoration, standby return and three owned daemons; or the
		// completion acceptance's runtime, central and blocklist restoration
		// after up to four owned restarts.
		return 3 * time.Minute
	}
	if p.rbdMirrorSelected() {
		// A checkpoint that failed before its own finish still terminates
		// the mirror, the destination cluster and the netem helper here.
		return 3 * time.Minute
	}
	if p.mdsSessions == "1" || p.mdsCephFS == "1" || p.mdsCephFSData == "1" || p.rbdBase == "1" {
		// The CephFS data cleanup reads the namespace back natively before
		// removing two identities. The RBD cleanup removes its owned pool (with the temporary
		// mon_allow_pool_delete window, the three images) and two identities.
		return 2 * time.Minute
	}
	return time.Minute
}

// osdPGV8 selects the explicit-PG fixture: an eight-PG replicated data pool
// with HASHPSPOOL (also the global pool default before daemon startup, so the
// .mgr pool has it too), the controlled legacy CRUSH map, automatic CRUSH
// updates and the balancer off. The caller uses the natively observed PG and
// target; the product computes no hash-to-PG or CRUSH placement. Tentacle's
// default optimal CRUSH is outside this fixture: CRUSH_TUNABLES5 shares its
// bit with NEW_OSDOPREPLY_ENCODING. Preparing this fixture does not show that
// a Go session negotiated the profile's feature bits.
func (p profile) osdPGV8() bool {
	// This is the shared explicit-PG topology gate, not a request-version selector.
	return p.osdProfile == "replicated-pg-v8" || p.osdProfile == "replicated-pg-reply-v8" || p.osdProfile == "replicated-pg-request-v9-reply-v8"
}

func (p profile) monitors() string { return p.monAddresses }

func (p profile) containerEnvironment() map[string]string {
	env := map[string]string{
		"CEPH_MSGR_TEST_MDS_FRESH":               p.mdsFresh,
		"CEPH_MSGR_TEST_MDS_COMMANDS":            p.mdsCommands,
		"CEPH_MSGR_TEST_MDS_SESSIONS":            p.mdsSessions,
		"CEPH_MSGR_TEST_MDS_CEPHFS":              p.mdsCephFS,
		"CEPH_MSGR_TEST_MDS_CEPHFS_RANKS":        p.mdsCephFSRanks,
		"CEPH_MSGR_TEST_MDS_CEPHFS_RANKS_REPLAY": p.mdsCephFSRanksReplay,
		"CEPH_MSGR_TEST_MDS_CEPHFS_COMPLETION":   p.mdsCephFSCompletion,
		"CEPH_MSGR_TEST_MDS_CEPHFS_DATA":         p.mdsCephFSData,
		"CEPH_MSGR_TEST_RBD":                     p.rbdBase,
		"CEPH_MSGR_TEST_KEY_TYPE":                p.keyType,
		"CEPH_MSGR_TEST_SERVICE_CIPHER":          p.serviceCipher,
		"CEPH_MSGR_TEST_IP_FAMILY":               p.ipFamily,
		"CEPH_MSGR_TEST_MGR_COUNT":               p.mgrCount,
		"CEPH_MSGR_TEST_RUNTIME":                 p.runtime,
		"CEPH_MSGR_TEST_RACE":                    p.race,
		"CEPH_MSGR_TEST_EXPIRE_TICKETS":          p.expireTickets,
		"CEPH_MSGR_TEST_IDLE_SESSIONS":           p.idleSessions,
		"CEPH_MSGR_TEST_AUTH_EPOCH":              p.authEpoch,
		"CEPH_MSGR_TEST_SHORT_TICKETS":           p.shortTickets,
		"CEPH_MSGR_TEST_NATIVE_MGR_PREPARATION":  p.nativeMgrPreparation,
		"CEPH_MSGR_TEST_MON_PING":                p.monPing,
		"CEPH_MSGR_TEST_OSD_CACHE_POOLS":         p.osdCachePools,
		"CEPH_MSGR_TEST_MGR_SERVICE":             p.mgrService,
		"CEPH_MSGR_TEST_MODE_REJECTION":          p.modeRejection,
		"CEPH_MSGR_TEST_CONNECTION_MODE":         p.connectionMode,
		"CEPH_MSGR_TEST_MAPPED_IPV6":             p.mappedIPv6,
		"CEPH_MSGR_TEST_OSD":                     p.osd,
		"CEPH_MSGR_TEST_OSD_STORE":               p.osdStore,
		"CEPH_MSGR_TEST_OSD_REPLICAS":            p.osdReplicas,
		"CEPH_MSGR_TEST_OSD_PROFILE":             p.osdProfile,
		"CEPH_MSGR_STRESS_DURATION":              p.stressDuration,
		"CEPH_MSGR_TEST_OSD_SOAK":                p.osdSoak,
		"CEPH_MSGR_TEST_MON_FREEZE":              p.monFreeze,
		"CEPH_MSGR_TEST_CONTROL_DIAGNOSTICS":     "1",
	}
	// A declared input reaches the native tests and capability requirement
	// terms as its own variable.
	for variable, value := range p.declaredInputs() {
		env[variable] = value
	}
	return env
}

func (p profile) testEnvironment(controlDir, proxy string) map[string]string {
	copyErrorMarker := ""
	if p.copyFromLateErrorSelected() {
		copyErrorMarker = "1"
	}
	multiChunkMarker := ""
	if p.copyFromMultiChunkSelected() {
		multiChunkMarker = "1"
	}
	mirrorMarker := ""
	if p.rbdMirrorSelected() {
		mirrorMarker = "1"
	}
	shardHistory := ""
	if p.shardHistorySelected() {
		shardHistory = "1"
	}
	monKillMarker := ""
	if p.monForcedKillSelected() {
		monKillMarker = "1"
	}
	marker := ""
	mapMarker := ""
	if p.sparseReadOracle != "" {
		marker = "1"
	}
	if p.mapExtOracle != "" {
		mapMarker = "1"
	}
	u64Marker := ""
	if p.u64XattrOracle != "" {
		u64Marker = "1"
	}
	writeSameMarker := ""
	if p.writeSameOracle != "" {
		writeSameMarker = "1"
	}
	result := map[string]string{
		"CEPH_MSGR_TEST_MDS_FRESH":               p.mdsFresh,
		"CEPH_MSGR_TEST_MDS_COMMANDS":            p.mdsCommands,
		"CEPH_MSGR_TEST_MDS_SESSIONS":            p.mdsSessions,
		"CEPH_MSGR_TEST_MDS_CEPHFS":              p.mdsCephFS,
		"CEPH_MSGR_TEST_MDS_CEPHFS_RANKS":        p.mdsCephFSRanks,
		"CEPH_MSGR_TEST_MDS_CEPHFS_RANKS_REPLAY": p.mdsCephFSRanksReplay,
		"CEPH_MSGR_TEST_MDS_CEPHFS_COMPLETION":   p.mdsCephFSCompletion,
		"CEPH_MSGR_TEST_MDS_CEPHFS_DATA":         p.mdsCephFSData,
		"CEPH_MSGR_TEST_RBD":                     p.rbdBase,
		copyFromLateErrorMarker:                  copyErrorMarker,
		copyFromMultiChunkMarker:                 multiChunkMarker,
		rbdMirrorMarker:                          mirrorMarker,
		monForcedKillMarker:                      monKillMarker,
		shardHistoryMarker:                       shardHistory,
		"CEPH_MSGR_NATIVE_SPARSE_READ_ORACLE":    marker,
		"CEPH_MSGR_NATIVE_MAPEXT_ORACLE":         mapMarker,
		"CEPH_MSGR_NATIVE_U64_XATTR_ORACLE":      u64Marker,
		"CEPH_MSGR_NATIVE_WRITESAME_ORACLE":      writeSameMarker,
		"CEPH_MSGR_MONITORS":                     p.monitors(),
		"CEPH_MSGR_KEY_FILE":                     filepath.Join(controlDir, "key"),
		"CEPH_MSGR_IDENTITY":                     "client.test",
		"CEPH_MSGR_FSID":                         p.fsid,
		"CEPH_MSGR_TEST_PEERS":                   p.peers,
		"CEPH_MSGR_CONTROL_DIR":                  controlDir,
		"CEPH_MSGR_TEST_KEY_TYPE":                p.keyType,
		"CEPH_MSGR_TEST_SERVICE_CIPHER":          p.serviceCipher,
		"CEPH_MSGR_TEST_MGR_COUNT":               p.mgrCount,
		"CEPH_MSGR_TEST_EXPIRE_TICKETS":          p.expireTickets,
		"CEPH_MSGR_TEST_IDLE_SESSIONS":           p.idleSessions,
		"CEPH_MSGR_TEST_AUTH_EPOCH":              p.authEpoch,
		"CEPH_MSGR_TEST_AUTH_EPOCH_CYCLES":       p.authEpochCycles,
		"CEPH_MSGR_TEST_SHORT_TICKETS":           p.shortTickets,
		"CEPH_MSGR_TEST_NATIVE_MGR_PREPARATION":  p.nativeMgrPreparation,
		"CEPH_MSGR_TEST_MON_PING":                p.monPing,
		"CEPH_MSGR_TEST_OSD_CACHE_POOLS":         p.osdCachePools,
		"CEPH_MSGR_TEST_MGR_SERVICE":             p.mgrService,
		"CEPH_MSGR_TEST_MODE_REJECTION":          p.modeRejection,
		"CEPH_MSGR_TEST_CONNECTION_MODE":         p.connectionMode,
		"CEPH_MSGR_TEST_MAPPED_IPV6":             p.mappedIPv6,
		"CEPH_MSGR_TEST_OSD":                     p.osd,
		"CEPH_MSGR_TEST_OSD_STORE":               p.osdStore,
		"CEPH_MSGR_TEST_OSD_REPLICAS":            p.osdReplicas,
		"CEPH_MSGR_TEST_OSD_PROFILE":             p.osdProfile,
		"CEPH_MSGR_STRESS_DURATION":              p.stressDuration,
		"CEPH_MSGR_TEST_OSD_SOAK":                p.osdSoak,
		"CEPH_MSGR_TEST_MON_FREEZE":              p.monFreeze,
		"CEPH_MSGR_TEST_PROXY":                   proxy,
	}
	if p.osdSoak != "" {
		result["CEPH_MSGR_TEST_SOURCE_SHA"] = p.sourceSHA
	}
	if p.mdsFresh == "1" {
		result["CEPH_MSGR_MDS_FRESH_IDENTITY"] = p.mdsFreshIdentity
		result["CEPH_MSGR_MDS_FRESH_KEY_FILE"] = filepath.Join(controlDir, "mds-fresh.key")
	}
	if p.mdsCommands == "1" {
		result["CEPH_MSGR_MDS_COMMANDS_IDENTITY"] = p.mdsCommandsIdentity
		result["CEPH_MSGR_MDS_COMMANDS_KEY_FILE"] = filepath.Join(controlDir, "mds-commands.key")
	}
	if p.mdsSessions == "1" {
		result["CEPH_MSGR_MDS_SESSIONS_FOREIGN_IDENTITY"] = p.mdsSessionsForeignIdentity
		result["CEPH_MSGR_MDS_SESSIONS_FOREIGN_KEY_FILE"] = filepath.Join(controlDir, "mds-sessions-foreign.key")
	}
	if p.mdsCephFS == "1" {
		result["CEPH_MSGR_MDS_CEPHFS_IDENTITY"] = p.mdsCephFSIdentity
		result["CEPH_MSGR_MDS_CEPHFS_KEY_FILE"] = filepath.Join(controlDir, "mds-cephfs.key")
		result["CEPH_MSGR_MDS_CEPHFS_RECOVERY_IDENTITY"] = p.mdsCephFSRecoveryIdentity
		result["CEPH_MSGR_MDS_CEPHFS_RECOVERY_KEY_FILE"] = filepath.Join(controlDir, "mds-cephfs-recovery.key")
	}
	if p.mdsCephFSRanks == "1" {
		result["CEPH_MSGR_MDS_CEPHFS_RANKS_IDENTITY"] = p.mdsCephFSRanksIdentity
		result["CEPH_MSGR_MDS_CEPHFS_RANKS_KEY_FILE"] = filepath.Join(controlDir, "mds-cephfs-ranks.key")
	}
	if p.mdsCephFSCompletion == "1" {
		result["CEPH_MSGR_MDS_CEPHFS_COMPLETION_IDENTITY"] = p.mdsCephFSCompletionIdentity
		result["CEPH_MSGR_MDS_CEPHFS_COMPLETION_KEY_FILE"] = filepath.Join(controlDir, "mds-cephfs-completion.key")
	}
	if p.mdsCephFSData == "1" {
		result["CEPH_MSGR_MDS_CEPHFS_DATA_IDENTITY"] = p.mdsCephFSDataIdentity
		result["CEPH_MSGR_MDS_CEPHFS_DATA_KEY_FILE"] = filepath.Join(controlDir, "mds-cephfs-data.key")
	}
	if p.rbdBase == "1" {
		result["CEPH_MSGR_RBD_BASE_IDENTITY"] = p.rbdBaseIdentity
		result["CEPH_MSGR_RBD_BASE_KEY_FILE"] = filepath.Join(controlDir, "rbd-base.key")
	}
	return result
}

// mgrServiceColdBootstrap reports the MON-only cold-bootstrap fixture running
// its own closed observation recipe.
func (p profile) mgrServiceColdBootstrap() bool {
	return p.mgrService == "1" && p.mgrCount == "0" && p.osd == "0" && p.runsDefault()
}
