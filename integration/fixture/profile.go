package main

import (
	"fmt"
	"path/filepath"
	"slices"
	"strings"
)

const profileDefaultImage = "quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9"

// profile describes a development fixture, not product connection options.
type profile struct {
	monAddresses, fsid, peers, mdsFreshIdentity, mdsCommandsIdentity      string
	mdsFresh, mdsCommands                                                 string
	image, keyType, serviceCipher, ipFamily, mgrCount, runtime, race      string
	expireTickets, idleSessions, authEpoch, shortTickets                  string
	sparseReadOracle                                                      string
	osdProfile                                                            string
	nativeMgrPreparation                                                  string
	modeRejection, connectionMode, mappedIPv6, osd, osdStore, osdReplicas string
	stressDuration, testRun, testTimeout, diagnostics                     string
}

func loadProfile(getenv func(string) string) (profile, error) {
	value := func(name, fallback string) string {
		if result := getenv(name); result != "" {
			return result
		}
		return fallback
	}
	p := profile{
		mdsFresh:             value("CEPH_MSGR_TEST_MDS_FRESH", "0"),
		mdsCommands:          value("CEPH_MSGR_TEST_MDS_COMMANDS", "0"),
		osdProfile:           value("CEPH_MSGR_TEST_OSD_PROFILE", "legacy-v6"),
		sparseReadOracle:     getenv("CEPH_MSGR_TEST_SPARSE_READ_ORACLE"),
		image:                value("CEPH_MSGR_TEST_IMAGE", profileDefaultImage),
		keyType:              value("CEPH_MSGR_TEST_KEY_TYPE", "aes256k"),
		ipFamily:             value("CEPH_MSGR_TEST_IP_FAMILY", "4"),
		mgrCount:             value("CEPH_MSGR_TEST_MGR_COUNT", "2"),
		runtime:              value("CEPH_MSGR_TEST_RUNTIME", "container"),
		race:                 value("CEPH_MSGR_TEST_RACE", "0"),
		expireTickets:        value("CEPH_MSGR_TEST_EXPIRE_TICKETS", "0"),
		idleSessions:         value("CEPH_MSGR_TEST_IDLE_SESSIONS", "0"),
		authEpoch:            value("CEPH_MSGR_TEST_AUTH_EPOCH", "0"),
		shortTickets:         value("CEPH_MSGR_TEST_SHORT_TICKETS", "0"),
		nativeMgrPreparation: value("CEPH_MSGR_TEST_NATIVE_MGR_PREPARATION", "0"),
		modeRejection:        getenv("CEPH_MSGR_TEST_MODE_REJECTION"),
		connectionMode:       value("CEPH_MSGR_TEST_CONNECTION_MODE", "secure"),
		mappedIPv6:           value("CEPH_MSGR_TEST_MAPPED_IPV6", "0"),
		osd:                  value("CEPH_MSGR_TEST_OSD", "0"),
		osdStore:             value("CEPH_MSGR_TEST_OSD_STORE", "memstore"),
		osdReplicas:          value("CEPH_MSGR_TEST_OSD_REPLICAS", "1"),
		stressDuration:       getenv("CEPH_MSGR_STRESS_DURATION"),
		testRun:              getenv("CEPH_MSGR_TEST_RUN"),
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
		{"CEPH_MSGR_TEST_SERVICE_CIPHER", p.serviceCipher, []string{"aes", "aes256k"}},
		{"CEPH_MSGR_TEST_RUNTIME", p.runtime, []string{"container", "host"}},
		{"CEPH_MSGR_TEST_RACE", p.race, []string{"0", "1"}},
		{"CEPH_MSGR_TEST_EXPIRE_TICKETS", p.expireTickets, []string{"0", "1"}},
		{"CEPH_MSGR_TEST_IDLE_SESSIONS", p.idleSessions, []string{"0", "1"}},
		{"CEPH_MSGR_TEST_AUTH_EPOCH", p.authEpoch, []string{"0", "1"}},
		{"CEPH_MSGR_TEST_SHORT_TICKETS", p.shortTickets, []string{"0", "1"}},
		{"CEPH_MSGR_TEST_NATIVE_MGR_PREPARATION", p.nativeMgrPreparation, []string{"0", "1"}},
		{"CEPH_MSGR_TEST_MODE_REJECTION", p.modeRejection, []string{"", "mon", "mgr"}},
		{"CEPH_MSGR_TEST_CONNECTION_MODE", p.connectionMode, []string{"secure", "crc"}},
		{"CEPH_MSGR_TEST_MAPPED_IPV6", p.mappedIPv6, []string{"0", "1"}},
		{"CEPH_MSGR_TEST_OSD", p.osd, []string{"0", "1"}},
		{"CEPH_MSGR_TEST_OSD_PROFILE", p.osdProfile, []string{"legacy-v6", "replicated-pg-v8", "replicated-pg-reply-v8"}},
		{"CEPH_MSGR_TEST_OSD_STORE", p.osdStore, []string{"memstore", "bluestore"}},
		{"CEPH_MSGR_TEST_OSD_REPLICAS", p.osdReplicas, []string{"1", "3"}},
		{"CEPH_MSGR_TEST_MGR_COUNT", p.mgrCount, []string{"0", "2"}},
		{"CEPH_MSGR_TEST_IP_FAMILY", p.ipFamily, []string{"4", "6"}},
	} {
		if !slices.Contains(choice.allowed, choice.value) {
			return profile{}, fmt.Errorf("%s must be one of %s", choice.name, strings.Join(choice.allowed, ", "))
		}
	}
	if p.osdPGV8() && p.osd != "1" {
		return profile{}, fmt.Errorf("%s requires CEPH_MSGR_TEST_OSD=1", p.osdProfile)
	}
	if p.sparseReadOracle != "" && p.osd != "1" {
		return profile{}, fmt.Errorf("native sparse-read oracle requires OSD fixture")
	}
	if p.osdReplicas != "1" && p.osd != "1" {
		return profile{}, fmt.Errorf("replicated fixture requires CEPH_MSGR_TEST_OSD=1")
	}
	if p.osdReplicas == "3" && p.osdStore != "bluestore" {
		return profile{}, fmt.Errorf("replicated crash profile requires BlueStore")
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
		selected = "^TestCeph(OSD|MonOSDMaps).*Integration$"
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
	if p.mdsFresh == "1" {
		selected = "^TestCephMDSFreshIntegration$"
		if p.mdsCommands == "1" {
			selected = "^TestCephMDSCommandsIntegration$"
		}
		if p.osd != "1" || !p.mdsFreshOSDAllowed() || p.osdReplicas != "1" || p.sparseReadOracle != "" ||
			p.testRun != "" && p.testRun != selected {
			return profile{}, fmt.Errorf("MDS fixture requires its exclusive selector %s and one admitted OSD profile", selected)
		}
	}
	if p.testRun == "" {
		p.testRun = selected
	}
	return p, nil
}

func (p profile) mdsFreshOSDAllowed() bool {
	if p.mdsCommands == "1" {
		return p.osdProfile == "legacy-v6"
	}
	return p.osdPGV8()
}

func (p profile) osdPGV8() bool {
	return p.osdProfile == "replicated-pg-v8" || p.osdProfile == "replicated-pg-reply-v8"
}

func (p profile) monitors() string { return p.monAddresses }

func (p profile) containerEnvironment() map[string]string {
	return map[string]string{
		"CEPH_MSGR_TEST_MDS_FRESH":              p.mdsFresh,
		"CEPH_MSGR_TEST_MDS_COMMANDS":           p.mdsCommands,
		"CEPH_MSGR_TEST_KEY_TYPE":               p.keyType,
		"CEPH_MSGR_TEST_SERVICE_CIPHER":         p.serviceCipher,
		"CEPH_MSGR_TEST_IP_FAMILY":              p.ipFamily,
		"CEPH_MSGR_TEST_MGR_COUNT":              p.mgrCount,
		"CEPH_MSGR_TEST_RUNTIME":                p.runtime,
		"CEPH_MSGR_TEST_RACE":                   p.race,
		"CEPH_MSGR_TEST_EXPIRE_TICKETS":         p.expireTickets,
		"CEPH_MSGR_TEST_IDLE_SESSIONS":          p.idleSessions,
		"CEPH_MSGR_TEST_AUTH_EPOCH":             p.authEpoch,
		"CEPH_MSGR_TEST_SHORT_TICKETS":          p.shortTickets,
		"CEPH_MSGR_TEST_NATIVE_MGR_PREPARATION": p.nativeMgrPreparation,
		"CEPH_MSGR_TEST_MODE_REJECTION":         p.modeRejection,
		"CEPH_MSGR_TEST_CONNECTION_MODE":        p.connectionMode,
		"CEPH_MSGR_TEST_MAPPED_IPV6":            p.mappedIPv6,
		"CEPH_MSGR_TEST_OSD":                    p.osd,
		"CEPH_MSGR_TEST_OSD_STORE":              p.osdStore,
		"CEPH_MSGR_TEST_OSD_REPLICAS":           p.osdReplicas,
		"CEPH_MSGR_TEST_OSD_PROFILE":            p.osdProfile,
		"CEPH_MSGR_STRESS_DURATION":             p.stressDuration,
		"CEPH_MSGR_TEST_CONTROL_DIAGNOSTICS":    "1",
	}
}

func (p profile) testEnvironment(controlDir, proxy string) map[string]string {
	marker := ""
	if p.sparseReadOracle != "" {
		marker = "1"
	}
	result := map[string]string{
		"CEPH_MSGR_TEST_MDS_FRESH":              p.mdsFresh,
		"CEPH_MSGR_TEST_MDS_COMMANDS":           p.mdsCommands,
		"CEPH_MSGR_NATIVE_SPARSE_READ_ORACLE":   marker,
		"CEPH_MSGR_MONITORS":                    p.monitors(),
		"CEPH_MSGR_KEY_FILE":                    filepath.Join(controlDir, "key"),
		"CEPH_MSGR_IDENTITY":                    "client.test",
		"CEPH_MSGR_FSID":                        p.fsid,
		"CEPH_MSGR_TEST_PEERS":                  p.peers,
		"CEPH_MSGR_CONTROL_DIR":                 controlDir,
		"CEPH_MSGR_TEST_KEY_TYPE":               p.keyType,
		"CEPH_MSGR_TEST_SERVICE_CIPHER":         p.serviceCipher,
		"CEPH_MSGR_TEST_MGR_COUNT":              p.mgrCount,
		"CEPH_MSGR_TEST_EXPIRE_TICKETS":         p.expireTickets,
		"CEPH_MSGR_TEST_IDLE_SESSIONS":          p.idleSessions,
		"CEPH_MSGR_TEST_AUTH_EPOCH":             p.authEpoch,
		"CEPH_MSGR_TEST_SHORT_TICKETS":          p.shortTickets,
		"CEPH_MSGR_TEST_NATIVE_MGR_PREPARATION": p.nativeMgrPreparation,
		"CEPH_MSGR_TEST_MODE_REJECTION":         p.modeRejection,
		"CEPH_MSGR_TEST_CONNECTION_MODE":        p.connectionMode,
		"CEPH_MSGR_TEST_MAPPED_IPV6":            p.mappedIPv6,
		"CEPH_MSGR_TEST_OSD":                    p.osd,
		"CEPH_MSGR_TEST_OSD_STORE":              p.osdStore,
		"CEPH_MSGR_TEST_OSD_REPLICAS":           p.osdReplicas,
		"CEPH_MSGR_TEST_OSD_PROFILE":            p.osdProfile,
		"CEPH_MSGR_STRESS_DURATION":             p.stressDuration,
		"CEPH_MSGR_TEST_PROXY":                  proxy,
	}
	if p.mdsFresh == "1" {
		result["CEPH_MSGR_MDS_FRESH_IDENTITY"] = p.mdsFreshIdentity
		result["CEPH_MSGR_MDS_FRESH_KEY_FILE"] = filepath.Join(controlDir, "mds-fresh.key")
	}
	if p.mdsCommands == "1" {
		result["CEPH_MSGR_MDS_COMMANDS_IDENTITY"] = p.mdsCommandsIdentity
		result["CEPH_MSGR_MDS_COMMANDS_KEY_FILE"] = filepath.Join(controlDir, "mds-commands.key")
	}
	return result
}
