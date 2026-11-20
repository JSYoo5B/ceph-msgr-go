package osd

import "testing"

// opcodeStatus says whether this repository emits an operation, and for an
// operation it does not emit, why. Unsupported means only that this client
// never sends the operation; it does not claim the daemon lacks a handler,
// enables no feature bit and restores no older public API.
type opcodeStatus int

const (
	// implemented: SupportedOperation admits the code. Its bounded subset is
	// stated on the operation's constant and validation code.
	implemented opcodeStatus = iota + 1
	// noServerPath: the pinned osd/PrimaryLogPG.cc do_osd_ops has no case
	// for it, so sending it would be refused.
	noServerPath
	// daemonOnly: an OSD suboperation (the SUB group) with no do_osd_ops
	// case or Objecter builder; replication and recovery use their own
	// messages. Not a client primitive.
	daemonOnly
	// daemonOriginated: handled, but only the OSD's own Objecter sends it.
	daemonOriginated
	// deferred: a current server path with no Objecter or librados builder
	// and no consumer need here.
	deferred
	// legacyFormat: a current server path for a pre-OMAP (tmap) or
	// namespace-unaware format whose replacement is implemented.
	legacyFormat
	// cacheTierDependent: a librados primitive meaningful only with a
	// configured hit set; it needs a native cache-tier fixture first (#8).
	cacheTierDependent
)

// tentacleOpcodes is the closed CEPH_OSD_OP_* enum of the pinned source,
// Ceph v20.2.4 (commit 7f793731f1b39eb4f465e960113d2363c311b964),
// include/rados.h __CEPH_FORALL_OSD_OPS, one entry per name, with this
// repository's status on each entry's own line. Unsupported classes were read
// from osd/PrimaryLogPG.cc, osd/osd_types.cc, osdc/Objecter.h and librados/
// at the same tag; they are a source classification, not interoperability
// results, and their share is not a completion measure. The enum does not
// grow within Tentacle, so implementing an operation changes its one line.
var tentacleOpcodes = []struct {
	name   string
	code   uint16
	status opcodeStatus
}{
	{"READ", 0x1201, implemented},
	{"STAT", 0x1202, implemented},
	{"MAPEXT", 0x1203, implemented},
	{"CHECKSUM", 0x121f, implemented},
	{"MASKTRUNC", 0x1204, noServerPath}, // osd_types.cc mode switch only; no builder.
	{"SPARSE_READ", 0x1205, implemented},
	{"NOTIFY", 0x1206, implemented},
	{"NOTIFY_ACK", 0x1207, implemented},
	{"ASSERT_VER", 0x1208, implemented},
	{"LIST_WATCHERS", 0x1209, implemented},
	{"LIST_SNAPS", 0x120a, implemented},
	{"SYNC_READ", 0x120b, deferred}, // Handled with READ, which is implemented.
	{"WRITE", 0x2201, implemented},
	{"WRITEFULL", 0x2202, implemented},
	{"TRUNCATE", 0x2203, implemented},
	{"ZERO", 0x2204, implemented},
	{"DELETE", 0x2205, implemented},
	{"APPEND", 0x2206, implemented},
	{"STARTSYNC", 0x2207, deferred}, // A no-result nop write.
	{"SETTRUNC", 0x2208, noServerPath},
	{"TRIMTRUNC", 0x2209, deferred}, // Falls through into TRUNCATE.
	{"TMAPUP", 0x320a, legacyFormat},
	{"TMAPPUT", 0x220b, legacyFormat},
	{"TMAPGET", 0x120c, legacyFormat}, // EOPNOTSUPP on erasure-coded pools.
	{"CREATE", 0x220d, implemented},
	{"ROLLBACK", 0x220e, implemented},
	{"WATCH", 0x220f, implemented},
	{"OMAPGETKEYS", 0x1211, implemented},
	{"OMAPGETVALS", 0x1212, implemented},
	{"OMAPGETHEADER", 0x1213, implemented},
	{"OMAPGETVALSBYKEYS", 0x1214, implemented},
	{"OMAPSETVALS", 0x2215, implemented},
	{"OMAPSETHEADER", 0x2216, implemented},
	{"OMAPCLEAR", 0x2217, implemented},
	{"OMAPRMKEYS", 0x2218, implemented},
	{"OMAPRMKEYRANGE", 0x222c, implemented},
	{"OMAP_CMP", 0x1219, implemented},
	{"COPY_FROM", 0x221a, implemented},
	{"COPY_FROM2", 0x222d, implemented},
	{"UNDIRTY", 0x221c, implemented},
	{"ISDIRTY", 0x121d, implemented},
	{"COPY_GET", 0x121e, daemonOriginated}, // PrimaryLogPG copy/promote; no librados entry.
	{"CACHE_FLUSH", 0x821f, implemented},
	{"CACHE_EVICT", 0x8220, implemented},
	{"CACHE_TRY_FLUSH", 0x8221, implemented},
	{"TMAP2OMAP", 0x3222, legacyFormat},
	{"SETALLOCHINT", 0x2223, implemented},
	{"CACHE_PIN", 0x2224, implemented},
	{"CACHE_UNPIN", 0x2225, implemented},
	{"WRITESAME", 0x2226, implemented},
	{"CMPEXT", 0x1220, implemented},
	{"SET_REDIRECT", 0x2227, implemented},
	{"SET_CHUNK", 0x8228, implemented},
	{"TIER_PROMOTE", 0x2229, implemented},
	{"UNSET_MANIFEST", 0x222a, implemented},
	{"TIER_FLUSH", 0x822b, implemented},
	{"TIER_EVICT", 0x822c, implemented},
	{"GETXATTR", 0x1301, implemented},
	{"GETXATTRS", 0x1302, implemented},
	{"CMPXATTR", 0x1303, implemented},
	{"SETXATTR", 0x2301, implemented},
	{"SETXATTRS", 0x2302, noServerPath},
	{"RESETXATTRS", 0x2303, noServerPath}, // Objecter.h builder only.
	{"RMXATTR", 0x2304, implemented},
	{"PULL", 0x4001, daemonOnly},
	{"PUSH", 0x4002, daemonOnly},
	{"BALANCEREADS", 0x4003, daemonOnly},
	{"UNBALANCEREADS", 0x4004, daemonOnly},
	{"SCRUB", 0x4005, daemonOnly},
	{"SCRUB_RESERVE", 0x4006, daemonOnly},
	{"SCRUB_UNRESERVE", 0x4007, daemonOnly},
	{"SCRUB_MAP", 0x4009, daemonOnly},
	{"CALL", 0x1401, implemented},
	{"PGLS", 0x1501, legacyFormat}, // PGNLS is the namespace-aware form.
	{"PGLS_FILTER", 0x1502, legacyFormat},
	{"PG_HITSET_LS", 0x1503, cacheTierDependent},
	{"PG_HITSET_GET", 0x1504, cacheTierDependent},
	{"PGNLS", 0x1505, implemented},
	{"PGNLS_FILTER", 0x1506, implemented},
	{"SCRUBLS", 0x1507, implemented},
}

// TestOpcodeCatalogMatchesTheEncoder binds every pinned opcode's status to
// the encoder: SupportedOperation admits exactly the implemented entries, and
// no code outside the pinned enum.
func TestOpcodeCatalogMatchesTheEncoder(t *testing.T) {
	status := map[uint16]opcodeStatus{}
	names := map[string]bool{}
	for _, op := range tentacleOpcodes {
		if names[op.name] || status[op.code] != 0 {
			t.Fatalf("duplicate opcode %s %#x", op.name, op.code)
		}
		if op.status < implemented || op.status > cacheTierDependent {
			t.Fatalf("%s has no status", op.name)
		}
		names[op.name], status[op.code] = true, op.status
	}
	for code := 0; code <= 0xffff; code++ {
		want := status[uint16(code)] == implemented
		if got := SupportedOperation(uint16(code)); got != want {
			t.Errorf("SupportedOperation(%#x) = %v, catalog implemented = %v", code, got, want)
		}
	}
}
