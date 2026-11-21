package msgr

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// PGStatsMessage is received by the MGR daemon role only: MPGStats, which a
// stock OSD sends its MGR with every report. Header versions 1 and 2 are
// read (pool statistics from 2), with header compat at most 1;
// DecodePGStats.
const PGStatsMessage uint16 = 87

// MPGStats (ceph/ceph/blob/v20.2.4/src/messages/MPGStats.h): the Paxos
// service prefix, the FSID, the sender's osd_stat_t, a map of pg_t to
// pg_stat_t, the OSD's map epoch, an encoded zero utime_t and (v2+) a map of
// pool ID to store_statfs_t. The nested encoders are
// ceph/ceph/blob/v20.2.4/src/osd/osd_types.cc (osd_stat_t 14/2, pg_stat_t
// 30/22, object_stat_collection_t 2/2, object_stat_sum_t 20/14,
// objectstore_perf_stat_t 2/2, pg_shard_t 1/1), osd_types.h
// (store_statfs_t 1/1, pg_t) and ceph/ceph/blob/v20.2.4/src/common/histogram.cc
// (pow2_hist_t 1/1). Every field the pinned decoders read is returned; no
// statistic is computed, summed or interpreted. Native DaemonServer replies
// nothing to MPGStats.
//
// Structure versions: a Tentacle OSD sends osd_stat_t 14, pg_stat_t 30 and
// object_stat_sum_t 20. Those and newer versions whose compat allows them
// are read (a newer structure's appended tail is skipped); older ones,
// which only releases before Tentacle produce, are refused, except
// objectstore_perf_stat_t version 1, which a Tentacle OSD always sends:
// version 2 needs OS_PERF_STAT_NS, which Tentacle's CEPH_FEATURES_ALL does
// not contain. Its millisecond latencies are converted to nanoseconds as
// native decode does, so a version-1 value is a whole number of
// milliseconds. object_stat_sum_t is read only at version 20, the in-memory
// layout native little-endian decode copies; another version is refused.

// PGID is a pg_t: pool and placement seed.
type PGID struct {
	Pool uint64
	Seed uint32
}

// EVersion is an eversion_t: version and epoch.
type EVersion struct {
	Version uint64
	Epoch   uint32
}

// UTime is a utime_t: seconds and nanoseconds.
type UTime struct{ Seconds, Nanoseconds uint32 }

// PGShard is a pg_shard_t: OSD and shard (-1 for a replicated PG).
type PGShard struct {
	OSD   int32
	Shard int8
}

// ObjectStatSum is object_stat_sum_t version 20, in its field order.
type ObjectStatSum struct {
	NumBytes, NumObjects, NumObjectClones, NumObjectCopies                        int64
	NumObjectsMissingOnPrimary, NumObjectsDegraded, NumObjectsUnfound             int64
	NumRead, NumReadKB, NumWrite, NumWriteKB, NumScrubErrors                      int64
	NumObjectsRecovered, NumBytesRecovered, NumKeysRecovered                      int64
	NumShallowScrubErrors, NumDeepScrubErrors, NumObjectsDirty, NumWhiteouts      int64
	NumObjectsOmap, NumObjectsHitSetArchive, NumObjectsMisplaced                  int64
	NumBytesHitSetArchive, NumFlush, NumFlushKB, NumEvict, NumEvictKB, NumPromote int64
	NumFlushModeHigh, NumFlushModeLow, NumEvictModeSome, NumEvictModeFull         int32
	NumObjectsPinned, NumObjectsMissing, NumLegacySnapsets, NumLargeOmapObjects   int64
	NumObjectsManifest, NumOmapBytes, NumOmapKeys, NumObjectsRepaired             int64
}

// objectStatSumSize is sizeof(object_stat_sum_t): 36 int64 and 4 int32.
const objectStatSumSize = 36*8 + 4*4

// PGLocationCount is one object_location_counts entry: a set of shards
// and its count.
type PGLocationCount struct {
	Shards []PGShard
	Count  int32
}

// SnapInterval is one interval_set<snapid_t> entry: start and length.
type SnapInterval struct{ Start, Length uint64 }

// PGStat is pg_stat_t version 30. State is the 64-bit state (the two
// encoded halves joined). The scrub schedule fields are pg_scrubbing_status_t's.
type PGStat struct {
	StructVersion                  uint8
	Version                        EVersion
	ReportedSeq                    uint64
	ReportedEpoch                  uint32
	State                          uint64
	LogStart, OnDiskLogStart       EVersion
	Created, LastEpochClean        uint32
	Parent                         PGID
	ParentSplitBits                uint32
	LastScrub                      EVersion
	LastScrubStamp                 UTime
	Sum                            ObjectStatSum
	LogSize, OnDiskLogSize         int64
	Up, Acting                     []int32
	LastFresh, LastChange          UTime
	LastActive, LastClean          UTime
	LastUnstale                    UTime
	MappingEpoch                   uint32
	LastDeepScrub                  EVersion
	LastDeepScrubStamp             UTime
	StatsInvalid                   bool
	LastCleanScrubStamp            UTime
	LastBecameActive               UTime
	DirtyStatsInvalid              bool
	UpPrimary, ActingPrimary       int32
	OmapStatsInvalid               bool
	HitSetStatsInvalid             bool
	BlockedBy                      []int32
	LastUndegraded, LastFullsized  UTime
	HitSetBytesStatsInvalid        bool
	LastPeered, LastBecamePeered   UTime
	PinStatsInvalid                bool
	SnapTrimQueueLen               uint32
	PurgedSnaps                    []SnapInterval
	ManifestStatsInvalid           bool
	AvailNoMissing                 []PGShard
	ObjectLocationCounts           []PGLocationCount
	LastScrubDuration              int32
	ScrubScheduledAt               UTime
	ScrubDurationSeconds           int32
	ScrubSchedStatus               uint16
	ScrubActive, ScrubDeep         bool
	ScrubPeriodic                  bool
	ObjectsScrubbed                int64
	ScrubDuration                  float64
	ObjectsTrimmed                 int64
	SnapTrimDuration               float64
	LogDupsSize                    int64
	ScrubOSDToRespond              uint16
	ScrubOrdinalOfRequestedReplica uint8
	ScrubNumToReserve              uint8
}

// StoreStatfs is store_statfs_t.
type StoreStatfs struct {
	Total, Available, InternallyReserved uint64
	Allocated, DataStored                int64
	DataCompressed                       int64
	DataCompressedAllocated              int64
	DataCompressedOriginal               int64
	OmapAllocated, InternalMetadata      int64
}

// OSDAlerts is one os_alerts entry: an OSD-local index and its alert map.
type OSDAlerts struct {
	Index  int32
	Alerts map[string]string
}

// OSDPingTime is one hb_pingtime entry, in microseconds as encoded.
type OSDPingTime struct {
	OSD                           int32
	LastUpdate                    uint32
	BackPing, BackMin, BackMax    [3]uint32
	BackLast                      uint32
	FrontPing, FrontMin, FrontMax [3]uint32
	FrontLast                     uint32
}

// OSDStat is osd_stat_t version 14. KB, KBUsed, KBAvail, KBUsedData,
// KBUsedOmap and KBUsedMeta are the encoded compatibility fields; Statfs
// is the authoritative store_statfs_t. NumHBOut is the encoded legacy
// vector (always empty from Tentacle).
type OSDStat struct {
	StructVersion                      uint8
	KB, KBUsed, KBAvail                int64
	SnapTrimQueueLen, NumSnapTrimming  int32
	HBPeers                            []int32
	NumHBOut                           []int32
	OpQueueAgeHist                     []int32
	PerfStatVersion                    uint8
	CommitLatencyNS, ApplyLatencyNS    uint64
	UpFrom                             uint32
	Seq                                uint64
	NumPGs                             uint32
	KBUsedData, KBUsedOmap, KBUsedMeta int64
	Statfs                             StoreStatfs
	Alerts                             []OSDAlerts
	NumShardsRepaired                  uint64
	NumOSDs, NumPerPoolOSDs            uint32
	NumPerPoolOmapOSDs                 uint32
	PingTimes                          []OSDPingTime
}

// PGStatEntry is one pg_stat map entry.
type PGStatEntry struct {
	PG   PGID
	Stat PGStat
}

// PoolStatfs is one pool_stat map entry.
type PoolStatfs struct {
	Pool   int64
	Statfs StoreStatfs
}

// PGStats is a received MPGStats. PGs and PoolStats are in the encoded
// (ascending key) order.
type PGStats struct {
	Version            uint16
	PaxosVersion       uint64
	PaxosSessionMon    int16
	PaxosSessionMonTID uint64
	FSID               [16]byte
	OSD                OSDStat
	PGs                []PGStatEntry
	Epoch              uint32
	Stamp              UTime
	PoolStats          []PoolStatfs
}

const maxPGStatEntries = 1 << 20

func decodePGID(d *wire.Decoder) PGID {
	if v := d.U8(); d.Err() == nil && v != 1 {
		d.Fail(fmt.Errorf("%w: pg_t version %d", wire.ErrVersion, v))
	}
	pg := PGID{Pool: d.U64(), Seed: d.U32()}
	d.U32() // the removed "preferred" field, always -1
	return pg
}

func decodeEVersion(d *wire.Decoder) EVersion { return EVersion{Version: d.U64(), Epoch: d.U32()} }
func decodeUTime(d *wire.Decoder) UTime       { return UTime{Seconds: d.U32(), Nanoseconds: d.U32()} }

func decodeInt32s(d *wire.Decoder) []int32 {
	n := d.Count(4, maxPGStatEntries)
	out := make([]int32, 0, n)
	for i := 0; i < n && d.Err() == nil; i++ {
		out = append(out, int32(d.U32()))
	}
	return out
}

func decodePGShard(d *wire.Decoder) PGShard {
	v, p := structure(d, 1)
	s := PGShard{OSD: int32(p.U32()), Shard: int8(p.U8())}
	end(d, v, 1, p)
	return s
}

func decodeStoreStatfs(d *wire.Decoder) StoreStatfs {
	v, p := structure(d, 1)
	s := StoreStatfs{Total: p.U64(), Available: p.U64(), InternallyReserved: p.U64()}
	for _, f := range []*int64{&s.Allocated, &s.DataStored, &s.DataCompressed, &s.DataCompressedAllocated, &s.DataCompressedOriginal, &s.OmapAllocated, &s.InternalMetadata} {
		*f = int64(p.U64())
	}
	end(d, v, 1, p)
	return s
}

// atLeast refuses a structure older than the Tentacle version.
func atLeast(d *wire.Decoder, what string, version, minimum uint8) {
	if d.Err() == nil && version < minimum {
		d.Fail(fmt.Errorf("%w: %s version %d is older than Tentacle's %d", wire.ErrVersion, what, version, minimum))
	}
}

func decodeObjectStatSum(d *wire.Decoder) ObjectStatSum {
	v, p := structure(d, 20)
	if d.Err() == nil && v != 20 {
		d.Fail(fmt.Errorf("%w: object_stat_sum_t version %d (only 20 is read)", wire.ErrVersion, v))
	}
	var s ObjectStatSum
	raw := p.Raw(objectStatSumSize)
	if raw != nil {
		i64 := func(i int) int64 { return int64(binary.LittleEndian.Uint64(raw[i*8:])) }
		fields := []*int64{&s.NumBytes, &s.NumObjects, &s.NumObjectClones, &s.NumObjectCopies,
			&s.NumObjectsMissingOnPrimary, &s.NumObjectsDegraded, &s.NumObjectsUnfound,
			&s.NumRead, &s.NumReadKB, &s.NumWrite, &s.NumWriteKB, &s.NumScrubErrors,
			&s.NumObjectsRecovered, &s.NumBytesRecovered, &s.NumKeysRecovered,
			&s.NumShallowScrubErrors, &s.NumDeepScrubErrors, &s.NumObjectsDirty, &s.NumWhiteouts,
			&s.NumObjectsOmap, &s.NumObjectsHitSetArchive, &s.NumObjectsMisplaced,
			&s.NumBytesHitSetArchive, &s.NumFlush, &s.NumFlushKB, &s.NumEvict, &s.NumEvictKB, &s.NumPromote}
		for i, f := range fields {
			*f = i64(i)
		}
		off := len(fields) * 8
		for i, f := range []*int32{&s.NumFlushModeHigh, &s.NumFlushModeLow, &s.NumEvictModeSome, &s.NumEvictModeFull} {
			*f = int32(binary.LittleEndian.Uint32(raw[off+4*i:]))
		}
		off += 16
		for i, f := range []*int64{&s.NumObjectsPinned, &s.NumObjectsMissing, &s.NumLegacySnapsets, &s.NumLargeOmapObjects,
			&s.NumObjectsManifest, &s.NumOmapBytes, &s.NumOmapKeys, &s.NumObjectsRepaired} {
			*f = int64(binary.LittleEndian.Uint64(raw[off+8*i:]))
		}
	}
	end(d, v, 20, p)
	return s
}

func decodeObjectStatCollection(d *wire.Decoder) ObjectStatSum {
	v, p := structure(d, 2)
	atLeast(d, "object_stat_collection_t", v, 2)
	sum := decodeObjectStatSum(p)
	// The legacy per-category map<string, object_stat_sum_t>: Tentacle
	// encodes it empty; a nonempty one is read and dropped, as native does.
	n := p.Count(10, maxMgrMapEntries)
	for i := 0; i < n && p.Err() == nil; i++ {
		_ = p.String()
		decodeObjectStatSum(p)
	}
	end(d, v, 2, p)
	return sum
}

func f64(d *wire.Decoder) float64 { return math.Float64frombits(d.U64()) }

func decodePGStat(d *wire.Decoder) PGStat {
	v, p := structure(d, 30)
	atLeast(d, "pg_stat_t", v, 30)
	s := PGStat{StructVersion: v}
	s.Version = decodeEVersion(p)
	s.ReportedSeq, s.ReportedEpoch = p.U64(), p.U32()
	low := p.U32()
	s.LogStart, s.OnDiskLogStart = decodeEVersion(p), decodeEVersion(p)
	s.Created, s.LastEpochClean = p.U32(), p.U32()
	s.Parent = decodePGID(p)
	s.ParentSplitBits = p.U32()
	s.LastScrub, s.LastScrubStamp = decodeEVersion(p), decodeUTime(p)
	s.Sum = decodeObjectStatCollection(p)
	s.LogSize, s.OnDiskLogSize = int64(p.U64()), int64(p.U64())
	s.Up, s.Acting = decodeInt32s(p), decodeInt32s(p)
	s.LastFresh, s.LastChange, s.LastActive, s.LastClean, s.LastUnstale = decodeUTime(p), decodeUTime(p), decodeUTime(p), decodeUTime(p), decodeUTime(p)
	s.MappingEpoch = p.U32()
	s.LastDeepScrub, s.LastDeepScrubStamp = decodeEVersion(p), decodeUTime(p)
	s.StatsInvalid = p.Bool()
	s.LastCleanScrubStamp, s.LastBecameActive = decodeUTime(p), decodeUTime(p)
	s.DirtyStatsInvalid = p.Bool()
	s.UpPrimary, s.ActingPrimary = int32(p.U32()), int32(p.U32())
	s.OmapStatsInvalid, s.HitSetStatsInvalid = p.Bool(), p.Bool()
	s.BlockedBy = decodeInt32s(p)
	s.LastUndegraded, s.LastFullsized = decodeUTime(p), decodeUTime(p)
	s.HitSetBytesStatsInvalid = p.Bool()
	s.LastPeered, s.LastBecamePeered = decodeUTime(p), decodeUTime(p)
	s.PinStatsInvalid = p.Bool()
	s.SnapTrimQueueLen = p.U32()
	s.State = uint64(low) | uint64(p.U32())<<32
	n := p.Count(16, maxPGStatEntries)
	for i := 0; i < n && p.Err() == nil; i++ {
		s.PurgedSnaps = append(s.PurgedSnaps, SnapInterval{Start: p.U64(), Length: p.U64()})
	}
	s.ManifestStatsInvalid = p.Bool()
	n = p.Count(11, maxPGStatEntries)
	for i := 0; i < n && p.Err() == nil; i++ {
		s.AvailNoMissing = append(s.AvailNoMissing, decodePGShard(p))
	}
	n = p.Count(8, maxPGStatEntries)
	for i := 0; i < n && p.Err() == nil; i++ {
		var c PGLocationCount
		shards := p.Count(11, maxPGStatEntries)
		for j := 0; j < shards && p.Err() == nil; j++ {
			c.Shards = append(c.Shards, decodePGShard(p))
		}
		c.Count = int32(p.U32())
		s.ObjectLocationCounts = append(s.ObjectLocationCounts, c)
	}
	s.LastScrubDuration = int32(p.U32())
	s.ScrubScheduledAt = decodeUTime(p)
	s.ScrubDurationSeconds = int32(p.U32())
	s.ScrubSchedStatus = p.U16()
	s.ScrubActive, s.ScrubDeep, s.ScrubPeriodic = p.Bool(), p.Bool(), p.Bool()
	s.ObjectsScrubbed = int64(p.U64())
	s.ScrubDuration = f64(p)
	s.ObjectsTrimmed = int64(p.U64())
	s.SnapTrimDuration = f64(p)
	s.LogDupsSize = int64(p.U64())
	s.ScrubOSDToRespond = p.U16()
	s.ScrubOrdinalOfRequestedReplica, s.ScrubNumToReserve = p.U8(), p.U8()
	end(d, v, 30, p)
	return s
}

func decodeOSDStat(d *wire.Decoder) OSDStat {
	v, p := structure(d, 14)
	atLeast(d, "osd_stat_t", v, 14)
	s := OSDStat{StructVersion: v}
	s.KB, s.KBUsed, s.KBAvail = int64(p.U64()), int64(p.U64()), int64(p.U64())
	s.SnapTrimQueueLen, s.NumSnapTrimming = int32(p.U32()), int32(p.U32())
	s.HBPeers, s.NumHBOut = decodeInt32s(p), decodeInt32s(p)
	hv, hist := structure(p, 1)
	s.OpQueueAgeHist = decodeInt32s(hist)
	end(p, hv, 1, hist)
	pv, perf := structure(p, 2)
	s.PerfStatVersion = pv
	if pv >= 2 {
		s.CommitLatencyNS, s.ApplyLatencyNS = perf.U64(), perf.U64()
	} else {
		s.CommitLatencyNS, s.ApplyLatencyNS = uint64(perf.U32())*1_000_000, uint64(perf.U32())*1_000_000
	}
	end(p, pv, 2, perf)
	s.UpFrom, s.Seq, s.NumPGs = p.U32(), p.U64(), p.U32()
	s.KBUsedData, s.KBUsedOmap, s.KBUsedMeta = int64(p.U64()), int64(p.U64()), int64(p.U64())
	s.Statfs = decodeStoreStatfs(p)
	n := p.Count(8, maxMgrMapEntries)
	for i := 0; i < n && p.Err() == nil; i++ {
		s.Alerts = append(s.Alerts, OSDAlerts{Index: int32(p.U32()), Alerts: decodeSortedStringMap(p)})
	}
	s.NumShardsRepaired = p.U64()
	s.NumOSDs, s.NumPerPoolOSDs, s.NumPerPoolOmapOSDs = p.U32(), p.U32(), p.U32()
	count := int32(p.U32())
	if p.Err() == nil && (count < 0 || int64(count)*88 > int64(p.Remaining())) {
		p.Fail(wire.ErrLimit)
	}
	for i := int32(0); i < count && p.Err() == nil; i++ {
		t := OSDPingTime{OSD: int32(p.U32()), LastUpdate: p.U32()}
		for _, a := range []*[3]uint32{&t.BackPing, &t.BackMin, &t.BackMax} {
			a[0], a[1], a[2] = p.U32(), p.U32(), p.U32()
		}
		t.BackLast = p.U32()
		for _, a := range []*[3]uint32{&t.FrontPing, &t.FrontMin, &t.FrontMax} {
			a[0], a[1], a[2] = p.U32(), p.U32(), p.U32()
		}
		t.FrontLast = p.U32()
		s.PingTimes = append(s.PingTimes, t)
	}
	end(d, v, 14, p)
	return s
}

// DecodePGStats reads MPGStats header versions 1 and 2. The data segment
// carries nothing and must be empty. A legacy OSD's zero num_osds is
// returned as encoded (native decode raises it to 1).
func DecodePGStats(m MessageData, limit uint32) (s PGStats, err error) {
	defer wrapDecode("PG statistics", &err)
	d, err := frontDecoder(m, PGStatsMessage, 1, 2, limit)
	if err != nil {
		return s, err
	}
	if len(m.Data) != 0 {
		return s, ErrFrame
	}
	s.Version = m.Version
	s.PaxosVersion, s.PaxosSessionMon, s.PaxosSessionMonTID = d.U64(), int16(d.U16()), d.U64()
	copy(s.FSID[:], d.Raw(16))
	s.OSD = decodeOSDStat(d)
	n := d.Count(17+6, maxPGStatEntries)
	s.PGs = make([]PGStatEntry, 0, n)
	for i := 0; i < n && d.Err() == nil; i++ {
		pg := decodePGID(d)
		s.PGs = append(s.PGs, PGStatEntry{PG: pg, Stat: decodePGStat(d)})
	}
	s.Epoch = d.U32()
	s.Stamp = decodeUTime(d)
	if m.Version >= 2 {
		n := d.Count(8+6, maxPGStatEntries)
		for i := 0; i < n && d.Err() == nil; i++ {
			s.PoolStats = append(s.PoolStats, PoolStatfs{Pool: int64(d.U64()), Statfs: decodeStoreStatfs(d)})
		}
	}
	if err := d.Done(); err != nil {
		return PGStats{}, err
	}
	return s, nil
}
