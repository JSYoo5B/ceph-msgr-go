package msgr

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// MonMgrReportMessage is sent by the MGR daemon role to its MON only
// (protocol stage 3): MMonMgrReport with header version 3 and compat 1
// (EncodeMonMgrReport), the Paxos service prefix (version 0,
// deprecated session MON -1, its TID 0), a health_check_map_t, the encoded
// ServiceMap bufferlist (empty when the report carries none), a map of
// string to ProgressEvent (v2+) and the reporting GID (v3+). The data
// segment carries the PGMapDigest. Wire authority, pinned Ceph v20.2.4
// 7f793731f1b39eb4f465e960113d2363c311b964:
// ceph/ceph/blob/v20.2.4/src/messages/MMonMgrReport.h,
// ceph/ceph/blob/v20.2.4/src/messages/PaxosServiceMessage.h,
// ceph/ceph/blob/v20.2.4/src/mon/health_check.h (health_check_t 2/1,
// health_check_map_t 1/1), ceph/ceph/blob/v20.2.4/src/include/health.h
// (severity as one byte), ceph/ceph/blob/v20.2.4/src/mon/mon_types.h
// (ProgressEvent 2/1) and ceph/ceph/blob/v20.2.4/src/mon/PGMap.cc
// (PGMapDigest 5/1).
//
// The receiving MON (ceph/ceph/blob/v20.2.4/src/mon/MgrStatMonitor.cc,
// preprocess_report and prepare_report) replies nothing, drops a report
// whose nonzero GID is not the MgrMap's active GID, and otherwise decodes
// the digest from the data segment, replaces its pending health checks
// with the report's, replaces its pending ServiceMap only with a nonempty
// one and replaces its progress events. That decode runs outside any
// exception handler, so a digest the MON cannot decode, an empty one
// included, can end the MON; PGMapDigest::decode also asserts a structure
// version of at least 4. EncodeMonMgrReport therefore refuses a digest
// DecodePGMapDigest does not read completely, and a ServiceMap that
// DecodeServiceMap does not read (the MON decodes the committed one only
// when it next loads its state, and a failure there loses the progress
// events stored after it).
//
// The native sender re-encodes the digest only toward a MON without
// SERVER_NAUTILUS and SERVER_MIMIC, which a Tentacle MON always has; this
// library negotiates both on its MON sessions and sends the digest bytes
// unchanged.
const MonMgrReportMessage uint16 = 0x706

// Health check severities (health_status_t).
const (
	HealthError uint8 = 0
	HealthWarn  uint8 = 1
	HealthOK    uint8 = 2
)

// HealthCheck is one health_check_map_t entry, keyed by Code. Detail is
// the ordered detail list.
type HealthCheck struct {
	Code     string
	Severity uint8
	Summary  string
	Detail   []string
	Count    int64
}

// ProgressEvent is one progress_events entry, keyed by ID.
type ProgressEvent struct {
	ID          string
	Message     string
	Progress    float32
	AddToStatus bool // add_to_ceph_s
}

// MonMgrReport is the content of one MMonMgrReport. ServiceMap is an
// encoded ServiceMap or empty; Digest is one encoded PGMapDigest.
type MonMgrReport struct {
	HealthChecks []HealthCheck
	ServiceMap   []byte
	Progress     []ProgressEvent
	GlobalID     uint64
	Digest       []byte
}

// ErrMgrReportContent refuses report content the receiving MON could not
// decode or that a native map could not hold: a duplicate health check code
// or progress event ID, an unknown severity, an empty or malformed digest
// or a malformed ServiceMap.
var ErrMgrReportContent = errors.New("ceph messenger: invalid MGR report content")

// EncodeMonMgrReport builds MMonMgrReport 3/1. Health checks and progress
// events are written in ascending key order, as their std::maps are; the
// digest goes into the data segment unchanged. The whole frame must fit
// limit (wire.ErrLimit).
func EncodeMonMgrReport(r MonMgrReport, limit uint32) (MessageData, error) {
	refuse := func(format string, args ...any) (MessageData, error) {
		return MessageData{}, fmt.Errorf("%w: %s", ErrMgrReportContent, fmt.Sprintf(format, args...))
	}
	checks := slices.Clone(r.HealthChecks)
	slices.SortFunc(checks, func(a, b HealthCheck) int { return strings.Compare(a.Code, b.Code) })
	for i, c := range checks {
		if i > 0 && checks[i-1].Code == c.Code {
			return refuse("duplicate health check %q", c.Code)
		}
		if c.Severity > HealthOK {
			return refuse("health check %q severity %d", c.Code, c.Severity)
		}
	}
	events := slices.Clone(r.Progress)
	slices.SortFunc(events, func(a, b ProgressEvent) int { return strings.Compare(a.ID, b.ID) })
	for i, e := range events {
		if i > 0 && events[i-1].ID == e.ID {
			return refuse("duplicate progress event %q", e.ID)
		}
	}
	if len(r.Digest) == 0 {
		return refuse("no PGMapDigest")
	}
	if uint64(len(r.Digest))+uint64(len(r.ServiceMap))+41 > uint64(limit) {
		return MessageData{}, wire.ErrLimit
	}
	if _, err := DecodePGMapDigest(r.Digest, limit); err != nil {
		return refuse("digest: %v", err)
	}
	if len(r.ServiceMap) != 0 {
		if _, err := DecodeServiceMap(MessageData{Type: ServiceMapMessage, Front: r.ServiceMap}, limit); err != nil {
			return refuse("service map: %v", err)
		}
	}
	e := wire.Encoder{}
	e.U64(0)      // version
	e.U16(0xffff) // deprecated_session_mon, -1
	e.U64(0)      // deprecated_session_mon_tid
	m := wire.Encoder{}
	m.U32(uint32(len(checks)))
	for _, c := range checks {
		m.String(c.Code)
		h := wire.Encoder{}
		h.U8(c.Severity)
		h.String(c.Summary)
		h.U32(uint32(len(c.Detail)))
		for _, d := range c.Detail {
			h.String(d)
		}
		h.U64(uint64(c.Count))
		m.Struct(2, 1, h.Data)
	}
	e.Struct(1, 1, m.Data)
	e.Bytes(r.ServiceMap)
	e.U32(uint32(len(events)))
	for _, p := range events {
		e.String(p.ID)
		v := wire.Encoder{}
		v.String(p.Message)
		v.U32(math.Float32bits(p.Progress))
		if p.AddToStatus {
			v.U8(1)
		} else {
			v.U8(0)
		}
		e.Struct(2, 1, v.Data)
	}
	e.U64(r.GlobalID)
	if uint64(len(e.Data))+uint64(len(r.Digest))+41 > uint64(limit) {
		return MessageData{}, wire.ErrLimit
	}
	return MessageData{Type: MonMgrReportMessage, Version: 3, CompatVersion: 1, Priority: 127, Front: e.Data, Data: bytes.Clone(r.Digest)}, nil
}

// PoolStat is pool_stat_t version 7, the version a Tentacle MGR encodes.
type PoolStat struct {
	Sum                    ObjectStatSum
	LogSize, OnDiskLogSize int64
	Up, Acting             int32
	Store                  StoreStatfs
	NumStoreStats          int32
}

// PGMapDigest is a decoded PGMapDigest (structure 5/1; version 4 lacks
// UnavailablePGs). Collections are in encoded order: the native
// unordered maps are written in hash order.
type PGMapDigest struct {
	StructVersion                             uint8
	NumPG, NumPGActive, NumPGUnknown, NumOSDs int64
	PoolSums                                  []PGMapDigestPool
	Sum                                       PoolStat
	OSDSum                                    OSDStat
	PGsByState                                []PGMapDigestState
	PGsByOSD                                  []PGMapDigestOSD
	PGsByPool                                 []PGMapDigestCount
	OSDLastSeq                                []uint64
	PoolDeltas                                []PGMapDigestDelta
	PoolDeltaStamps                           []PGMapDigestStamp
	SumDelta                                  PoolStat
	StampDelta                                UTime
	AvailableByRule                           []PGMapDigestCount
	PurgedSnaps                               []PGMapDigestSnaps
	OSDSumByClass                             []PGMapDigestClass
	UnavailablePGs                            []PGMapDigestUnavailable
}

// PGMapDigestPool is one pg_pool_sum entry.
type PGMapDigestPool struct {
	Pool int32
	Stat PoolStat
}

// PGMapDigestState is one num_pg_by_state entry.
type PGMapDigestState struct {
	State uint64
	Count int32
}

// PGMapDigestOSD is one num_pg_by_osd entry (pg_count).
type PGMapDigestOSD struct {
	OSD                          int32
	Acting, UpNotActing, Primary int32
}

// PGMapDigestCount is one num_pg_by_pool (pool, PGs) or
// avail_space_by_rule (rule, bytes) entry.
type PGMapDigestCount struct {
	Key, Value int64
}

// PGMapDigestDelta is one per_pool_sum_delta entry.
type PGMapDigestDelta struct {
	Pool  int64
	Stat  PoolStat
	Stamp UTime
}

// PGMapDigestStamp is one per_pool_sum_deltas_stamps entry.
type PGMapDigestStamp struct {
	Pool  int64
	Stamp UTime
}

// PGMapDigestSnaps is one purged_snaps entry.
type PGMapDigestSnaps struct {
	Pool      int64
	Intervals []SnapInterval
}

// PGMapDigestClass is one osd_sum_by_class entry.
type PGMapDigestClass struct {
	Class string
	Stat  OSDStat
}

// PGMapDigestUnavailable is one pool_pg_unavailable_map entry.
type PGMapDigestUnavailable struct {
	Pool uint64
	PGs  []PGID
}

func decodePoolStat(d *wire.Decoder) PoolStat {
	v, p := structure(d, 7)
	atLeast(d, "pool_stat_t", v, 7)
	s := PoolStat{Sum: decodeObjectStatCollection(p)}
	s.LogSize, s.OnDiskLogSize = int64(p.U64()), int64(p.U64())
	s.Up, s.Acting = int32(p.U32()), int32(p.U32())
	s.Store = decodeStoreStatfs(p)
	s.NumStoreStats = int32(p.U32())
	end(d, v, 7, p)
	return s
}

// DecodePGMapDigest reads one complete PGMapDigest, the whole of b, with
// every nested structure PGMapDigest::decode reads: pool_stat_t 7,
// osd_stat_t 14 and object_stat_sum_t 20 (pgstats.go), the version a
// Tentacle MGR encodes with CEPH_FEATURES_ALL. Structure version 4, the
// oldest PGMapDigest::decode accepts, and 5 are read; an older one is
// refused (the native decoder asserts on it), and a newer one with compat
// at most 5 has its appended tail skipped. No statistic is interpreted.
func DecodePGMapDigest(b []byte, limit uint32) (g PGMapDigest, err error) {
	defer wrapDecode("PG map digest", &err)
	d := wire.NewDecoderLimit(b, limit)
	v, p := structure(d, 5)
	atLeast(d, "PGMapDigest", v, 4)
	g.StructVersion = v
	g.NumPG, g.NumPGActive, g.NumPGUnknown, g.NumOSDs = int64(p.U64()), int64(p.U64()), int64(p.U64()), int64(p.U64())
	n := p.Count(4+6, maxPGStatEntries)
	for i := 0; i < n && p.Err() == nil; i++ {
		g.PoolSums = append(g.PoolSums, PGMapDigestPool{Pool: int32(p.U32()), Stat: decodePoolStat(p)})
	}
	g.Sum = decodePoolStat(p)
	g.OSDSum = decodeOSDStat(p)
	n = p.Count(12, maxPGStatEntries)
	for i := 0; i < n && p.Err() == nil; i++ {
		g.PGsByState = append(g.PGsByState, PGMapDigestState{State: p.U64(), Count: int32(p.U32())})
	}
	n = p.Count(16, maxPGStatEntries)
	for i := 0; i < n && p.Err() == nil; i++ {
		g.PGsByOSD = append(g.PGsByOSD, PGMapDigestOSD{OSD: int32(p.U32()), Acting: int32(p.U32()), UpNotActing: int32(p.U32()), Primary: int32(p.U32())})
	}
	n = p.Count(16, maxPGStatEntries)
	for i := 0; i < n && p.Err() == nil; i++ {
		g.PGsByPool = append(g.PGsByPool, PGMapDigestCount{Key: int64(p.U64()), Value: int64(p.U64())})
	}
	n = p.Count(8, maxPGStatEntries)
	for i := 0; i < n && p.Err() == nil; i++ {
		g.OSDLastSeq = append(g.OSDLastSeq, p.U64())
	}
	n = p.Count(8+6+8, maxPGStatEntries)
	for i := 0; i < n && p.Err() == nil; i++ {
		pool := int64(p.U64())
		g.PoolDeltas = append(g.PoolDeltas, PGMapDigestDelta{Pool: pool, Stat: decodePoolStat(p), Stamp: decodeUTime(p)})
	}
	n = p.Count(16, maxPGStatEntries)
	for i := 0; i < n && p.Err() == nil; i++ {
		g.PoolDeltaStamps = append(g.PoolDeltaStamps, PGMapDigestStamp{Pool: int64(p.U64()), Stamp: decodeUTime(p)})
	}
	g.SumDelta = decodePoolStat(p)
	g.StampDelta = decodeUTime(p)
	n = p.Count(12, maxPGStatEntries)
	for i := 0; i < n && p.Err() == nil; i++ {
		g.AvailableByRule = append(g.AvailableByRule, PGMapDigestCount{Key: int64(int32(p.U32())), Value: int64(p.U64())})
	}
	n = p.Count(12, maxPGStatEntries)
	for i := 0; i < n && p.Err() == nil; i++ {
		s := PGMapDigestSnaps{Pool: int64(p.U64())}
		count := p.Count(16, maxPGStatEntries)
		for j := 0; j < count && p.Err() == nil; j++ {
			s.Intervals = append(s.Intervals, SnapInterval{Start: p.U64(), Length: p.U64()})
		}
		g.PurgedSnaps = append(g.PurgedSnaps, s)
	}
	n = p.Count(4+6, maxPGStatEntries)
	for i := 0; i < n && p.Err() == nil; i++ {
		class := p.String()
		g.OSDSumByClass = append(g.OSDSumByClass, PGMapDigestClass{Class: class, Stat: decodeOSDStat(p)})
	}
	if v >= 5 {
		n = p.Count(12, maxPGStatEntries)
		for i := 0; i < n && p.Err() == nil; i++ {
			u := PGMapDigestUnavailable{Pool: p.U64()}
			count := p.Count(17, maxPGStatEntries)
			for j := 0; j < count && p.Err() == nil; j++ {
				u.PGs = append(u.PGs, decodePGID(p))
			}
			g.UnavailablePGs = append(g.UnavailablePGs, u)
		}
	}
	end(d, v, 5, p)
	if err := d.Done(); err != nil {
		return PGMapDigest{}, err
	}
	return g, nil
}
