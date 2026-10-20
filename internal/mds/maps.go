package mds

import (
	"bytes"

	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
	"github.com/JSYoo5B/ceph-msgr-go/internal/wire"
)

// Independently written from pinned Ceph v20.2.4 commit
// 7f793731f1b39eb4f465e960113d2363c311b964: messages/MFSMapUser.h,
// messages/MMDSMap.h, mds/{FSMapUser,MDSMap}.{h,cc}, include/CompatSet.h,
// mds/mdstypes.cc and include/cephfs/types.h. LGPL-2.1 sources; no code copied.

func frameFront(m msgr.MessageData, typ, version, compat uint16, limit uint32) ([]byte, error) {
	if limit == 0 {
		limit = wire.DefaultLimit
	}
	if uint64(len(m.Front))+uint64(len(m.Middle))+uint64(len(m.Data))+41 > uint64(limit) {
		return nil, wire.ErrLimit
	}
	if m.Type != typ || len(m.Middle) != 0 || len(m.Data) != 0 {
		return nil, msgr.ErrFrame
	}
	if m.Version != version || m.CompatVersion != compat {
		return nil, wire.ErrVersion
	}
	return bytes.Clone(m.Front), nil
}

// currentStruct deliberately rejects schema extensions until their ordering
// and semantics have been checked against a pinned current native producer.
func currentStruct(d *wire.Decoder, version, compat uint8) *wire.Decoder {
	v, c := d.U8(), d.U8()
	data := d.Bytes()
	body := wire.NewDecoderLimit(data, uint32(len(data)))
	body.Fail(d.Err())
	if v != version || c != compat {
		body.Fail(wire.ErrVersion)
	}
	return body
}

func decodeTime(d *wire.Decoder) Time { return Time{Seconds: d.U32(), Nanoseconds: d.U32()} }

func DecodeFSMapUser(m msgr.MessageData, limit uint32) (FSMapUser, error) {
	var result FSMapUser
	raw, err := frameFront(m, FSMapUserMessage, 1, 1, limit)
	if err != nil {
		return result, err
	}
	if limit == 0 {
		limit = wire.DefaultLimit
	}
	d := wire.NewDecoderLimit(raw, limit)
	result.Epoch = d.U32()
	result.RawMap = raw[len(raw)-d.Remaining():]
	body := currentStruct(d, 1, 1)
	innerEpoch := body.U32()
	result.LegacyClientFSCID = int32(body.U32())
	n := body.Count(14, MaxMapEntries)
	result.Filesystems = make([]Filesystem, n)
	seen := make(map[int32]struct{}, n)
	for i := range result.Filesystems {
		entry := currentStruct(body, 1, 1)
		fs := Filesystem{ID: int32(entry.U32()), Name: entry.String()}
		if _, exists := seen[fs.ID]; exists {
			entry.Fail(ErrMap)
		}
		seen[fs.ID] = struct{}{}
		body.Fail(entry.Done())
		result.Filesystems[i] = fs
	}
	if innerEpoch != result.Epoch {
		body.Fail(ErrMap)
	}
	d.Fail(body.Done())
	if err := d.Done(); err != nil {
		return FSMapUser{}, err
	}
	result.RawFront = raw
	return result, nil
}

// DecodeMDSMapMessage owns the full incoming front and nested raw map view.
// Epoch consistency is checked; session FSID matching and epoch policy belong
// to the receiving connection, not this standalone wire decoder.
func DecodeMDSMapMessage(m msgr.MessageData, limit uint32) (MDSMapMessage, error) {
	var result MDSMapMessage
	raw, err := frameFront(m, MDSMapMessageType, 2, 1, limit)
	if err != nil {
		return result, err
	}
	if limit == 0 {
		limit = wire.DefaultLimit
	}
	d := wire.NewDecoderLimit(raw, limit)
	copy(result.FSID[:], d.Raw(16))
	result.Epoch = d.U32()
	mapBytes := d.Bytes()
	result.MapFSName = d.String()
	if err := d.Done(); err != nil {
		return MDSMapMessage{}, err
	}
	result.Map, err = decodeMDSMap(mapBytes, limit)
	if err != nil {
		return MDSMapMessage{}, err
	}
	if result.Map.Epoch != result.Epoch {
		return MDSMapMessage{}, ErrMap
	}
	result.RawFront = raw
	return result, nil
}

// DecodeMDSMap owns one raw MDSMap blob. It does not sanitize pool IDs, infer
// rank availability, normalize compatibility masks or apply map transitions.
func DecodeMDSMap(data []byte, limit uint32) (MDSMap, error) {
	if limit == 0 {
		limit = wire.DefaultLimit
	}
	if uint64(len(data)) > uint64(limit) {
		return MDSMap{}, wire.ErrLimit
	}
	return decodeMDSMap(bytes.Clone(data), limit)
}

func decodeMDSMap(data []byte, limit uint32) (MDSMap, error) {
	var result MDSMap
	d := wire.NewDecoderLimit(data, limit)
	body := currentStruct(d, 5, 4)
	result.Epoch = body.U32()
	result.Flags = body.U32()
	result.LastFailure = body.U32()
	result.Root = int32(body.U32())
	result.SessionTimeout = body.U32()
	result.SessionAutoclose = body.U32()
	result.MaxFileSize = body.U64()
	result.MaxMDS = int32(body.U32())
	// Map key + current info envelope and all mandatory empty collections.
	// Refuse impossible daemon counts before allocating the decoded table.
	n := body.Count(128, MaxMapEntries)
	result.Daemons = make([]Daemon, n)
	var previousGID uint64
	for i := range result.Daemons {
		gid := body.U64()
		daemon := decodeDaemon(body)
		if daemon.GID != gid || i > 0 && gid <= previousGID {
			body.Fail(ErrMap)
		}
		previousGID = gid
		result.Daemons[i] = daemon
	}
	pools := body.Count(8, MaxMapEntries)
	result.DataPools = make([]int64, pools)
	for i := range result.DataPools {
		result.DataPools[i] = int64(body.U64())
	}
	result.CASPool = int64(body.U64())
	if body.U16() != 19 {
		body.Fail(wire.ErrVersion)
	}
	result.Compat = decodeCompat(body)
	result.MetadataPool = int64(body.U64())
	result.Created = decodeTime(body)
	result.Modified = decodeTime(body)
	result.TableServer = int32(body.U32())
	result.In = decodeRankSet(body)
	n = body.Count(8, MaxMapEntries)
	result.Incarnations = make([]RankIncarnation, n)
	for i := range result.Incarnations {
		entry := RankIncarnation{Rank: int32(body.U32()), Incarnation: int32(body.U32())}
		if i > 0 && entry.Rank <= result.Incarnations[i-1].Rank {
			body.Fail(ErrMap)
		}
		result.Incarnations[i] = entry
	}
	n = body.Count(12, MaxMapEntries)
	result.Up = make([]RankGID, n)
	for i := range result.Up {
		entry := RankGID{Rank: int32(body.U32()), GID: body.U64()}
		if i > 0 && entry.Rank <= result.Up[i-1].Rank {
			body.Fail(ErrMap)
		}
		result.Up[i] = entry
	}
	result.Failed = decodeRankSet(body)
	result.Stopped = decodeRankSet(body)
	result.LastFailureOSDEpoch = body.U32()
	result.EverAllowedFeatures = body.U8()
	result.ExplicitlyAllowedFeatures = body.U8()
	result.InlineDataEnabled = body.Bool()
	result.Enabled = body.Bool()
	result.Name = body.String()
	result.Damaged = decodeRankSet(body)
	result.Balancer = body.String()
	result.StandbyCountWanted = int32(body.U32())
	result.OldMaxMDS = int32(body.U32())
	result.RetiredMinimumClientRelease = body.U8()
	featureBytes := body.U32()
	if featureBytes > MaxClientFeatureBytes || featureBytes%8 != 0 {
		body.Fail(wire.ErrLimit)
	}
	result.RequiredClientFeatures = body.Raw(featureBytes)
	result.BalanceRankMask = body.String()
	result.MaxXattrSize = body.U64()
	result.QuiesceLeader = body.U64()
	n = body.Count(8, MaxMapEntries)
	result.QuiesceMembers = make([]uint64, n)
	seen := make(map[uint64]struct{}, n)
	for i := range result.QuiesceMembers {
		gid := body.U64()
		if _, exists := seen[gid]; exists {
			body.Fail(ErrMap)
		}
		seen[gid] = struct{}{}
		result.QuiesceMembers[i] = gid
	}
	d.Fail(body.Done())
	if err := d.Done(); err != nil {
		return MDSMap{}, err
	}
	result.Raw = data
	return result, nil
}

func decodeDaemon(d *wire.Decoder) Daemon {
	body := currentStruct(d, 10, 4)
	result := Daemon{GID: body.U64(), Name: body.String(), Rank: int32(body.U32()), Incarnation: int32(body.U32()), State: int32(body.U32()), StateSequence: body.U64()}
	result.Addresses = msgr.DecodeAddresses(body)
	result.LaggySince = decodeTime(body)
	result.StandbyForRank = int32(body.U32())
	result.StandbyForName = body.String()
	result.ExportTargets = decodeRankSet(body)
	result.MDSFeatures = body.U64()
	result.JoinFSCID = int32(body.U32())
	result.StandbyReplay = body.Bool()
	result.Flags = body.U64()
	result.Compat = decodeCompat(body)
	d.Fail(body.Done())
	return result
}

func decodeRankSet(d *wire.Decoder) []int32 {
	n := d.Count(4, MaxMapEntries)
	ranks := make([]int32, n)
	for i := range ranks {
		rank := int32(d.U32())
		if i > 0 && rank <= ranks[i-1] {
			d.Fail(ErrMap)
		}
		ranks[i] = rank
	}
	return ranks
}
func decodeCompat(d *wire.Decoder) CompatSet {
	return CompatSet{Compatible: decodeFeatureSet(d), ReadOnlyCompatible: decodeFeatureSet(d), Incompatible: decodeFeatureSet(d)}
}
func decodeFeatureSet(d *wire.Decoder) FeatureSet {
	result := FeatureSet{Mask: d.U64()}
	n := d.Count(12, MaxFeatureNames)
	result.Names = make([]NamedFeature, n)
	for i := range result.Names {
		entry := NamedFeature{ID: d.U64(), Name: d.String()}
		if i > 0 && entry.ID <= result.Names[i-1].ID {
			d.Fail(ErrMap)
		}
		result.Names[i] = entry
	}
	return result
}
