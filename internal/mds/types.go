// Package mds implements the bounded current Tentacle MDS wire codecs: FSMapUser
// and MDSMap, MClientSession and reclaim, MClientRequest/Reply/Forward,
// MClientReconnect and the capability, lease, snapshot and quota messages. It
// does not select a filesystem or rank, authenticate an MDS, or implement
// CephFS I/O.
//
// Each codec accepts exactly its current pinned form (for example MMDSMap
// header 2/compat 1, inner MDSMap struct 5/compat 4, mds_info_t struct
// 10/compat 4); newer compatible structs and trailing bytes are refused rather
// than skipped as native DECODE_START would. FSMapUser carries no cluster FSID
// and MMDSMap no FSCID; its map_fs_name does not bind a filesystem, so callers
// supply the FSCID. CompatSet masks and names are kept as encoded, without the
// native in-memory repairs, and the required-client feature_bitset_t is kept as
// raw bytes: preserving a requirement does not mean the client implements it.
// Collections are capped (MaxMapEntries, MaxFeatureNames, MaxClientFeatureBytes)
// and counts are checked against minimum wire size before allocation. The
// current MMDSMap encoding is selected by a peer with PGID64, MDSENC, MSG_ADDR2
// and SERVER_NAUTILUS (messages/MMDSMap.h encode_payload).
//
// The codecs are independently written from LGPL-2.1 Ceph v20.2.4 sources;
// no code is copied. The native vectors under testdata were produced by fresh
// native objects through canonical Message::encode, whose zero-compat
// normalization is why FSMapUser compat 0 is refused here.
package mds

import (
	"errors"
	"github.com/JSYoo5B/ceph-msgr-go/internal/msgr"
)

const (
	// FSMapUserMessage is received in the exact current header form 1/1.
	FSMapUserMessage uint16 = 103
	// MDSMapMessageType is received in the exact current header form 2/1.
	MDSMapMessageType     uint16 = 21
	MaxMapEntries         uint32 = 4096
	MaxFeatureNames       uint32 = 1024
	MaxClientFeatureBytes uint32 = 64 << 10
)

var ErrMap = errors.New("ceph mds: invalid current map wire data")

const (
	StateDNE           int32 = 0
	StateStopped       int32 = -1
	StateBoot          int32 = -4
	StateStandby       int32 = -5
	StateCreating      int32 = -6
	StateStarting      int32 = -7
	StateStandbyReplay int32 = -8
	StateNull          int32 = -10
	StateReplay        int32 = 8
	StateResolve       int32 = 9
	StateReconnect     int32 = 10
	StateRejoin        int32 = 11
	StateClientReplay  int32 = 12
	StateActive        int32 = 13
	StateStopping      int32 = 14
	StateDamaged       int32 = 15
)

const (
	MapNotJoinable               uint32 = 1 << 0
	MapAllowSnaps                uint32 = 1 << 1
	MapAllowMultiMDSSnaps        uint32 = 1 << 4
	MapAllowStandbyReplay        uint32 = 1 << 5
	MapRefuseClientSession       uint32 = 1 << 6
	MapRefuseStandbyForAnotherFS uint32 = 1 << 7
	MapBalanceAutomate           uint32 = 1 << 8
	DaemonFrozen                 uint64 = 1
)

type Time struct{ Seconds, Nanoseconds uint32 }
type Filesystem struct {
	ID   int32
	Name string
}

// FSMapUser has no cluster FSID on its wire. IDs are signed fs_cluster_id_t,
// independent of the UUID carried by an MDSMapMessage. Filesystems keep order.
type FSMapUser struct {
	Epoch             uint32
	LegacyClientFSCID int32
	Filesystems       []Filesystem
	RawFront, RawMap  []byte
}

type NamedFeature struct {
	ID   uint64
	Name string
}

// FeatureSet retains the encoded mask and names, without native decoder
// compatibility fixups or assuming any unknown feature is implemented.
type FeatureSet struct {
	Mask  uint64
	Names []NamedFeature
}
type CompatSet struct{ Compatible, ReadOnlyCompatible, Incompatible FeatureSet }

type Daemon struct {
	GID                      uint64
	Name                     string
	Rank, Incarnation, State int32
	StateSequence            uint64
	Addresses                []msgr.Address
	LaggySince               Time
	StandbyForRank           int32
	StandbyForName           string
	ExportTargets            []int32
	MDSFeatures              uint64
	JoinFSCID                int32
	StandbyReplay            bool
	Flags                    uint64
	Compat                   CompatSet
}

type RankIncarnation struct{ Rank, Incarnation int32 }
type RankGID struct {
	Rank int32
	GID  uint64
}

// MDSMap retains all fields in the current v5/compat4, extension19 envelope.
// Unknown flags, daemon states and feature bits remain raw observations.
// RequiredClientFeatures is the little-endian feature_bitset_t byte vector;
// this is a separate contract from Messenger negotiation and CompatSet.
type MDSMap struct {
	Epoch, Flags, LastFailure                      uint32
	Root                                           int32
	SessionTimeout, SessionAutoclose               uint32
	MaxFileSize                                    uint64
	MaxMDS                                         int32
	Daemons                                        []Daemon
	DataPools                                      []int64
	CASPool                                        int64
	Compat                                         CompatSet
	MetadataPool                                   int64
	Created, Modified                              Time
	TableServer                                    int32
	In                                             []int32
	Incarnations                                   []RankIncarnation
	Up                                             []RankGID
	Failed, Stopped                                []int32
	LastFailureOSDEpoch                            uint32
	EverAllowedFeatures, ExplicitlyAllowedFeatures uint8
	InlineDataEnabled, Enabled                     bool
	Name                                           string
	Damaged                                        []int32
	Balancer                                       string
	StandbyCountWanted, OldMaxMDS                  int32
	RetiredMinimumClientRelease                    uint8
	RequiredClientFeatures                         []byte
	BalanceRankMask                                string
	MaxXattrSize                                   uint64
	QuiesceLeader                                  uint64
	QuiesceMembers                                 []uint64
	Raw                                            []byte
}

type MDSMapMessage struct {
	FSID      [16]byte
	Epoch     uint32
	Map       MDSMap
	MapFSName string
	RawFront  []byte
}
