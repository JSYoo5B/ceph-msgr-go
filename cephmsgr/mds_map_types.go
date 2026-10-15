package cephmsgr

import (
	"fmt"
	"net/netip"

	"github.com/jsyoo5b/ceph-msgr-go/internal/mds"
)

// FilesystemInfo preserves one filesystem ID and name from FSMapUser, in
// server order. The signed ID is independent of the cluster UUID.
type FilesystemInfo struct {
	ID   int32
	Name string
}

// FSMapUserUpdate is a decoded MON filesystem directory observation. Its wire
// format carries no cluster UUID. RawFront and RawMap retain the complete
// message front and nested map encoding; treat their bytes as read-only.
type FSMapUserUpdate struct {
	Epoch             uint32
	LegacyClientFSCID int32
	Filesystems       []FilesystemInfo
	// SourceGeneration identifies the local delivery session, not a native
	// Ceph map generation or proof a notification followed a new subscription.
	// Late same-topic frames may satisfy a later watch's StartEpoch.
	SourceGeneration uint64
	RawFront, RawMap []byte
}

// MDSMapUpdate preserves a decoded MON MDS map observation. It does not select
// an MDS, establish an MDS session, or implement CephFS operations.
type MDSMapUpdate struct {
	FSID string
	// FilesystemID is the locally requested subscription binding. It is not
	// encoded in the MDSMap message; MapFSName is the received wire name.
	FilesystemID int32
	Epoch        uint32
	MapFSName    string
	Map          MDSMapMetadata
	// SourceGeneration identifies the local delivery session, not a native
	// Ceph map generation, MDS session recovery generation, or proof that a
	// notification followed a new subscription.
	SourceGeneration uint64
	// RawFront and RawMap are retained complete wire encodings. Treat their
	// bytes, and map slice fields backed by them, as read-only.
	RawFront, RawMap []byte
}

// MDSMapTime is the wire ceph_timespec pair, without host-time conversion.
type MDSMapTime struct{ Seconds, Nanoseconds uint32 }

// MDSMapAddress preserves entity_addr_t type, nonce, endpoint and IPv6
// flow/scope metadata, including an unspecified address.
type MDSMapAddress struct {
	Type, Nonce       uint32
	Endpoint          netip.AddrPort
	FlowInfo, ScopeID uint32
}

// MDSMapNamedFeature preserves an encoded feature ID and its wire name.
type MDSMapNamedFeature struct {
	ID   uint64
	Name string
}

// MDSMapFeatureSet preserves the encoded mask and names without compatibility
// fixups. Observing a feature does not imply client support for it.
type MDSMapFeatureSet struct {
	Mask  uint64
	Names []MDSMapNamedFeature
}

// MDSMapCompatSet contains the map or daemon's raw compatibility requirements.
// These are independent of Messenger negotiation and do not advertise support.
type MDSMapCompatSet struct {
	Compatible, ReadOnlyCompatible, Incompatible MDSMapFeatureSet
}

// MDSMapDaemon preserves one daemon record, including unknown states, flags
// and feature bits. Its presence does not establish reachability or readiness.
type MDSMapDaemon struct {
	GID                      uint64
	Name                     string
	Rank, Incarnation, State int32
	StateSequence            uint64
	Addresses                []MDSMapAddress
	LaggySince               MDSMapTime
	StandbyForRank           int32
	StandbyForName           string
	ExportTargets            []int32
	MDSFeatures              uint64
	JoinFSCID                int32
	StandbyReplay            bool
	Flags                    uint64
	Compat                   MDSMapCompatSet
}

// MDSMapRankIncarnation preserves one rank's wire incarnation.
type MDSMapRankIncarnation struct{ Rank, Incarnation int32 }

// MDSMapRankGID preserves one rank-to-daemon assignment.
type MDSMapRankGID struct {
	Rank int32
	GID  uint64
}

// MDSMapMetadata preserves the current Tentacle map fields without applying
// transitions or interpreting availability. Unknown flags, states and feature
// bytes remain raw observations. RequiredClientFeatures is the little-endian
// feature_bitset_t byte vector; it neither advertises implemented CephFS
// features nor replaces the distinct Compat and Messenger feature contracts.
type MDSMapMetadata struct {
	Epoch, Flags, LastFailure                      uint32
	Root                                           int32
	SessionTimeout, SessionAutoclose               uint32
	MaxFileSize                                    uint64
	MaxMDS                                         int32
	Daemons                                        []MDSMapDaemon
	DataPools                                      []int64
	CASPool                                        int64
	Compat                                         MDSMapCompatSet
	MetadataPool                                   int64
	Created, Modified                              MDSMapTime
	TableServer                                    int32
	In                                             []int32
	Incarnations                                   []MDSMapRankIncarnation
	Up                                             []MDSMapRankGID
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
}

func publicFSMapUser(m mds.FSMapUser, generation uint64) FSMapUserUpdate {
	result := FSMapUserUpdate{
		Epoch: m.Epoch, LegacyClientFSCID: m.LegacyClientFSCID,
		SourceGeneration: generation, RawFront: m.RawFront, RawMap: m.RawMap,
	}
	if m.Filesystems != nil {
		result.Filesystems = make([]FilesystemInfo, len(m.Filesystems))
		for i, fs := range m.Filesystems {
			result.Filesystems[i] = FilesystemInfo{ID: fs.ID, Name: fs.Name}
		}
	}
	return result
}

func formatMDSMapFSID(fsid [16]byte) string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", fsid[:4], fsid[4:6], fsid[6:8], fsid[8:10], fsid[10:])
}

func publicMDSMap(m mds.MDSMapMessage, filesystemID int32, generation uint64) MDSMapUpdate {
	w := m.Map
	result := MDSMapUpdate{
		FSID: formatMDSMapFSID(m.FSID), FilesystemID: filesystemID,
		Epoch: m.Epoch, MapFSName: m.MapFSName, SourceGeneration: generation,
		RawFront: m.RawFront, RawMap: w.Raw,
		Map: MDSMapMetadata{
			Epoch: w.Epoch, Flags: w.Flags, LastFailure: w.LastFailure, Root: w.Root,
			SessionTimeout: w.SessionTimeout, SessionAutoclose: w.SessionAutoclose,
			MaxFileSize: w.MaxFileSize, MaxMDS: w.MaxMDS,
			DataPools: w.DataPools, CASPool: w.CASPool, Compat: publicMDSCompat(w.Compat),
			MetadataPool: w.MetadataPool, Created: MDSMapTime(w.Created), Modified: MDSMapTime(w.Modified),
			TableServer: w.TableServer, In: w.In, Failed: w.Failed, Stopped: w.Stopped,
			LastFailureOSDEpoch: w.LastFailureOSDEpoch, EverAllowedFeatures: w.EverAllowedFeatures,
			ExplicitlyAllowedFeatures: w.ExplicitlyAllowedFeatures, InlineDataEnabled: w.InlineDataEnabled,
			Enabled: w.Enabled, Name: w.Name, Damaged: w.Damaged, Balancer: w.Balancer,
			StandbyCountWanted: w.StandbyCountWanted, OldMaxMDS: w.OldMaxMDS,
			RetiredMinimumClientRelease: w.RetiredMinimumClientRelease, RequiredClientFeatures: w.RequiredClientFeatures,
			BalanceRankMask: w.BalanceRankMask, MaxXattrSize: w.MaxXattrSize,
			QuiesceLeader: w.QuiesceLeader, QuiesceMembers: w.QuiesceMembers,
		},
	}
	if w.Daemons != nil {
		result.Map.Daemons = make([]MDSMapDaemon, len(w.Daemons))
		for i, d := range w.Daemons {
			p := MDSMapDaemon{
				GID: d.GID, Name: d.Name, Rank: d.Rank, Incarnation: d.Incarnation,
				State: d.State, StateSequence: d.StateSequence, LaggySince: MDSMapTime(d.LaggySince),
				StandbyForRank: d.StandbyForRank, StandbyForName: d.StandbyForName, ExportTargets: d.ExportTargets,
				MDSFeatures: d.MDSFeatures, JoinFSCID: d.JoinFSCID, StandbyReplay: d.StandbyReplay,
				Flags: d.Flags, Compat: publicMDSCompat(d.Compat),
			}
			if d.Addresses != nil {
				p.Addresses = make([]MDSMapAddress, len(d.Addresses))
				for j, a := range d.Addresses {
					p.Addresses[j] = MDSMapAddress{Type: a.Type, Nonce: a.Nonce, Endpoint: a.Endpoint, FlowInfo: a.FlowInfo, ScopeID: a.ScopeID}
				}
			}
			result.Map.Daemons[i] = p
		}
	}
	if w.Incarnations != nil {
		result.Map.Incarnations = make([]MDSMapRankIncarnation, len(w.Incarnations))
		for i, rank := range w.Incarnations {
			result.Map.Incarnations[i] = MDSMapRankIncarnation(rank)
		}
	}
	if w.Up != nil {
		result.Map.Up = make([]MDSMapRankGID, len(w.Up))
		for i, rank := range w.Up {
			result.Map.Up[i] = MDSMapRankGID(rank)
		}
	}
	return result
}

func publicMDSCompat(c mds.CompatSet) MDSMapCompatSet {
	return MDSMapCompatSet{
		Compatible:         publicMDSFeatures(c.Compatible),
		ReadOnlyCompatible: publicMDSFeatures(c.ReadOnlyCompatible),
		Incompatible:       publicMDSFeatures(c.Incompatible),
	}
}

func publicMDSFeatures(f mds.FeatureSet) MDSMapFeatureSet {
	result := MDSMapFeatureSet{Mask: f.Mask}
	if f.Names != nil {
		result.Names = make([]MDSMapNamedFeature, len(f.Names))
		for i, name := range f.Names {
			result.Names[i] = MDSMapNamedFeature(name)
		}
	}
	return result
}
