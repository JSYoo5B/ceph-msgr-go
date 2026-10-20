package cephmsgr

import "github.com/JSYoo5B/ceph-msgr-go/internal/osd"

const (
	// OSDRollback restores the head from the clone that covers
	// OSDOperation.Snapshot, using the committed snapset. When no clone
	// covers that snap id, the OSD deletes the head (or leaves a whiteout
	// when clones remain) and still reports success. A snap id above the
	// object's snapset sequence only advances the version. It must be the
	// only operation in its request; the request's SnapContext still applies,
	// so a newer context first clones the pre-rollback head.
	OSDRollback OSDOpcode = OSDOpcode(osd.Rollback)
	// OSDListSnapshots returns the object's clones and snapset sequence. It
	// must be the only operation, with a zero ReadSnapshot and no
	// SnapContext; the client sends CEPH_SNAPDIR itself.
	OSDListSnapshots OSDOpcode = OSDOpcode(osd.ListSnaps)

	// OSDHeadSnapshot is CEPH_NOSNAP: the head entry ID in a listing.
	OSDHeadSnapshot uint64 = osd.NoSnap
	// OSDMaxSnapshot is CEPH_MAXSNAP, the largest snap id a request names.
	OSDMaxSnapshot uint64 = osd.MaxSnap
	// OSDOldSnapContext is EOLDSNAPC (85) from include/rados.h, returned
	// before execution when OrderSnapshots is set and the SnapContext
	// sequence is older than the object's. On Linux 85 is also ERESTART.
	OSDOldSnapContext int32 = osd.OldSnapContext
)

// OSDSnapContext is the write SnapContext of a mutation request. Snapshots
// are nonzero, strictly descending and not above Sequence. The OSD clones the
// pre-write head when Snapshots[0] is newer than the object's snapset
// sequence; the clone ID is Sequence. The client does not allocate snap ids or
// compare them with the pool: a pool in pool-snapshot mode substitutes its
// own context on the server unless EnforceSnapContext is set.
type OSDSnapContext struct {
	Sequence  uint64
	Snapshots []uint64
}

// OSDListSnapshotsInput bounds the listing output accepted from the OSD.
type OSDListSnapshotsInput struct{ MaxOutputBytes uint32 }

// OSDSnapshotList is the decoded obj_list_snap_response_t. Clones keep the
// OSD's order; a head that is not a whiteout is the last entry with ID
// OSDHeadSnapshot and no snapshots or overlap. Snapshots are ascending.
type OSDSnapshotList struct {
	Sequence uint64
	Clones   []OSDClone
}

type OSDClone struct {
	ID        uint64
	Snapshots []uint64
	Overlap   []OSDExtent
	Size      uint64
}

type OSDExtent struct{ Offset, Length uint64 }

func publicSnapshotList(data []byte) OSDSnapshotList {
	list, err := osd.DecodeSnapList(data)
	if err != nil {
		return OSDSnapshotList{}
	}
	result := OSDSnapshotList{Sequence: list.Seq, Clones: make([]OSDClone, len(list.Clones))}
	for i, c := range list.Clones {
		clone := OSDClone{ID: c.ID, Snapshots: c.Snaps, Size: c.Size, Overlap: make([]OSDExtent, len(c.Overlap))}
		for j, e := range c.Overlap {
			clone.Overlap[j] = OSDExtent{Offset: e.Offset, Length: e.Length}
		}
		result.Clones[i] = clone
	}
	return result
}
