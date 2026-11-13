package mds

import "github.com/JSYoo5B/ceph-msgr-go/internal/wire"

// Independently written from pinned Ceph v20.2.4 commit
// 7f793731f1b39eb4f465e960113d2363c311b964: messages/MClientReply.h
// (openc_response_t), include/interval_set.h (encode), mds/Server.cc
// (set_reply_extra_bl) and client/Client.cc (Client::make_request). LGPL-2.1
// sources; no code copied.

// FeatureDelegIno is CEPHFS_FEATURE_DELEG_INO (mds/cephfs_features.h:42): a
// session that declares it receives openc_response_t create extras.
const FeatureDelegIno = 13

// MaxDelegatedInodeRanges bounds the interval count of one openc_response_t.
// The MDS delegates at most mds_client_prealloc_inos inodes (default 1000) to
// a session, so a grant never needs more intervals than that.
const MaxDelegatedInodeRanges uint32 = 1 << 16

// bareCreatedInodeBytes is the REPLY_CREATE_INODE extra: one u64 inode.
const bareCreatedInodeBytes = 8

// InodeRange is one interval of an interval_set<inodeno_t>: Length inode
// numbers starting at Start.
type InodeRange struct {
	Start, Length uint64
}

// CreateExtra is the extra blob of a reply to an inode-creating request.
// Delegation reports the form the MDS chose: false for the bare 8-byte
// created inode, true for openc_response_t (struct v1/compat1), which the MDS
// sends when mds_allow_async_dirops is set and the session declared DELEG_INO.
// Delegated holds the raw ranges of that form in wire order; it is empty when
// the MDS delegated nothing with this reply. The ranges are a server grant
// for asynchronous creates, which this library does not issue: nothing here
// allocates from them or tracks them.
type CreateExtra struct {
	Ino        uint64
	Delegation bool
	Delegated  []InodeRange
}

// DecodeCreateExtra decodes the extra blob of a CREATE reply or of a
// completed-request reply for an inode-creating request. The two native forms
// have disjoint sizes: exactly 8 bytes, or an envelope of at least 18. An
// empty blob (a reply without a created inode) is an error. Within
// openc_response_t the interval count is bounded before allocation, and a
// truncated pair, an empty interval, an interval whose exclusive end does not
// fit in 64 bits, and intervals that are not ascending and disjoint are
// rejected: interval_set encodes a sorted map of nonempty disjoint intervals.
func DecodeCreateExtra(raw []byte) (CreateExtra, error) {
	d := wire.NewDecoderLimit(raw, uint32(len(raw)))
	if len(raw) == bareCreatedInodeBytes {
		c := CreateExtra{Ino: d.U64()}
		return c, d.Done()
	}
	b := currentStruct(d, 1, 1)
	c := CreateExtra{Ino: b.U64(), Delegation: true}
	n := b.Count(16, MaxDelegatedInodeRanges)
	if n != 0 {
		c.Delegated = make([]InodeRange, n)
	}
	var end uint64
	for i := range c.Delegated {
		r := InodeRange{Start: b.U64(), Length: b.U64()}
		if b.Err() != nil {
			break
		}
		// next is the exclusive end. It must stay above Start: an empty
		// interval and one that reaches or passes 2^64 both fail this.
		next := r.Start + r.Length
		if next <= r.Start || r.Start < end {
			b.Fail(ErrRequest)
			break
		}
		c.Delegated[i], end = r, next
	}
	d.Fail(b.Done())
	if err := d.Done(); err != nil {
		return CreateExtra{}, err
	}
	return c, nil
}
