package osd

// ErasureReexecutedVector reports a vector that an erasure-coded primary may
// answer with repeated output: one that combines an extent read the primary
// completes asynchronously (READ, SPARSE_READ, CHECKSUM, CMPEXT) with an
// operation whose output the primary produces while it runs the vector
// (STAT, ISDIRTY, GETXATTR, GETXATTRS, the OMAP reads, CALL, LIST_WATCHERS,
// LIST_SNAPS, MAPEXT).
//
// Pinned Ceph 20.2.4 7f793731f1b39eb4f465e960113d2363c311b964,
// PrimaryLogPG.cc: on an EC pool do_read and do_sparse_read queue their read
// and the pass goes on, while do_checksum and do_extent_cmp queue theirs and
// end the pass with -EINPROGRESS. When a pass queued reads, execute_ctx
// starts them and OpContext::finish_read calls execute_ctx again once they
// complete: it resets the object state and the transaction but not the
// operations' outputs, takes each queued read's result from its finisher and
// runs the other operations again, from the first. So an operation with
// output that runs in more than one pass appends its output once per pass,
// and a CALL runs its method each time. An operation runs in every pass that
// reaches it, and every pass but the last queued a read, so the count
// depends on its place in the vector. Guards without output (ASSERT_VER,
// CMPXATTR, OMAP_CMP) evaluate to the same result in each pass, and a vector
// without an extent read takes one pass. A replicated primary reads
// synchronously in one pass.
//
// The client refuses every such vector for an erasure-coded placement before
// submission, rather than the order-dependent subset that the server
// answers with repeated output; the object-hash profile names no placement,
// so this does not apply to it. LGPL-2.1; independently translated, no
// source copied.
func ErasureReexecutedVector(ops []Operation) bool {
	if len(ops) < 2 {
		return false
	}
	var async, output bool
	for _, op := range ops {
		switch op.Code {
		case Read, SparseRead, Checksum, CompareExtent:
			async = true
		case Stat, Isdirty, GetXattr, GetXattrs, OmapGetKeys, OmapGetValues, OmapGetHeader, OmapGetValuesByKeys, Call, ListWatchers, ListSnaps, MapExt:
			output = true
		}
	}
	return async && output
}
