// Package osd implements the bounded Tentacle wire codecs for explicitly
// targeted object requests. It does not discover or route objects.
//
// The codecs are independently translated from Ceph v20.2.4
// (ceph/ceph/blob/v20.2.4/src/include/rados.h, src/messages/MOSDOp.h,
// src/messages/MOSDOpReply.h, src/osd/PrimaryLogPG.cc). Those sources carry
// LGPL-2.1 notices; no native implementation is copied.
//
// # Request profiles
//
// The base object-hash profile encodes MOSDOp/MOSDOpReply v6 and requires
// OBJECTLOCATOR and PGID64; a peer requiring NEW_OSDOP_ENCODING or
// NEW_OSDOPREPLY_ENCODING is refused. The v8 request (EncodeRequestV8), v8
// reply (DecodeReplyV8) and untraced v9 request (EncodeRequestV9) codecs are
// selected only by an explicit profile, never as a fallback from the peer's
// features or the received header. The caller supplies pool, namespace,
// locator key, object, raw placement hash, target, address and map epoch; in
// v6 the raw hash is echoed in pg_t and is not an actual PG. There is no
// hashing, CRUSH placement, redirect following or retry.
//
// RESEND_ON_SPLIT, RADOS_BACKOFF, OSDMAP_PG_UPMAP, CRUSH_CHOOSE_ARGS and
// SERVER_LUMINOUS share the incarnation-2 mask 1<<21|1<<57
// (src/include/ceph_features.h). Bit 21 cannot be added to the v6 encoder:
// without NEW_OSDOP_ENCODING native MOSDOp stays v6 and sends the raw hash,
// while a bit-21 server treats get_spg() as the actual PG (src/osd/OSD.cc).
// NEW_OSDOPREPLY_ENCODING shares bit 58 with CRUSH_TUNABLES5, so negotiating
// it selects the reply encoding and implements no CRUSH evaluation.
//
// # Operation vectors
//
// Each packed operation is 38 bytes (opcode u16, flags u32, 28-byte union,
// payload length u32); inputs concatenate in vector order in the data
// segment. Vectors hold 1 to 64 operations, and request input plus expected
// reply output are bounded by the frame limit before allocation. The vector
// layout, input order, per-operation output budget and reply split are the
// same in request v6/v8/v9 and reply v6/v8, so operation_codec.go owns them;
// envelopes, identity and feature checks stay in each version's entry point.
// Unknown opcodes and flags are never sent. EXCL is admitted only on CREATE;
// FAILOK keeps an operation's negative result and lets later operations run.
// Mutations set WRITE, reads and guards READ, and every request asks for
// ONDISK and RETURNVEC without a preliminary ACK. A nonzero mtime on a vector
// that does not write is refused because it would set nothing.
//
// One vector is one native transaction: an ordinary guard failure keeps its
// staged writes invisible. Ordered operations promise no general
// read-your-staged-writes view and no atomicity across requests or objects.
// Reads that could compare committed bytes with a staged full-object digest
// are refused before transmission: a READ or CMPEXT at offset zero covering a
// preceding WRITEFULL or APPEND payload, and every positive offset-zero READ
// or CMPEXT after an offset-zero WRITE or WRITESAME, because a newer remote
// truncate sequence can clip the write below its requested length before the
// digest is set (PrimaryLogPG.cc do_read and the WRITE truncate path). A
// native WRITEFULL then full-READ vector made the primary mark its object
// unreadable; the WRITE, APPEND and CMPEXT cases are source-derived. The bound
// survives FAILOK operations and later writes.
//
// The extent truncate fields are sent only on READ, WRITE and TRUNCATE, as
// native CephFS clients do. A size requires a nonzero sequence; the pair
// (sequence 1, size all ones), which the OSD rewrites to (0, 0), is refused,
// and a sequenced TRUNCATE must carry Offset equal to TruncateSize because the
// OSD asserts it. A WRITE older than the object's truncate sequence is trimmed
// to the object's size, even with zero truncate fields, so a successful WRITE
// reply may echo a shorter Length: the bytes actually written.
//
// # Results and uncertainty
//
// The server's PG, epoch, versions, raw flags, aggregate result, per-op
// results and payload boundaries stay separate. A missing object can be
// refused before the vector runs, leaving per-op results zero. An aggregate
// success with a negative non-FAILOK per-op result, or a reply whose opcode,
// extent or payload does not correspond to the submitted operation, is a
// malformed completion: the session fails and a sent request keeps an unknown
// outcome. A successful mutation must carry ONDISK; that includes SET_CHUNK,
// TIER_FLUSH and TIER_EVICT, which are READ requests that change manifests or
// data (RequiresOnDisk). A negative server result is a known refusal and
// needs no ONDISK. A Messenger ACK proves neither execution nor durability,
// and MemStore reports ONDISK yet loses its contents on an OSD restart.
//
// # Messages on an OSD connection
//
// Type 41 MOSDMap, 43 MOSDOpReply, 44 MWatchNotify and 61 MOSDBackoff are
// separate classes (src/include/ceph_fs.h). Only a reply can complete a
// pending request, by header TID; a control whose TID equals a pending
// request's must never complete it. MOSDMap envelopes v1 and v4 are accepted
// with opaque inner maps; a batch may start with a full map and is not a
// complete incremental chain (src/osd/OSD.cc build_incremental_map_msg).
// Without RADOS_BACKOFF the OSD may instead hold requests in its own queues,
// so the absence of a backoff is no promise of prompt service.
package osd
