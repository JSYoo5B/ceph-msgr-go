# Explicit request resubmission (original identity, FLAG_RETRY)

Reference: Ceph 20.2.4, commit `7f793731f1b39eb4f465e960113d2363c311b964`
(`include/rados.h` CEPH_OSD_FLAG_RETRY, `messages/MOSDOp.h` retry_attempt and
reqid, `osd/PrimaryLogPG.cc` do_op duplicate detection through
`pg_log.get_log().get_request()`; LGPL-2.1, independently translated).

`OSDRequest.Resubmit` (an earlier `OSDSubmission`) with `RetryAttempt >= 1`
repeats a request under its original transaction ID and incarnation, with
`CEPH_OSD_FLAG_RETRY` and the attempt in the front. The OSD then answers a
write whose reqid is still in its PG log from the log (original result, user
version and replay version) instead of executing it again. This turns an
unknown outcome into a known one without a second execution, which a new
request with a new ID cannot do. Nothing resubmits automatically; the caller
chooses whether, when and how often.

Rules enforced locally (refused before submission, no ID consumed): the
submission must carry this client's global ID and the request's
`ClientIncarnation`, its transaction must have been issued by this client and
must not be pending on the connection (`ErrTransactionInUse` from the
session), the attempt is 1 through `OSDMaxRetryAttempt`, and resubmission is
not offered for watch/notify. The caller must resend the same operations; the
client cannot verify that, and a different vector under a reused ID is the
caller's error. The reply must echo the attempt.

A late reply to an earlier attempt of the same transaction ID cannot be told
from the answer to the resubmission except by its echoed attempt: such a reply
fails the session and ends the resubmission as an unknown outcome (the request
may still have executed). Resubmit on a fresh connection after the original's
connection ended, as the verified flows do, rather than on a session that may
still deliver the original's reply.

Limits: the server's duplicate detection covers only entries still in the PG
log or its duplicate tracking (trimming ends it; a trimmed request was
observed live to execute a second time under its original identity), applies to writes, and is not an exactly-once
guarantee for the logical operation. The log entry and its reqid replicate
with the write, so a replacement primary after a map change answers from its
own copy: verified for an exclusive CREATE and a lost-reply APPEND resubmitted
to the new primary after SIGKILL of the original (the client still routes the
resubmission by the caller's new placement and does not check the reqid).
A PG split also carries the entry: an APPEND whose reply was lost before the
split, resubmitted to the child PG by the caller's new placement, is answered
from the child's log with the original user version and not executed again;
likewise an APPEND lost in the child is answered from the parent's log after
the PGs merge back. A primary that is SIGKILLed and restarted answers from
the PG log it reloads from BlueStore. Reads need no duplicate detection.

Verification: [OSD_RESUBMIT_VALIDATION.md](../../integration/fixture/OSD_RESUBMIT_VALIDATION.md).
