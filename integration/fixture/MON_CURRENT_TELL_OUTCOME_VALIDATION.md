# Current-MON Tell outcome after connection loss

This test-only consumer extends the daemon-local runtime-option experiment in
[MON_TELL_OUTCOME_VALIDATION.md](MON_TELL_OUTCOME_VALIDATION.md) to `MonTell`.
The named `MonTellTo` cell remains separate: it owns a private connection,
whereas `MonTell` uses the admitted primary MON session and recovery may choose
another MON. No product API or replay policy changes.

## Recipe

`TestCephAppliedCurrentMonitorTellIsNotReplayedIntegration` uses the same closed
native oracle and bounded `log_max_recent` candidate as the named cell:

1. Independently observe the owned MON's runtime override and effective value.
   Refuse a baseline outside 1..19998; choose A=baseline+1 and B=baseline+2,
   without reducing retained crash-log capacity. Bootstrap the subject Go
   client through owned `mon.b` and confirm its admitted source role.
2. Submit A once through `MonTell`, withhold that connection's reads, and
   independently observe A before the original caller can receive a reply.
   In CRC mode retain the complete logical MCommand write's peer, connection,
   TID, sequence and input hashes. These are local transmission observations,
   not server execution counts or an authentication-frame capture.
3. An independent client explicitly writes B to that same MON, and fresh
   native reads must show B with runtime-override provenance. End only the
   withheld connection. Require the original admitted primary session to end,
   the original call to preserve `OutcomeUnknownError` with no acknowledged
   result, and `WaitMonReady` to admit a different session.
4. Read B through fresh exact-target Tell connections on both clients and a
   fresh native handle. Keep the MON process/rank stable. Require exactly one
   complete original CRC logical write across every observed connection and
   peer, so recovery onto another MON cannot hide an outbound repeat. A later
   read or recovery does not acknowledge the original mutation.
5. Restore the original override once, or unset only an introduced override.
   Do not repeat an uncertain restoration. Repeat only fresh native reads of
   effective value and override provenance to confirm the original baseline.

The current-channel identity file is distinct from the named cell's file.
It labels the sampled primary authentication ID as such and does not claim
an independently captured Tell authentication identity. Unit tests preserve
that distinction and reject missing/non-owned bootstrap endpoints.

## Reproduction and attribution

Use an authorized Linux runner with the existing execution/check locks, the
pinned Tentacle 20.2.4 image and source, AES credentials/service cipher, CRC,
IPv4 and host runtime:

```sh
CEPH_MSGR_TEST_KEY_TYPE=aes \
CEPH_MSGR_TEST_SERVICE_CIPHER=aes \
CEPH_MSGR_TEST_CONNECTION_MODE=crc \
CEPH_MSGR_TEST_IP_FAMILY=4 \
CEPH_MSGR_TEST_RUNTIME=host \
CEPH_MSGR_TEST_RUN='^TestCephApplied(Current)?MonitorTellIsNotReplayedIntegration$' \
sh integration/run.sh
```

The exact required parent names must report PASS, with no skip, independently
of a successful fixture exit. The PR records the tested commit, attempt,
image/profile, actual native observations, restoration and owned-resource
cleanup. This recipe and unit coverage alone are not native acceptance.

The reference is Ceph v20.2.4,
`7f793731f1b39eb4f465e960113d2363c311b964`, with image
`quay.io/ceph/ceph:v20.2.4@sha256:6bb1c8a42fbc0bf87938946990b65174466997bc11c31eb5a323225a779fd8f9`.

## Limits

One unchanged MON process and one daemon-local option do not establish MON
restart durability, server exactly-once execution, secure-frame identity,
other profiles, MGR Tell or targeted OSD/PG/MDS outcome acceptance. The fresh
reads intentionally name the original MON even if the primary client recovers
onto another MON. Raw signed native results and the caller's unknown outcome
remain distinct; the test does not infer operational retry policy.
