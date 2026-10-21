# MDS service-ticket foundation

Reference: Ceph v20.2.4, commit
`7f793731f1b39eb4f465e960113d2363c311b964`, `src/include/msgr.h:92`
(`CEPH_ENTITY_TYPE_MDS = 0x02`), `auth/cephx/CephxClientHandler.cc:61-97`
and `auth/cephx/CephxProtocol.h` (`CephXAuthenticate`,
`CephXAuthorizeChallenge`, `CephXAuthorize`, `CephXAuthorizeReply`). These
sources carry LGPL-2.1 notices; no native implementation is copied.

`ServiceMDS` is an independently optional addition to the required AUTH/MGR
mask. It may be selected with or without OSD. Zero still requests AUTH/MGR
only. AUTH or MGR cannot be removed, and MON/CLIENT/unknown or combined
service authorizer identities remain rejected. Parsing a ticket for a service
that was not requested does not enable an authorizer for that service.

The existing Authenticate v3 `other_keys` mask carries the selected services
on initial authentication and renewal, including value copies of the client.
MDS authorizers use the MDS ticket, service ID and existing Authorize v2
nonce/challenge format. Reply nonce validation and connection-secret rules
remain identical to the existing service authorizer contract. Ticket updates
remain atomic across malformed later entries, and renewals may decrypt an
MDS ticket blob using its previous MDS service key. Existing authorizers keep
their original service key when a new ticket snapshot is published.

`mds_services_test.go` contains synthetic AES and aes256k exchanges in CRC
and secure-secret modes, with and without OSD selection. It covers the selected
wire service mask, service-key separation, challenge/reply nonce checks,
atomic renewal refusal, successful renewal and expired-ticket rejection.
These tests are not independent native MDS authentication proof. This contract
describes the ticket foundation, not the current boundary of all MDS support.
Public `Options.EnableMDS`, targeted connections and later session/request/
recovery primitives are described by the
[MDS session contract](../../cephmsgr/MDS_SESSION_CONTRACT.md) and
[CephFS contract](../../cephmsgr/MDS_CEPHFS_CONTRACT.md). Their independent
evidence is separate from these ticket tests. See the
[exchange ledger](EXCHANGE_LEDGER.md) for source-attributed fresh-MDS
authentication profiles; none of these unit tests alone establishes CephFS.
