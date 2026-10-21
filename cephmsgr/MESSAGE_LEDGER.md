# Client message type ledger

Reference: Ceph 20.2.4, commit `7f793731f1b39eb4f465e960113d2363c311b964`.
This ledger lists every message type constant in the product tree with the
envelope version the Go client sends or accepts, read from the encoder and
decoder code on 2026-10-21. "Sent" means the client builds the message;
"Received" means the client handles it as an inbound message; "Defined only" means the constant exists
but no product code builds or handles the message. All received messages first require compat at most header version. Additional
header checks vary by decoder: exact versions, compatibility ceilings and
inner-structure versions are stated separately below. Decoders without an
additional header-version check are marked explicitly; no uniform exact-version
refusal policy is implied.
Contract documents hold the field-level rules and the evidence profiles.
`TestMessageLedgerCoversEveryType` inventories explicit `uint16` message constants
in the msgr/mds/osd source files, including shared-ID names. It rejects missing,
duplicate or undeclared rows and mismatched names. Session operation codes and
frame tags are separate inventories. This is a source reading, not an
interoperability result. The nested authentication exchanges are recorded in
[the CephX exchange ledger](../internal/cephx/EXCHANGE_LEDGER.md), rather than
being implied by the defined-only AuthMessage/AuthReplyMessage constants.

| Type | Message | Direction | Header version / compat | Contract |
| --- | --- | --- | --- | --- |
| 2 | PingMessage | Sent; reply received | sent 1/1; reply must be 1 with compat 0, empty middle/data, transaction 0 | [MON_PING_CONTRACT.md](MON_PING_CONTRACT.md) |
| 4 | MonMapMessage | Received | compat above 1 refused; the inner map envelope has its own version | [MAP_VERSION_CONTRACT.md](MAP_VERSION_CONTRACT.md) |
| 5 | MonGetMapMessage | Sent | 1/0 | [SPEC.md](../SPEC.md) |
| 6 | MonGetOSDMapMessage | Sent | 1/0 | [SPEC.md](../SPEC.md) |
| 13 | StatfsMessage | Sent | 2/1 | [STATFS_CONTRACT.md](STATFS_CONTRACT.md) |
| 14 | StatfsReplyMessage | Received | no additional header-version check; exact 56-byte front, empty middle/data | [STATFS_CONTRACT.md](STATFS_CONTRACT.md) |
| 15 | SubscribeMessage | Sent | 3/1 | [SERVICE_MAP_CONTRACT.md](SERVICE_MAP_CONTRACT.md) |
| 16 | SubscribeAckMessage | Defined only | no encoder or decoder | none |
| 17 | AuthMessage | Defined only | CephX runs inside msgr2 frames, not this message | none |
| 18 | AuthReplyMessage | Defined only | as AuthMessage | none |
| 19 | MonGetVersionMessage | Sent | 1/0 | [MAP_VERSION_CONTRACT.md](MAP_VERSION_CONTRACT.md) |
| 20 | MonGetVersionReplyMessage | Received | 1 or 2; no additional compat guard; empty middle/data | [MAP_VERSION_CONTRACT.md](MAP_VERSION_CONTRACT.md) |
| 21 | MDSMapMessageType | Received | exact current form 2/1 | [MDS_MAPS_CONTRACT.md](MDS_MAPS_CONTRACT.md) |
| 22 | ClientSessionMessage | Sent and received | 1 or 7, compat 1 | [MDS_SESSION_CONTRACT.md](MDS_SESSION_CONTRACT.md) |
| 23 | ClientReconnectMessage | Sent (decoder for verification) | 5/4 | [MDS_CEPHFS_CONTRACT.md](MDS_CEPHFS_CONTRACT.md) |
| 24 | ClientRequestMessage | Sent (decoder for verification) | 6/1 | [MDS_CEPHFS_CONTRACT.md](MDS_CEPHFS_CONTRACT.md) |
| 25 | ClientRequestForwardMessage | Received | exact 1/1 | [MDS_CEPHFS_CONTRACT.md](MDS_CEPHFS_CONTRACT.md) |
| 26 | ClientReplyMessage | Received | exact 1/1 | [MDS_CEPHFS_CONTRACT.md](MDS_CEPHFS_CONTRACT.md) |
| 27 | ClientReclaimMessage | Sent | 1/1 | [MDS_CEPHFS_CONTRACT.md](MDS_CEPHFS_CONTRACT.md) |
| 28 | ClientReclaimReplyMessage | Received | exact 1/1 | [MDS_CEPHFS_CONTRACT.md](MDS_CEPHFS_CONTRACT.md) |
| 41 | OSDMapMessage | Received | 1/1 or 4/3 | [MAP_VERSION_CONTRACT.md](MAP_VERSION_CONTRACT.md) |
| 42 | OSDOpMessage (osd `RequestMessage`) | Sent | 8 or 9, compat 3, by selected profile | [OSD CONTRACT](../internal/osd/CONTRACT.md) |
| 43 | OSDOpReplyMessage (osd `ReplyMessage`) | Received | 6/2 (base) or 8/2 (reply-v8 profile) | [OSD CONTRACT](../internal/osd/CONTRACT.md) |
| 44 | WatchNotifyMessage | Received | exact 3/1; inner event version 1 | [OSD CONTRACT](../internal/osd/CONTRACT.md) |
| 48 | PoolOpReplyMessage | Received | compat above 1 refused | [POOL_OP_CONTRACT.md](POOL_OP_CONTRACT.md) |
| 49 | PoolOpMessage | Sent | 4/2 | [POOL_OP_CONTRACT.md](POOL_OP_CONTRACT.md) |
| 50 | MonCommandMessage | Sent | 1/0 | [SPEC.md](../SPEC.md) |
| 51 | MonCommandReplyMessage | Received | compat at most 1; matched by type and transaction | [SPEC.md](../SPEC.md) |
| 52 | LogMessage | Received | compat above 1 refused | [SPEC.md](../SPEC.md) |
| 61 | BackoffMessage | Sent and received | exact 1/1 | [OSD CONTRACT](../internal/osd/CONTRACT.md) |
| 62 | ConfigMessage | Received | version at least 1, compat at most 1 | [SPEC.md](../SPEC.md) |
| 63 | GetConfigMessage | Sent | 1/1 | [SPEC.md](../SPEC.md) |
| 97 | TellCommandMessage | Sent | 1/0 | [MGR_TELL_CONTRACT.md](MGR_TELL_CONTRACT.md) |
| 98 | TellCommandReplyMessage | Received | compat at most 1; matched by type and transaction | [MGR_TELL_CONTRACT.md](MGR_TELL_CONTRACT.md) |
| 103 | FSMapUserMessage | Received | exact current form 1/1 | [MDS_MAPS_CONTRACT.md](MDS_MAPS_CONTRACT.md) |
| 0x310 | ClientCapsMessage | Sent and received | 12/1 | [MDS_CEPHFS_CONTRACT.md](MDS_CEPHFS_CONTRACT.md) |
| 0x311 | ClientLeaseMessage | Sent and received | 1/1 | [MDS_CEPHFS_CONTRACT.md](MDS_CEPHFS_CONTRACT.md) |
| 0x312 | ClientSnapMessage | Received | exact 1/1 | [MDS_CEPHFS_CONTRACT.md](MDS_CEPHFS_CONTRACT.md) |
| 0x313 | ClientCapReleaseMessage | Sent (decoder for verification) | 2/1 | [MDS_CEPHFS_CONTRACT.md](MDS_CEPHFS_CONTRACT.md) |
| 0x314 | ClientQuotaMessage | Received | exact 1/1 | [MDS_CEPHFS_CONTRACT.md](MDS_CEPHFS_CONTRACT.md) |
| 0x700 | MgrOpenMessage | Sent (opt-in service daemon) | 3/1 | [MGR_SERVICE_CONTRACT.md](MGR_SERVICE_CONTRACT.md) |
| 0x701 | MgrConfigureMessage | Received (opt-in service daemon) | versions 1-4, compat exactly 1; nonempty middle/data refused; unimplemented extension bytes preserved raw | [MGR_SERVICE_CONTRACT.md](MGR_SERVICE_CONTRACT.md) |
| 0x702 | MgrReportMessage | Sent (opt-in service daemon) | 9/1 | [MGR_SERVICE_CONTRACT.md](MGR_SERVICE_CONTRACT.md) |
| 0x704 | MgrMapMessage | Received | compat above 1 refused | [SPEC.md](../SPEC.md) |
| 0x705 | MgrDigestMessage | Received | version at least 1, compat at most 1 | [SPEC.md](../SPEC.md) |
| 0x707 | ServiceMapMessage | Received | no additional header-version check; empty middle/data; ServiceMap/service structs compat at most 1 and daemon struct compat at most 2 | [SERVICE_MAP_CONTRACT.md](SERVICE_MAP_CONTRACT.md) |
| 0x708 | MgrCloseMessage | Sent and received (opt-in service daemon) | exact 1/1 on echo; nonempty middle/data refused | [MGR_SERVICE_CONTRACT.md](MGR_SERVICE_CONTRACT.md) |
| 0x709 | MgrCommandMessage | Sent | 1/0 | [SPEC.md](../SPEC.md) |
| 0x70a | MgrCommandReplyMessage | Received | compat at most 1; matched by type and transaction | [SPEC.md](../SPEC.md) |
| 0x70b | MgrUpdateMessage | Sent (opt-in service daemon) | 2/1 | [MGR_SERVICE_CONTRACT.md](MGR_SERVICE_CONTRACT.md) |

The service-daemon rows use `Options.EnableMgrService` and `OpenMgrService`,
`Configure`, `Report`, `UpdateMetadata` and `Close`; ordinary MGR command sessions
emit none of these service lifecycle messages. `TestMgrOpenEncoding`,
`TestMgrUpdateEncoding`, `TestMgrReportEncoding`, `TestMgrConfigureDecoding`,
`TestMgrCloseEncodingAndEchoDecoding` and
`TestMgrServiceOpenUpdateReportAndAcknowledgedClose` are synthetic checks;
the retained selected native evidence
is [MGR_SERVICE_VALIDATION.md](../integration/fixture/MGR_SERVICE_VALIDATION.md).

Not represented by a constant, hence not implemented: CephFS client metrics
(CEPH_MSG_CLIENT_METRICS 29), the full FSMap (type 45), and daemon-only message types outside this ledger. This client does not
implement daemon peer roles. The table above is not an advertisement of support
for message versions a daemon may also send; the checks listed in each row bound acceptance; they do not establish a blanket rejection of every other header version.
