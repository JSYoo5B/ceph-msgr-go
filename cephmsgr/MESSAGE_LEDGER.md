# Client message type ledger

Reference: Ceph 20.2.4, commit `7f793731f1b39eb4f465e960113d2363c311b964`.
This ledger lists every message type constant in the product tree with the
envelope version the Go client sends or accepts, read from the encoder and
decoder code on 2026-10-21. "Sent" means the client builds the message;
"Received" means a decoder accepts it; "Defined only" means the constant exists
but no product code builds or handles the message. A version that is not
accepted is refused with a typed version error and never reinterpreted.
Contract documents hold the field-level rules and the evidence profiles.
`TestMessageLedgerCoversEveryType` fails if a constant's value or name is not
in this table. This is a source reading, not an interoperability result.

| Type | Message | Direction | Header version / compat | Contract |
| --- | --- | --- | --- | --- |
| 2 | PingMessage | Sent; reply received | sent 1/1; reply must be 1 with compat 0, empty middle/data, transaction 0 | [MON_PING_CONTRACT.md](MON_PING_CONTRACT.md) |
| 4 | MonMapMessage | Received | compat above 1 refused; the inner map envelope has its own version | [MAP_VERSION_CONTRACT.md](MAP_VERSION_CONTRACT.md) |
| 5 | MonGetMapMessage | Sent | 1 | [SPEC.md](../SPEC.md) |
| 6 | MonGetOSDMapMessage | Sent | 1 | [SPEC.md](../SPEC.md) |
| 13 | StatfsMessage | Sent | 2/1 | [STATFS_CONTRACT.md](STATFS_CONTRACT.md) |
| 14 | StatfsReplyMessage | Received | see `statfs.go`; empty middle/data | [STATFS_CONTRACT.md](STATFS_CONTRACT.md) |
| 15 | SubscribeMessage | Sent | 3/1 | [SERVICE_MAP_CONTRACT.md](SERVICE_MAP_CONTRACT.md) |
| 16 | SubscribeAckMessage | Defined only | no encoder or decoder | none |
| 17 | AuthMessage | Defined only | CephX runs inside msgr2 frames, not this message | none |
| 18 | AuthReplyMessage | Defined only | as AuthMessage | none |
| 19 | MonGetVersionMessage | Sent | 1 | [MAP_VERSION_CONTRACT.md](MAP_VERSION_CONTRACT.md) |
| 20 | MonGetVersionReplyMessage | Received | 1 or 2; empty middle/data | [MAP_VERSION_CONTRACT.md](MAP_VERSION_CONTRACT.md) |
| 21 | MDSMapMessageType | Received | exact current form 2/1 | [MDS_MAPS_CONTRACT.md](MDS_MAPS_CONTRACT.md) |
| 22 | ClientSessionMessage | Sent and received | 1 or 7, compat 1 | [MDS_SESSION_CONTRACT.md](MDS_SESSION_CONTRACT.md) |
| 23 | ClientReconnectMessage | Sent (decoder for verification) | 5/4 | [MDS_CEPHFS_CONTRACT.md](MDS_CEPHFS_CONTRACT.md) |
| 24 | ClientRequestMessage | Sent | 6/1 | [MDS_CEPHFS_CONTRACT.md](MDS_CEPHFS_CONTRACT.md) |
| 25 | ClientRequestForwardMessage | Received | exact 1/1 | [MDS_CEPHFS_CONTRACT.md](MDS_CEPHFS_CONTRACT.md) |
| 26 | ClientReplyMessage | Received | exact 1/1 | [MDS_CEPHFS_CONTRACT.md](MDS_CEPHFS_CONTRACT.md) |
| 27 | ClientReclaimMessage | Sent | 1/1 | [MDS_CEPHFS_CONTRACT.md](MDS_CEPHFS_CONTRACT.md) |
| 28 | ClientReclaimReplyMessage | Received | exact 1/1 | [MDS_CEPHFS_CONTRACT.md](MDS_CEPHFS_CONTRACT.md) |
| 41 | OSDMapMessage | Received | 1/1 or 4/3 | [MAP_VERSION_CONTRACT.md](MAP_VERSION_CONTRACT.md) |
| 42 | OSDOpMessage (osd `RequestMessage`) | Sent | 8 or 9, compat 3, by selected profile | [OSD CONTRACT](../internal/osd/CONTRACT.md) |
| 43 | OSDOpReplyMessage (osd `ReplyMessage`) | Received | 6/2 (base) or 8/2 (reply-v8 profile) | [OSD CONTRACT](../internal/osd/CONTRACT.md) |
| 44 | WatchNotifyMessage | Received | decoded per the watch contract | [OSD CONTRACT](../internal/osd/CONTRACT.md) |
| 48 | PoolOpReplyMessage | Received | compat above 1 refused | [POOL_OP_CONTRACT.md](POOL_OP_CONTRACT.md) |
| 49 | PoolOpMessage | Sent | 4/2 | [POOL_OP_CONTRACT.md](POOL_OP_CONTRACT.md) |
| 50 | MonCommandMessage | Sent | 1 | [SPEC.md](../SPEC.md) |
| 51 | MonCommandReplyMessage | Received | matched to the pending request by type and transaction | [SPEC.md](../SPEC.md) |
| 52 | LogMessage | Received | compat above 1 refused | [SPEC.md](../SPEC.md) |
| 61 | BackoffMessage | Sent and received | exact 1/1 | [OSD CONTRACT](../internal/osd/CONTRACT.md) |
| 62 | ConfigMessage | Received | version at least 1, compat at most 1 | [SPEC.md](../SPEC.md) |
| 63 | GetConfigMessage | Sent | 1/1 | [SPEC.md](../SPEC.md) |
| 97 | TellCommandMessage | Sent | 1 | [MGR_TELL_CONTRACT.md](MGR_TELL_CONTRACT.md) |
| 98 | TellCommandReplyMessage | Received | matched to the pending request by type and transaction | [MGR_TELL_CONTRACT.md](MGR_TELL_CONTRACT.md) |
| 103 | FSMapUserMessage | Received | exact current form 1/1 | [MDS_MAPS_CONTRACT.md](MDS_MAPS_CONTRACT.md) |
| 0x310 | ClientCapsMessage | Sent and received | 12/1 | [MDS_CEPHFS_CONTRACT.md](MDS_CEPHFS_CONTRACT.md) |
| 0x311 | ClientLeaseMessage | Sent and received | 1/1 | [MDS_CEPHFS_CONTRACT.md](MDS_CEPHFS_CONTRACT.md) |
| 0x312 | ClientSnapMessage | Received | exact 1/1 | [MDS_CEPHFS_CONTRACT.md](MDS_CEPHFS_CONTRACT.md) |
| 0x313 | ClientCapReleaseMessage | Sent and received | 2/1 | [MDS_CEPHFS_CONTRACT.md](MDS_CEPHFS_CONTRACT.md) |
| 0x314 | ClientQuotaMessage | Received | exact 1/1 | [MDS_CEPHFS_CONTRACT.md](MDS_CEPHFS_CONTRACT.md) |
| 0x704 | MgrMapMessage | Received | compat above 1 refused | [SPEC.md](../SPEC.md) |
| 0x705 | MgrDigestMessage | Received | version at least 1, compat at most 1 | [SPEC.md](../SPEC.md) |
| 0x707 | ServiceMapMessage | Received | empty middle/data; see `servicemap.go` | [SERVICE_MAP_CONTRACT.md](SERVICE_MAP_CONTRACT.md) |
| 0x709 | MgrCommandMessage | Sent | 1 | [SPEC.md](../SPEC.md) |
| 0x70a | MgrCommandReplyMessage | Received | matched to the pending request by type and transaction | [SPEC.md](../SPEC.md) |

Not represented by a constant, hence not implemented: CephFS client metrics
(CEPH_MSG_CLIENT_METRICS 29), the full FSMap (type 45), and every
daemon-to-daemon message. The table above is not an advertisement of support
for message versions a daemon may also send; unaccepted versions are refused.
