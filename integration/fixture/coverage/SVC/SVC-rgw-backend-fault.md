# SVC-rgw-backend-fault

This file holds one SVC row of the [native path coverage map](../README.md).

## Rows

| ID | Path | Class | Setup and control | Independent observation | Fault and cleanup | Evidence | Visibility |
| --- | --- | --- | --- | --- | --- | --- | --- |
| SVC-rgw-backend-fault | RGW gateway OSD and MGR sessions across a backend OSD and manager restart | `released` | Released `rgw.Run` one-MON, one-MGR, one-BlueStore-OSD, one-gateway secure aes256k fixture; one S3 PUT and one S3 GET outstanding while the OSD is stopped, one S3 PUT while the manager is stopped, S3 as input only | Native oracle: MON OSD/manager maps (down and up epochs, new nonces and GIDs), gateway GlobalID/TID-attributed reception at the restarted OSD in its new epochs including `watch reconnect` on the control objects, the gateway's own `messenger dump` connections to the new OSD and manager nonces, the restarted manager's `service status` stamp, native head bytes and index keys; each overlapping request's HTTP outcome classified separately | Graceful Docker stop and start of the owned OSD and manager once each, no request reissued; S3 deletion with native head/index absence, owned user and bucket removal and independently confirmed container, volume and network absence | `rgw.aes256k.ip4.container.t-rgwbackendfault`, `TestRGWNativeBackendFaultIntegration`, [RGW_NATIVE_VALIDATION.md](../../RGW_NATIVE_VALIDATION.md) | dedicated row; one selected aes256k/secure/IPv4 container profile; graceful stops only (no killed OSD, partition or fault during a multi-stripe upload); the gateway's internal queueing of the overlapping requests is not observed |

## Updates

2026-11-20: added for issue #13 with the backend-fault checkpoint. Its
pull request records the local native result; the hosted native workflow has
not run it yet.
