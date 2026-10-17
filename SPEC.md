# ceph-msgr-go 스펙 및 검증 기준

조사 및 구현 기준일: 2026-10-01. 통신 계층으로의 범위 조정일: 2026-10-03. OSD 통신 기초 추가 결정일: 2026-10-04. 최종 테스트 환경 범위 및 후속 제품 통신 요구 확정일: 2026-10-13. 우선순위·진행상황 정리일: 2026-10-14. O0/O1 제한 범위 구현·native 검증 갱신일: 2026-10-15. O2 OMAP·object class CALL과 D1 OSD·PG 명령 구현·native 검증 갱신일: 2026-10-15. 이 문서는 프로젝트 정책과 검증 기준을 정의한다. 첫 MON/MGR 구현과 Ceph 20.2.4 실서버 검증을 완료했다. 현재 API, 재현 방법 및 검증 범위는 [README.md](README.md)에 기록한다.

## 확정된 프로젝트 정책

- Ceph Messenger를 Go에서 직접 구현한다. 제품 실행과 빌드에 go-ceph, librados, CGO, Ceph CLI 및 다른 네이티브 라이브러리를 요구하지 않는다.
- 운영체제별 Ceph 설치 없이 Go가 지원하는 대상 운영체제에서 동작하도록 설계한다. 운영체제에 종속된 기본 경로보다 프로그램으로 전달하는 설정을 우선한다.
- 최소 지원 Ceph 계열은 Tentacle, 즉 20.2 계열이다. 이전 Ceph 계열을 위한 호환 코드는 추가하지 않는다.
- 현재 스펙에 맞춰 API를 설계한다. API 변경을 이유로 이전 API와의 하위 호환성을 요구하는 작업은 거부한다. 이 정책을 변경하려면 사용자가 명시적으로 범위를 변경해야 한다.
- 이 저장소는 통신 계층만 구현한다. 첫 구현은 raw MON/MGR 명령 송수신과 이를 위한 인증, wire 메시지·지도 codec, 구독, 세션 처리와 복구에 집중한다. 다음 단계는 OSD 통신과 객체 I/O를 위한 wire 기초다.
- 실제 사용은 별도 레이어·저장소에서 구현한다. 명령 JSON 구성, 명령 schema 및 응답 JSON의 의미 해석, text keyring·설정 로딩, 명령별 typed 관리 API, 모듈·설정 정책과 운영 workflow는 제품 범위에 포함하지 않는다.
- OSD service ticket, 명시적으로 지정한 OSD 연결, 호출자가 준비한 객체 locator·raw hash·지도 epoch를 사용하는 stat/read 요청과 응답을 범위에 추가한다. 자동 배치·CRUSH 계산, 자동 라우팅·재시도 정책, 고수준 RADOS API, RBD, CephFS는 이 단계에 포함하지 않는다. 객체 쓰기와 다른 mutation opcode는 별도 구현·검증 단위로 추가한다. 호출자가 준비한 풀·OSD 관리 명령 JSON도 MON/MGR로 전송할 수 있지만 해당 관리 기능 자체를 구현하는 것은 아니다.
- 2026-10-04 후속 사용자 결정으로 MON의 명시적인 raw OSDMap 조회·구독을 추가한다. 최신 full·지정 epoch 범위·연속 수신을 제공하며 지도 해석·적용·CRUSH 배치와 객체 요청 재실행은 사용 계층에 둔다.
- 2026-10-13 스펙 보강 결정으로 상위 레이어가 Tentacle의 RADOS·RBD·CephFS 기능을 구현하는 데 필요한 OSD 전반·MDS·대상 지정 명령의 통신 계약을 후속 제품 요구로 추가한다. 현재 stat/read 단계의 지원 범위와 분리하며, 고수준 사용 API와 자동 배치·운영 정책은 계속 별도 레이어에 둔다. 상세 요구와 단계별 완료 기준은 아래 후속 제품 통신 계약을 따른다.

## 작업 우선순위와 진행상황

### 우선순위의 의미와 다음 작업

아래 우선순위는 2026-10-14 기준 구현 순서다. P0부터 착수하되 각 항목의
선행 조건을 먼저 충족한다. 낮은 우선순위도 최종 필수 요구에서 제외하지 않는다.
현재 동작의 보안·데이터 정확성·취소/종료 회귀는 해당 단계보다 우선해 수정한다.
세부 작업 ID·선행 조건·native 완료 기준은 [후속 단계표](#후속-milestone과-완료-기준)에 둔다.

| 수준 | 구현 순서와 목적 | 진행상황 |
| --- | --- | --- |
| 기반 | 기존 Messenger·CephX·MON/MGR·OSD stat/read·raw 지도 수신을 유지 | 아래 제한 범위에서 구현 및 실검증 기록 있음 |
| P0 | 객체 mutation의 입력·완료 계약과 최소 쓰기·복합 요청: O0 → O1 | 기존 11개 stat/read·mutation/guard opcode 구현 및 single BlueStore·3-replica primary SIGKILL native 검증; [WRITESAME](integration/fixture/OSD_WRITESAME_VALIDATION.md) 별도 추가·CRC 3 BlueStore와 secure single MemStore에서 각각 19사례 검증; WRITESAME primary SIGKILL은 [3 BlueStore 1 사례](integration/fixture/OSD_WRITESAME_VALIDATION.md#primary-sigkill)로 검증; O0/O1 전체 완료 아님 |
| P1 | OSD metadata·snapshot·class call, 열거·이벤트·backoff, OSD/PG 명령과 replicated feature 범위 확대 | 제한 xattr 5 opcode·OMAP 읽기 3 opcode·OMAP header 읽기와 쓰기 5 opcode(RMKEYRANGE 포함)·OMAP_CMP·object class CALL·allocation hint·snapshot read/write context·ROLLBACK·LIST_SNAPS·fresh native wire/Go 비교 구현; D1 OSD·PG 대상 raw 명령과 LIST_WATCHERS·MAPEXT·CRC32C CHECKSUM 구현; bounded PGNLS·watch 등록/알림·명시 ACK 및 bounded backoff·원 epoch ACK·늦은 ACK tombstone·writer gate 구현; 제한 HASHPSPOOL·8 PG v8 request profile 및 명시 reply-v8 profile 실검증(구성별 기존 13 사례와 별도 새 복합 요청 3 사례); 기존 request-v8/reply-v6의 [실제 pg_upmap/items](integration/fixture/OSD_UPMAP_VALIDATION.md)는 3 BlueStore와 별도 2-replica 소유 풀에서 CRC/AES IPv4·secure/AES256K IPv6 각 2 사례 검증; [untraced request-v9/reply-v8](integration/fixture/OSD_REQUEST_V9_VALIDATION.md) 고정 profile과 fresh native 5종·single BlueStore CRC/AES IPv4 및 single MemStore secure/AES256K IPv6 각 16 사례 검증 |
| P2 | MDS 인증·지도·session → MDS 대상 명령 및 metadata·필수 이벤트 → 상태 복구·결합 I/O | MDS ticket·공개 MON 지도 구독·명시 대상 공개 MDS 연결/raw 명령·갱신 구현 및 제한 실검증; [CephFS session reclaim(MClientReclaim/Reply)](integration/fixture/MDS_RECLAIM_VALIDATION.md) 구현·실검증(대상 session 제거·blocklist·주소 일치, 없는 uuid, native encoder vector·EPERM·failover 미검증); CephFS session OPEN/갱신/CLOSE 구현 및 제한 실검증; opt-in metadata 요청·push·cap 응답 구현(native vector 일치와 실제 MDS 실검증); Messenger 재개·새 incarnation reconnect·REPLAY/같은 TID 재제출·공유 nonce fencing·다중 rank forward와 standby takeover 실검증; MDS와 결합한 OSD 데이터 I/O 미검증 |
| P3 | EC·추가 feature/플랫폼 profile와 장시간 검증, 후속 제품 경로를 지원하는 최종 시험 환경 | 현재 일부 profile만 실검증 기록 있음; 전체 완료 아님 |

**O0 wire 목록·O1 공개 계약과 제한 replicated 객체 쓰기·조건·복합 요청을
구현했다.** Single BlueStore에서는 native 실행·readback·응답 손실·결과
불명확·재실행 금지와 정상 재시작·SIGKILL을 검증했다. 별도의
3-replica/min_size 2 profile에서는 실제 primary SIGKILL 뒤 남은 두 replica의
native/Go bytes·version·mtime과 추가 쓰기 없는 복구를 검증했다. 같은
3-OSD profile의 전체 선택 시험 12개도 통과했으며 25개 mutation/guard
사례에는 CMPEXT EOF short length·zero-extension·rollback·FAILOK가 포함된다.
CRC/aes256k·single MemStore host profile도 같은 25개 사례와 mixed vector 5개를
포함한 9개 시험을 통과했다(BlueStore 전용 3개 제외). 추가 CRC/AES/AES·IPv4
host-relay 3-BlueStore profile도 같은 25개 사례·mixed vector 5개·실제 primary
SIGKILL을 포함한 12개 시험을 첫 실행에 통과했다. 이는 해당 profile의 로컬
검증이며 현재 원격 CI 전체 통과 판정이 아니다.
별도 fresh native mutation/guard encoder vector 5개는 front/data 생성과
Go byte 일치를 검증했다. 명시적 offline seq/ACK/priority 입력의 native
41-byte header 직렬화와 Go frame 비교도 완료했다. 이는 실제 ProtocolV2
송신 세션의 증거가 아니다. 후속 event 실검증과 넓은 구성·내구성 범위는 남았다.
같은 CRC/AES/AES host-relay 3-BlueStore 구성의 별도 동시성 시험에서는
동일 객체·연결의 16개 동시 two-op WRITE와 16개 동시 two-op APPEND를
native bytes·record 원자성·version·mtime·ONDISK·유일 TID와 대조해 통과했다.
이 시험의 성공을 다른 구성이나 요청 간 실행 순서 보장으로 확대하지 않는다.
O1은 읽기 opcode 벡터의 존재나 제한 profile의 통과로 전체 완료 처리하지 않는다.
P2의 최소 metadata 사용에도 필수 capability/lease와 복구
계약이 필요하므로 이를 P3로 미뤄 MDS 지원을 선언하지 않는다.
각 단계의 native fixture·control·oracle은 해당 구현을 검증하기 전에 준비한다.
E1의 P3는 최종 전 경로 환경 완료를 점검하는 순서이며 P0–P2에서 필요한
시험 환경의 준비를 뒤로 미루는 뜻이 아니다.

IPv6·secure·aes256k client/service key·Linux arm64 container의
3-replica BlueStore 구성도 전체 선택 12개, mutation/guard 25개와 mixed vector
5개를 첫 native 실행에 통과했다. 이 결과는 해당 legacy CRUSH profile의
IPv6 endpoint 검증이며 Windows 또는 모든 주소/저장소 조합의 증거는 아니다.

### 현재 구현과 검증 기록

확인 기준은 checkout의 제품 소스 `adc1ecd347a7dd647338ade45f7649533f729408`
및 기존 README의 실행 기록에서 시작했다. 2026-10-15 O0/O1 갱신은
현재 checkout의 구현과 [로컬 native 검증 기록](integration/fixture/OSD_MUTATIONS_VALIDATION.md)을
추가 대조했다. 초기 SHA의 새 실행이나 현재 원격 CI 전체 통과를 뜻하지 않는다.
`구현 있음`은 아래 제한 범위의 코드/API가 있다는 뜻이고, `실검증 기록`은
README에 실제 daemon·native oracle 결과가 기록되어 있다는 뜻이다.
시험 파일의 존재·합성 peer 성공과 실서버 통과를 동일 상태로 표시하지 않는다.

| ID | 구현 범위와 상태 | 기록된 검증과 남은 제한 | 근거 |
| --- | --- | --- | --- |
| B1 | msgr2.1 secure/CRC, CephX aes/aes256k, MON/MGR ticket 갱신·세션 복구: 구현 있음 | 20.2.4의 역할·키·주소별 실검증 기록; CRC·혼합 cipher·Windows 조합을 모두 검증한 것은 아님; [b01a3cf 인증 epoch CI 실패](integration/fixture/SERVICE_KEY_EPOCH_FAILURE_20261015.md)는 두 번째 키 폐기 후 첫 cold MGR 인증의 ticket 복호화 거절(-13)이며, MON이 새 MonMap을 보낸 뒤 새 key를 적용하기 전에 갱신 handshake가 이전 key의 ticket을 받은 경쟁으로 분석됨(같은 MON의 첫 MonMap 응답 뒤 한 번 더 인증하도록 수정; 로컬 두 profile 600 cold cycle에서 -13 없음, CI amd64 재확인 전); MON/MGR/OSD는 cookie 재개 없음; opt-in CephFS profile의 MDS 연결만 cookie·sequence 기반 Messenger 재개 구현(합성 검증과 실제 MDS의 유휴·진행 중 요청 재개 실검증) | [session](internal/session/handshake.go), [CephX](internal/cephx/auth.go), [검증 기록](README.md#검증-결과) |
| B2 | raw MON/MGR 명령·현재 daemon Tell·이름 지정 MON Tell 및 active MGR Tell, 준비/지도 대기·wire metadata: 구현 있음 | native 명령·지도 및 failover·context·Close 시험 기록; OSD/PG 대상 명령은 D1, 명시 MDS raw 명령은 D2에서 구현; [이름 지정 active MGR](integration/fixture/MGR_NAMED_TELL_VALIDATION.md)은 exact bare name과 GID·원주소·client ID를 고정하고 변경 시 다른 active로 대체하지 않음(CRC/AES IPv4 host·secure/AES256K IPv6 container 각 7 사례); standby 명령은 미지원 | [명령 API](cephmsgr/api.go), [MON 대상](cephmsgr/named_mon.go), [MGR 대상 계약](cephmsgr/MGR_TELL_CONTRACT.md), [지도 대기](cephmsgr/map_wait.go), [검증 기록](README.md#검증-결과) |
| B3 | MON log·config·digest 구독: 구현 있음 | native oracle·갱신·복구·bounded 큐 시험 기록; 권한 ACK·완전한 변경 이력·원격 unsubscribe 보장 없음 | [로그](README.md#mon-로그), [설정](README.md#mon-설정), [digest](README.md#mon-health상태-구독) |
| B4 | 명시적 OSD 연결·ticket·v6 stat/read·MAPEXT·CRC32C/xxHash CHECKSUM·mutation/guard opcode 벡터·per-op 결과: 구현 있음 | 20.2.4 legacy CRUSH, HASHPSPOOL/upmap 미사용 replicated pool의 bytes/stat2·갱신, single/3-replica BlueStore mutation·compound·primary SIGKILL 실검증 기록; snapshot은 O2에서 별도 추적; EC와 일반 최신 CRUSH 미지원; IPv6 secure/aes256k 3-replica BlueStore 검증; OSD Windows 및 그 밖의 주소 조합 미검증 | [OSD API](cephmsgr/osd.go), [요청](cephmsgr/osd_request.go), [encoder](internal/osd/request.go), [native 시험](integration/osd_test.go), [지원 범위](README.md#osd-statread-통신-기초) |
| B5 | MON raw OSDMap 최신 full·range 조회/연속 구독, OSD map 전달: 구현 있음 | native decoder/encoder와 epoch·bytes 비교 및 갱신·단절 기록; 내포 map 해석·적용·CRUSH는 사용 계층 책임; 전체 incremental chain 보장 없음 | [MON 조회](cephmsgr/mon_osd_request.go), [MON 구독](cephmsgr/mon_osd_maps.go), [OSD 전달](cephmsgr/osd_maps.go), [지원 범위](README.md#mon-raw-osdmap-조회구독) |
| B6 | operation context·결과 불명확·자동 재실행 금지·bounded 입력/큐·Close: 현재 경로에 구현 있음 | 실제 명령 결과·취소·장애 및 합성 peer 경합 시험 기록; sent mutation의 결과 불명확·로컬 identity·재실행 금지 검증; MDS는 M0–M2의 opt-in profile과 [명시적 복구 계약·검증 범위](cephmsgr/MDS_CEPHFS_CONTRACT.md)로 제한 | [수명과 복구](README.md#수명과-복구), [검증 기록](README.md#검증-결과), [공통 후속 계약](#공통-공개-계약과-복구-책임) |
| B7 | 제품 Go 모듈과 native fixture 모듈 분리: 구현 있음 | CGO=0 빌드/검사 및 Linux·Darwin·선택 Windows MON/MGR 실검증 기록; 모든 OS·주소·역할 profile의 통과를 뜻하지 않음 | [제품 모듈](go.mod), [fixture 모듈](integration/fixture/go.mod), [검증 기록](README.md#검증-결과) |

B1–B7은 초기 제한 범위의 구현 현황이며 완전한 RADOS/RBD/CephFS 지원표가
아니다. MDS·mutation·EC 구현이 진행되면 해당 작업 ID의 부분 구현 내용을
적고 표를 갱신한다. 별도 상위 저장소의 구현 진행률은 이 저장소에서 추정하지 않는다.

### 남은 검증과 상태 갱신 규칙

- 현재 README에는 별도 장시간 실행의 16분 31초 실패와 원인 미확정 기록이
  있다. 이후 3분 부하 통과를 그 원인의 해결로 처리하지 않는다. 1시간 이상
  운영, 미검증 패치·구성·주소/OS 조합은 P3의 별도 검증 과제로 추적한다.
- 최종 환경은 기존 `integration/fixture` 실행기가 있어도 전체 완료가 아니다.
  Mutation·MDS·EC·native RBD/CephFS와 daemon 간 필수 경로별 준비·control·
  oracle·장애·정리 recipe의 완료 여부를 별도 기록한다. 이 문서 정리에서
  fixture 저장소의 새 상태를 조사하거나 환경 완료를 판정한 것은 아니다.
- 작업별 구현 상태는 `미구현 → 부분 구현 → 구현 있음`, 검증 상태는
  `미검증 → 합성/단위 시험 → 제한 구성 실검증 → 해당 단계 수용 기준 완료`로
  따로 기록한다. `진행 중`에는 실제 코드/계약 산출물과 남은 작업을 붙이고,
  단순 우선순위 지정이나 스펙 추가를 구현 착수로 표시하지 않는다.
- 상태 변경 시 확인일·정확한 제품 SHA·변경 범위·증거 링크·Ceph/이미지·
  암호/주소/OS/profile·실행/skip·남은 제한을 갱신한다. Native 실행 기록은
  README에 두고 이 표에서 링크하며 문서 정리일과 시험 실행일을 구분한다.
  이후 소스의 과거 통과 기록 재사용에는 영향 범위와 재검증 필요 여부를 남긴다.

## 최종 목표: Messenger 통신 및 I/O 전반의 테스트 환경

2026-10-13 사용자 결정으로 최종 테스트 환경의 기준을 **선택한 Ceph 릴리스에서
Messenger를 사용하는 모든 통신 및 I/O 경로의 구현과 검증을 재현할 수 있는
환경 구축**으로 정한다. 현재의 MON/MGR 및 OSD stat/read는 이 목표의 초기
단계다. 현재 제품 API나 이미 작성된 시험만으로 최종 요구사항을 제한하지 않는다.

대상 메시지·opcode·feature는 고정 Ceph 소스와 실제 daemon의 동작을 기준으로
목록화한다. 아래 영역은 출발점이며 완전한 메시지 목록을 대신하지 않는다.
새 경로가 확인되면 요구 목록에 추가하고 필요한 topology·control·oracle을
평가한다. 아직 구현하지 않은 제품 기능도 후속 단계의 테스트 환경 요구로 기록한다.

| 영역 | 최종 테스트 환경이 표현해야 할 조건과 독립 검증 |
| --- | --- |
| 공통 Messenger·CephX | 서비스별 인증·권한·ticket 갱신/만료·key 회전, framing·feature 협상, 주소·nonce·identity, ACK·keepalive·session reset/reconnect, 결과 불명확·취소·동시성·종료 |
| CLIENT ↔ MON/MGR | raw 명령·bulk/binary 응답·Tell, 인증과 발견, 지도·로그·config·digest 구독, 지도 epoch·권한·failover·재구독 |
| CLIENT ↔ OSD | stat/read뿐 아니라 쓰기·부분 쓰기·append·truncate·삭제, 복합 opcode, xattr·omap·class call·watch/notify·snapshot 조건 등 참조 릴리스의 객체 I/O; 원본 bytes·metadata·version·per-op 결과와 완료 의미 |
| CLIENT ↔ MDS 및 CephFS I/O | MDS 인증·발견·session·metadata 요청, capability/lease·회수·reconnect, namespace·inode·snapshot·다중 rank/standby 전환과 이에 연결된 OSD 데이터 I/O |
| RBD의 기반 Messenger I/O | native RBD로 준비한 image·snapshot·clone·namespace와 관련 객체/metadata, 실제 read/write·이름/상태 변경을 기반 OSD 요청 및 독립 데이터 oracle과 대조 |
| daemon 간 Messenger 통신 | 실제 MON/MGR/OSD/MDS의 상호 통신, quorum·지도 배포·heartbeat·peering·replication·recovery·backfill·metadata 이동을 필요한 topology와 public/cluster 경로에서 유발하고 관찰 |
| 상위 서비스가 사용하는 경로 | RGW·mirror daemon 등 native 서비스가 사용하는 Messenger 기반 통신과 I/O를 유발할 fixture·oracle; HTTP/S3 같은 별도 전송은 서비스 입력/관찰 경로로 구분 |

RBD는 OSD 기반 I/O와 구분된 별도 Messenger 서버 역할로 취급하지 않는다.
CephFS는 MDS metadata 통신과 OSD 데이터 I/O를 함께 확인한다. Native 서비스의
정상 동작만으로 Go의 같은 wire 경로가 구현되었다고 판정하지 않는다.
Daemon 간 경로는 실제 Ceph daemon으로 생성하고 관찰하며, 그 경로의 시험
환경 구축이 Go 제품을 MON/OSD/MDS 서버로 구현해야 한다는 뜻은 아니다.

현재 제품은 Tentacle 이상, msgr2.1 secure/CRC와 pure Go 정책을 유지한다.
프로토콜 버전·압축·추가 feature는 시험 프로필에 명시하고, 현재 지원 검증과
미지원 협상/거절 검증 및 후속 구현 요구를 구분한다. 제품의 msgr1/msgr2.0
fallback이나 고수준 RADOS/RBD/CephFS API를 이 환경 목표만으로 추가하지 않는다.
후속 wire 기능은 codec·공개 계약·native oracle·완료 기준을 가진 별도 milestone으로
추가한다. Native CLI/librados/librbd/libcephfs 등은 개발 fixture와 독립 oracle에만
사용하며 제품 빌드·실행·전이 의존성의 native 금지 정책을 유지한다.

### 환경의 구축 및 완료 기준

- 각 경로별 요구 목록에 참조 release/commit, 메시지·opcode·feature, 구현 단계,
  필요한 daemon 수·역할·주소·network·pool/PG/replica/EC 조건, 준비할 데이터와
  인증·권한·cipher·TTL, control 입력과 oracle 출력, 오류·장애·복구·정리 수용
  기준을 기록한다. CRUSH·배치·라우팅 metadata의 준비와 검증도 test-only 소비
  계층에 둔다. EC와 추가 feature profile은 현재 replicated stat/read 검증과 분리한다.
- Fixture가 실제 native 작업의 상태를 만들고 관찰할 수 있음을 먼저 확인한다.
  환경 준비 검증, Go wire 구현 검증, 상위 서비스 동작 검증의 결과를 각각 기록한다.
  접속 성공·실행 파일 존재·generic Exec·custom adapter만으로 기능 제공을 완료로
  표시하지 않는다. 각 미완료 항목에는 부족한 primitive/recipe/검증과 선행 조건을 남긴다.
- Native client/daemon의 독립 oracle로 데이터 bytes·metadata·version·지도 epoch·
  request identity·서버 결과를 대조한다. 쓰기는 ACK·응답·실제 반영·내구성의
  해당 native 완료 의미를 구분하고, 응답 손실 후 readback과 daemon/OSD 재시작
  등으로 필요한 후조건을 검증한다. 불명확한 mutation을 자동 재실행하지 않는다.
- 경로별 daemon 정지/재시작/교체, peer 선택·방향·단계별 지연/단절, 부분 전송,
  늦은 응답·서버 거절·epoch 변경·fullness·blocklist와 취소/종료 경합을 재현한다.
  Wire 손상·합성 peer·fuzz는 제품/소비자 도구, native 상태·topology·network
  장애와 복원은 fixture의 책임으로 구분한다. 같은 시험 조건을 표현하는 공개
  primitive가 있으면 재사용하고, 필요한 image extension은 명시적으로 선택한다.
- 역할·작업별로 release·암호·주소 family·플랫폼·host/container·이미지의 검증
  조합을 명시한다. 공식 이미지와 역할 이미지의 실제 작업을 가능한 한 같은
  release·architecture·설정·입력으로 비교하고 immutable digest와 로그를 남긴다.
  Preflight는 해당 작업의 실행·oracle·정리 성공을 대신하지 않는다.
- 모든 프로필은 disposable 자원과 bounded 대기·명령·출력·진단을 사용한다.
  소유 container/network/volume/process 및 변경한 native 상태의 정리·복원을
  확인하고, 실패 시 원인·단계·정확한 SHA/이미지·증거를 credential 없이 보존한다.
- 모든 필수 경로에 재현 가능한 준비/control/oracle/장애/정리 recipe와 native
  성공 근거가 있어야 **최종 환경 구축 완료**로 판정한다. Go의 미구현 기능은
  별도 구현 상태로 남긴다. 환경 기능이 미제공·미검증이거나 필수 실행이 skip된
  경우에는 전체 완료를 선언하지 않는다. 플랫폼 제약과 선행 구현은 명시적으로
  추적하며 현재 API나 성공한 일부 프로필만으로 남은 경로를 제외하지 않는다.

### Testcontainers 요구사항과 반복 점검

`ceph-testcontainers-go`/images에 대한 요구 검토는 위 최종 목표를 기준으로
현재 시험과 후속 I/O 단계에 필요한 환경 기능을 함께 대조한다. 각 요구는
`released version`, `main`, `미머지 PR`,
`공개 primitive로 가능하지만 recipe/검증 부족`, `구현 없음`, `실검증 미확정`으로
구분하고 Go의 구현 단계도 별도 기록한다.

기존 공개 기능이 해당 조건을 실제 표현하는지 확인한 후 재사용한다. Main이나
기존 PR의 구현이 의도를 충족하면 중복 요구 이슈를 만들지 않고 릴리스 대기를
보고한다. 아직 부족한 환경 기능은 기존 이슈를 먼저 확인하고 근거와 단계별
수용 기준을 보완한다. 공식 이미지에서 가능한 작업이 담당 역할 이미지에서
실패하면 images 저장소에 구체적인 성공/실패 증거와 회귀 시험 요구를 남긴다.
소비자 전용 oracle/relay와 고수준 API 구현의 부재를 fixture 기능 부재로 바꾸어
보고하지 않는다. 반대로 현재 제품이 사용하지 않는다는 이유로 위 최종 환경의
필수 primitive 요구를 제외하지 않는다.

환경 경로 구분의 참조: [Tentacle 아키텍처](https://docs.ceph.com/en/tentacle/architecture/),
[CephFS metadata/data 경로](https://docs.ceph.com/en/tentacle/cephfs/),
[Messenger 모드와 network 설정](https://docs.ceph.com/en/tentacle/rados/configuration/msgr2/).

## 통신 계층과 사용 계층의 경계

제품은 호출자가 선택한 MON/MGR 경로로 raw JSON·bulk 입력을 전송하고 원본
출력 bytes·상태 문자열·서버 코드를 전달한다. 요청별 context, 동시성, 인증·
ticket 갱신, 주소 발견, 세션 복구와 결과 불명확 처리는 이 계층의 책임이다.
구체적인 명령 선택·인자 구성·응답 해석과 재시도·운영 판단은 사용 계층이 맡는다.
전송 입력의 JSON object·prefix 형식과 크기 검사는 통신 API의 입력 계약이며
명령별 schema 검증은 아니다.

MonMap·MgrMap·MLog·MConfig·MMgrDigest의 wire 구조를 읽고 그 metadata를
원문 그대로 전달하는 기능은 유지한다. MgrMap의 module option·activation
metadata·service URI를 읽는 것과 설정 적용·실행 모듈 계산·HTTP 접속은
구분한다. 후자는 별도 사용 계층의 책임이다. CephX의 encoded credential을
읽는 `ParseKey`는 인증 codec이며 text keyring의 entity 선택·파일 로딩과 다르다.

`NewCommand`, `ParseKeyring`와 typed command-description API는 이 경계에
맞춰 제품에서 제거한다. 호환 wrapper나 별도 공개 helper package를 만들지
않는다. 명령 catalog도 호출자가 raw 명령으로 조회하고 해석한다. Native oracle과
실클러스터 시험을 위한 명령 구성·결과 해석 helper는 `integration/*_test.go`에만
두며 모듈 활성화·설정 변경·MGR 전환은 disposable fixture의 통신 검증에 사용한다.

## 후속 제품 통신 계약

이 절은 2026-10-13 요청에 따라 추가한 **제품의 후속 통신 요구**다.
위의 최종 시험 환경 목표와 별개로 Go 통신 계층이 제공해야 할 계약을 정한다.
현재 구현·검증 완료 선언이 아니며 아래 milestone의 공개 함수명·타입은
아직 확정하지 않는다. 각 milestone에서 wire 목록·API·독립 oracle을 함께
확정한다. 현재 stat/read의 제한을 후속 제품의 최종 범위로 해석하지 않는다.

목표는 상위 레이어가 고정 Tentacle의 기능을 구현할 수 있도록 필요한
통신 primitive를 제공하는 것이다. go-ceph `v0.41.0`의 공개 기능군은 초기
누락 점검 기준으로 사용하되 Ceph 고정 소스가 wire 규약의 기준이다.
이후 go-ceph 변경은 별도 검토하며 이전 Ceph 계열·C ABI·함수명·native handle
호환성을 제품 목표로 추가하지 않는다. go-ceph의 모든 기능 제공은 통신과
상위 구현을 각각 검증해야 하는 결합 목표이며 이 스펙의 존재로 보장되지 않는다.

### 기능 목록과 레이어별 책임

각 milestone은 기능 → 메시지/opcode·encoding·feature → 공개 입출력 →
사용 계층 책임 → native oracle·장애 시험의 추적표를 작성해야 한다.
목록에 참조 release/commit, 필수 선행 기능, 지원할 profile, 구현·검증 상태를
기록하고 누락·미지원 항목도 남긴다. 아래 표는 필수 기능군이며 opcode 전수
목록을 대신하지 않는다. 고정 소스의 client 메시지와 상위 기능을 함께 대조해
필수 경로를 보완하며 관련 요구가 미완료이면 해당 기능군 완료를 선언하지 않는다.

| 기능군 | 통신 계층의 후속 요구 | 별도 사용 계층의 책임 |
| --- | --- | --- |
| 대상 지정 명령 | OSD·PG·MDS의 raw 명령·bulk 입력·binary 응답, service 인증과 대상 식별 | 명령 schema, 대상 선택·wildcard 확장, typed 관리·운영 정책 |
| 객체 요청 | 읽기·쓰기·삭제·복합 opcode·조건·xattr·omap·class call·snapshot의 wire 계약 | 객체 API, pool·namespace 선택, OSDMap 적용·CRUSH·배치·라우팅 |
| 객체 열거·이벤트 | PG 열거 요청·응답과 cursor, watch/notify·ack·오류와 연결 제어 이벤트 | pool 전체 열거·중복 처리, lock 정책, 이벤트 handler와 재등록 판단 |
| MDS | 서비스 인증·지도·session·metadata 요청·응답·capability/lease·reconnect 메시지 | filesystem 선택·요청 라우팅, 경로·inode·파일 handle·캐시·권한과 일관성 판단 |
| RBD 기반 통신 | RBD가 요구하는 OSD opcode·class payload·snapshot·watch/notify 전달(기초 [실검증](integration/fixture/RBD_BASE_VALIDATION.md): layering image의 header class call·head/snapshot 객체·LIST_SNAPS·SPARSE_READ·header watch/notify ack가 native librbd와 일치. [쓰기 실검증](integration/fixture/RBD_WRITE_VALIDATION.md): image SnapContext를 붙인 data 쓰기와 OSD clone, `rbd.set_size`·`rbd.metadata_set`, managed-lock 형식의 `lock.lock`·`lock.unlock`, librbd가 ack하고 반영하는 HeaderUpdate notify가 native librbd와 일치. [잠금 실검증](integration/fixture/RBD_LOCK_VALIDATION.md): exclusive-lock·object-map·fast-diff image에서 managed lock 소유자 판독, RequestLock·AcquiredLock·ReleasedLock notify와 librbd 자동 정책의 lock 양도, Go의 object map 갱신과 data 쓰기가 native librbd·`rbd object-map check`와 일치. librbd처럼 read mode `lock.assert_locked`와 `rbd.object_map_update`, `create(true)`와 `rbd.set_id`를 한 요청으로 보낸 결과도 native와 일치. clone은 [clone 실검증](integration/fixture/RBD_CLONE_VALIDATION.md), journaling은 [journal 실검증](integration/fixture/RBD_JOURNAL_VALIDATION.md)에서 journal header·entry 해석, Go 소비자 client 등록·commit, header notify, lock 아래 append와 librbd replay가 native와 일치) | image metadata·striping·parent/clone·copy-up·lock·group·diff·mirror·migration 로직 |
| CephFS 기반 통신 | MDS wire metadata·이벤트와 연결된 OSD I/O의 입력·결과 보존([파일 데이터 실검증](integration/fixture/MDS_CEPHFS_DATA_VALIDATION.md): striping 쓰기·cap flush, sparse 읽기, TRUNC와 truncate_size·truncate_seq를 실은 OSD READ·WRITE·TRUNCATE, revoke 중 flush, snapshot clone이 두 구성에서 native libcephfs와 일치) | 파일 layout·OSD 대상 계산, 데이터·metadata flush와 POSIX 의미 구현 |

RADOS object class의 이름·method·입출력은 원본 bytes로 전달한다.
RBD·lock 등 class별 schema·결과 해석은 상위에 두며 서버에 설치된 class나
권한·지원 여부를 통신 연결 성공으로 보장하지 않는다. CephFS volume/subvolume
관리 명령과 MDS 파일 I/O는 별도 기능군이다. RBD를 별도 Messenger 서버로
취급하거나 image 작업을 MON/MGR 명령만으로 대체하지 않는다.
RGW Admin Operations·HTTP/S3와 native pointer·라이브러리 버전 조회는
Messenger 제품의 기능이 아니다. 필요한 HTTP 클라이언트와 서비스 사용은
상위에 두고 native 라이브러리를 제품 의존성으로 추가하지 않는다.

### OSD 전체 요청·응답과 제어 이벤트

- 현재 MOSDOp/MOSDOpReply stat/read를 기반으로 write/writefull·append·
  truncate·zero·create·delete, 비교·version 조건, xattr·omap, object class call,
  allocation hint, snapshot read/write context·rollback을 단계별로 추가한다.
  Opcode별 flag·payload·data 분할·길이 제한·결과 encoding을 고정 소스로
  정하고 unsupported opcode·flag 조합은 송신 전에 거절한다.
- 한 객체에 대한 ordered compound operation과 per-op 결과·출력 경계를
  보존한다. Native의 조건 실패·fail-ok·원자성 계약을 opcode별로 명시하며
  한 요청을 여러 독립 요청으로 나눠 같은 의미라고 표시하지 않는다.
  서로 다른 객체·요청에 원자성이나 순서 보장을 추가하지 않는다.
- 입력에는 pool·namespace·locator·object·hash, 대상 OSD·지도 epoch와
  필요한 PG/shard·snapshot·요청 flag를 표현한다. Raw hash와 실제 PG seed를
  구분하며 encoding별 의미를 검증한다. PG 열거의 범위·filter·cursor와
  raw 항목·continuation·종료 정보를 상위에 전달하고 pool 순회는 수행하지 않는다.
- 응답은 request identity·incarnation·attempt, epoch·redirect metadata,
  전체 및 per-op 서버 코드, object version·원본 bytes와 완료 관련 flag를
  보존한다. Messenger ACK, 서버 실행 결과, 적용·내구성 완료를 구분하고
  지원하는 Tentacle profile에서 관찰한 완료 의미를 명시한다. Native 문서의
  complete/safe 구분을 모든 encoding에 두 응답이 온다는 가정으로 옮기지 않는다.
- Watch 등록·갱신·해제와 notify·notify ack·timeout·watch 오류의 wire를
  제공한다. Cookie·notify ID·peer·payload와 등록 수명을 보존하며 응답 TID가
  없는 unsolicited 이벤트도 별도 dispatcher로 받는다. 연결 단절을 watch의
  원격 해제나 notify 성공으로 해석하지 않는다. Lock schema와 handler 정책은
  상위가 맡고, 사용자 처리가 끝나기 전에 의미가 있는 ack를 생성하지 않는다.
- OSDMap·backoff/unblock 등 연결 제어 메시지는 참조 소스에서 필수 목록과
  handshake를 확정한다. 순수 protocol ack·대상 세션별 송신 제한은 통신
  계층이 처리하고 지도 적용·대상 변경·요청 재제출은 상위가 판단한다.
  지연 중에도 요청 context·Close는 동작해야 하며 제어 메시지를 일반 응답으로
  오인하거나 자동 재실행 근거로 사용하지 않는다.
- Replicated 및 EC의 요구를 별도 profile로 정의한다. CRUSH·upmap·pool의
  필수 feature 중 외부 지도 적용·배치가 필요한 것은 상위 구현과 결합해
  검증하고, wire 처리가 필요한 것은 통신 계층에 구현한다. Caller가 임의 feature
  bit를 광고하는 우회는 허용하지 않는다. 협상한 역할별 feature와 실제 지원
  계약을 일치시키며 MON admission 예외는 계속 MON에만 한정한다.

### MDS 인증·지도·session·metadata 통신

- MDS CephX service ticket·갱신, secure/CRC 연결, FSMap 계열·MDSMap의
  획득·구독과 bounded codec을 추가한다. FSID·filesystem ID·epoch,
  rank·daemon identity·원주소·상태·필수 client feature를 보존한다.
  권한상 보이지 않는 지도와 빈 지도·등록 ACK·연결 성공을 구분한다.
  Filesystem 선택과 metadata 대상 라우팅은 상위가 결정한다.
- Messenger feature와 MDS session의 client feature를 별도로 검증하고
  미지원 필수 feature는 접속 또는 session 준비 단계에서 명시적으로 거절한다.
  후속 연결도 caller가 선택한 동일한 msgr2.1 secure/CRC 정책을 따르며
  보안 모드·프로토콜·압축 fallback을 추가하지 않는다.
- Caller가 지정한 MDS에 대한 session open/renew/close와 stale·reject·
  eviction·재접속 상태를 구현하고 session generation·identity·sequence를
  노출한다. Messenger 연결 준비와 metadata session 준비를 구분한다.
  Multi-rank·standby 전환 시 새 daemon과 이전 rank의 관계를 지도에 따라
  검증하며 server가 요구하는 session 복구를 fresh lossy 연결로 대체하지 않는다.
- Lookup·getattr/setattr·open/create·readdir·link/unlink·rename·mkdir/rmdir·
  symlink·xattr·file lock·snapshot 등 고정 릴리스 metadata 요청의 wire
  field를 표현한다. Caller 제공 credential·inode/path·flag·조건·payload를
  인코딩하고 request ID, forward·retry·replay 관련 정보, 초기/unsafe 결과와
  safe 완료의 구분, 서버 결과와 inode/dentry·layout·snapshot metadata 및
  raw 부속 bytes를
  보존한다. OS credential 조회·경로 해석·파일 handle 관리는 상위 책임이다.
- Capability grant/revoke/recall·release·flush/ack, lease·snapshot realm 등
  필수 비동기 메시지와 재접속 payload를 codec·수신/송신 계약에 포함한다.
  CephX 권한과 inode capability를 구분하며 inode·cap ID·sequence·generation·
  flush ID·dirty metadata 등 상위가 일관성을 판단할 정보를 생략하지 않는다.
  캐시 상태·dirty data·file lock·capability의 의미 상태는 상위가 소유하고,
  통신 계층은 session·메시지 연관과 wire 제약을 소유한다.
- 복구에 필요한 상위 상태를 받는 명시적인 입력 계약과 복구 완료·거절·
  기한 만료 이벤트를 제공한다. 상태 제출·flush 완료·cap 반환을 상위가
  확인하기 전에 통신 계층이 이를 완료한 응답으로 대신하지 않는다.
  상위 미응답·큐 overflow·상태 누락으로 protocol 진행이 불가능하면
  제한된 시간 안에 session을 실패시키고 진행 중 요청의 불명확성을 드러낸다.
  MDS reconnect/replay와 OSD 데이터 flush의 결합 의미는 소비자 시험에서 검증한다.

### 대상 지정 raw 명령

OSD·PG·MDS 대상 명령에 필요한 메시지와 reply codec·권한·service 인증을
고정 소스에서 확정한다. Caller는 정확한 daemon identity 또는 PG와 지도
metadata를 지정한다. 통신 계층은 요청 context 내에서 대상·FSID·원주소와
session의 관계를 검증하고 raw 명령 벡터·bulk 입력·binary 출력·signed 서버
코드·상태 문자열을 보존한다. MonCommand/MgrCommand로 조용히 경로를
바꾸지 않으며 OSD 객체 opcode와 daemon-local 명령을 구분한다.
지도 변경·대상 소멸·권한 거절·전송 전 실패와 전송 후 결과 불명확을 구분한다.
PG primary 재선택, 대상 이동·fan-out·wildcard 확장은 상위가 결정한다.

### 공통 공개 계약과 복구 책임

- Request/reply와 unsolicited 이벤트에 peer identity·session generation·
  request/event ID·지도 epoch를 해당 wire가 제공하는 범위에서 전달한다.
  Wire에 없는 generation·cursor는 로컬 metadata로 명확히 구분하며 이전
  session의 늦은 응답을 새 요청의 결과로 배분하지 않는다.
- Request ID 발급·충돌 검증·pending 분배는 통신 계층이 맡는다. 후속 API는
  상위가 동일한 논리 요청의 protocol 재제출을 검토할 수 있도록 원래 identity와
  완료 상태를 보존한다. Messenger 재전송·service session 복구·MDS replay·
  새로운 객체/명령 실행을 구분하고 허용한 경로·identity 수명·중복 억제의
  유효 범위를 native oracle로 검증한다. 요청 bytes 보존만으로 exactly-once를
  주장하지 않으며 불명확한 mutation을 새 session에서 자동 재실행하지 않는다.
- 명령·객체·metadata·제어 송신은 operation context와 bounded 입력·in-flight
  제한을 가진다. 순서가 필요한 이벤트는 count·bytes 상한의 FIFO로 전달한다.
  Latest 값으로 교체하는 stream은 해당 wire 의미가 허용하는 경우에만 사용한다.
  Overflow 시 stream만 또는 session 전체를 실패시킬지 기능별로 명시하고
  correctness에 필요한 이벤트를 조용히 버리지 않는다.
- Event 소비자와 사용자 handler가 공유 수신 loop를 막지 않도록 한다.
  Next의 로컬 대기 취소·stream 수명 취소·원격 해제·protocol ack를 구분하고,
  최초 terminal cause·이미 접수한 이벤트·Close의 정리/대기 의미를 정의한다.
  동시 요청·제어 송신·인증 갱신·늦은 이벤트와의 경합을 검증한다.
- Frame/front/middle/data·opcode별 입력과 event bytes의 소유권을 명시한다.
  인코딩 상한·불일치·미지원 조합은 가능하면 송신 전에 거절한다. Unknown
  metadata는 허용하는 encoding에서 보존하되 필수 message/feature의 미구현을
  opaque frame 통과로 숨기지 않는다. Arbitrary raw frame 전송·임의 feature
  광고를 확장 API의 대체로 제공하지 않는다.

### 후속 milestone과 완료 기준

위 우선순위와 같은 작업 ID로 계약·선행 조건·진행상황·완료 기준을 관리한다.

| ID · 우선순위 | 제품에서 확정·구현할 계약 | 선행 조건 | 진행상황 | 필수 독립 검증 |
| --- | --- | --- | --- | --- |
| O0 · P0 | mutation별 opcode/flag·request identity·per-op 출력·완료/내구성·event 계약 확정 | B1·B4·B6 | 80개 Tentacle opcode 목록과 O1 입력/identity/per-op/ONDISK 공개 계약 확정; mutation decoder vector 5개와 별도 fresh native encoder front/data/header vector 5개 검증(offline header 입력); map/reply·future backoff/watch 목록과 미지원 control TID 충돌·combined feature 거절 합성 시험 완료; event 실검증·후속 opcode 계약 미완료 ([객체 계약](internal/osd/CONTRACT.md), [event 계약](internal/osd/EVENT_CONTRACT.md)) | native encoder/decoder·서버 처리 경로로 입력/결과 계약 대조; O1 수용 시험 확정 |
| O1 · P0 | replicated write/writefull·append·truncate·zero·create/delete와 조건·compound | O0 | 기존 11개 stat/read·mutation/guard opcode 구현; single BlueStore 11개·3-replica/min_size 2 전체 선택 12개 시험 통과; 25 mutation/guard 사례(CMPEXT EOF normalized length·zero-extension 포함)·mixed read/stat 5개 vector·응답 손실·정상 재시작·SIGKILL 검증; 실제 primary SIGKILL 후 남은 2 replica native/Go bytes·version·mtime·재실행 금지 검증; 넓은 matrix/host·device 전원 장애 미완료; WRITEFULL/APPEND/offset 0 WRITE 뒤 offset 0 READ/CMPEXT full-payload 조합 거절; [WRITESAME](integration/fixture/OSD_WRITESAME_VALIDATION.md) 별도 추가·CRC 3 BlueStore와 secure single MemStore에서 각각 19사례 검증; WRITESAME primary SIGKILL은 [3 BlueStore 1 사례](integration/fixture/OSD_WRITESAME_VALIDATION.md#primary-sigkill)로 검증 | native 쓰기·조건 실패·복합 원자성·readback·OSD 재시작·응답 손실·재실행 금지 |
| O2 · P1 | xattr·omap·class call·allocation hint·snapshot context·rollback | O1; opcode별 O0 계약 보완 | 부분 구현: [xattr 5 opcode](internal/osd/METADATA_CONTRACT.md)의 fresh wire·43 사례·loss/Close/SIGKILL·서버 제한/rollback·readonly EPERM·namespace 격리를 CI 9 OSD profile에서 검증(bc2b9df 42/42); [OMAP 읽기 3 opcode](internal/osd/OMAP_CONTRACT.md)의 fresh encoder/decoder 5 vector·Go bytes 대조, CRC/AES host 3 BlueStore Go/native 23 사례 및 secure AES256K CGO0 container 전체 OSD 22 시험 통과; [OMAP header·쓰기 5 opcode](integration/fixture/OSD_OMAP_WRITE_VALIDATION.md)의 fresh encoder/decoder 5 vector·Go bytes 대조와 CRC/AES host 3 BlueStore Go/native 17 사례 통과(쓰기 vector의 64-byte RETURNVEC 출력 한도 EOVERFLOW와 primary cache의 object_info 선반영 관찰 포함); [OMAP_CMP](integration/fixture/OSD_OMAP_COMPARE_VALIDATION.md)의 fresh encoder/decoder 5 vector·Go bytes 대조와 CRC/AES host 3 BlueStore 25 사례 통과(단일 assertion 17개는 Go 결과·native C prval·signed-char 모델 일치, arm64 OSD에서 signed 순서 확인); [OMAP 범위 삭제](integration/fixture/OSD_OMAP_RANGE_VALIDATION.md)의 반열린 구간·빈/역순 구간·ENOENT·FAILOK을 CRC/AES host 3 BlueStore 8 사례로 검증; [object class CALL](integration/fixture/OSD_CALL_VALIDATION.md)의 hello class 18 사례(Go 결과·출력 = native C prval·출력)를 CRC/AES host 3 BlueStore에서 검증; [복합 CALL](integration/fixture/OSD_CALL_COMPOUND_VALIDATION.md)은 guard·읽기, FAILOK 아닌 CREATE, read mode CALL 뒤의 CALL만 허용하고 허용 11·거절 5 사례를 CRC/AES host 3 BlueStore와 secure/AES256K IPv6 container의 3 BlueStore·single MemStore에서 native librados와 대조(write method 뒤 전체 읽기 method는 실제 OSD가 primary 복구를 반복해 거절); [allocation hint](integration/fixture/OSD_ALLOC_HINT_VALIDATION.md)의 object_info 기록을 ceph-dencoder oracle로 6 사례 검증; [snapshot context·ROLLBACK·LIST_SNAPS](integration/fixture/OSD_SNAPSHOT_VALIDATION.md)의 fresh encoder/decoder 8 vector·Go bytes 대조와 self-managed snap의 clone 생성·snap 읽기·ORDERSNAP·rollback·whiteout·목록을 native C·rados CLI·ceph-dencoder와 비교; [MAPEXT·CRC32C CHECKSUM](integration/fixture/OSD_READ_METADATA_VALIDATION.md)의 bounded 단독 head 읽기와 fresh native wire 벡터·실제 native 클라이언트 비교 구현; 후속 [xxHash32/64 CHECKSUM](integration/fixture/OSD_CHECKSUM_ALGORITHMS_VALIDATION.md)은 같은 메서드의 알고리즘별 seed/결과 폭·상한 구현, fresh native 15 vector 및 CRC/AES IPv4 host 3 BlueStore·secure/AES256K IPv6 container single MemStore에서 각각 24 사례 native 대조 통과; 후속 [snapshot CHECKSUM](integration/fixture/OSD_SNAPSHOT_CHECKSUM_VALIDATION.md)은 같은 메서드의 ReadSnapshot으로 clone/head 선택을 전달, 두 구성에서 각각 39 snapshot·24 head 사례와 요청 전후 native bytes/version/mtime·clone/whiteout 정리 검증(첫 CRC 상세 기록 수집 누락은 별도 보존); [snapshot MAPEXT](integration/fixture/OSD_SNAPSHOT_MAPEXT_VALIDATION.md)은 기존 ReadSnapshot으로 clone/head 지도 조회를 전달하고 두 구성에서 각각 exact-range 16 사례·기존 head 8 검사를 통과(동일 native offset/length, 별도 selected STAT2 version, BlueStore/MemStore 표현 차이와 상태 불변·정리 확인); [SPARSE_READ](internal/osd/SPARSE_READ_CONTRACT.md)의 bounded 단독 head extent/data codec·fresh native 5 vector 구현 및 CRC/AES host 3 BlueStore·secure/AES256K IPv6 container single MemStore에서 동일 범위 native 10 사례 통과, read snapshot SPARSE_READ 16 사례도 같은 두 구성에서 native와 일치(librbd parent·clone snapshot 형식); 단독 head [COPY_FROM/COPY_FROM2](integration/fixture/OSD_COPY_FROM_VALIDATION.md)는 native 관측 oracle로 4개 profile 7 사례(source snapshot 포함)와 응답 손실 통과(native encoder vector·복합·장애 미검증); [소유 추가 pool](integration/fixture/OSD_POOL_VALIDATION.md)에서 cross-pool·namespace COPY_FROM2와 pool snapshot 모드(읽기·LIST_SNAPS·ROLLBACK)를 4개 profile로 검증; [FULL_TRY/FULL_FORCE 요청 flag](integration/fixture/OSD_FULL_VALIDATION.md)를 quota full pool에서 4개 profile로 검증(기본 쓰기는 무응답·결과 불명확, FULL_TRY는 -122 거절, FULL_FORCE는 쓰기·삭제 성공; full ratio를 낮춘 OSD fullness에서 FULL_TRY -ENOSPC·FULL_FORCE 진행·복원 뒤 일반 쓰기 응답을 2개 BlueStore profile에서 검증; 장애 중 미검증); [BALANCE_READS/LOCALIZE_READS replica read](integration/fixture/OSD_REPLICA_READ_VALIDATION.md)를 3 BlueStore 3개 profile에서 primary·두 replica 모두 native와 일치로 검증; 마지막 쓰기 미확정과 recovery 보류 중 재합류 replica의 객체 부재가 각각 알려진 -EAGAIN(재전송 없음)으로 끝나고 이후 caller 재요청이 native bytes로 응답함을 3개 profile에서 검증(자동 replica 선택·primary 장애 중 미검증); [명시적 재제출(원 TID·incarnation·FLAG_RETRY)](integration/fixture/OSD_RESUBMIT_VALIDATION.md)을 6개 profile에서 검증: 완료된 요청과 응답 손실로 결과가 불명확한 APPEND가 재실행 없이 서버 PG log 응답으로 확정; 3 BlueStore에서 primary SIGKILL 뒤 새 primary로의 재제출도 EXCL CREATE·응답 손실 APPEND 모두 복제된 PG log로 응답; PG log·dup 추적을 줄여 trim된 요청의 재제출이 같은 identity로 다시 실행됨(한계)을 2개 profile에서 실측(split/merge 미검증); [MON MPoolOp(self-managed snapshot id 할당·삭제, pool snapshot 생성·삭제)](integration/fixture/MON_POOL_OP_VALIDATION.md)을 4개 profile에서 native 독립 관측으로 검증, read-only 권한 identity의 모든 종류 요청이 상태 변화 없이 EPERM 응답, peon MON 경유 요청의 leader 전달과 공유 id 순서, 응답 손실·session 상실 시 결과 불명확과 재실행 없음, leader MON 정지 중 peon 경유 요청의 선출 뒤 1회 실행 확인(pool 범위 OSD 쓰기 권한 경로·접속 MON 자체 장애 중 미검증); [MON statfs(MStatfs: cluster·pool 사용량)](integration/fixture/MON_STATFS_VALIDATION.md)를 4개 profile에서 native `rados_cluster_stat`와 대조; `pg` 읽기 권한 없는 identity의 요청은 무응답으로 결과 불명확 처리 뒤 session 유지 확인(pool 형태의 native twin·FSID 불일치 무응답 미검증); [MGR service map 구독(MServiceMap)](cephmsgr/SERVICE_MAP_CONTRACT.md)으로 native 등록 서비스 daemon을 `service dump`와 대조; [MON map version(MMonGetVersion)](cephmsgr/MAP_VERSION_CONTRACT.md)을 native dump epoch와 대조; [OMAP 쓰기 SIGKILL 지속성·응답 손실 뒤 재제출](integration/fixture/OSD_OMAP_WRITE_VALIDATION.md#durability-and-lost-replies-2026-10-17) 검증; [FAILOK CALL](internal/osd/CLASS_CONTRACT.md) 허용·실패 method의 staged 쓰기가 커밋됨을 관찰 고정; OMAP 새 CI matrix·권한·다중 assertion C++ oracle·snapshot 쓰기 [SIGKILL 지속성](integration/fixture/OSD_SNAPSHOT_VALIDATION.md#sigkill-durability-2026-10-17) 검증; snapshot 쓰기 응답 손실 뒤 재제출(clone 1개 유지) 검증; EC 세부 미완료 | native metadata·class 원본 bytes·snapshot 읽기/쓰기·rollback·오류/조건 비교 |
| O3 · P1 | PG 열거·cursor, watch/notify·ack/오류, backoff/unblock | O0; 알림 송신 opcode는 O1/O2; 이벤트 공통 계약 | 부분 구현: [LIST_WATCHERS](internal/osd/WATCHERS_CONTRACT.md)의 단독 head 조회·bounded watcher metadata·fresh native 5 vector; [실검증](integration/fixture/OSD_WATCHERS_VALIDATION.md)은 CRC/AES host 3 BlueStore 및 secure/aes256k IPv6 container single MemStore의 native watcher 2개·CLI·Go 목록 일치/빈 목록/ENOENT/unwatch; [PGNLS](internal/osd/PG_LIST_CONTRACT.md)의 한 페이지·opaque cursor·명시 PG/primary와 [watch/notify](internal/osd/WATCH_NOTIFY_CONTRACT.md)의 등록·명시 ping/unwatch/ACK·bounded 이벤트·signed partial timeout 구현; native PGNLS 5·watch/notify 16 wire vector 및 CRC/AES host 3 BlueStore·secure/AES256K IPv6 container single MemStore의 각 9 사례 통과; [backoff](internal/osd/BACKOFF_CONTRACT.md)의 bounded gate·명시 원 epoch ACK·late ACK tombstone·FIFO writer 순서와 재실행 금지 구현; CRC/AES single BlueStore 및 secure/AES256K IPv6 single MemStore에서 정상/늦은 ACK 각 2 사례와 native 수신 history·정확히 한 번의 fresh append 검증; [명시 watch 재연결](integration/fixture/OSD_WATCH_RECONNECT_VALIDATION.md)의 확인된 등록·cookie 유지·세대 증가·이전 제어 차단·unknown 결과 보존 구현; CRC/AES host 3 BlueStore 및 secure/AES256K IPv6 container single MemStore에서 각각 1 사례(native 알림/ACK·해제·정리) 통과; [live DISCONNECT](integration/fixture/OSD_WATCH_DISCONNECT_VALIDATION.md)(ping 없는 timeout·다른 client의 삭제, 3 BlueStore IPv4·MemStore v9 IPv6) 검증; primary SIGKILL 뒤 새 primary로의 명시 RECONNECT(cookie 유지·세대 증가·native 목록 1개·다른 client notify/ACK 완료)를 3 BlueStore 2개 profile에서 검증; 연결 끊긴 동안 보낸 notify가 RECONNECT 뒤 재전달되어 ACK로 완료됨을 2개 profile에서 검증; `allow r` identity의 WATCH는 EPERM·NOTIFY는 허용됨을 검증; [실제 PG split/merge](integration/fixture/OSD_SPLIT_VALIDATION.md)를 single BlueStore CRC/AES 및 single MemStore secure/AES256K IPv6에서 각 1 사례 검증(8→16→8·기존 tuple/unknown 보존·명시 fresh child 요청·원복/정리); [객체 BLOCK·failover 실검증](integration/fixture/OSD_OBJECT_BACKOFF_VALIDATION.md)은 3 BlueStore의 두 구성에서 `osd_backoff_on_degraded`로 만든 단일 객체 BLOCK/UNBLOCK의 원 epoch ACK·버려진 요청의 결과 불명확·명시 재제출 1회 실행과 진행 중 primary SIGKILL 뒤 재실행 없음·새 primary 명시 재제출 1회 실행을 native와 비교; 후속 [범위 gate 실검증](integration/fixture/OSD_BACKOFF_SCOPE_VALIDATION.md)에서 실제 PG/shard·head hobject 범위만 송신 전 거절하도록 수정, 같은 PG/연결의 다른 객체 STAT·READ 및 native bytes/version/mtime 일치를 CRC/AES IPv4 host·secure/AES256K IPv6 container의 3 BlueStore에서 검증(두 첫 실행 통과; 자동 대기/재실행 없음); raw OSDMap은 B5 | native 열거·알림 ack/timeout·제어 제한·큐 overflow·갱신/단절과 identity/수명 |
| D1 · P1 | OSD·PG 대상 raw 명령·bulk 입력·reply 연관 | B4·B5; 해당 명령 encoding·권한 확정 | 구현 있음: [OSD·PG 명령](integration/fixture/OSD_COMMAND_VALIDATION.md)의 `Command`/`PGCommand`, 명시 PG·primary 검증, 재전송 없음; CRC/AES host 3 BlueStore 11 사례(native byte 일치·권한 -13·비-primary -11·없는 PG -2·일시정지 취소/Close) 통과; [OSD SIGKILL 뒤 명령](integration/fixture/OSD_COMMAND_VALIDATION.md#osd-sigkill-2026-10-17) 검증(진행 중 명령의 SIGKILL은 미재현); 권한 변경 전파·CI matrix 미검증 | native 동일 대상의 raw/binary 출력·권한 오류·지도/대상 변경·취소 |
| O4 · P1 | 일반 replicated 구성의 필수 OSD feature·encoding 범위 확대 | B4·B5 및 각 feature가 요구하는 O1–O3; 사용 계층 배치 입력 | 부분 구현: [replicated-PG profile](internal/osd/O4_CONTRACT.md)의 Dial 전 고정 profile 선택·MON HASHPSPOOL admission·v8 request/실제 PG·v6 reply·backoff 구현; legacy CRUSH·HASHPSPOOL·8 PG에서 CRC/AES host 3 BlueStore 및 secure/AES256K IPv6 container single MemStore 각각 객체 I/O 5·PG 열거 3 사례 통과; [명시 reply-v8 profile](internal/osd/OSD_REPLY_V8_CONTRACT.md)의 OSD bit58 필수 협상·strict v8/compat2·Request/ListPG·동일 backoff/placement gate 구현; native 5 reply vector와 공개 합성 오류/identity/ONDISK/FAILOK·no replay 검증; [실검증](integration/fixture/OSD_REPLY_V8_VALIDATION.md)은 CRC/AES host IPv4 single BlueStore 및 secure/AES256K container IPv6 single MemStore에서 각각 기존 13 사례 통과와 별도 새 복합 요청 3 사례 통과로 구분(첫 전체 실행은 오류 응답 버전 기대값 오류로 실패, 제품 원본 값 보존은 정상); single OSD 실제 split/merge는 별도 O3 수용 시험 통과; 기본 CRUSH·실제 upmap·넓은 feature/configuration 검증 미완료; 제한 feature 거절은 B4 | HASHPSPOOL·CRUSH/upmap 등 profile별 native 대상/결과 및 미지원 거절 비교 |
| M0 · P2 | MDS ticket·FSMap/MDSMap·feature·session과 상태/복구 입출력 계약 | B1·B6 및 MDS 필수 wire 목록 확정 | 내부 기반: [MDS service ticket/authorizer](internal/cephx/MDS_SERVICE_CONTRACT.md)의 선택·갱신·키 분리·nonce/secret·atomic refusal을 AES/aes256k 합성 시험으로 검증; [FSMapUser/MDSMap codec](internal/mds/CONTRACT.md)의 현재 envelope·rank/state/address/feature/raw metadata를 canonical fresh native 5 vector·bounded/race/fuzz로 검증; [공개 raw MON 지도 구독](cephmsgr/MDS_MAPS_CONTRACT.md)의 Dial 전 MDSENC 선택·명시 FSCID·bounded FIFO·source/UUID/epoch·동일 MON 재등록 차단·취소/복구 구현; [실검증](integration/fixture/MDS_MAPS_VALIDATION.md)은 single BlueStore CRC/AES 및 single MemStore secure/AES256K IPv6에서 각각 MDS 지도 1·기존 OSD 12 사례 결합 통과; [내부 fresh MDS 연결](internal/session/MDS_FRESH_CONTRACT.md)의 고정 role·rank 0·stateful flags/cookie·feature 격리·복구 제어 거절·no replay 구현; [실제 인증/keepalive](integration/fixture/MDS_FRESH_VALIDATION.md)는 CRC/AES host IPv4 single BlueStore 2.42초 및 secure/AES256K container IPv6 single MemStore 2.21초에 actual client/service key type·단일 MON/MDS dial·live 연결 중 OPEN 세션 부재·FS/2 pool/identity 및 각 9 container 정리 검증; [공개 MDS 연결/명령](cephmsgr/MDS_COMMAND_CONTRACT.md)은 D2에서 추가; selected MDS ticket의 실제 예약 갱신·같은 GlobalID의 explicit 새 연결 검증; [CephFS session](cephmsgr/MDS_SESSION_CONTRACT.md)의 OPEN/RENEWCAPS/STALE 재개/CLOSE·REJECT·이벤트·주소 guard 구현, fresh native MClientSession 8 vector 일치, [실검증](integration/fixture/MDS_SESSIONS_VALIDATION.md)은 CRC/AES host IPv4 BlueStore와 secure/AES256K container IPv6 MemStore에서 각 9 사례 통과(native `session ls` metadata·feature `0x1ff`, 갱신, STALE 재개, 확인된 종료, 세 REJECT 원문, eviction 뒤 Lost·정확한 주소 blocklist); MDS failover 뒤 reconnect·eviction fencing·다중 rank는 M2에서 실검증; autoclose eviction 미검증 | native 지도·권한·session 준비/거절·갱신·최소 복구와 제한 입력 비교 |
| D2 · P2 | MDS 대상 raw 명령·bulk 입력·reply 연관 | M0 | [공개 API](cephmsgr/MDS_COMMAND_CONTRACT.md): 독립 EnableMDS/service2·명시 FSCID/rank/address/nonce·bounded 연결/cache·canonical type97/98 v1/compat1·raw binary/signed code·로컬 TID/GlobalID/generation·취소/종료 결과 불명확·자동 재실행 금지 구현; [실검증](integration/fixture/MDS_COMMAND_VALIDATION.md)은 legacy CRUSH/NoHashPSPOOL 단일 OSD의 CRC/AES host IPv4 BlueStore 10.53초와 secure/AES256K container IPv6 MemStore 10.32초에 각각 11사례 통과: native version/unknown-prefix/readonly -13·동시8요청·대상/cache 제한·예약 MDS ticket 갱신과 explicit 새 연결·sent cancel/Close·OPEN 세션 부재/OSD dial0; FSCID는 local metadata이며 filesystem 권한 격리 아님; CephFS session·재개는 M0–M2 소관; 명령 대상의 다중 rank 전환·failover 미검증 | 현재 rank0/address/nonce와 변경 대상의 local refusal·합성 wrong-rank/nonce 검증; 실제 다중 rank 전환·failover는 미검증 |
| M1 · P2 | metadata 요청·응답, capability/lease·snapshot 이벤트와 필수 제어 송신 | M0; capability/flush·복구 계약; 데이터 사용은 O1/O2 | 부분 구현: [opt-in CephFS profile](cephmsgr/MDS_CEPHFS_CONTRACT.md)의 필수 Messenger feature·client feature 선언, MClientRequest v6/head v3 송신·unsafe/safe 응답·forward 반환·session 종료 시 결과 불명확, versioned trace/readdir/snap trace 해석, capability·lease·snap·quota push 계수와 이벤트, 명시적 cap/lease 응답 송신 구현; request/forward/reply native 7 vector와 capability·cap release·lease·snap·quota native 10 vector 일치; [실검증](integration/fixture/MDS_CEPHFS_VALIDATION.md)은 CRC/AES host IPv4 BlueStore와 secure/AES256K container IPv6 MemStore에서 각 9 사례 통과(native LibCephFS 대조 mkdir/create/setxattr/readdir/lookup, native writer의 REVOKE 대기와 Go UPDATE 응답, cap 반환, 확인된 종료); [push 실검증](integration/fixture/MDS_CEPHFS_PUSH_VALIDATION.md)은 두 구성에서 각 7 사례 통과(dentry lease REVOKE/RELEASE, snapshot UPDATE/SPLIT/DESTROY, quota, RECALL_STATE와 cap 반환, cache drop 중 FLUSHMSG ack); EXPORT/IMPORT·forward·다중 rank는 [rank 실검증](integration/fixture/MDS_CEPHFS_RANKS_VALIDATION.md)에서 통과; [완료 실검증](integration/fixture/MDS_CEPHFS_COMPLETION_VALIDATION.md)에서 FORCE_RO·lease RENEW·FLUSHSNAP 통과; [파일 데이터 실검증](integration/fixture/MDS_CEPHFS_DATA_VALIDATION.md)에서 dirty FILE_WR UPDATE·FLUSH_ACK, buffered write의 REVOKE→OSD writeback→cap 반환과 단일 snapshot 읽기 통과; [dirty-data cap-snap 실검증](integration/fixture/MDS_CEPHFS_CAPSNAP_VALIDATION.md)에서 두 구성 각 12개 data 사례 통과: 두 snapshot 전 A/B dirty 세대 보관, old-context OSD writeback 뒤 각각 FLUSHSNAP·정확한 inode/follows/TID/dirty ACK와 A/B snapshot·C head의 native bytes/size/mtime·clone 대조; local pending ACK 중첩 관측(서버 실행 동시성은 미입증), dirty cap-snap의 MDS failover/reconnect는 미검증 | native metadata·다중 client capability/lease·snapshot·서버 거절·취소/종료 |
| M2 · P2 | 상위 상태 제출·flush/ack·reconnect/replay·다중 rank·OSD 결합 I/O | M1·O1/O2; 상위 test-only 상태 제공자 | 부분 구현: CephFS profile MDS 연결의 cookie·sequence 기반 Messenger 재개(RETRY/RETRY_GLOBAL/RESET, 미확인 frame 재전송, gapless 수신; 실제 MDS에서 유휴·진행 중 요청 재개 검증)와 새 MDS incarnation에 대한 MClientReconnect v5 상태 제출·OPEN/CLOSE 판정 구현(합성 검증, MClientReconnect native 3 vector 일치); 호출자가 명시한 결과 불명확 요청의 같은 TID 재전송(unsafe는 REPLAY와 대상 inode, 무응답은 retry 증가, cap release 제거)을 reconnect보다 먼저 송신(합성 검증); CephFS profile의 MDS·OSD 연결 공유 nonce로 eviction blocklist가 OSD I/O까지 fencing; [실검증](integration/fixture/MDS_CEPHFS_RECOVERY_VALIDATION.md)은 두 구성에서 각 5 사례 통과(rank 0 MDS 재시작 뒤 재개 RESET→Lost, 새 incarnation reconnect OPEN, REPLAY create 같은 inode로 safe 완료, 무응답 create 1회 실행, eviction 뒤 OSD -108, FlushJournal 지연 단축); [다중 rank 실검증](integration/fixture/MDS_CEPHFS_RANKS_VALIDATION.md)은 active 2·standby 1 구성의 두 profile에서 rank별 session, forward와 같은 TID 재제출 API, cap EXPORT/IMPORT와 FLUSHMSG ack, rank 0 standby takeover 뒤 reconnect 통과; [완료 실검증](integration/fixture/MDS_CEPHFS_COMPLETION_VALIDATION.md)에서 응답을 잃은 create·mkdir(LOOKUP 변환)과 REPLAY의 완료 기록 응답, reconnect 거절과 시한 초과 뒤 CLOSE·OSD -108 통과; [standby-replay·3 rank 실검증](integration/fixture/MDS_CEPHFS_RANKS_REPLAY_VALIDATION.md)에서 2단 forward 재제출, export 중 요청 forward, follower takeover 뒤 reconnect 통과; rank 0 외 follower·forward 중 rank 장애 미검증 | MDS failover·eviction·blocklist·dirty state·늦은 응답·데이터/metadata 후조건 |
| V1 · P3 | EC/shard·추가 feature/주소/OS 및 장시간 시험 profile | 해당 OSD/MDS 기능과 독립 native fixture/oracle | 부분 구현: 2026-10-17 사용자 결정으로 CRUSH_V2(bit 36)를 MON·OSD admission 전용으로 광고하고 `OSDPlacement.Erasure/Shard`로 EC PG primary shard를 지정; k=2,m=1 overwrite pool에서 쓰기·부분 덮어쓰기·append·xattr·truncate·삭제를 native와 대조하고 OMAP -95 거절, 잘못된 shard 무응답을 확인([검증](integration/fixture/OSD_ERASURE_VALIDATION.md)); EC PG listing(shard 지정)·한 shard OSD down 상태의 재구성 읽기·primary SIGKILL 뒤 새 shard로만 응답함을 검증; self-managed snapshot·shard down 중 쓰기와 복구 뒤 일관성 검증; watch/notify 검증; Tentacle optimized EC(`allow_ec_optimizations`)에서 같은 시나리오와 primary 장애 뒤 parity shard 승격을 검증하고, SERVER_TENTACLE 미선언 세션에서 OSD가 pg_temp 있는 optimized PG의 shard를 pre-Tentacle primary-first 번호로 해석함을 실측해 `OSDPlacement.PrimaryFirst`로 실제 shard를 그 번호로 encode(backoff gate는 EC PG의 모든 shard 차단); 다른 EC profile 미검증, MSR rule은 `require_min_compat_client squid`가 필요해 미지원; 현재 profile은 B1–B7; [OSD soak](integration/fixture/OSD_SOAK_VALIDATION.md)(worker별 client·연결 교체·짧은 deadline의 결과 불명확을 명시 재제출로만 확정, 끝에 native 비교)과 OSD SIGKILL 반복 중 caller식 재배치·명시 재제출 soak을 2개 profile에서 2분 통과·CI 4분 행 추가; MON 장애 동반·더 긴 장시간 검증 미완료 | EC 별 데이터/조건/완료 의미, OSD IPv6/Windows 등 선택 profile·갱신/장애·장시간 실패 원인 |
| E1 · P3 | 최종 환경의 mutation·MDS·RBD/CephFS 기반·daemon 간 경로 recipe | native fixture와 topology/control/oracle; Go wire 검증에는 해당 제품 단계 | 실행기 부분 제공; 전 경로 환경 완료 미확정 | 경로별 준비·실행·native oracle·장애·정리 근거; 환경/Go/상위 결과 별도 기록 |

이 표의 후속 기능은 구현·검증 미완료다. 세부 작업은 세분화할 수 있지만
필수 요구를 생략하지 않는다. P3도 최종 목표의 필수 검증이며 M0–M2와 같이
이미 구현한 기능의 정확성에 필요한 복구는 해당 구현 단계에서 완료해야 한다.
예를 들어 대상 MDS 명령은 MDS 인증·발견이, metadata 사용은 필요한
capability/lease 이벤트가 먼저 있어야 한다. EC는 별도 구현·검증 milestone이며
현재 replicated stat/read의 성공을 EC 지원으로 승격하지 않는다.

각 단계의 완료에는 고정 wire 목록·공개 입출력·상위 책임·unsupported 오류,
독립 bytes/metadata/서버 결과 oracle, 인증 갱신·보안 모드·동시성·취소·Close와
장애·복구 시험이 필요하다. 완료·내구성은 해당 Tentacle 서버의 flag·처리 경로와
native client로 검증하고 version별 의미를 기록한다. 필수 case가 skip되거나
준비만 성공한 경우에는 구현 또는 검증 미완료로 남긴다. 합성 peer·fuzz·race와
CGO=0·대상 OS 검증도 수행하되 실제 Ceph 상대 시험을 대신하지 않는다.

Native librbd/libcephfs가 만든 데이터만 Go에서 읽는 시험은 기초 검증이다.
상위 RBD·CephFS 기능 동등성은 소비자 구현으로 실제 작업·복구·동시성을
실행한 별도 결과가 필요하다. 통신 단계 완료, 상위 기능 완료, 최종 환경 구축
완료를 각각 기록하며 daemon 간 환경의 완성을 Go 서버 구현 요구로 바꾸지 않는다.

참조: [Tentacle librados 계약](https://docs.ceph.com/en/tentacle/rados/api/librados/),
[CephFS I/O 경로](https://docs.ceph.com/en/tentacle/cephfs/cephfs-io-path/),
[inode capability](https://docs.ceph.com/en/tentacle/cephfs/capabilities/),
[CephFS 지도·client feature 관리](https://docs.ceph.com/en/tentacle/cephfs/administration/).
Wire encoding·ID·feature와 라이선스/출처는 위 고정 Ceph commit의 메시지·client
소스를 milestone별로 확인한다. 이 목록은 소스 전수 분석·상호운용 검증의
완료 기록이 아니다. Go-ceph의 공개 API는 기능 누락 비교에만 사용하며
native 구현 코드나 의존성을 제품에 가져오지 않는다.

## 참조 버전과 지원의 의미

현재 조사에서 확인한 최신 Tentacle 패치는 `v20.2.4`다. 최초 구현의 고정 참조는 이 태그와 커밋 `7f793731f1b39eb4f465e960113d2363c311b964`로 한다. 변경되는 `tentacle` 브랜치는 후속 변경 조사에 사용한다.

Tentacle을 최소 계열로 삼는 것과 모든 20.2 패치 및 이후 모든 계열에서 동작을 검증했다는 것은 다르다. 실제 지원표에는 Ceph 버전, 인증 키 타입, 연결 모드 및 통합 테스트 결과를 기록한다. 이후 계열은 검증 후 지원 대상으로 추가한다.

msgr2.1 기능 비트는 Tentacle 버전 증명이 아니다. 런타임에서는 인증된 MonMap의 `min_mon_release >= 20`을 요구한다. 이는 클러스터의 최소 MON 계열 설정 확인이며 각 daemon의 정확한 패치 버전 증명이 아니다. 최소 설정이 이전 계열인 업그레이드 중 클러스터도 거부한다. 실서버 검증에 사용한 정확한 패치와 커밋은 시험 이미지의 `ceph --version`으로 확인했다.

참조: [Tentacle 릴리스 노트](https://docs.ceph.com/en/latest/releases/tentacle/), [고정 소스](https://github.com/ceph/ceph/tree/7f793731f1b39eb4f465e960113d2363c311b964).

## 구현할 프로토콜

초기 전송은 msgr2.1을 대상으로 한다. msgr1과 msgr2.0 연결로의 fallback을 만들지 않는다. 협상한 필수 기능이 부족하면 명시적인 오류를 반환한다. 아직 구현하지 않은 기능을 지원한다고 광고하지 않는다.

2026-10-03 사용자 결정으로 공개 연결 모드에 명시적 `crc` 선택을 추가한다.
`Options.ConnectionMode`의 기본값은 `SecureMode`이며 `CRCMode`를 지정할 때만
인증 후에도 msgr2.1 CRC framing을 유지한다. 두 모드 모두 CephX 인증과
AUTH_SIGNATURE transcript 검증을 수행한다. CRC는 이후 통신을 암호화하지
않으며 frame CRC32C는 전송 손상 검출용이다. 각 연결은 선택한 모드 하나만
제안하고 서버가 다른 모드를 선택하면 거부한다. MON/MGR 재접속·ticket 갱신·
이름 지정 MON Tell의 독립 연결에도 동일한 선택을 적용하며 자동 fallback은 없다.
인증 전 교환에는 두 모드 모두 CRC framing을 사용한다. 압축 협상 절차는
구현하되 알고리즘 목록을 비워 압축을 사용하지 않는다. 압축 frame은 거부한다.

고정 Tentacle 소스의 `Auth.h`는 CRC에서 connection secret 길이 0을 요청한다.
MON 응답의 암호화된 빈 secret과 MGR authorizer의 nonce-only version 1을
CRC에서 처리하되, ticket 암호·nonce·구조를 계속 검증한다. Secure는 version 2와
최소 40-byte connection secret을 요구한다. 길이 검증을 완화해 인증 미완료를
성공으로 처리하지 않으며 AUTH_DONE과 transcript 서명 확인 전에는 세션을
게시하지 않는다.

참조: [인증 모드와 secret 길이](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/auth/Auth.h),
[MON secret 응답](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/auth/cephx/CephxServiceHandler.cc),
[MGR authorizer 응답 version](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/auth/cephx/CephxProtocol.h).

2026-10-17 사용자 결정으로 CRUSH_V2(bit 36)는 erasure-code rule이 있으면 MON과 OSD가 모든 CLIENT에 요구하므로 MON·OSD 세션 모두에 admission 전용으로 광고한다(CRUSH 계산 지원이 아님, [EC 계약](internal/osd/ERASURE_CONTRACT.md)). 실제 Tentacle MON은 명령 전용 CLIENT에도 CRUSH 세대 비트를 접속 요건으로 요구했다. 초기 구현은 고정 참조에서 확인한 이 비트들을 MON 세션에서만 광고한다. 이는 명령 전용 접속을 위한 제한된 예외이며 CRUSH 계산 지원을 의미하지 않는다. OSD 단계에서도 이 예외를 확장하지 않으며 OSDMap 구독·자동 배치를 암묵적으로 시작하지 않는다. 구현하지 않은 필수 기능은 오류로 거부한다.

## OSD 통신과 객체 I/O 기초

2026-10-04 사용자 결정으로 OSD 통신을 제품 범위에 추가한다. 첫 목표는
OSD CephX ticket 획득·갱신, secure/CRC 연결, 호출자가 명시한 OSD 대상의
stat/read 요청과 bounded 응답 처리다. 연결 대상 주소·OSD ID와 요청의
pool·object locator·raw hash·지도 epoch는 사용 계층이 제공한다.
풀·객체 이름만 받아 위치를 계산하거나 다른 OSD로 요청을 옮기지 않는다.

첫 codec은 Tentacle의 실제 feature 협상으로 선택하는 MOSDOp v6와
MOSDOpReply v6다. v6의 요청·응답 `pg_t` seed는 객체의 raw hash이며 실제
PG 번호와 다르다. 독립적인 PG/shard 지정과 erasure-coded pool 지원은
이 milestone에서 제공하지 않는다. 초기 실서버 시험은 replicated pool이다.

OSD의 public CLIENT 정책은 `stateless_registered_server`인 lossy 세션이다.
Cookie 기반 재개는 선행 필수가 아니지만 같은 client의 새 연결이 기존
연결을 대체할 수 있으므로 대상별 세션을 공유하고 수명·갱신을 관리한다.
OSDMap의 CRUSH·upmap·pool 설정에 따라 CLIENT 필수 feature가 달라지므로
현재 구현이 제공하지 않는 feature는 명시적으로 거부한다. MON admission
예외를 OSD에 재사용하거나 호출자가 임의 feature 비트를 광고하게 하지 않는다.

`MOSDOp/MOSDOpReply`의 request identity, incarnation, retry attempt, 지도
epoch, per-op 결과, 객체 version 및 원본 data를 보존한다. Messenger ACK를
객체 실행 성공이나 durable write 완료로 해석하지 않는다. Context 취소는
로컬 대기만 종료하며 전송이 시작된 요청은 결과 불명확 계약을 유지한다.
새 세션에서 요청을 자동 재실행하지 않고 지도·redirect·서버 오류 정보는
사용 계층이 판단할 수 있게 전달한다. 아직 구현하지 않은 opcode, reply
encoding 또는 필수 feature는 조용히 대체하지 않는다.

검증은 disposable OSD와 replicated pool을 사용해 native client가 준비한
객체의 stat/read 결과·부분 읽기·binary bytes·서버 오류를 비교한다. 미지원
feature 접속 거부, 인증 갱신, 동시 호출, 취소와 종료도 검사한다. 제한한
fixture 구성에서 성공한 결과를 일반 CRUSH 설정이나 완전한 RADOS 지원으로
표시하지 않는다. 제품 코드에는 Ceph CLI·librados 의존성을 넣지 않는다.

후속 통신 구현은 OSD가 stale epoch 요청 등에 응답해 보내는 `MOSDMap`을
bounded FIFO로 전달한다. 현재 Tentacle feature 협상의 v1 및 current v4
envelope에서 FSID·trim/latest epoch·ordered full/incremental blob·원본 front를
보존한다. 내포한 지도는 opaque bytes이며 CRUSH·OSDMap 적용과 target 선택은
사용 계층의 책임이다. 이 OSD 수신 경로 자체는 MON 구독을 시작하지 않는다.
큐 손실이 incremental chain을 깨뜨리지 않도록 overflow는 OSD 세션을 실패시키고
이미 접수한 지도는 첫 terminal cause보다 먼저 전달한다. Context는 map의
로컬 수신 대기만 취소한다. 지도 수신을 요청 완료나 자동 재실행의 근거로 삼지 않는다.

참조: [OSD 연결 정책](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/ceph_osd.cc#L598),
[OSDMap 기능 요건](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/OSDMap.cc#L1754),
[MOSDOp](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/messages/MOSDOp.h),
[MOSDOpReply](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/messages/MOSDOpReply.h),
[MOSDMap envelope·peer feature 재인코딩](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/messages/MOSDMap.h#L106),
[OSD map 전달](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/osd/OSD.cc#L7555).
고정 소스의 LGPL-2.1 고지를 확인하며 C++ 코드를 복사하지 않고 wire 의미를
독립적인 Go 구현으로 작성한다. 이 절은 목표이며 검증된 지원 목록은 아니다.

## MON raw OSDMap 획득

`RequestOSDMaps`의 zero 요청은 cursor 0의 ONETIME 구독으로 최신 full map을
받는다. 지정한 full/incremental inclusive range는 `MMonGetOSDMap`으로 요청한다.
응답에는 TID가 복사되지 않으므로 각 조회는 별도 인증·MonMap admission을
통과한 MON 연결을 소유하고 다른 조회·구독과 응답을 격리한다. `MaxInFlight`를
기존 명령과 공유하며 operation context와 Client Close가 setup·수신·정리를 소유한다.
서버는 retained/current epoch와 count/bytes 상한으로 범위를 자를 수 있다.
한 개의 빈 또는 partial envelope도 정상 응답이며 자동 범위 완료·재요청은 없다.

`WatchOSDMaps`는 client당 하나의 shared MON 구독으로 OSD ticket 없이 동작한다.
0은 full baseline, 양의 `StartEpoch`는 inclusive history를 요청한다. MON `osd`
read capability가 없으면 서버는 구독을 조용히 버릴 수 있으므로 로컬 등록·ACK·
timeout을 권한 승인·거절이나 지도 chain 완료로 해석하지 않는다. FIFO 64개와
byte bound를 적용하며 overflow는 watch만 끝내고 shared MON 명령을 보존한다.
인증 갱신과 MON 재접속은 마지막 큐 접수 blob의 최대 epoch + 1부터 재구독하며
최초 StartEpoch보다 낮추지 않는다. `NewestMap`은 광고값이고 적용 cursor가 아니다.
Trim fallback·빈 batch·중복·이전 watch의 늦은 메시지를 원본 그대로 드러낸다.
내포한 map 해석·적용, 누락 판단과 전체 chain 확보는 사용 계층이 맡는다.

MON에도 이미 구현한 PGID64 wire capability를 협상해 classic v6 full/incremental
map을 받는다. CRUSH admission 예외를 확장하는 것이 아니며 PGPOOL3·OSDMAP_ENC·
INCSUBOSDMAP·backoff/upmap·새 객체 reply feature를 함께 추가하지 않는다.
내포한 지도는 여전히 opaque bytes다. 독립 native class decoder와 같은 peer
feature의 native encoder로 FSID·epoch·원본 bytes를 대조한다. Full map은
개발용 `ceph osd getmap <epoch>`로 같은 epoch의 canonical map을 받아 native
encoder 입력으로 삼는다. 서버의 canonical→peer 변환과 같은 경로이며,
수신한 full map을 decode→encode했을 때 unordered blocklist의 순서가 바뀌는
것을 byte 오류로 오인하지 않는다. Incremental은 ordered container와 opaque
embedded full/CRUSH bytes를 보존하므로 수신 blob의 native 재인코딩 bytes를
직접 비교한다. JSON dump는 지도 데이터 일부를 생략하므로 byte 비교를 대신하지 않는다.

참조: [MMonGetOSDMap](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/messages/MMonGetOSDMap.h),
[range 응답](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/mon/OSDMonitor.cc#L2819),
[구독·trim 처리](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/mon/OSDMonitor.cc#L4867),
[구독 권한](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/mon/Monitor.cc#L5394).
LGPL-2.1 고지를 확인하고 C++ 코드를 복사하지 않고 wire 의미를 독립 구현한다.

## Messenger 구현 항목

구현 항목은 다음과 같다.

1. Ceph little-endian 인코딩, 길이 제한, 구조체의 version/compat version, 주소와 주소 벡터, 시간·문자열·컨테이너 인코딩.
2. Banner, msgr2 feature 협상, HELLO 및 인증 frame 교환.
3. 32-byte preamble, segment, CRC32C, secure framing, AES-GCM tag 검증과 방향별 nonce 상태.
4. AUTH_SIGNATURE를 포함한 인증 교환 및 세션 식별.
5. Message header, front/middle/data, sequence, ACK, keepalive.
6. Global ID, sequence, 종료, MON/MGR 변경과 재인증을 처리하는 상태 머신. MON/MGR/OSD는 fresh lossy session으로 복구하며 이전 Messenger cookie로 session을 재개하지 않는다. opt-in CephFS profile의 MDS 연결은 native의 lossless client처럼 SESSION_RECONNECT로 같은 Messenger session을 재개하고 확인받지 못한 frame만 원래 sequence로 재전송한다. 새 세션으로 명령을 재실행하지 않는다. 이 현재 방식은 후속 MDS session·capability·요청 복구 계약을 대신하지 않는다.
7. 서버가 보내는 입력의 길이·개수·산술 overflow 검증과 메모리 사용 상한.

Tentacle이 실제 사용하는 구조체의 이전 encoding version을 읽는 것은 현재 wire 규약 구현이다. 이를 이전 Ceph 계열이나 이전 공개 API 지원과 혼동하지 않는다. C++ 구현의 객체 수명·락·메모리 배치를 그대로 이식하지 않고 wire 의미와 상태 전이를 Go로 표현한다.

참조: [Tentacle msgr2 규약](https://docs.ceph.com/en/tentacle/dev/msgr2/), [고정 frame 정의](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/msg/async/frames_v2.h).

## CephX와 MON/MGR

CephX는 MON 인증, global ID, service ticket 획득, MGR authorizer, ticket 만료·갱신까지 구현해야 한다. 한 번 인증에 성공하는 것으로 완료하지 않는다.

20.2.4에는 인증 보안 수정과 새 CephX 키 타입 `aes256k`가 도입됐다. 최초 PoC는 이 키 타입을 사용하는 현재 Tentacle 클러스터에서 검증한다. 기존 `aes`와 새 `aes256k`의 지원 여부는 지원표에서 구분해야 하며, 기존 키만으로 동작하는 구현을 현재 Tentacle 인증 지원 완료로 표시하지 않는다. Tentacle 초기 패치와 기존 키까지 지원할 때 필요한 범위는 고정 소스와 실클러스터에서 확인한다. 이는 최소 지원 계열을 임의로 변경하는 근거가 아니다.

CephX 키·ticket 암호와 Messenger secure framing 암호는 별개의 계층이다. 새 키 타입이 도입됐다고 Messenger의 AES-GCM frame 규약까지 바뀌었다고 판단하지 않는다. 일반 CephX 설명 문서에만 의존하지 않고 참조 패치의 소스와 보안 수정도 확인한다.

MON 기능:

- 여러 seed 주소, IPv4/IPv6 및 msgr2 주소를 통한 bootstrap.
- FSID 일치 확인, MonMap 수신·갱신, 접속 MON 변경.
- 명령 메시지와 응답, transaction ID에 따른 동시 요청 분배.
- MonMap의 정확한 MON 이름을 지정하는 독립 daemon-local Tell.
- MGR 발견에 필요한 MgrMap 구독 및 갱신.
- cluster log 구독, service cursor와 메모리 상한을 갖는 수신 stream.
- 인증된 client identity의 effective config를 raw 전체 map으로 받는 구독과
  초기 map 요청. Host는 호출자의 `Options.Hostname`을 보내고 device class는
  비운다. 반환 map을 Go Options에 적용하지 않는다.
- MON의 주기적인 `mgrdigest` 구독으로 health detail·MON 상태 raw JSON을 받는다.
  MGR entity나 연결을 요구하지 않으며 지도·cursor가 없는 최신 값 stream이다.

MGR 기능:

- MgrMap의 active MGR 주소를 사용한 별도 연결과 service 인증.
- MGR 명령과 응답, 서버 오류 code·message의 원문 보존. 모듈 미지원·권한 오류의 의미 해석은 사용 계층이 맡는다.
- active MGR 변경 시 연결 갱신과 진행 중 요청의 결과 처리.

MON 명령과 MGR 명령은 명시적인 API로 구분한다. 명령 prefix만 보고 임의로 경로를 추측하지 않는다. 명령의 JSON, bulk 입력, binary 출력, 상태 문자열과 서버 오류 코드를 각각 보존한다.

현재 접속한 MON과 active MGR의 daemon-local 명령은 `MonTell`·`MgrTell`로 구분한다. `MonTellTo(ctx, name, command)`는 MonMap의 정확한 bare MON 이름을 지정한다. Prefix 제거·숫자의 rank 해석·wildcard 확장은 제공하지 않으며 MGR 대상은 active MGR로 한정한다. Tentacle의 `MCommand`(97)는 인증된 nonzero FSID와 command 문자열 벡터를 보내며, TID는 Messenger header에 둔다. `MCommandReply`(98)의 raw data, signed code, 상태 문자열을 보존한다. MGR의 zero-FSID legacy module 경로로 바꾸지 않는다.

`MgrTellTo(ctx, name, command)`는 현재 available active의 정확한 bare 이름에 한정한다. MgrMap standby에는 주소가 없으므로 standby 접속·승격 대기를 제공하지 않는다. 기존 MGR session·ticket을 공유하고 최초 선택한 name·GID·보존된 v2 원주소 벡터·client GlobalID를 setup과 writer admission에서 고정한다. Binding이 바뀌면 전송 전 로컬 거절 또는 시작한 명령의 결과 불명확을 반환하며, 다른 active로 대체하거나 자동 재실행하지 않는다. [공개 계약](cephmsgr/MGR_TELL_CONTRACT.md)과 [두 구성의 focused 수용 결과](integration/fixture/MGR_NAMED_TELL_VALIDATION.md)를 구분한다.

이름 지정 MON Tell은 주 MON admission 후 지도에서 대상의 v2 원주소를 선택한다. 이름·rank·전체 주소 벡터를 보존하며 family·nonce·scope·flow를 접속 문자열로 재해석하지 않는다. 지정 MON에 global ID 0으로 새 CephX 인증을 수행하고 `MMonGetMap`(5, header 1/compat 0, 빈 payload)의 독립 지도에서 FSID·최소 지원 계열·같은 이름과 원주소의 일치를 확인한다. 주 MON/MGR 연결·auth·map·log watch는 이 결과로 변경하지 않는다. 주 지도에 대상이 없으면 `ErrMonitorNotFound`, 독립 admission의 대상 연관이 바뀌면 `ErrMonitorTargetChanged`로 전송 전 실패를 알린다. 전송 후 변경은 결과 불명확 원인이 될 수 있다.

일반 명령과 같은 `MaxInFlight` 슬롯을 입력 복사 전에 얻는다. Operation context는 전체 호출과 Client Close를 연결하며 `ConnectTimeout`은 endpoint별 setup만 제한한다. Client Close는 독립 handshake와 session 정리를 완료할 때까지 기다린다. 전송 전 setup의 일시적인 전송·연결 오류에 한해 같은 이름의 다른 v2 주소를 시도할 수 있지만, 다른 MON으로 대체하거나 전송 결과가 불명확한 Tell을 다시 실행하지 않는다.

Tell도 일반 관리 명령과 같은 context·동시 호출·결과 불명확·자동 재실행 금지 계약을 따른다. native Ceph client의 Tell 재전송 동작은 복사하지 않는다. MON Tell의 read·write·execute 또는 MGR Tell의 allow-all 권한 부족은 인증 실패와 구분한 서버 명령 오류다.

참조: [MCommand](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/messages/MCommand.h), [MCommandReply](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/messages/MCommandReply.h), [daemon-local command 처리](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/common/admin_socket.cc).

이름 지정 경로의 참조는 [MonClient의 독립 Tell 연결](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/mon/MonClient.cc#L1238)과 [MMonGetMap](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/messages/MMonGetMap.h)이다. LGPL-2.1 고지를 확인했고 C++ 구현을 복사하지 않고 wire 의미를 독립 작성한다.

MON 로그는 `log-debug/info/sec/warn/error`의 연속 구독과 `MLog`(52)를 사용한다.
현재 LogEntry v5의 entity·rank·주소·원본 timestamp·sequence·priority·text·channel을
보존하며 기존 feature 집합을 확장하지 않는다. Batch version은 MON log-service
cursor이며 entry sequence와 다르다. Start cursor 0은 서버의 마지막 committed
batch, 나머지는 inclusive cursor다. 현재 admission을 통과한 MON과 FSID만 허용하고
재접속은 마지막으로 큐에 접수한 version 다음부터 구독한다. 최대 uint64는
증가시키지 않고 명시적으로 거부한다.

구독 등록 성공이나 SubscribeAck를 읽기 권한 승인으로 해석하지 않는다.
종료는 로컬 처리이며 원격 unsubscribe와 무손실 수신을 보장하지 않는다.
서버의 history 누락 WARN 등 낮은 priority도 거르지 않고 raw entry로 보존한다.
MLog에는 구독 generation ID가 없어 같은 세션의 이전 watch에서 늦게 온 batch를
완전히 구분할 수 없다. C++ 구현을 복사하지 않고 고정 wire 의미를 독립 구현하며
관련 header·LogMonitor의 LGPL-2.1 고지와 개발 fixture 출처를 확인한다.

참조: [MLog](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/messages/MLog.h),
[LogEntry v5](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/common/LogEntry.cc#L203),
[EntityName](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/common/entity_name.h),
[LogMonitor cursor·history 처리](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/mon/LogMonitor.cc#L1090).

MON config는 연속 `config` 구독과 `MGetConfig`(63)를 보내고 `MConfig`(62)의
전체 effective map을 받는다. Message header는 version 1/compat 1이며 payload는
raw string의 map이다. 설정 revision·FSID·cursor·연관된 request ID·구독
generation은 payload에 없다. 인증된 자기 client identity와 `Options.Hostname`,
빈 class를 요청하며 반환 map을 Go Options에 적용하지 않는다. Hostname의 기본값은
빈 문자열이고 모든 `MMonSubscribe`에도 같은 값을 그대로 보낸다. Initial config
요청과 지속 구독의 서버 선택을 일치시키며 재등록·인증 갱신·MON 복구에도 유지한다.
Watch별 selector 변경·OS 조회·환경변수 읽기·hostname 정규화는 제공하지 않는다.
서버는 session별로 변하지
않은 map 전송을 생략할 수 있어 재등록 때도 MGetConfig를 함께 보낸다.
하나의 unread map을 전체 교체하며 empty map과 absent key 삭제를 보존한다.
현재 admission을 통과한 source만 받으며 로그 watch와 명령 슬롯을 공유하지
않는다. 복구는 새 전체 map 요청이며 중간 변경 이력·원격 unsubscribe·무손실
수신을 보장하지 않는다. 같은 session의 이전 watch 응답은 구분할 수 없다.

참조: [MConfig](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/messages/MConfig.h),
[MGetConfig](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/messages/MGetConfig.h),
[ConfigMonitor](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/mon/ConfigMonitor.cc),
[ConfigMap](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/mon/ConfigMap.cc).
LGPL-2.1 또는 LGPL-3 선택 고지를 확인했으며 C++ 코드를 복사하지 않고
wire 의미를 독립 작성했다. Native CLI oracle과 설정 변경은 개발 fixture에만 둔다.

Hostname wire 참조는 같은 고정 커밋의
[MMonSubscribe v3](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/messages/MMonSubscribe.h),
[MON session hostname 수신](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/mon/Monitor.cc#L5368)과
[지속 config 선택](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/mon/ConfigMonitor.cc#L880)이다.
Native oracle의 hostname 입력은
[MonClient](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/mon/MonClient.cc#L1126)와
[NODE_NAME 처리](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/common/hostname.cc#L21)를 확인했다.
Native의 OS 조회·이름 축약 동작은 Go 제품에 옮기지 않는다. Fixture는 서버의
[host 상위 location 조회](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/crush/CrushWrapper.cc#L791)를
구분하는 빈 topology만 사용하며 Go의 CRUSH·OSDMap 지원을 의미하지 않는다.
LGPL-2.1 및 COPYING의 LGPL-2.1/LGPL-3 선택 고지를 확인했고 C++ 코드를 복사하지 않았다.
독립 native client의 설정 한 개를 비교하며 기본값·타입 변환이 있는 native view를
전체 raw MConfig map과 같다고 주장하지 않는다.

참조: [CephX 보안 수정](https://docs.ceph.com/en/latest/security/CVE-2025-30156/), [CephX 개요](https://docs.ceph.com/en/tentacle/dev/cephx/), [librados 명령 입출력 계약](https://docs.ceph.com/en/tentacle/rados/api/librados/).

## Go API 설계 기준

현재 MON/MGR 공개 API는 `ParseKey`, `Dial`, `MonCommand`, `MgrCommand`, `MonTell`, `MonTellTo`, `MgrTell`, `MgrTellTo`,
`WaitMonReady`, `WaitMgrReady`,
`WaitMonMap`, `WaitMgrMap`,
`WatchLogs`, `WatchConfig`, `WatchDigest`, `Snapshot`, `Close`를 중심으로 한다.
연결·명령·상태 타입은 `Options`, `Command`, `Result`, `State`다.
`MonitorState.Members`는 `MonitorMember{Name, Rank}`로 인증된 MonMap의
이름을 조회하며 같은 epoch의 rank 순서로 독립 복사한다. 이름은
`MonTellTo`에 사용하고, rank·멤버십을 daemon의 현재 준비 상태로 해석하지 않는다.
`ManagerState.Standbys`는 `StandbyManager{Name, GlobalID}`로 같은 인증된
MgrMap의 대기 MGR을 독립 복사한다. 가용성이나 새로운 접속 경로를 추가하지 않는다.
`ManagerState.EnabledModules`는 같은 지도의 명시적인 활성화 set이며 always-on
전체 목록이 아니다. `AvailableModules`는 active daemon이 보고한
`ManagerModule{Name, CanRun, ErrorString}`를 원본 순서대로 독립 복사한다.
Load 가능 보고를 실행 중 상태·명령 권한으로 바꾸지 않는다.
`ManagerModule.Options`는 option map key와 `ManagerModuleOption`의 이름,
원문 uint8 Type·Level과 uint32 Flags, 기본값·범위·설명·enum·tag·관련 옵션을
보존한다. 중첩 map과 slice를 독립 복사하고 설정값을 검증하거나 적용하지 않는다.
`ManagerState.AlwaysOnModules`는 raw uint32 release 코드별 상시 활성화 정책을,
`ForceDisabledModules`는 서버가 강제 비활성화한 set을 같은 epoch에서 보존한다.
중첩 목록을 독립 복사하며 현재 실행 set이나 적용 release를 추측하지 않는다.
이전 release 코드가 Tentacle metadata에 있다는 것은 이전 계열 지원이 아니다.
`ManagerState.Services`는 같은 지도의 active module 이름 → raw URI map을
독립 복사한다. URL 파싱·HTTP 접속·준비 상태·권한 확인은 수행하지 않는다.
로그는 `LogOptions`, `LogBatch`, `LogEntry`, `LogStream.Next`·`Close`로 제공한다.
설정은 `ConfigOptions`, `ConfigStream.Next`·`Close`로 raw `map[string]string`을
전달한다. 하나의 unread 전체 map을 후속 map으로 교체하며 caller가 소유한다.
Digest는 `DigestOptions`, `ClusterDigest`, `DigestStream.Next`·`Close`로 제공한다.
Health detail과 MON 상태 bytes를 해석 없이 한 쌍으로 유지하며 최신 unread 값으로
교체한다. 구독 성공은 서버의 권한 확인이 아니고 전달 시점·이력을 보장하지 않는다.
go-ceph 및 C API의 함수 이름·타입과 호환시키는 것을 목표로 하지 않는다.
OSD와 후속 MDS·대상 지정 명령·이벤트 API도 위 후속 제품 통신 계약을 따른다.
아래 현재 API의 함수 목록만으로 후속 제품 범위를 제한하지 않는다.

- `ParseKey(base64Key)`는 호출자가 선택한 encoded CephX credential을 검증한다. Identity 선택과 text keyring·설정 파일 로딩은 사용 계층이 맡는다.
- `Dial(ctx, options)`는 bootstrap과 초기 인증을 취소할 수 있어야 한다. Dial context의 종료가 성공적으로 생성된 client의 전체 수명을 자동으로 종료하지 않도록 한다.
- `Options.Hostname`은 caller가 선택한 통신 입력이며 client 수명 동안 불변이다. Hostname·identity를 포함한 MON 제어 메시지가 `MaxFrameSize`를 넘으면 인코딩·접속 전에 `ErrLimitExceeded`로 거절한다. 서버의 bootstrap frame 크기까지 보장하는 검사는 아니다.
- `MonCommand(ctx, command)`와 `MgrCommand(ctx, command)`는 요청별 취소와 deadline을 지원한다. `Command.JSON`과 `Input`을 전달하고 `Result`를 그대로 반환한다. 명령별 typed API나 성공 출력의 application JSON parser를 추가하지 않는다.
- `MonTellTo(ctx, name, command)`는 정확한 MON 이름을 지정하고 독립 인증·지도 검증·호출·정리까지 요청 context를 적용한다. 알려진 대상 없음·지도 변경·전송 전 취소와 전송 후 결과 불명확을 구분하며 주 연결과 다른 명령의 수명을 보존한다.
- `WaitMonReady(ctx)`와 `WaitMgrReady(ctx)`는 관리 명령이나 명령 슬롯 없이 연결 준비를 기다린다. MGR 대기는 발견과 별도 인증 연결을 포함한다. 취소는 해당 대기만 끝내며 성공은 이후 명령 성공을 보장하지 않는다. `Snapshot()`은 네트워크 요청 없이 현재 상태를 복사한다.
- `WaitMonMap(ctx, afterEpoch)`·`WaitMgrMap(ctx, afterEpoch)`는 기존 map 알림에서 strictly newer authenticated epoch를 기다리고 Snapshot과 같은 독립 복사본을 반환한다. 0은 이미 받은 nonzero 지도를 허용하며 중간 epoch·준비 상태를 보장하지 않는다. 새 명령·구독·접속·worker·명령 슬롯을 만들지 않고 MON 복구 중에도 대기한다. 개별 context 종료는 해당 대기만, Client Close와 명시적인 인증 거절은 각 기존 오류로 해제한다.
- `WatchLogs(ctx, options)`는 client당 하나의 worker를 로컬에 비동기 등록하며 명령 슬롯을 사용하지 않는다. context는 복구를 포함한 watch 전체 수명에 적용한다. `Next(ctx)` 취소는 해당 대기만 끝내며 이미 취소된 context는 접수한 큐를 소비하지 않는다.
- 로그 큐는 최대 64 batch와 보수적인 보유 bytes 추정치로 제한한다. `MaxBufferedBytes` 기본값은 `MaxFrameSize`, 허용 범위는 1 KiB–1 GiB이며 정확한 Go heap 상한은 아니다. overflow는 watch만 종료하며 이미 접수한 batch와 일반 명령 연결을 유지한다.
- `LogStream.Close`는 worker를 종료하고 watch 슬롯을 해제한다. 살아 있는 Next context는 이미 접수한 큐를 먼저 읽고 최초 종료 원인을 계속 받는다. Client Close도 watch worker를 종료하고 기다린다. 취소·overflow·인증·프로토콜 등 먼저 기록된 원인을 이후 Close가 덮어쓰지 않는다.
- 응답은 원본 data bytes와 상태 문자열을 보존한다. 모든 응답이 JSON이라고 가정하지 않는다.
- 서버 오류 코드와 Go의 context·네트워크·프로토콜 오류를 구분한다. 서버 코드는 호스트 OS의 errno 숫자로 재해석하지 않는다.
- client에서 동시 명령 호출을 허용한다. 요청 하나의 deadline을 공유 TCP 연결에 직접 적용하지 않는다.
- `MaxFrameSize`는 frame 논리 크기, `MaxInFlight`는 동시 명령 수를 제한한다. 대기 슬롯을 얻기 전에는 bulk 입력을 복사하지 않는다. 기본값은 각각 16 MiB와 64다.
- `ErrLimitExceeded`는 frame·인코딩·인증 transcript의 byte 또는 collection 상한 초과를 표시한다. 고정 상한도 포함하며 watch overflow·슬롯 대기·잘못된 Options 값과 구분한다. 유효한 frame의 로컬 상한 초과는 `ErrMalformedMessage`가 아니지만 길이·개수 검증 실패는 두 원인을 함께 보존할 수 있다. 송신 전 거절은 결과 불명확 오류가 아니며 전송 후 실패는 기존 결과 불명확 계약을 따른다.
- 로그와 config watch의 최초 종료 원인은 해당 watch에 먼저 기록된 원인이다. Source 오류가 watch에 기록되기 전에 수명 context의 취소를 먼저 관찰하면 context 오류로 끝날 수 있다. 이미 기록된 종료 원인과 접수한 데이터는 이후 취소나 Close가 덮어쓰지 않는다.
- 취소 시 pending 요청과 자원을 정리한다. 늦은 응답을 안전하게 소비한다. 일부 전송한 frame을 방치해 연결 framing을 깨뜨리지 않는다.
- context 취소는 서버에서 명령 실행을 취소하거나 이미 적용된 변경을 되돌린다는 보장이 아니다.
- transport ACK는 관리 명령 완료 응답과 다르다. 연결 단절 후 변경 명령의 실행 결과가 불명확하면 이를 명시적으로 반환한다.
- Messenger가 보장하는 세션 재개와 애플리케이션 명령 재실행을 구분한다. 새 세션에서 결과가 불명확한 변경 명령을 자동으로 다시 실행하지 않는다.
- `Close`는 내부 작업과 연결을 종료하고 대기 중 호출을 해제해야 한다. 종료·취소·재연결의 경합을 검증한다.

개발 시험에서 사용하는 명령 설명 JSON은 고정 [cmdparse formatter](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/common/cmdparse.cc#L136),
[MON catalog](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/mon/Monitor.cc#L3460),
[MGR catalog](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/mgr/DaemonServer.cc#L1596),
[flags](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/mon/MonCommand.h#L27)를 참조한다.
`cmdparse.cc`의 GPLv2와 MON/MGR 파일의 LGPL-2.1 고지를 확인했으며 C++ 코드를
복사하지 않고 시험용 JSON 비교를 독립 작성한다. 이 parser는 제품에 포함하지 않는다. 연결 feature에 따라 `req`의 JSON 타입과
`positional` 출력이 달라지므로 native oracle과의 비교에서 이 두 속성의 표현만
구분하고 제품이 받은 원본은 그대로 보존한다.

MGR standby는 고정 [MgrMap과 StandbyInfo](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/mon/MgrMap.h#L189)의
LGPL-2.1 고지를 확인하고 wire 의미를 독립 구현한다. `StandbyInfo`의 gid·name을
보존하며 standby child envelope의 module·feature metadata는 공개 기능 범위에 넣지 않는다.
실제 daemon이 만든 기존 IPv6 MgrMap fixture와 같은 epoch의 native `mgr dump`
결과를 oracle로 사용하고 standby만 바뀌는 지도와 active 전환을 검증한다.

Active 모듈 metadata는 같은 [MgrMap의 ModuleInfo](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/mon/MgrMap.h#L129)의
LGPL-2.1 고지를 확인하고 wire prefix를 독립 구현한다. 명시적인 module set과
active `name/can_run/error_string`과 option schema를 보존한다. 같은 epoch의 `mgr dump`와 비교하고 module disable/enable,
active 전환, 목록 소유권과 Close 후 보존을 검증한다.

ModuleInfo v2의 option map은 [ModuleOption v1](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/mon/MgrMap.h#L37)을
독립 구현한다. Map key와 내부 name을 구분하며 기본값·min/max 등 문자열을
수치·시간으로 변환하지 않는다. 고정 [Option type·level 코드](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/common/options.h#L16)의
일반 LGPL-2.1 또는 LGPL-3 고지를 확인했다. Native dump는 type·level을 이름으로
출력하므로 oracle 비교본에서만 고정 코드로 되돌리고 제품의 unknown 코드도
그대로 유지한다. Native formatter의 unknown은 원래 코드를 복구할 수 없어
oracle에서 추측하지 않는다. 전체 schema와 active 전환, 중첩 소유권을 비교하고
실제 설정값이 바뀌어도 보고된 기본값과 구분되는지 검증한다.

상시 활성화·강제 비활성화 정책은 같은 MgrMap v8·v14 tail의 wire 의미를
독립 구현한다. Release 코드와 set의 문자열을 보존하며 blocklist client 주소·
기능 비트·OSD epoch·flags는 공개 기능으로 추가하지 않는다. 같은 epoch의 native
`mgr dump`와 비교하고 두 모듈의 강제 비활성화·MGR 전환·재활성화를 검증한다.
Oracle은 반복되는 `module` object key를 모두 보존하며 release 이름만 고정
[release 코드](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/common/ceph_releases.h#L13)로
되돌린다. 일반 LGPL-2.1 또는 LGPL-3 고지를 확인했으며 C++ 코드를 복사하지 않는다.

서비스 URI는 같은 MgrMap의 raw map을 독립 구현한다. 고정
[active beacon 반영](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/mon/MgrMonitor.cc#L515)과
[active 제거](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/mon/MgrMonitor.cc#L930)의
LGPL-2.1 고지를 확인했다. 새 지도는 전체 map을 교체한다. 실제 Prometheus 모듈의
광고를 같은 epoch의 `mgr dump`와 비교하고 active 전환과 모듈 비활성화에 따른
주소 변경·삭제를 검증한다. 서비스 모듈과 native CLI는 개발 fixture에만 사용한다.
Fixture의 Prometheus 광고는 고정
[localized listener와 set_uri](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/pybind/mgr/prometheus/module.py#L2408)의
의미를 사용하며 Python 코드를 제품에 포함하지 않는다.

Digest는 고정 [MMgrDigest](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/messages/MMgrDigest.h#L35),
[MON 전송](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/mon/MgrMonitor.cc#L645),
[health JSON](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/mon/HealthMonitor.cc#L470)을
참조하며 LGPL-2.1 고지를 확인했다. 두 bufferlist의 wire 의미를 독립 작성한다.
현재 admission을 통과한 MON만 수신하고 재접속 때 start=0/flags=0으로 다시
구독한다. 같은 daemon의 native `mon_status`와 native `health detail`을 대조한다.
비교본에서만 uptime·quorum_age·live feature_map을 제외하며 제품의 원본은 보존한다.

개발 fixture의 text keyring은 고정 [KeyRing writer](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/auth/KeyRing.cc#L232)의
LGPL-2.1 고지를 확인하고 시험용 credential 선택을 독립 구현한다. 제품은 keyring
텍스트를 받지 않는다. 실제 native `auth get`
출력을 여러 entity로 합친 keyring을 `--print-key`와 대조하고 선택한 키로 MON/MGR를
인증한다. Ceph 도구는 시험 fixture의 생성·oracle에만 사용한다.

## 기존 기반의 구현·검증 기준

아래 목록은 B1–B7의 기존 기반을 구현·검증할 때의 기준이다. 항목별 현재
상태와 제한은 앞의 진행표 및 README를 따른다. 새로운 후속 작업의 착수
순서는 [우선순위 표](#우선순위의-의미와-다음-작업)를 따르며 이 목록을
미착수 작업 목록이나 전체 기능 완료 선언으로 해석하지 않는다.

1. 고정 소스에서 codec, frame, feature, CephX 인증 흐름의 최소 집합을 추출한다. 암호 및 frame의 독립적인 test vector를 확보한다.
2. Go만으로 msgr2.1 secure 및 `aes256k`를 사용하는 MON 인증과 관리 조회 명령을 수행한다.
3. MgrMap을 받아 active MGR에 연결하고 실제 MGR 명령을 수행한다.
4. ticket 갱신, MON/MGR failover, 연결 단절과 session reset을 검증한다.
5. 동시 요청 중 개별 context 취소, 늦은 응답, 결과 불명확, Close 경합을 검증한다.
6. 제품의 전이 의존성을 포함해 CGO·네이티브 라이브러리·CLI 의존성이 없는지 확인하고 대상 OS에서 빌드·실행을 검증한다.
7. MON 로그는 독립 raw wire 입력 및 native CLI oracle과 대조한다. 실제 로그 수신, cursor 복구, ticket 갱신, 제한된 큐, watch·Next context와 Close를 검증하며 단순 SubscribeAck를 권한 확인으로 사용하지 않는다.
8. 이름 지정 MON Tell은 독립 native daemon status의 name·rank·FSID와 대조한다. 없는 이름의 접속 전 거절, 다른 MON으로 대체하지 않음, 새 private 인증 ID와 주 상태 보존, 동시 호출·전송 후 불확실성·setup과 session의 Close 소유권을 검증한다.
9. MON 이름 조회는 독립 native CLI 지도와 대조한다. 네트워크 없는 조회와 반환 slice의 소유권, 주 지도 변경·거절·stale source·private Tell의 격리, ticket 갱신과 Close 후 마지막 정보 보존을 검증한다.
10. 지도 대기 API는 실제 MON add/rm과 MGR 전환의 새 epoch를 native 지도와 비교한다. 명령 슬롯 점유 중에도 진행하고, MON 재접속을 잠시 막은 상태에서 대기가 유지된 뒤 학습한 MON의 재구독으로 새 MGR 지도를 받는지 검증한다. 개별 context 취소와 Client Close도 검증한다.
11. CephX 내부 plaintext는 유효한 암호 envelope에 넣은 독립 입력과 크기 제한이 있는 fuzz로 검증한다. Decoder의 읽기 실패와 완전한 version·nonce·암호 오류를 구분하고, 실패 시 identity·ticket·credential·부분 출력을 게시하지 않는지 공개 setup 경로까지 확인한다. 정상 실서버 인증과 ticket 갱신 검증도 두 키 타입별로 유지한다.
12. CRC는 실제 CRC-only MON/MGR와 두 CephX 키 타입별로 검증한다. 정확한 단일 모드 제안, secure 선택 거부, transcript 서명, 인증 이후 CRC 손상 거부, 일반·Tell·이름 지정 Tell과 raw binary·오류 보존을 확인한다. 기존 갱신·장애 복구·취소·종료 시험도 CRC 선택으로 수행한다. CLI나 native daemon 내부 client의 fixture 설정을 Go 제품의 모드 fallback으로 간주하지 않는다.

구현한 encoder와 decoder끼리의 round trip만으로 wire 호환성을 입증하지 않는다. Ceph에서 얻은 fixture 및 실제 Ceph 상대 검증을 사용한다. parser fuzzing, race 검사, 장시간 ticket 갱신, 장애 주입을 포함한다. 클러스터 구성과 테스트 oracle을 위한 Ceph CLI·컨테이너 사용은 개발 도구이며 제품의 런타임 의존성과 구분한다.

개발 실행기는 별도 `integration/fixture` Go 1.25 모듈에서 공개
`ceph-testcontainers-go v0.1.0`의 `ceph.Run`으로 container·network·준비 대기·
cleanup을 관리한다. 제품 Go 1.24 모듈과 의존성은 분리한다. 현재 컨테이너
하나의 MON 3개·MGR 0/2개·선택 OSD 구성과 native bootstrap·control·oracle은
기존 adapter에 남긴다. 상위 typed fixture에 없는 키·초기 auth-map cipher,
CRC·IPv6·mapped IPv6·mkfs 전 memstore profile을 기본 topology로 대체해
검증을 줄이지 않는다. TTL은 기존 `TemporaryConfig`로 설정할 수 있으며
CRUSH map import·pool 설정은 native `Ceph`·`Exec`·file-copy로 표현할 수 있다.
이 개발 도구 교체 자체를 제품 상호운용 검증 결과로 해석하지 않는다.

## 변경 빈도와 AI 활용에 대한 판단

확인한 사실:

- msgr2.1 도입 PR은 2020-06-20에 병합됐으며 Tentacle 문서도 이 규약을 설명한다.
- `tentacle` 브랜치의 `frames_v2.h`, `crypto_onwire.cc`, `ProtocolV2.cc` 변경 이력을 조사했다. frame 정의에는 2020년 msgr2.1, 2021년 압축·CRC 관련 변경 등이 보인다. 최근 ProtocolV2 변경에는 재연결·종료·객체 수명 및 인증 오류 처리 수정이 있다. 이 제한된 조사에서 릴리스마다 frame 전체를 재설계한다는 근거는 발견하지 못했다.
- 20.2.4의 새 CephX 키 타입은 stable patch에서도 인증 구현을 추가해야 할 수 있음을 보여준다. transport wire 안정성과 전체 client 구현의 유지보수 비용은 별개다.
- Rook의 2018년 PR #1362는 Ceph 자체 빌드와 fork 유지 부담을 줄이고 upstream 패키지를 쓰는 변경이다. 조사한 Rook 기록에서는 “pure Go Messenger를 구현하다 빈번한 규약 변경 때문에 포기했다”는 구체적인 시도와 중단 사유를 확인하지 못했다. 이 주장은 사실로 전제하지 않는다.

판단:

Tentacle 이상, MON/MGR 우선, 이전 API 호환성 의무 없음이라는 범위라면 추진할 가치가 있다. AI는 C++ 인코딩·상태 전이를 읽고 Go codec을 작성하거나 소스 변경을 분류하는 작업에 도움을 줄 수 있다. 그러나 암호 정확성, 실제 서버와의 호환성, 장애 상황의 결과 의미 및 장시간 운영 검증을 대체하지 않는다. 구현 기간이나 AI의 배수 단축 효과는 이 프로젝트에서 측정되지 않았다.

유지보수는 Ceph 전체 변경을 이식하는 방식보다 다음의 영향을 추적하는 방식으로 운영한다: Messenger wire/feature, CephX와 crypto, MonClient/MgrClient, 사용하는 메시지·지도 encoding. 명령 schema 변경은 raw 전송 계약에 영향을 주는지와 시험 oracle의 변경을 확인하되 명령별 사용 API로 이식하지 않는다. C++ 내부 refactor는 wire나 외부 동작이 바뀌는지 확인한 뒤 필요한 경우에만 반영한다.

업그레이드마다 소스 차이 분석과 독립 fixture·실클러스터 검증을 수행한다. 기존 버전과 새 버전의 실패 차이를 찾아 실제 규약 변경과 구현 결함을 구분한다. 단순 연결 성공 또는 AI 코드 생성 속도를 유지 가능성의 근거로 삼지 않는다.

첫 실증 기준인 현재 Tentacle의 새 키 인증, MON 명령, MGR 명령, ticket 갱신과 failover가 함께 동작하는 PoC는 완료했다. 짧은 ticket TTL을 적용한 격리 시험이며 장시간 운영이나 이후 Ceph 패치에 대한 검증을 대신하지 않는다. 이후 패치 하나에 대한 차이 분석과 재검증 비용을 기록해 유지보수 가능성을 판단한다.

참조: [msgr2.1 도입 PR](https://github.com/ceph/ceph/pull/35078), [frame 변경 이력](https://github.com/ceph/ceph/commits/tentacle/src/msg/async/frames_v2.h), [wire crypto 변경 이력](https://github.com/ceph/ceph/commits/tentacle/src/msg/async/crypto_onwire.cc), [ProtocolV2 변경 이력](https://github.com/ceph/ceph/commits/tentacle/src/msg/async/ProtocolV2.cc), [Rook의 Ceph 빌드 제거 PR](https://github.com/rook/rook/pull/1362).
