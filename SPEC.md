# ceph-msgr-go 초기 스펙 및 타당성 판단

조사 및 구현 기준일: 2026-10-01. 이 문서는 프로젝트 정책과 검증 기준을 정의한다. 첫 MON/MGR 구현과 Ceph 20.2.4 실서버 검증을 완료했다. 현재 API, 재현 방법 및 검증 범위는 [README.md](README.md)에 기록한다.

## 확정된 프로젝트 정책

- Ceph Messenger를 Go에서 직접 구현한다. 제품 실행과 빌드에 go-ceph, librados, CGO, Ceph CLI 및 다른 네이티브 라이브러리를 요구하지 않는다.
- 운영체제별 Ceph 설치 없이 Go가 지원하는 대상 운영체제에서 동작하도록 설계한다. 운영체제에 종속된 기본 경로보다 프로그램으로 전달하는 설정을 우선한다.
- 최소 지원 Ceph 계열은 Tentacle, 즉 20.2 계열이다. 이전 Ceph 계열을 위한 호환 코드는 추가하지 않는다.
- 현재 스펙에 맞춰 API를 설계한다. API 변경을 이유로 이전 API와의 하위 호환성을 요구하는 작업은 거부한다. 이 정책을 변경하려면 사용자가 명시적으로 범위를 변경해야 한다.
- 첫 구현은 MON/MGR 관리 명령과 이를 위한 인증, 지도, 세션 처리에 집중한다. 객체 I/O는 후속 업무다.
- RADOS 객체 읽기/쓰기, CRUSH 배치 계산, OSD 데이터 연결, RBD, CephFS는 초기 범위에 포함하지 않는다. MON/MGR 명령을 통한 풀·OSD 관리까지 제외한다는 뜻은 아니다.

## 참조 버전과 지원의 의미

현재 조사에서 확인한 최신 Tentacle 패치는 `v20.2.4`다. 최초 구현의 고정 참조는 이 태그와 커밋 `7f793731f1b39eb4f465e960113d2363c311b964`로 한다. 변경되는 `tentacle` 브랜치는 후속 변경 조사에 사용한다.

Tentacle을 최소 계열로 삼는 것과 모든 20.2 패치 및 이후 모든 계열에서 동작을 검증했다는 것은 다르다. 실제 지원표에는 Ceph 버전, 인증 키 타입, 연결 모드 및 통합 테스트 결과를 기록한다. 이후 계열은 검증 후 지원 대상으로 추가한다.

msgr2.1 기능 비트는 Tentacle 버전 증명이 아니다. 런타임에서는 인증된 MonMap의 `min_mon_release >= 20`을 요구한다. 이는 클러스터의 최소 MON 계열 설정 확인이며 각 daemon의 정확한 패치 버전 증명이 아니다. 최소 설정이 이전 계열인 업그레이드 중 클러스터도 거부한다. 실서버 검증에 사용한 정확한 패치와 커밋은 시험 이미지의 `ceph --version`으로 확인했다.

참조: [Tentacle 릴리스 노트](https://docs.ceph.com/en/latest/releases/tentacle/), [고정 소스](https://github.com/ceph/ceph/tree/7f793731f1b39eb4f465e960113d2363c311b964).

## 구현할 프로토콜

초기 전송은 msgr2.1을 대상으로 한다. msgr1과 msgr2.0 연결로의 fallback을 만들지 않는다. 협상한 필수 기능이 부족하면 명시적인 오류를 반환한다. 아직 구현하지 않은 기능을 지원한다고 광고하지 않는다.

공개 연결 모드는 `secure`만 제공한다. 인증 전 교환에는 프로토콜이 요구하는 CRC framing을 사용하며 `secure` 실패 시 downgrade하지 않는다. 압축 협상 절차는 구현하되 알고리즘 목록을 비워 압축을 사용하지 않는다. 압축 frame은 거부한다.

실제 Tentacle MON은 명령 전용 CLIENT에도 CRUSH 세대 비트를 접속 요건으로 요구했다. 초기 구현은 고정 참조에서 확인한 이 비트들을 MON 세션에서만 광고한다. 이는 명령 전용 접속을 위한 제한된 예외이며 CRUSH 계산 지원을 의미하지 않는다. OSDMap을 구독·해석하거나 OSD에 접속하지 않는다. 그 밖의 구현하지 않은 필수 기능은 오류로 거부한다.

구현 항목은 다음과 같다.

1. Ceph little-endian 인코딩, 길이 제한, 구조체의 version/compat version, 주소와 주소 벡터, 시간·문자열·컨테이너 인코딩.
2. Banner, msgr2 feature 협상, HELLO 및 인증 frame 교환.
3. 32-byte preamble, segment, CRC32C, secure framing, AES-GCM tag 검증과 방향별 nonce 상태.
4. AUTH_SIGNATURE를 포함한 인증 교환 및 세션 식별.
5. Message header, front/middle/data, sequence, ACK, keepalive.
6. Global ID, sequence, 종료, MON/MGR 변경과 재인증을 처리하는 상태 머신. 현재는 fresh lossy session으로 복구하며 이전 Messenger cookie로 session을 재개하는 기능은 구현하지 않는다. 새 세션으로 명령을 재실행하지 않는다.
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
  초기 map 요청. Host·device class는 비워 요청하고 Go Options에는 적용하지 않는다.
- MON의 주기적인 `mgrdigest` 구독으로 health detail·MON 상태 raw JSON을 받는다.
  MGR entity나 연결을 요구하지 않으며 지도·cursor가 없는 최신 값 stream이다.

MGR 기능:

- MgrMap의 active MGR 주소를 사용한 별도 연결과 service 인증.
- MGR 명령과 응답, 모듈 미지원·권한 오류의 구분.
- active MGR 변경 시 연결 갱신과 진행 중 요청의 결과 처리.

MON 명령과 MGR 명령은 명시적인 API로 구분한다. 명령 prefix만 보고 임의로 경로를 추측하지 않는다. 명령의 JSON, bulk 입력, binary 출력, 상태 문자열과 서버 오류 코드를 각각 보존한다.

현재 접속한 MON과 active MGR의 daemon-local 명령은 `MonTell`·`MgrTell`로 구분한다. `MonTellTo(ctx, name, command)`는 MonMap의 정확한 bare MON 이름을 지정한다. Prefix 제거·숫자의 rank 해석·wildcard 확장은 제공하지 않으며 MGR 대상은 active MGR로 한정한다. Tentacle의 `MCommand`(97)는 인증된 nonzero FSID와 command 문자열 벡터를 보내며, TID는 Messenger header에 둔다. `MCommandReply`(98)의 raw data, signed code, 상태 문자열을 보존한다. MGR의 zero-FSID legacy module 경로로 바꾸지 않는다.

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
generation은 payload에 없다. 인증된 자기 client identity와 빈 host/class를
요청하며 반환 map을 Go Options에 적용하지 않는다. 서버는 session별로 변하지
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

참조: [CephX 보안 수정](https://docs.ceph.com/en/latest/security/CVE-2025-30156/), [CephX 개요](https://docs.ceph.com/en/tentacle/dev/cephx/), [librados 명령 입출력 계약](https://docs.ceph.com/en/tentacle/rados/api/librados/).

## Go API 설계 기준

공개 API는 `ParseKey`, `ParseKeyring`, `NewCommand`, `Dial`, `MonCommand`, `MgrCommand`, `MonTell`, `MonTellTo`, `MgrTell`,
`MonCommandDescriptions`, `MgrCommandDescriptions`, `MonTellDescriptions`,
`MonTellToDescriptions`, `MgrTellDescriptions`, `WaitMonReady`, `WaitMgrReady`,
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
`ManagerState.Services`는 같은 지도의 active module 이름 → raw URI map을
독립 복사한다. URL 파싱·HTTP 접속·준비 상태·권한 확인은 수행하지 않는다.
로그는 `LogOptions`, `LogBatch`, `LogEntry`, `LogStream.Next`·`Close`로 제공한다.
설정은 `ConfigOptions`, `ConfigStream.Next`·`Close`로 raw `map[string]string`을
전달한다. 하나의 unread 전체 map을 후속 map으로 교체하며 caller가 소유한다.
Digest는 `DigestOptions`, `ClusterDigest`, `DigestStream.Next`·`Close`로 제공한다.
Health detail과 MON 상태 bytes를 해석 없이 한 쌍으로 유지하며 최신 unread 값으로
교체한다. 구독 성공은 서버의 권한 확인이 아니고 전달 시점·이력을 보장하지 않는다.
go-ceph 및 C API의 함수 이름·타입과 호환시키는 것을 목표로 하지 않는다.

- `ParseKeyring(data, identity)`는 현재 Ceph가 기본 출력하는 text keyring에서 정확한 client identity의 활성 key를 선택해 기존 `ParseKey`로 검증한다. 다른 entity·pending key로 대체하지 않으며 caps나 OS의 파일 검색·설정 우선순위를 적용하지 않는다. 반환 Key는 입력과 독립적이다.
- `NewCommand(prefix, arguments)`는 Go named arguments를 표준 JSON으로 인코딩하며 호출자의 map과 원문 prefix를 보존한다. 선택한 prefix의 덮어쓰기는 거부하고, 인자 schema·권한·전송 경로를 추측하거나 bulk 입력을 JSON에 넣지 않는다.
- `Dial(ctx, options)`는 bootstrap과 초기 인증을 취소할 수 있어야 한다. Dial context의 종료가 성공적으로 생성된 client의 전체 수명을 자동으로 종료하지 않도록 한다.
- `MonCommand(ctx, command)`와 `MgrCommand(ctx, command)`는 요청별 취소와 deadline을 지원한다. 먼저 raw command API를 구현하고 필요한 typed API만 추가한다.
- `MonCommandDescriptions(ctx)`와 `MgrCommandDescriptions(ctx)`는 현재 서버의 관리 명령 metadata를 새로 조회한다. 같은 prefix의 여러 signature와 원본 ID·flags·JSON 속성을 보존하며 prefix로 중복 제거하거나 실행 경로·권한·모듈 활성화를 추측하지 않는다. 원본 `Result`를 유지하고 성공 응답의 JSON 해석 실패는 세션 실패나 결과 불명확으로 바꾸지 않는다.
- `MonTellDescriptions`, `MgrTellDescriptions`, `MonTellToDescriptions`는 관리 명령과 구분한 daemon-local admin schema를 조회한다. `sig/help`와 원본 속성을 보존하며 미제공된 module·permission·flags를 실행 권한 정보로 해석하지 않는다. admin formatter의 고정 feature 집합에 따라 인자 JSON boolean을 그대로 보존한다.
- `MonTellTo(ctx, name, command)`는 정확한 MON 이름을 지정하고 독립 인증·지도 검증·호출·정리까지 요청 context를 적용한다. 알려진 대상 없음·지도 변경·전송 전 취소와 전송 후 결과 불명확을 구분하며 주 연결과 다른 명령의 수명을 보존한다.
- `WaitMonReady(ctx)`와 `WaitMgrReady(ctx)`는 관리 명령이나 명령 슬롯 없이 연결 준비를 기다린다. MGR 대기는 발견과 별도 인증 연결을 포함한다. 취소는 해당 대기만 끝내며 성공은 이후 명령 성공을 보장하지 않는다. `Snapshot()`은 네트워크 요청 없이 현재 상태를 복사한다.
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

명령 설명 JSON은 고정 [cmdparse formatter](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/common/cmdparse.cc#L136),
[MON catalog](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/mon/Monitor.cc#L3460),
[MGR catalog](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/mgr/DaemonServer.cc#L1596),
[flags](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/mon/MonCommand.h#L27)를 참조한다.
`cmdparse.cc`의 GPLv2와 MON/MGR 파일의 LGPL-2.1 고지를 확인했으며 C++ 코드를
복사하지 않고 JSON 의미를 독립 작성한다. 연결 feature에 따라 `req`의 JSON 타입과
`positional` 출력이 달라지므로 native oracle과의 비교에서 이 두 속성의 표현만
구분하고 제품이 받은 원본은 그대로 보존한다.

MGR standby는 고정 [MgrMap과 StandbyInfo](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/mon/MgrMap.h#L189)의
LGPL-2.1 고지를 확인하고 wire 의미를 독립 구현한다. `StandbyInfo`의 gid·name을
보존하며 standby child envelope의 module·feature metadata는 공개 기능 범위에 넣지 않는다.
실제 daemon이 만든 기존 IPv6 MgrMap fixture와 같은 epoch의 native `mgr dump`
결과를 oracle로 사용하고 standby만 바뀌는 지도와 active 전환을 검증한다.

Active 모듈 metadata는 같은 [MgrMap의 ModuleInfo](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/mon/MgrMap.h#L129)의
LGPL-2.1 고지를 확인하고 wire prefix를 독립 구현한다. 명시적인 module set과
active `name/can_run/error_string`을 보존하며 module_options는 공개
모델에 넣지 않는다. 같은 epoch의 `mgr dump`와 비교하고 module disable/enable,
active 전환, 목록 소유권과 Close 후 보존을 검증한다.

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

Text keyring은 고정 [KeyRing writer](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/auth/KeyRing.cc#L232)의
LGPL-2.1 고지를 확인하고 출력 문법을 독립 구현한다. 중복 section·key와 전체
ConfUtils의 quoting·continuation 문법은 범위에 넣지 않는다. 실제 native `auth get`
출력을 여러 entity로 합친 keyring을 `--print-key`와 대조하고 선택한 키로 MON/MGR를
인증한다. Ceph 도구는 시험 fixture의 생성·oracle에만 사용한다.

## 구현 및 검증 순서

1. 고정 소스에서 codec, frame, feature, CephX 인증 흐름의 최소 집합을 추출한다. 암호 및 frame의 독립적인 test vector를 확보한다.
2. Go만으로 msgr2.1 secure 및 `aes256k`를 사용하는 MON 인증과 관리 조회 명령을 수행한다.
3. MgrMap을 받아 active MGR에 연결하고 실제 MGR 명령을 수행한다.
4. ticket 갱신, MON/MGR failover, 연결 단절과 session reset을 검증한다.
5. 동시 요청 중 개별 context 취소, 늦은 응답, 결과 불명확, Close 경합을 검증한다.
6. 제품의 전이 의존성을 포함해 CGO·네이티브 라이브러리·CLI 의존성이 없는지 확인하고 대상 OS에서 빌드·실행을 검증한다.
7. MON 로그는 독립 raw wire 입력 및 native CLI oracle과 대조한다. 실제 로그 수신, cursor 복구, ticket 갱신, 제한된 큐, watch·Next context와 Close를 검증하며 단순 SubscribeAck를 권한 확인으로 사용하지 않는다.
8. 이름 지정 MON Tell은 독립 native daemon status의 name·rank·FSID와 대조한다. 없는 이름의 접속 전 거절, 다른 MON으로 대체하지 않음, 새 private 인증 ID와 주 상태 보존, 동시 호출·전송 후 불확실성·setup과 session의 Close 소유권을 검증한다.
9. MON 이름 조회는 독립 native CLI 지도와 대조한다. 네트워크 없는 조회와 반환 slice의 소유권, 주 지도 변경·거절·stale source·private Tell의 격리, ticket 갱신과 Close 후 마지막 정보 보존을 검증한다.
10. CephX 내부 plaintext는 유효한 암호 envelope에 넣은 독립 입력과 크기 제한이 있는 fuzz로 검증한다. Decoder의 읽기 실패와 완전한 version·nonce·암호 오류를 구분하고, 실패 시 identity·ticket·credential·부분 출력을 게시하지 않는지 공개 setup 경로까지 확인한다. 정상 실서버 인증과 ticket 갱신 검증도 두 키 타입별로 유지한다.

구현한 encoder와 decoder끼리의 round trip만으로 wire 호환성을 입증하지 않는다. Ceph에서 얻은 fixture 및 실제 Ceph 상대 검증을 사용한다. parser fuzzing, race 검사, 장시간 ticket 갱신, 장애 주입을 포함한다. 클러스터 구성과 테스트 oracle을 위한 Ceph CLI·컨테이너 사용은 개발 도구이며 제품의 런타임 의존성과 구분한다.

## 변경 빈도와 AI 활용에 대한 판단

확인한 사실:

- msgr2.1 도입 PR은 2020-06-20에 병합됐으며 Tentacle 문서도 이 규약을 설명한다.
- `tentacle` 브랜치의 `frames_v2.h`, `crypto_onwire.cc`, `ProtocolV2.cc` 변경 이력을 조사했다. frame 정의에는 2020년 msgr2.1, 2021년 압축·CRC 관련 변경 등이 보인다. 최근 ProtocolV2 변경에는 재연결·종료·객체 수명 및 인증 오류 처리 수정이 있다. 이 제한된 조사에서 릴리스마다 frame 전체를 재설계한다는 근거는 발견하지 못했다.
- 20.2.4의 새 CephX 키 타입은 stable patch에서도 인증 구현을 추가해야 할 수 있음을 보여준다. transport wire 안정성과 전체 client 구현의 유지보수 비용은 별개다.
- Rook의 2018년 PR #1362는 Ceph 자체 빌드와 fork 유지 부담을 줄이고 upstream 패키지를 쓰는 변경이다. 조사한 Rook 기록에서는 “pure Go Messenger를 구현하다 빈번한 규약 변경 때문에 포기했다”는 구체적인 시도와 중단 사유를 확인하지 못했다. 이 주장은 사실로 전제하지 않는다.

판단:

Tentacle 이상, MON/MGR 우선, 이전 API 호환성 의무 없음이라는 범위라면 추진할 가치가 있다. AI는 C++ 인코딩·상태 전이를 읽고 Go codec을 작성하거나 소스 변경을 분류하는 작업에 도움을 줄 수 있다. 그러나 암호 정확성, 실제 서버와의 호환성, 장애 상황의 결과 의미 및 장시간 운영 검증을 대체하지 않는다. 구현 기간이나 AI의 배수 단축 효과는 이 프로젝트에서 측정되지 않았다.

유지보수는 Ceph 전체 변경을 이식하는 방식보다 다음의 영향을 추적하는 방식으로 운영한다: Messenger wire/feature, CephX와 crypto, MonClient/MgrClient, 사용하는 메시지·지도 encoding, 지원하는 명령 schema. C++ 내부 refactor는 wire나 외부 동작이 바뀌는지 확인한 뒤 필요한 경우에만 반영한다.

업그레이드마다 소스 차이 분석과 독립 fixture·실클러스터 검증을 수행한다. 기존 버전과 새 버전의 실패 차이를 찾아 실제 규약 변경과 구현 결함을 구분한다. 단순 연결 성공 또는 AI 코드 생성 속도를 유지 가능성의 근거로 삼지 않는다.

첫 실증 기준인 현재 Tentacle의 새 키 인증, MON 명령, MGR 명령, ticket 갱신과 failover가 함께 동작하는 PoC는 완료했다. 짧은 ticket TTL을 적용한 격리 시험이며 장시간 운영이나 이후 Ceph 패치에 대한 검증을 대신하지 않는다. 이후 패치 하나에 대한 차이 분석과 재검증 비용을 기록해 유지보수 가능성을 판단한다.

참조: [msgr2.1 도입 PR](https://github.com/ceph/ceph/pull/35078), [frame 변경 이력](https://github.com/ceph/ceph/commits/tentacle/src/msg/async/frames_v2.h), [wire crypto 변경 이력](https://github.com/ceph/ceph/commits/tentacle/src/msg/async/crypto_onwire.cc), [ProtocolV2 변경 이력](https://github.com/ceph/ceph/commits/tentacle/src/msg/async/ProtocolV2.cc), [Rook의 Ceph 빌드 제거 PR](https://github.com/rook/rook/pull/1362).
