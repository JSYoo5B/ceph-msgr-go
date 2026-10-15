# ceph-msgr-go

Ceph Tentacle의 MON/MGR 및 명시적으로 지정한 OSD와 msgr2.1로 통신하는
native Go 라이브러리다. Messenger·CephX·세션·raw 관리 명령·wire 지도·구독과
OSD stat/read와 제한된 replicated 객체 쓰기 wire를 구현한다.
제품은 Go 표준 라이브러리만 사용하며 CGO, go-ceph, librados, Ceph CLI를
요구하지 않는다. 최소 Ceph 계열은 Tentacle(20.2)이다.

고정 wire 참조는 Ceph `v20.2.4`, 커밋
`7f793731f1b39eb4f465e960113d2363c311b964`다.
프로젝트 정책과 변경 빈도 조사는 [SPEC.md](SPEC.md)에 기록했다.
공개 API 변경 시 이전 API를 위한 호환 wrapper를 추가하지 않는다.

## 저장소의 역할

이 저장소는 통신 계층을 제공한다. 실제 사용은 별도 레이어·저장소에서 구현한다.

| 이 저장소 | 사용 레이어 |
| --- | --- |
| Messenger framing·CephX·인증 갱신 | credential 선택·keyring/설정 로딩 |
| MON/MGR 발견·세션·복구·요청별 context | 운영 작업과 재시도 정책 |
| caller가 준비한 raw JSON·bulk 입력 송신, 원본 응답·오류 반환 | 명령 JSON 구성·schema 검증·응답 JSON 해석·typed 관리 API |
| MonMap·MgrMap·log·config·digest wire codec과 수신 | 설정 적용·모듈 정책·서비스 URI 사용 |
| OSD service ticket·명시적 연결·bounded 객체 read/write·조건 벡터·raw OSDMap 수신 | 내포한 OSDMap 해석·적용·객체 hash·CRUSH 배치·대상 선택·재시도·고수준 객체 API |

지도에 실린 module metadata·option·activation policy·service URI는 받은 값을
보존한다. 실행 모듈 계산, 설정 적용과 서비스 접속은 사용 레이어가 맡는다.
명령 JSON 구성·keyring 텍스트 선택·명령 catalog 해석 helper는 제품에서 제거했으며
실제 Ceph 대조 시험에서 필요한 부분만 `integration/*_test.go`에 둔다.
시험의 설정 변경·모듈 활성화·MGR 전환은 disposable fixture의 통신 검증이다.

통신 계층 분리 후 CGO=0 전체 Go 검사, raw 응답 경로의 race 검사, vet와
Windows 빌드를 통과했다. 실제 20.2.4의 Linux IPv6·aes256k 및 Darwin
IPv4·AES/race에서 raw 인자·bulk 입력, 관리·daemon-local catalog, 선택한
credential의 MON/MGR 인증, MGR 전환 중 wire metadata 보존을 다시 검증했다.

## 사용

Go 1.24 이상을 대상으로 한다. import 경로는
`github.com/jsyoo5b/ceph-msgr-go/cephmsgr`이며 패키지 이름은 `cephmsgr`다.
배포된 module tag는 아직 없다. 제품 Go 코드는 `cephmsgr/`, wire·인증·세션
구현은 `internal/`, 공개 API 통합시험과 실행 도구는 `integration/`에 둔다.

```go
key, err := cephmsgr.ParseKey(encodedKey) // keyring 전체가 아닌 base64 key 값
if err != nil {
    return err
}
bootstrap, stop := context.WithTimeout(context.Background(), 10*time.Second)
client, err := cephmsgr.Dial(bootstrap, cephmsgr.Options{
    Monitors: []string{"v2:192.0.2.10:3300/0", "v2:192.0.2.11:3300/0"},
    Identity: "client.management",
    Key: key,
    ExpectedFSID: clusterFSID,
})
stop() // 성공한 client의 수명은 이 context와 독립적이다.
if err != nil {
    return err
}
defer client.Close()

ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
// 실제 명령 선택과 JSON 구성은 사용 레이어가 맡는다.
command := cephmsgr.Command{JSON: []byte(`{"prefix":"status","format":"json"}`)}
result, err := client.MonCommand(ctx, command)
if err != nil {
    return err
}
fmt.Printf("%s\n", result.Data)
```

`Options.ConnectionMode`의 기본값은 `cephmsgr.SecureMode`다. CRC 연결은
`ConnectionMode: cephmsgr.CRCMode`로 명시적으로 선택한다. 이 선택은
MON/MGR/OSD와 ticket 갱신·재접속·이름 지정 MON Tell 연결 전체에 적용된다.
선택한 모드 하나만 제안하며 서버의 다른 모드 선택을 거부한다.
두 모드 모두 CephX와 인증 transcript 서명을 검증한다. CRC는 인증 이후
통신을 암호화하지 않으며 CRC32C는 손상 검출만 제공한다.

호출자는 exact client identity와 선택한 base64 CephX key를 전달한다.
`ParseKey`는 인증 credential codec이며 keyring 텍스트·설정 파일 선택은 사용
레이어가 처리한다. `Command.JSON`에는 준비한 Ceph command object를 넣고
`Command.Input`에는 별도의 bulk bytes를 지정한다. 라이브러리는 object·prefix
형식과 송신 크기를 검사하며 명령별 인자 schema는 해석하지 않는다.

`MgrCommand`는 active MGR에 별도로 인증해 명령을 보낸다. 예를 들어
`{"prefix":"pg stat","format":"json"}`을 사용할 수 있다. MON/MGR 경로는
호출자가 선택한다. 각 명령은 해당 identity의 Ceph 권한에 따라 처리된다.

`MonTell`은 현재 접속한 MON, `MgrTell`은 현재 active MGR의 daemon-local
명령을 실행한다. 예를 들어 `{"prefix":"version","format":"json"}`을 보낼 수
있다. MON 재접속·ticket 갱신으로 `MonTell`의 대상은 바뀔 수 있으며,
`MonTellTo(ctx, "b", command)`는 MonMap의 정확한 bare name `b`를 지정한다.
`mon.b` prefix를 제거하거나 숫자를 rank로 해석하지 않으며 wildcard를
확장하지 않는다. MGR 대상은 현재 active MGR이다. Tell은 daemon의
admin 명령 schema를 사용한다. MON은 read·write·execute, MGR은 allow-all
권한을 요구하며, 인증된 read-only 계정의 거절은 `CommandError`로 반환한다.
Tell에도 요청별 context·raw 출력·결과 불명확 및 자동 재실행 금지 계약이 적용된다.

`MonTellTo`는 먼저 주 MON의 admission을 기다리고 그 지도에서 대상 주소를
선택한다. 지정 MON에 global ID 0으로 시작하는 독립 인증을 거친 뒤, 한 번 받은
MonMap의 FSID·최소 지원 계열·이름과 원래 주소를 확인한다. 주소의 family·
nonce·scope·flow도 보존한다. 주 client의 global ID·ticket·MON/MGR 연결과
로그 구독을 교체하지 않는다. 일반 명령과 `MaxInFlight` 슬롯을 공유하며
입력 복사는 슬롯을 얻은 뒤 수행한다. `ConnectTimeout`은 endpoint별 setup을
제한하고, 연결 후 Tell 대기는 호출 context에 따른다. `Close`는 이 독립
연결의 진행 중 setup과 session 정리도 기다린다.

주 지도에 이름이 없으면 `ErrMonitorNotFound`, 독립 admission 지도에서
이름이나 원래 주소가 달라졌으면 `ErrMonitorTargetChanged`를 반환하며
명령은 아직 전송하지 않았다. 전송 후 지도 변경은 결과 불명확 오류의 원인이
될 수 있다. 주소를 여러 개 시도해도 지정한 이름의 v2 주소만 사용하며,
대상이 불가능하면 다른 MON으로 바꾸지 않는다.

`Command.Input`은 bulk 입력 bytes다. `Result.Data`는 binary를 포함한 원본
출력이고, `Message`는 상태 문자열, `Code`는 서버가 반환한 숫자다.
출력을 항상 JSON으로 해석하거나 서버 코드를 호스트 OS의 errno로 바꾸지 않는다.
음수 코드는 `*CommandError`와 함께 반환하며 `Result`도 보존한다.
서버의 명시적인 인증 거절은 `*AuthenticationError`로 반환한다.
`errors.As`로 인증 방법과 서버 코드를 확인할 수 있으며, 로컬 암호 검증
실패와 구분한다.

잘못된 Messenger frame·메시지 인코딩은 `errors.Is(err, ErrMalformedMessage)`로
확인할 수 있다. 완전히 받은 본문을 해석하다 발생한 `io.ErrUnexpectedEOF`도
원인으로 남으므로, 이 표시를 먼저 검사해 TCP 수신 중 단절과 구분한다.
이미 전송한 명령은 `OutcomeUnknownError`를 함께 유지하며 자동 재실행하지 않는다.

명령의 보수적인 송신 크기 검사와 frame·문자열·컨테이너·인증 transcript의
크기 또는 개수 상한 초과는
`errors.Is(err, ErrLimitExceeded)`로 확인한다. `MaxFrameSize` 외의 고정
상한도 포함한다. 유효한 서버 응답이 로컬 frame 상한을 넘는 경우에는
`ErrMalformedMessage`가 아니며, 인코딩의 길이·개수 검증 실패는 두 표시를
함께 가질 수 있다. 명령 입력을 송신 전에 거절하면 결과 불명확 오류가
아니지만, 전송 후 응답을 받지 못하면 `OutcomeUnknownError`를 보존한다.
Watch 큐 overflow·명령 슬롯 대기·잘못된 Options 값은 별도 오류다.

## MON raw OSDMap 조회·구독

`RequestOSDMaps(ctx, OSDMapRequest{})`는 최신 full map을 받는다. 지정 epoch의
full/incremental inclusive range도 조회할 수 있다. 응답은 같은 `OSDMapBatch`
타입이며 내포한 지도는 원본 bytes다. OSD ticket이나 `EnableOSD`는 필요 없다.

```go
latest, err := client.RequestOSDMaps(ctx, cephmsgr.OSDMapRequest{})
if err != nil {
    return err
}
batch, err := client.RequestOSDMaps(ctx, cephmsgr.OSDMapRequest{
    IncrementalFirst: firstEpoch, IncrementalLast: lastEpoch,
})
// 사용 레이어가 full baseline·증분 연속성·trim 및 추가 조회 여부를 판단한다.
```

각 range pair는 모두 0이면 비활성, 그렇지 않으면 `0 < First <= Last`다.
서버는 retained/current history와 count·bytes 상한으로 응답을 자를 수 있으며
full map부터 같은 예산을 사용한다. 한 개의 빈 또는 partial batch도 정상
응답이다. `NewestMap`을 요청 범위 완료나 지도 적용 epoch로 해석하지 않는다.
응답에 상관 ID가 없어 조회마다 독립 인증·MonMap admission을 통과한 연결을
사용한다. 동시 조회는 명령과 같은 `MaxInFlight`를 공유하고 shared MON의
identity·구독을 보존한다. 자동으로 다음 range를 조회하거나 요청을 재실행하지 않는다.

`WatchOSDMaps(ctx, OSDMapOptions{StartEpoch: epoch})`는 client당 하나의 연속
구독을 만든다. 0은 최신 full baseline, 양수는 inclusive history를 요청한다.
watch context는 복구를 포함한 수명, `Next(ctx)`의 context는 그 대기만 제한한다.
MON `osd` read capability가 필요하며 서버가 부족한 권한의 구독을 조용히
버릴 수 있다. 로컬 등록 성공이나 timeout을 서버 권한 판정으로 바꾸지 않는다.

큐는 FIFO 64개이며 `MaxBufferedBytes`는 기본 `MaxFrameSize`, 1 KiB–1 GiB다.
OSD 지도 큐와 같은 보수적 byte accounting을 사용한다. 초과하면
`ErrOSDMapWatchOverflow`로 watch만 끝내고 접수한 batch와 공유 MON 명령을 유지한다.
`Close`는 worker를 기다리며 `ErrOSDMapStreamClosed`, Client Close는 `ErrClosed`다.
접수한 batch는 처음 기록한 terminal 오류보다 먼저 drain한다.
인증 갱신·MON 전환은 마지막 접수 blob 최대 epoch + 1로 재구독한다.
이는 delivery cursor이며 map 적용을 뜻하지 않는다. 빈 batch·trim·중복과
동일 MON의 이전 watch에서 늦게 온 응답도 보존하므로 완전한 chain은 보장하지 않는다.
MON에는 구현된 PGID64 wire 기능을 협상하지만 CRUSH 계산·지도 적용·대상 선택은
사용 레이어에 둔다.

20.2.4·Darwin arm64·IPv4 host relay의 aes256k secure/CRC와 Linux arm64
container·AES secure/CRC에서 최신 full, 고정 full/incremental range, 미래 epoch의
빈 응답, 동시 private 조회와 연속 구독을 native decoder/encoder의 FSID·epoch·
정확한 bytes로 대조했다. Full map은 같은 epoch의 native canonical map을
peer feature로 인코딩해 비교하고, incremental은 수신 blob을 native 재인코딩해
비교한다. Full map의 unordered blocklist는 decode→encode에서 순서가 바뀔 수
있으므로 서버와 같은 canonical 입력을 사용한다. 구독은 만료 전 AUTH ticket 갱신 두 차례와 유일한
seed의 TCP 단절을 거쳐 learned MON에서 이어졌다. Linux 시험은 실제 pool을
포함하며 기존 OSD 지도 전달도 함께 통과했다. Synthetic peer는 취소·overflow·
queued cursor·최대 epoch·source guard·정리 경합과 최초 오류 보존을 검증한다.

```sh
CEPH_MSGR_TEST_RUNTIME=host \
  CEPH_MSGR_TEST_RUN='^TestCephMonOSDMaps.*Integration$' sh integration/run.sh
```

## MON raw FSMapUser·MDSMap 구독

`Options.EnableMDSMaps = true`를 Dial 전에 설정하면 MON에서 현재 Tentacle
MDSENC bit29를 지원·필수 기능으로 협상한다. MGR/OSD 기능이나 MDS service
티켓은 추가하지 않는다. `WatchFSMapUser`는 raw filesystem directory를,
`WatchMDSMaps`는 호출자가 명시한 nonnegative FSCID의 raw 지도를 구독한다.
파일시스템 선택·이름 조회·MDS daemon/session·CephFS 작업은 구현하지 않는다.

```go
options.EnableMDSMaps = true
client, err := cephmsgr.Dial(ctx, options)
if err != nil {
    return err
}
defer client.Close()
filesystems, err := client.WatchFSMapUser(ctx, cephmsgr.FSMapUserOptions{})
if err != nil {
    return err
}
defer filesystems.Close()
mdsMaps, err := client.WatchMDSMaps(ctx, filesystemID, cephmsgr.MDSMapOptions{})
if err != nil {
    return err
}
defer mdsMaps.Close()
update, err := mdsMaps.Next(ctx)
// update.Map은 pools/ranks/daemon state/feature 요구 등 수신한 wire metadata다.
// update.FilesystemID는 호출자가 준 local binding이며 응답에 실린 FSCID가 아니다.
```

두 종류는 client당 각각 하나이며 MON 명령과 함께 사용한다. StartEpoch는
inclusive, 0은 현재 관찰이다. 중복·역행 epoch는 무시하고 MON 전환 시 마지막
접수 epoch + 1로 재구독한다. 최대 uint32 epoch는 접수 후
`ErrMDSMapEpochExhausted`로 끝내며 wrap하지 않는다. 각 FIFO는 64개와 보수적인
metadata byte 예산으로 제한한다. `MaxBufferedBytes`는 기본 MaxFrameSize,
1 KiB–1 GiB이며 초과하면 `ErrMDSMapWatchOverflow`로 해당 stream만 끝낸다.
Next 대기 취소는 stream을 유지하고 lifetime 취소·Close는 로컬 작업이다.
접수한 지도는 최초 terminal 오류 전에 drain한다.

MMDSMap에는 FSCID가 없어 같은 MON session은 한 MDS watch에만 영구 binding된다.
Close 뒤 같은 ID라도 새 MDS watch는 `ErrMDSMapSourceBound`다. 새 Client를
사용하면 독립적으로 구독할 수 있다. FSMapUser는 같은 cluster-wide topic이라
Close 후 재구독할 수 있지만 이전 구독의 늦은 frame도 새 StartEpoch를 만족하면
전달될 수 있다. SourceGeneration은 같은 MON source에서 두 stream이 공유하는
로컬 delivery context이며 wire generation이나 새 구독 이후 전송의 증거가 아니다.
권한 부족·존재하지 않는 FSCID의 무응답을 서버 권한 판정으로 해석하지 않는다.

수신 UUID·outer/inner epoch·source identity를 검증하고 raw front/map과 unknown
state/flags/required-client-feature bytes를 보존한다. Native map 5개로 codec과
public projection을 대조했고 synthetic MON은 feature 협상·queue bounds·복구·
취소·tombstone·최초 오류를 검증한다. 실제 MON 연동 결과와 MDS daemon 지원은
별도 검증 범위다. 상세한 API·identity·미지원 경계는
[raw MON map 계약](cephmsgr/MDS_MAPS_CONTRACT.md)을 따른다.

[지도 구독 실검증](integration/fixture/MDS_MAPS_VALIDATION.md)은 CRC/AES host IPv4
single BlueStore와 secure/AES256K container IPv6 single MemStore에서 native 지도
식별자·epoch·pool 대조, 취소, 일반 MON 명령 유지와 OSD 연결 0회를 확인했다.
각 구성에서 MDS 지도 1개와 기존 OSD 12개 사례를 함께 실행했다. 실제 MDS daemon
인증·session·CephFS 지원은 별도 단계다.

별도 [내부 fresh MDS Messenger 연결](internal/session/MDS_FRESH_CONTRACT.md)은
고정 MDS role·명시 active rank·정확한 주소/nonce와 stateful 최초 연결을 구현했다.
CRC/AES host IPv4 및 secure/AES256K container IPv6의 실제 rank 0 MDS에서
각각 인증과 keepalive ACK가 통과했다. 실제 client/service key 종류, 단일
MON/MDS 연결, 살아 있는 연결의 OPEN 세션 부재와 자원 정리를
[검증 기록](integration/fixture/MDS_FRESH_VALIDATION.md)에 남겼다.
공개 raw 명령 경로는 아래 별도 API를 사용한다. CephFS OPEN·metadata I/O·연결
복구는 아직 구현하지 않았다.

## MDS 대상 raw 명령

`Options.EnableMDS = true`를 Dial 전에 설정하면 MON에서 MDS service ticket을
획득·갱신한다. OSD 연결과 지도 구독은 각각 독립 옵션이다. 호출자는 native 지도
등에서 확인한 FSCID·active rank·숫자 v2 주소와 명시 nonce를 전달한다.

```go
options.EnableMDS = true
client, err := cephmsgr.Dial(ctx, options)
if err != nil {
    return err
}
defer client.Close()
mds, err := client.OpenMDS(ctx, cephmsgr.MDSTarget{
    FilesystemID: filesystemID,
    Rank: rank,
    Address: addressWithNonce,
})
if err != nil {
    return err
}
defer mds.Close()
result, err := mds.Command(ctx, cephmsgr.Command{JSON: commandJSON, Input: input})
```

결과는 원시 binary `Data`·서버 `Message`·signed `Code`와 로컬 요청의 TID,
GlobalID, 연결 generation, 원래 target을 보존한다. 음수 서버 코드는
`CommandError`와 함께 반환한다. 전송 후 취소·종료·단절은 `OutcomeUnknownError`를
보존하며 재실행하지 않는다. 연결 실패 뒤에는 호출자가 Close하고 새 Open을
명시해야 한다. 연결 수는 `MaxMDSConnections`(기본 16), 진행 요청은 공유
`MaxInFlight`로 제한한다. `Snapshot().MDSTicket`은 비밀값 없이 갱신 시간을 제공한다.

FSCID는 로컬 캐시·결과 metadata이며 wire 인증 대상이 아니다. ServerIdent는
정확한 주소/nonce와 active rank를 확인하며 daemon GID를 증명하지 않는다.
CephFS OPEN 없이 명령을 보낼 수 있으나 native `allow_all` 권한 검사를 받는다.
`allow *`에 fsname을 붙여도 파일시스템별 명령 권한 격리를 보장하지 않는다.
명령 schema, 대상 발견·선택, 권한 정책은 상위 레이어가 담당한다.
[공개 계약](cephmsgr/MDS_COMMAND_CONTRACT.md)과
[실검증 기록](integration/fixture/MDS_COMMAND_VALIDATION.md)에 정확한 범위를 구분했다.
CRC/AES host IPv4 BlueStore와 secure/AES256K container IPv6 MemStore에서 각각
11개 사례가 통과했다. 검증 구성은 legacy CRUSH·HASHPSPOOL 미사용 단일 OSD와
active rank 0이다. raw 명령의 다중 rank 대상 전환과 MDS failover 중 명령은 미검증이다.
CephFS session과 metadata 통신의 검증은 아래 절에 따로 기록했다.

## CephFS MDS session

`MDSConnection.OpenSession`은 명시한 MDS 연결에서 CephFS client session을 연다.
REQUEST_OPEN을 보내고 OPEN을 기다리며, 연결 하나에 session 하나만 둔다.

```go
session, err := mds.OpenSession(ctx, cephmsgr.MDSSessionOptions{
    Metadata:      map[string]string{"entity_id": "app", "root": "/"},
    RenewInterval: 15 * time.Second, // MDSMap session_timeout보다 짧게
})
if err != nil {
    return err
}
defer session.Close(ctx)
```

`Metadata`는 그대로 전송하며 hostname·PID·버전을 추가하지 않는다. MDS는 `root`를
MDS 경로 권한과 대조하고 중복 `uuid`를 거절한다. 기본 profile이 선언하는 CephFS
client feature는 MIMIC까지의 bit 0–8로 고정되며 호출자가 넓힐 수 없다. 이후 형식이
필요한 REPLY_ENCODING 등은 아래의 `EnableCephFS` profile에서만 선언한다. 그래서
파일시스템이 선언하지 않은 feature를 필수로 요구하면 MDS의 `missing required features`
거절을 그대로 반환한다.

session이 열려 있거나 stale인 동안 라이브러리는 `RenewInterval`마다
REQUEST_RENEWCAPS를 보내고, native client처럼 OPEN과 STALE 직후에도 한 번 보낸다.
`Renew`는 명시적으로 갱신하고 응답을 기다린다. STALE·RECALL_STATE·FLUSHMSG·
FORCE_RO와 이후의 OPEN은 `Next`로 도착 순서대로 받는다. FLUSHMSG의 응답은
라이브러리가 대신 보내지 않으며 호출자가 `AcknowledgeFlush`를 호출한다.
이벤트 큐가 넘치면 MDS 연결 전체를 실패시킨다.

명시적인 REJECT는 `*MDSSessionRejectedError`(원문 `error_string`, blocklist flag)로,
OPEN 전 CLOSE는 `ErrMDSSessionClosed`로 반환한다. REQUEST_OPEN을 보낸 뒤 ctx가
끝나면 열리는 중인 session과 `OutcomeUnknownError`를 함께 반환한다. 이때 호출자는
`WaitOpen`으로 계속 기다리거나 `Close`로 끝낸다. `Close`는 REQUEST_CLOSE를 보내고
MDS의 CLOSE를 확인한다.

MDS는 session을 연결이 아니라 client GlobalID에 묶는다. 같은 GlobalID의 새 연결은
기존 session을 가져가므로, session이 살아 있거나 종료를 확인하지 못한 MDS 주소에는
같은 Client가 새 연결이나 두 번째 session을 만들지 않고 `ErrMDSSessionActive`를
반환한다. 확인되지 않은 원격 session은 MDS의 `session_autoclose` 뒤 eviction되며 기본
설정에서는 해당 주소가 blocklist된다. 기본값 `mds_defer_session_stale=true`에서는 cap이
없는 session을 그 전에 STALE로 표시하지 않는다. 다른 session이 필요하면 새 Client를 사용한다.
기본 profile에서는 연결마다 별도의 주소 nonce를 쓰지만 `EnableCephFS` client는 native처럼
MDS·OSD 연결이 하나의 nonce(`State.CephFSNonce`)를 공유한다. 그래서 MDS eviction이
남기는 `ip:0/nonce` blocklist가 같은 IP에서 나간 이 client의 OSD I/O도 막는다. 자세한 범위는 [session 계약](cephmsgr/MDS_SESSION_CONTRACT.md)에
정리했다. 실제 Tentacle 20.2.4 MDS에서 CRC/AES host IPv4 BlueStore와 secure/AES256K
container IPv6 MemStore 구성이 각각 9개 사례를 통과했다. Native `session ls`의
metadata·feature bit, 주기 갱신, STALE 뒤 자동 재개, 확인된 종료, 세 가지 REJECT
원문, eviction 뒤 Lost 상태와 정확한 주소의 blocklist를
[검증 기록](integration/fixture/MDS_SESSIONS_VALIDATION.md)에 남겼다.

## CephFS metadata 요청과 MDS push

`Options.EnableCephFS = true`(EnableMDS 필요)를 Dial 전에 설정하면 모든 MDS 연결이
CephFS profile을 사용한다. Messenger에서는 FLOCK·DIRLAYOUTHASH·CREATEPOOLID·
REPLY_CREATE_INODE·EXPORT_PEER·MDS_INLINE_DATA·MDS_QUOTA·FS_FILE_LAYOUT_V2를
지원·필수 기능으로 협상하며, 하나라도 없는 MDS는 handshake에서 거절한다. Session은
bit 0–9와 ALTERNATE_NAME·32BITS_RETRY_FWD·NEW_SNAPREALM_INFO·HAS_OWNER_UIDGID를
선언한다. Reclaim·multi-reconnect·delegated inode·metric 등은 선언하지 않는다.

```go
call, err := session.Submit(ctx, cephmsgr.MDSRequest{
    Op:   cephmsgr.MDSOpLookup,
    Path: cephmsgr.MDSFilePath{Ino: parentIno, Path: name},
    Args: args, // 호출자가 구성한 56-byte ceph_mds_request_args
    CallerUID: uid, CallerGID: gid, OwnerUID: uid, OwnerGID: gid,
})
if err != nil {
    return err
}
reply, err := call.Wait(ctx) // 첫 응답(unsafe 또는 safe)
trace, err := reply.DecodeTrace()
```

`Submit`은 호출자가 준비한 op·인자 union·경로·release·data segment를 MClientRequest
v6(request head v3)로 보낸다. `Wait`는 첫 응답을, `WaitSafe`는 journal에 기록된 safe
응답을 반환한다. Forward는 `*MDSRequestForwardedError`로 끝내며 라이브러리는 다시
보내지 않는다. Session이 먼저 끝나면 `OutcomeUnknownError`를 반환하고 요청을
재실행하지 않는다. 응답은 signed 결과 코드와 raw trace·extra·snap blob을 보존하며
`DecodeTrace`·`DecodeReaddir`·`DecodeSnapTrace`로 native client 순서대로 해석한다.

CephFS profile의 MDS 연결은 native client처럼 lossless로 동작한다. TCP 연결이
끊기면 같은 MDS 주소에 다시 인증하고 SESSION_RECONNECT로 Messenger session을
이어간다. 확인받지 못한 message는 원래 sequence로 다시 보내므로 CephFS session과
진행 중 요청이 유지되며 `MDSConnection.Resumes`로 재개 횟수를 확인한다. MDS가
RESET으로 답하거나 30초 안에 재개하지 못하면 연결을 끝내고 session은 Lost가 된다.
이것은 MDS 재시작 뒤의 CephFS reconnect와 다르다.

MDS daemon이 재시작하거나 failover하면 새 incarnation은 새 주소·nonce를 쓰므로
재개는 RESET으로 끝나고 session은 Lost가 된다. 호출자는 MDSMap에서 새 주소를 확인해
이전 연결을 닫고 새 연결을 연 뒤, MDS의 reconnect 단계 안에
`ReconnectSession(ctx, lost, state, options)`을 호출한다. 호출자가 보유한
capability와 snap realm 상태를 MClientReconnect v5로 보내며 MDS가 OPEN으로 답하면
새 session generation이 시작된다. CLOSE는 `ErrMDSSessionReconnectDenied`로 반환한다.
이전 session의 요청은 자동으로 다시 보내지 않는다. 호출자가
`MDSReconnectState.Resubmit`에 결과 불명확 요청을 명시하면 native client처럼
reconnect message보다 먼저 원래 TID로 보낸다. Unsafe 응답을 받은 요청은 REPLAY와
대상 inode를, 응답이 없던 요청은 retry 증가를 표시한다. MDS가 이미 journal에 기록한
TID는 완료 기록으로 답하고 다시 실행하지 않는다.

MDS가 보내는 capability·lease·snap realm·quota 메시지는 MDS와 같은 방식으로 push
sequence에 세고 `Next`로 도착 순서대로 전달한다. Revoke 응답, flush, capability
release, lease 응답은 호출자가 `SendCaps`·`ReleaseCaps`·`SendLease`로 명시적으로
보낸다. 경로 해석·inode cache·capability 상태·데이터 flush·reconnect·replay는
구현하지 않으며 상위 레이어가 담당한다. [CephFS 계약](cephmsgr/MDS_CEPHFS_CONTRACT.md)에
native vector 범위와 남은 검증을 구분했다. Request·reply·forward codec은 고정
소스의 native vector 7개와, capability·cap release·lease·snap·quota codec은 native
vector 10개와 일치했다. 실제 Tentacle 20.2.4 MDS에서 CRC/AES host IPv4 BlueStore와
secure/AES256K container IPv6 MemStore 구성이 각각 9개 사례를 통과했다. mkdir·create·
setxattr·readdir·lookup 결과를 native LibCephFS와 대조했고, native writer가 Go의 REVOKE
응답을 기다렸다가 진행하는 것과 cap 반환, 확인된 종료, 연결 끊김 뒤 재개를
[검증 기록](integration/fixture/MDS_CEPHFS_VALIDATION.md)에 남겼다. `FlushJournal`은
REQUEST_FLUSH_MDLOG로 journal flush를 요청하며, 응답·push·session 이벤트의 `Arrival`은
수신 순서를 하나의 번호로 알려준다.

[복구 검증 기록](integration/fixture/MDS_CEPHFS_RECOVERY_VALIDATION.md)에는 같은 두 구성에서
rank 0 MDS를 재시작한 결과를 남겼다. 이전 주소로의 재개는 SESSION_RESET으로 끝났고,
새 incarnation에 `ReconnectSession`이 OPEN을 받았다. REPLAY로 다시 보낸 create는 원래
inode로 safe 완료됐고 응답이 없던 create는 한 번만 실행됐다. Eviction 뒤 blocklist에는
`<ip>:0/<CephFSNonce>` 하나가 생겼고 같은 client의 OSD 읽기는 -108을 받았다.
`FlushJournal`은 unsafe→safe 지연을 최대 5초에서 수 ms로 줄였다.
[push 검증 기록](integration/fixture/MDS_CEPHFS_PUSH_VALIDATION.md)에는 dentry lease
REVOKE와 RELEASE 응답, snapshot UPDATE·SPLIT·DESTROY, quota, RECALL_STATE와 cap 반환,
`cache drop` 중 FLUSHMSG와 `AcknowledgeFlush` 응답을 실제 MDS에서 확인한 결과를 남겼다.
[다중 rank 검증 기록](integration/fixture/MDS_CEPHFS_RANKS_VALIDATION.md)에는 active rank 두 개와
standby 구성에서 rank별 session, rank 1로의 forward와 재제출, subtree 이동 중 cap
EXPORT·IMPORT와 FLUSHMSG 응답, rank 0 standby takeover 뒤 `ReconnectSession` 결과를 남겼다.
`ResubmitForwarded`는 forward된 요청을 원래 TID로 대상 rank session에 한 번 다시 보낸다.
[완료 기록 검증 기록](integration/fixture/MDS_CEPHFS_COMPLETION_VALIDATION.md)에는 MDS를
네 번 재시작하며 확인한 경계 사례를 남겼다. MDS가 실행하고 journal에 남겼지만 응답이
버려진 create와 mkdir을 retry 1로 다시 보내자 MDS는 완료 기록으로 답했다. create는
create 응답으로, mkdir은 LOOKUP으로 바뀐 응답으로 왔고 제품은 이 변환 응답을 받아들인다.
Unsafe 응답만 받은 create의 REPLAY는 다시 실행되지 않고 완료 기록의 inode로 답을 받았다.
`mds_deny_all_reconnect`와 reconnect 시한 초과에서는 `ErrMDSSessionReconnectDenied`가
나왔고 주소 instance가 blocklist에 올라 같은 OSD 연결의 읽기가 -108을 받았다. FORCE_RO
session event 뒤 mkdir은 -30, 조회는 성공했다. Dentry lease RENEW는 다음 sequence의 RENEW
응답을 받았고, native snapshot 뒤 보낸 FLUSHSNAP은 FLUSHSNAP_ACK를 받았다.

## RBD 기반 통신 검증

RBD는 별도 Messenger 서버가 아니므로 제품은 RBD API를 제공하지 않는다. 대신 native
librbd로 만든 image를 기존 명시적 OSD API만으로 읽어 대조했다. Image header·striping·객체
이름 해석은 시험 전용 코드(`integration/rbd_base_test.go`)에 두었다.
[검증 기록](integration/fixture/RBD_BASE_VALIDATION.md)에는 CRC/AES host IPv4 BlueStore와
secure/AES256K container IPv6 MemStore의 legacy-v6 단일 OSD에서 각각 5개 사례가 통과한 결과를
남겼다. `rbd` class의 get_id·get_size·get_features·get_snapcontext·snapshot_get 원본 출력,
data 객체의 head·snapshot 읽기, LIST_SNAPS, SPARSE_READ가 native librbd·librados와 byte
단위로 일치했다. librbd가 header에 건 watch를 LIST_WATCHERS로 확인했고, Go watch가
resize notify를 받아 ack하면 native resize가 바로 끝났다.

쓰기 경로는 같은 pool에 native librbd가 만든 두 번째 image로 검증했다(시험 전용 코드
`integration/rbd_write_test.go`). Go는 header의 `get_snapcontext`로 얻은 SnapContext를 붙여
data 객체에 SETALLOCHINT와 WRITE를 보냈고, OSD는 snapshot이 아직 필요로 하는 객체만
clone했다. Native librbd는 head에서 Go가 쓴 bytes를, snapshot에서 쓰기 전 bytes를 읽었다.
Go가 보낸 `rbd.set_size`와 `rbd.metadata_set`은 native librbd의 stat과 metadata 목록에 그대로
나타났다. librbd managed-lock 형식(`rbd_lock`, tag `internal`, cookie `auto <watch cookie>`)으로
건 `lock.lock`은 `lock.get_info`와 native `list_lockers`에 보였고 `lock.unlock` 뒤 사라졌다.
librbd가 image를 연 채로 있을 때 Go가 header 크기를 바꾸고 HeaderUpdate notify를 보내자
librbd는 `ResponseMessage(0)`으로 ack했고, image를 다시 열지 않고도 바뀐 크기를 읽었다.
[쓰기 검증 기록](integration/fixture/RBD_WRITE_VALIDATION.md)에 두 구성의 결과를 남겼다.
Exclusive-lock 경로는 같은 pool에 `exclusive-lock`·`object-map`·`fast-diff` feature로 만든
세 번째 image로 검증했다(시험 전용 코드 `integration/rbd_lock_test.go`). Native librbd가
image를 쓰기 모드로 열어 lock을 잡은 상태에서 Go는 `lock.get_info`와 LIST_WATCHERS로
소유자의 cookie(`auto <watch handle>`)와 tag(`internal`)를 읽었다. Go가 자기 header watch를
등록하고 RequestLock notify를 보내자 librbd는 기본 자동 정책대로 `ResponseMessage(0)`으로
답했고, object map과 header의 lock을 푼 뒤 ReleasedLock을 보냈다. Go는 librbd 형식으로
header와 object map에 lock을 걸고 AcquiredLock을 알린 다음 object map에 객체를
OBJECT_EXISTS로 기록하고 data 객체를 썼다. 이어서 native librbd의 쓰기가 lock을 요청하자
Go는 `ResponseMessage(0)`으로 답하고 lock을 풀었으며, librbd는 lock을 다시 잡아 쓰기를
마쳤다. Native librbd는 Go가 쓴 bytes를 읽었고 `rbd object-map check`는 불일치를 찾지
않았다. Go도 librbd처럼 `lock.assert_locked`(read mode)와 `rbd.object_map_update`를 한
요청에 담아 보냈다. 다른 cookie로 보낸 같은 요청은 native와 같은 -16으로 끝났고 object map은
바뀌지 않았다.
[잠금 검증 기록](integration/fixture/RBD_LOCK_VALIDATION.md)에 두 구성의 결과를 남겼다.
Clone/parent, journaling, lock 경합과 blocklist는 아직 검증하지 않았다.

## OSD 객체 요청 통신

`Options.EnableOSD: true`로 OSD service ticket 획득·갱신을 명시적으로 켠다.
이 선택은 OSDMap 구독이나 배치를 시작하지 않는다. 사용 레이어가 준비한
숫자 v2 주소·nonce와 OSD ID로 `OpenOSD`를 호출하고, 객체 locator·raw hash·
지도 epoch를 요청에 넣는다. 다음 변수들은 사용 레이어에서 받은 값이다.

```go
osd, err := client.OpenOSD(ctx, cephmsgr.OSDTarget{
    ID: targetID, Address: targetAddress,
})
if err != nil {
    return err
}
defer osd.Close()
reply, err := osd.Request(ctx, cephmsgr.OSDRequest{
    MapEpoch: mapEpoch,
    Object: cephmsgr.OSDObject{
        Pool: poolID, Name: objectName, Namespace: namespace,
        Key: locatorKey, Hash: rawObjectHash,
    },
    Operations: []cephmsgr.OSDOperation{
        {Code: cephmsgr.OSDStat},
        {Code: cephmsgr.OSDRead, Offset: offset, Length: length},
    },
})
// reply.Code·Operations[i].Result·Data·RawFront·RawData를 보존해 전달한다.
// err가 있어도 reply에 서버 결과나 수신한 원본 bytes가 있을 수 있다.
```

같은 ID·wire 주소의 연결은 하나의 handle을 공유한다. 다른 주소로 바꾸려면
기존 handle을 닫는다. `MaxOSDConnections`는 setup을 포함해 기본 16개를
허용하며 `MaxInFlight`는 MON/MGR/OSD 요청이 공유한다. Setup context는
성공한 handle의 수명과 독립적이다. Handle·client 종료는 연결 정리를 기다린다.

기본 `OSDProfileObjectHash`는 `MOSDOp/MOSDOpReply v6`를 사용한다.
요청·응답 `pg_t` seed는 **raw object hash**이며 실제 PG 번호가 아니다.
Replicated pool의 현재 객체에 대한 stat과 양의 길이 read, write/writefull,
append, truncate, zero, create/delete와 version/extent 조건을 제공한다.
제한 xattr GET/GETALL·STRING 비교·SET/RM도 같은 ordered vector에서 제공한다.
OMAP은 제한된 단일 페이지 GETKEYS/GETVALS와 지정 키 BYKEYS 읽기, raw header
읽기, OMAP_CMP 비교와 SETVALS·SETHEADER·CLEAR·RMKEYS·RMKEYRANGE 쓰기를 제공한다.
Stat bytes는 size와 mtime의 wire 출력이며 filesystem API로 변환하지 않는다.
Aggregate 서버 code와 operation별 result는 서로 다를 수 있다. 예를 들어
존재하지 않는 객체는 aggregate -2와 실행되지 않은 stat의 result 0을 반환한다.
`*OSDError`는 서버의 aggregate 음수 code를 보존한다.

`Options.OSDProfile=OSDProfileReplicatedPG`는 `Dial` 전에 선택하는 별도 고정
통신 계약이다. `EnableOSD`도 켜야 하며, MON에는 HASHPSPOOL 지원을, OSD에는
HASHPSPOOL·실제 PG·v8 request·backoff에 필요한 정해진 feature만 협상한다.
객체 요청마다 `Placement: &cephmsgr.OSDReplicatedPlacement{PG: actualPG,
Primary: targetID}`를 넣는다. `Object.Hash`는 계속 raw hash이며 `Placement.PG`
는 caller가 지도에서 계산한 실제 PG다. pool/primary가 대상과 다르거나 배치가
누락되면 송신 전에 거절한다. 이 profile의 v6 reply에는 실제 PG가 반환되고,
`Submission.Placement`는 전송 후 취소·종료에도 선택한 배치를 보존한다.
기본 profile은 객체 `Request`의 `Placement`를 받지 않는다. 두 profile 모두 CRUSH 계산·자동
배치·재라우팅을 하지 않는다. 현재 검증 구성은 legacy CRUSH와 HASHPSPOOL·8 PG의
replicated pool이며, 기본 optimal CRUSH·EC 지원으로 확대해 해석하지 않는다.
[O4 계약](internal/osd/O4_CONTRACT.md)에 feature와 검증 경계를 구분했다.
[PG 분할 실검증](integration/fixture/OSD_SPLIT_VALIDATION.md)은 single OSD에서
8→16→8 PG 변경, 기존 요청의 불명확 결과 보존, caller가 새 배치로 보낸 별도
읽기·쓰기와 설정 복구를 확인한다. 라이브러리가 분할을 감지하거나 요청을
재실행하지는 않는다.

`Options.OSDProfile=OSDProfileReplicatedPGReplyV8`은 같은 caller 배치·v8 request·
backoff 계약에 OSD bit58(NEW_OSDOPREPLY_ENCODING)을 필수로 추가한다. Request와
ListPG 모두 정확한 v8/compat2 reply만 받고 v6로 fallback하지 않는다. 기존
ObjectHash와 ReplicatedPG의 v6 reply 계약은 그대로다. MON은 두 replicated
profile에 같은 HASHPSPOOL 기능만 추가하며 MGR/MDS 기능은 바뀌지 않는다.
FAILOK·ONDISK·signed 서버 code·request identity 검증과 redirect를 따라가지
않는 정책도 공유한다. Native reply 5개와 기존 v6 vector, synthetic 공개 요청·
PGNLS·backoff 경로를 대조했다. 실제 새 profile은 legacy CRUSH·HASHPSPOOL·8 PG의
single OSD에서 CRC/AES host IPv4 BlueStore와 secure/AES256K container IPv6
MemStore를 검증했다. 각 구성의 기존 13개 case는 앞선 run에서 통과했으나 그
run 전체는 새 시험의 잘못된 zero-version 기대값으로 실패했다. 제품 변경 없이
기대값을 고친 새 3개 case는 별도 focused run에서 모두 통과했다. 이를 하나의
16-case 전체 통과나 기본 optimal CRUSH·EC 지원으로 해석하지 않는다.
오류 응답도 nonzero legacy/replay/user version과 앞선 READ bytes를 가질 수 있다.
이 필드나 음수 응답의 ONDISK flag를 변경의 commit·미실행 증거로 삼지 않는다.
[실제 검증 기록](integration/fixture/OSD_REPLY_V8_VALIDATION.md)과
[새 v8 reply 계약](internal/osd/OSD_REPLY_V8_CONTRACT.md)에 세부 경계를 적었다.

OSD 연결은 lossy session이며 전송한 요청을 재실행하지 않는다. 전송 후
취소·단절·손상된 응답에는 `OutcomeUnknownError`를 보존한다. Messenger ACK는
객체 요청 완료가 아니다. Redirect는 `ErrOSDRedirect`와 원본 metadata를
반환하고 따라가지 않는다. 연결 실패나 client global ID 변경 후에는 handle을
닫고 사용 레이어가 새 연결 여부를 결정한다. OSDMap 메시지는 다음의 raw
지도 큐로 전달한다. watch/notify 이벤트는 소유한 stream으로 전달하며,
replicated-PG profile은 backoff도 별도 처리한다. 미지원 control 메시지는
세션을 명시적으로 실패시킨다.
Watch/notify·backoff type에 요청과 같은 TID가 있어도 mutation 완료로
처리하지 않으며, 이미 보낸 요청은 결과 불명확과 local submission을 유지한다.
[control/event 계약](internal/osd/EVENT_CONTRACT.md)에 해당 합성 거절 시험과
후속 wire·feature·native 검증 요구를 기록했다.

replicated-PG 연결의 `NextBackoff(ctx)`는 원본 PG/shard·epoch·ID·범위와 인증
identity·연결 세대를 보존한다. `AckBackoff(ctx, block)`는 원 BLOCK의 명시적인
protocol ACK이며 객체 실행 완료가 아니다. 수신과 dequeue만으로 ACK하지 않는다.
차단 중 새 객체 요청은 전체 연결에서 미송신 `ErrOSDBackoff`로 거절한다.
UNBLOCK이 ACK보다 먼저 와도 원 BLOCK의 기록과 gate를 보존하며, 단일 writer가
원 ACK 차례에 도달한 뒤 해제한다. ACK가 큐에 접수된 뒤의 context 취소는
그 ACK를 철회하지 않는다.
이미 전송한 요청의 결과는 그대로 불명확할 수 있으며 UNBLOCK으로 재전송하지
않는다. 늦은 ACK와 writer 순서, 큐·잔류 ACK 기록의 제한은
[backoff 계약](internal/osd/BACKOFF_CONTRACT.md)에 기록한다.

한 객체의 쓰기·조건 vector는 단일 요청으로 전송한다. `Data`의 byte 수와
`Length`는 같아야 하며, truncate는 `Offset`에 새 크기를 넣는다.
`OSDAssertVersion`은 `Version`, `OSDCompareExtent`는 비교할 원본 `Data`를
받는다. `OSDExclusiveCreate`는 create에만 적용하며 `OSDFailOK`는 해당
operation의 실패를 보존하면서 뒤의 operation을 계속 실행하게 한다.

```go
reply, err := osd.Request(ctx, cephmsgr.OSDRequest{
    MapEpoch: epoch,
    Object: object, // caller가 준비한 locator와 raw placement hash
    ClientIncarnation: incarnation,
    ModificationTime: cephmsgr.OSDTime{Seconds: seconds, Nanoseconds: nanos},
    Operations: []cephmsgr.OSDOperation{
        {Code: cephmsgr.OSDAssertVersion, Version: expectedVersion},
        {Code: cephmsgr.OSDWriteFull, Length: uint64(len(data)), Data: data},
    },
})
```

입력 vector와 `Data`는 Request가 반환할 때까지 변경하지 않는다.

Xattr operation은 extent 대신 `Xattr` union을 쓴다. 이름은 빈 문자열이나
NUL을 허용하지 않으며 non-UTF8 bytes와 앞의 underscore를 그대로 보존한다.
SET/CMP의 값은 `Data`이고 nil/빈 값은 제거가 아니다. STRING 비교는 caller의
operand를 왼쪽에 두는 byte 비교다. `OSDGetXattr`·`OSDGetXattrs`는 양의 local
`MaxOutputBytes`를 요구하며 서버 truncation/pagination을 요청하지 않는다.
응답은 raw union 길이와 각 `Data`를 보존하고 GETALL map을 해석하지 않는다.

```go
reply, err := osd.Request(ctx, cephmsgr.OSDRequest{
    MapEpoch: epoch, Object: object,
    Operations: []cephmsgr.OSDOperation{
        {Code: cephmsgr.OSDCompareXattr,
         Xattr: cephmsgr.OSDXattrInput{Name: "tag", Comparison: cephmsgr.OSDXattrEqual},
         Data: expectedTag},
        {Code: cephmsgr.OSDSetXattr,
         Xattr: cephmsgr.OSDXattrInput{Name: "tag"}, Data: newTag},
    },
})
```

현재 SET/RM 뒤 같은 vector의 GET/GETALL/CMP는 staged-read 의미를 보장하지
못해 송신 전에 거절한다. 비교를 먼저 놓는 순서가 지원된다. U64 비교,
bulk SET/RESET, snapshot은 별도 미구현 단계다.

OMAP 읽기는 `Omap` 입력을 쓰며 extent·Version·Data·Xattr는 0이다.
`StartAfter`는 exclusive cursor, `Prefix`는 GETVALS의 byte prefix이며
`MaxEntries`가 0인 페이지도 원본 more 상태를 반환한다. `Keys`는 BYKEYS에만
쓰고 중복을 거절하며 복사본을 native 순서로 정렬한다. 문자열은 원본 wire
bytes로 보존한다. `MaxOutputBytes`는 local 응답 제한이며 서버 byte cap이나
자동 페이지 이동을 요청하지 않는다. GETKEYS/GETVALS의 raw `Data`는 native
set/map 뒤 more bool을 포함하고 BYKEYS map에는 more가 없다.

```go
reply, err := osd.Request(ctx, cephmsgr.OSDRequest{
    MapEpoch: epoch, Object: object,
    Operations: []cephmsgr.OSDOperation{
        {Code: cephmsgr.OSDGetOmapValues,
         Omap: cephmsgr.OSDOmapInput{
             StartAfter: cursor, Prefix: prefix, MaxEntries: 32,
             MaxOutputBytes: 64 << 10,
         }},
        {Code: cephmsgr.OSDGetOmapValuesByKeys,
         Omap: cephmsgr.OSDOmapInput{
             Keys: []string{"version", "tag"}, MaxOutputBytes: 4096,
         }},
    },
})
```

응답의 `Length`는 native가 echo한 encoded input byte 수이고, 출력 byte 수는
`PayloadLength`다. 누락된 키와 빈 값은 원본 map에서 구분한다. 출력 해석,
다음 cursor 선택과 여러 페이지의 일관성은 사용 레이어가 맡는다.
[OMAP 계약과 검증 범위](internal/osd/OMAP_CONTRACT.md)에 native C API의
NUL cursor/prefix 제한을 기록했다.

OMAP header와 쓰기도 같은 `Omap` 입력을 쓴다. `OSDGetOmapHeader`는 입력 없이
양의 `MaxOutputBytes`만 받고 raw header bytes를 길이 prefix 없이 `Data`로
반환한다. `OSDSetOmapValues`는 `Entries`, `OSDSetOmapHeader`는 `Header`,
`OSDRemoveOmapKeys`는 `Keys`만 받으며 `OSDClearOmap`은 입력이 없다.
`OSDRemoveOmapKeyRange`는 `RangeBegin`(포함)과 `RangeEnd`(제외)로 반열린 구간의
키를 지운다. 같은 끝점은 실행되는 빈 삭제다. 역순 구간은 작은 객체에서는 빈
삭제지만 키가 많으면 고정 RocksDB 경로가 그대로 DeleteRange에 넘기므로 송신 전에
거절한다. 쓰기의
`MaxOutputBytes`는 0이어야 한다. 중복 키는 송신 전에 거절하고 복사본을 native
byte 순서로 정렬한다. 빈 `Entries`·`Keys`도 실제로 전송하는 mutation이며 빈
`Header`는 CLEAR가 아니다. 서버의 CLEAR는 값과 header를 함께 지운다.

```go
reply, err := osd.Request(ctx, cephmsgr.OSDRequest{
    MapEpoch: epoch, Object: object,
    ModificationTime: cephmsgr.OSDTime{Seconds: seconds, Nanoseconds: nanos},
    Operations: []cephmsgr.OSDOperation{
        {Code: cephmsgr.OSDGetOmapHeader,
         Omap: cephmsgr.OSDOmapInput{MaxOutputBytes: 64}},
        {Code: cephmsgr.OSDSetOmapValues,
         Omap: cephmsgr.OSDOmapInput{Entries: []cephmsgr.OSDOmapEntry{
             {Key: "version", Value: newVersion},
         }}},
        {Code: cephmsgr.OSDRemoveOmapKeys,
         Omap: cephmsgr.OSDOmapInput{Keys: []string{"stale"}}},
    },
})
```

`OSDCompareOmap`은 `Assertions`만 받고 출력이 없으며 `MaxOutputBytes`는 0이다.
각 assertion은 저장된 값을 왼쪽 operand로 두는 native 의미를 따른다.
`OSDOmapLess`는 저장값이 `Value`보다 작다는 조건이다. 이는 operand를 왼쪽에
두는 xattr 비교와 반대다. 순서 비교는 고정 Tentacle 빌드의 `-fsigned-char`에
따라 byte를 signed char로 비교한 뒤 길이를 비교하므로 `bytes.Compare`와 다르다.
없는 키는 빈 값으로 비교하고 없는 객체는 `-ENOENT`다. 서버는 키 순서대로
평가해 처음 실패한 assertion에서 `-ECANCELED`를 반환한다. 지원하는 비교는
`OSDOmapEqual`·`OSDOmapGreater`·`OSDOmapLess`뿐이고 다른 식별자는 서버가 평가하지
않으므로 송신 전에 `ErrUnsupportedOSDOperation`으로 거절한다. 중복 키도 거절한다.

```go
reply, err := osd.Request(ctx, cephmsgr.OSDRequest{
    MapEpoch: epoch, Object: object,
    ModificationTime: cephmsgr.OSDTime{Seconds: seconds, Nanoseconds: nanos},
    Operations: []cephmsgr.OSDOperation{
        {Code: cephmsgr.OSDCompareOmap,
         Omap: cephmsgr.OSDOmapInput{Assertions: []cephmsgr.OSDOmapAssertion{
             {Key: "version", Value: expected, Comparison: cephmsgr.OSDOmapEqual},
         }}},
        {Code: cephmsgr.OSDSetOmapValues,
         Omap: cephmsgr.OSDOmapInput{Entries: []cephmsgr.OSDOmapEntry{
             {Key: "version", Value: next},
         }}},
    },
})
```

OMAP 쓰기 뒤 같은 vector의 OMAP 읽기는 staged 상태를 보장하지 못해 FAILOK
쓰기 뒤에도 `ErrUnsupportedOSDOperation`으로 송신 전에 거절한다. OMAP 비교도
읽기로 취급한다. DELETE 뒤의 OMAP·xattr 읽기도 committed metadata를 읽을 수
있어 같은 이유로 거절한다. 읽기와 비교를 먼저 두는 순서는 지원한다. 쓰기를 포함한 vector는 RETURNVEC 응답의 per-op 출력이
서버의 `osd_max_write_op_reply_len`(기본 64 bytes)을 넘으면 Tentacle이
aggregate와 해당 operation에 `-EOVERFLOW`를 돌려주고 transaction을 commit하지
않는다. 이 거절은 서버 설정에 따르므로 client가 미리 판단하지 않고
`*OSDError`로 그대로 전달한다. 큰 OMAP 읽기는 별도 읽기 요청으로 보낸다.
거절 응답에도 서버가 ACK·ONDISK flag를 붙이므로 음수 aggregate의 ONDISK를
commit 근거로 해석하지 않는다.

`OSDCall`은 RADOS object class method를 호출한다. `Class` 입력의 class·method
이름(각각 1~255 bytes, NUL 불가)과 `Data`의 입력 bytes를 그대로 보내고, method의
signed per-op 결과와 raw 출력을 돌려준다. 양수 결과도 보존한다. `Mode`는
`OSDCallRead`·`OSDCallWrite` 중 하나를 반드시 지정한다. 다만 이 값은 request
flag와 mtime 허용 여부만 정하고, Tentacle OSD는 method 등록 flag로 실제
읽기·쓰기를 판정한다. 그래서 read mode로 보낸 write method도 쓰기로 실행된다.
`MaxOutputBytes`는 양의 local 출력 제한이다. 한 요청에서 CALL은 guard·읽기,
FAILOK가 아닌 CREATE, read mode CALL 뒤에 올 수 있다. librbd가 쓰는
`create(true)`와 `rbd.set_id`, `lock.assert_locked`와 `rbd.object_map_update`
형식이 여기에 해당한다. FAILOK CALL, write mode CALL이나 그 밖의 staged write
뒤의 CALL, CALL 뒤의 읽기·guard는 송신 전에 거절한다. method의 data 읽기는
길이와 full-object digest를 staged 상태에서 가져오면서 bytes는 committed 저장소에서
읽는다. 그래서 쓰기 method 뒤에 전체를 읽는 method를 보내자 실제 Tentacle OSD가
응답 없이 primary 복구를 반복했다. Read mode는 method가 읽기 전용으로 등록됐다는
호출자의 선언이다. 첫 실패가 요청 전체를 중단하고 뒤의 operation은 결과 0과 빈
출력으로 돌아온다. 이 동작을 native librados와 대조한 결과는
[복합 CALL 검증 기록](integration/fixture/OSD_CALL_COMPOUND_VALIDATION.md)에 있다.
없는 class·method와 권한 부족은
서버 결과(`-1`·`-95` 등)로 그대로 전달한다. [CALL 계약](internal/osd/CLASS_CONTRACT.md)에
출력 한도와 결과 의미를 정리했다.

```go
reply, err := osd.Request(ctx, cephmsgr.OSDRequest{
    MapEpoch: epoch, Object: object,
    Operations: []cephmsgr.OSDOperation{{
        Code: cephmsgr.OSDCall, Data: input,
        Class: cephmsgr.OSDClassInput{
            Class: "hello", Method: "say_hello",
            Mode: cephmsgr.OSDCallRead, MaxOutputBytes: 4096,
        },
    }},
})
// reply.Operations[0].Result는 method 반환값, Data는 method 출력이다.
```

`OSDSetAllocHint`는 `AllocHint`의 예상 객체 크기·예상 쓰기 크기·hint flag를 보내는
advisory 쓰기다. 서버는 없는 객체를 만들고 값을 object_info에 기록한다. 고정
소스에 정의된 hint flag 비트만 허용하며, 서버가 검증 없이 기록하는 미지의 비트는
송신 전에 거절한다. native builder는 항상 FAILOK을 붙이지만 이 client는 caller가
지정한 flag만 보낸다. [allocation hint 계약](internal/osd/ALLOC_HINT_CONTRACT.md)에
근거를 정리했다.

`OSDMapExt`는 `Offset`·양수 `Length` 범위의 논리적 할당 구간을 읽고,
`MapExt.MaxOutputBytes`로 수신 출력 한도를 지정한다(최소 4 bytes).
`Operations[0].Extents`와 원본 `Data`를 함께 제공한다. 물리적 저장 위치나
객체 bytes를 반환하지 않으며 hole의 해석은 호출자 책임이다. 현재는 단독 head
요청만 허용한다. 저장소별 구간 표현과 독립 native 비교 범위는
[MAPEXT 계약](internal/osd/MAPEXT_CONTRACT.md)에 기록했다.

`OSDChecksum`은 단독 head 요청의 `Offset`·양수 `Length`에 대해 CRC32C를
계산하도록 요청한다. `Checksum.Type=OSDChecksumCRC32C`와 u32 `Seed`를 지정한다.
`ChunkSize=0`은 전체 요청 구간에 하나의 CRC, 양수 값은 해당 크기의 chunk별
CRC를 반환하며 요청 길이는 chunk 크기의 배수여야 한다.
결과의 `Length`는 EOF에서 줄 수 있고, chunk에 맞지 않게 줄면 서버 `-EINVAL`을
보존한다. EOF 이후 조회는 원래 길이와 빈 출력을 반환한다. 해석한 CRC 값은
`Operations[0].Checksum.Values`, 원본은 `Data`에 남는다. xxHash와 길이 0의
암묵적 전체 읽기는 거절한다. [CHECKSUM 계약](internal/osd/CHECKSUM_CONTRACT.md)과
[실검증 기록](integration/fixture/OSD_READ_METADATA_VALIDATION.md)에 범위를 구분했다.

`OSDSparseRead`는 단독 head 요청의 `Offset`·양수 `Length`에서 할당 구간과
그 구간의 bytes를 함께 읽는다. `SparseRead.MaxOutputBytes`는 encoded extent map과
byte buffer 전체의 로컬 출력 한도다(최소 8 bytes). 성공한
`Operations[0].SparseRead`는 순서가 있는 `Extents`와 이어 붙인 `Data`를 제공하고,
원본 encoded 출력은 operation의 `Data`에 남는다. 결과 `Length`는 반환된
데이터 길이이며 hole은 포함하지 않는다. [계약](internal/osd/SPARSE_READ_CONTRACT.md)에
범위·EOF·오류 검증을 정리했다.

`ListPG`는 caller가 지정한 실제 PG와 primary OSD에 `PGNLS` 한 페이지를 보낸다.
`OSDPGListRequest.PG.Seed`는 이미 접힌 실제 PG seed이며, `Primary`는 해당 연결의
OSD ID와 같아야 한다. 최초 `Cursor`는 비워 두고 이후에는 직전 `Cursor.Raw`를
그대로 넘긴다. `Count`는 서버 entry 한도이고 `MaxOutputBytes`는 로컬 수신 한도다.
빈 페이지도 계속될 수 있으므로 `End`로 종료를 판단한다. `Namespace="\x01"`은
모든 namespace를 지정한다. pool 순회·배치·epoch 변경·재시도는 caller 책임이다.
[PG 열거 계약](internal/osd/PG_LIST_CONTRACT.md)에 cursor와 aggregate result를 구분했다.

`WatchObject`는 명시적인 등록과 `Next` 이벤트 수신을, `NotifyObject`는 일반
operation reply와 별도 completion 이벤트를 제공한다. 두 API는 `Operations`가 빈
`OSDRequest`의 대상·객체 metadata와 `OSDWatchOptions`를 받는다. 등록 뒤 `Ping`,
알림 처리 뒤 `Acknowledge`, 원격 해제 `Unwatch`는 caller가 명시적으로 호출한다.
로컬 `Close`는 원격 해제가 아니며 자동 ping·ACK·재등록·notify 재실행은 없다.
큐는 stream별 64개와 지정한 byte 한도로 제한하고 접수한 이벤트 뒤 최초 오류를
전달한다. completion의 signed timeout·partial replies·missed identities와 원본
front/data를 보존한다. 직접 `Request`로 watch opcode를 보내는 경로는 거절한다.
[watch/notify 계약](internal/osd/WATCH_NOTIFY_CONTRACT.md)에 인증 identity·연결 세대와
cookie 소유권·불확실한 송신 결과의 의미를 기록했다.

`newConnection.ReconnectWatch(ctx, oldWatch, request)`는 성공이 확인된 watch를
같은 Client/global ID의 더 새 연결로 명시적으로 옮긴다. caller가 새 epoch·배치를
제공하고 `Operations`는 비워 둔다. cookie는 유지하고 등록 세대는 증가한다.
이전 handle의 Ping·Unwatch·ACK는 로컬에서 거절하지만 이미 받은 이벤트는 읽을 수
있다. 오류와 함께 새 handle이 반환되면 그 handle이 등록 권한을 소유한다.
불명확한 WATCH/UNWATCH 뒤의 복구, 자동 재등록·알림 재실행은 제공하지 않는다.
[재연결 실검증](integration/fixture/OSD_WATCH_RECONNECT_VALIDATION.md)에 실제 Ceph
구성과 native 알림·응답·해제 증거를 기록했다.

`OSDListWatchers`는 head 객체의 현재 watcher 등록 목록을 읽는다. 단독 요청만
허용하며 `ListWatchers.MaxOutputBytes`로 수신 출력의 로컬 한도를 지정한다(최소
10 bytes). 성공하면 `Operations[0].Watchers`에 watcher entity·cookie·등록 timeout·
주소(type/nonce·IPv4/IPv6·flow/scope)가 해석되고 원본은 `Data`에 남는다.
이 목록의 entity는 조회 client와 별개다. 목록 조회는 watch 등록이나 알림 수신,
watcher 생존 확인을 수행하지 않는다. [계약](internal/osd/WATCHERS_CONTRACT.md)과
[실제 Ceph 검증](integration/fixture/OSD_WATCHERS_VALIDATION.md)에 범위를 기록했다.

```go
reply, err := connection.Request(ctx, cephmsgr.OSDRequest{
    MapEpoch: epoch, Object: object, // caller가 준비한 OSD 대상·배치 정보
    Operations: []cephmsgr.OSDOperation{{
        Code: cephmsgr.OSDListWatchers,
        ListWatchers: cephmsgr.OSDListWatchersInput{MaxOutputBytes: 64 << 10},
    }},
})
// 성공 시 reply.Operations[0].Watchers와 원본 Data를 사용한다.
```

snapshot은 caller가 MON에서 할당한 self-managed snap id를 그대로 전달한다. 이
client는 snap id를 할당·삭제하지 않고 pool의 snapshot 상태와 대조하지도 않는다.
읽기 전용 요청의 `ReadSnapshot`은 관찰할 clone을 고른다. 0은 head이며 reply는
snap id를 되돌려주지 않는다. 쓰기 요청의 `SnapContext`는 0이 아닌 snap id를
내림차순으로 담고 `Sequence`보다 클 수 없다. 서버는 `Snapshots[0]`이 객체의
snapset seq보다 새로우면 쓰기 전 head를 clone하며 clone id는 `Sequence`다.
`OrderSnapshots`를 켜면 더 오래된 context의 쓰기를 실행 전에
`OSDOldSnapContext`(EOLDSNAPC, 85)로 거절한다. snapshot을 지정한 쓰기, 읽기
요청의 SnapContext·ORDERSNAP, 잘못된 순서의 snap id는 송신 전에 거절한다.

`OSDListSnapshots`는 단독 요청으로만 보내며 client가 CEPH_SNAPDIR을 직접 지정한다.
결과는 `Operations[0].Snapshots`에 해석되고 raw bytes도 `Data`에 남는다.
`OSDRollback`도 단독 요청이다. `Snapshot`을 덮는 clone으로 head의 data·xattr·OMAP을
되돌린다. 덮는 clone이 없으면 서버는 head를 삭제하거나 clone이 남아 있을 때
whiteout으로 바꾸고 그래도 성공을 보고한다. 객체 snapset seq보다 큰 snap id는
version만 올린다. 요청 mtime이 0이면 head의 기존 mtime을 유지한다.
[snapshot 계약](internal/osd/SNAPSHOT_CONTRACT.md)에 근거를 정리했다.

```go
// 쓰기: snap 7이 객체의 snapset seq보다 새로우면 쓰기 전 head가 clone 7이 된다.
_, err := osd.Request(ctx, cephmsgr.OSDRequest{
    MapEpoch: epoch, Object: object, ModificationTime: mtime,
    SnapContext: cephmsgr.OSDSnapContext{Sequence: 7, Snapshots: []uint64{7, 3}},
    Operations: []cephmsgr.OSDOperation{{Code: cephmsgr.OSDWriteFull, Length: uint64(len(data)), Data: data}},
})
// 읽기: snap 3 시점의 clone을 읽는다.
reply, err := osd.Request(ctx, cephmsgr.OSDRequest{
    MapEpoch: epoch, Object: object, ReadSnapshot: 3,
    Operations: []cephmsgr.OSDOperation{{Code: cephmsgr.OSDRead, Length: 4096}},
})
// 목록: clone id·snap·overlap·크기와 snapset seq를 돌려준다.
reply, err = osd.Request(ctx, cephmsgr.OSDRequest{
    MapEpoch: epoch, Object: object,
    Operations: []cephmsgr.OSDOperation{{Code: cephmsgr.OSDListSnapshots,
        ListSnapshots: cephmsgr.OSDListSnapshotsInput{MaxOutputBytes: 4096}}},
})
```

`ModificationTime`의 0은 그대로 전송하며 client 시각으로 대체하지 않는다.
쓰기 성공은 서버의 `OSDReplyOnDisk`를 요구한다. 이것은 저장소가 보고한
완료이며, MemStore 재시작 후 persistence나 전원 장애 내구성 보장이 아니다.

불명확한 결과에도 `reply.Submission`은 할당된 TID·인증 global ID·incarnation·
입력 epoch·attempt를 보존한다. 이 로컬 metadata는 서버 실행 증거가 아니다.
`reply.Transaction`과 서버 원본 출력은 실제 응답을 받은 경우에만 의미가 있다.
한 객체 vector의 원자성을 서로 다른 요청/객체의 원자성 또는 일반적인
read-your-staged-writes 보장으로 확대하지 않는다.

[O0/O1 wire 계약](internal/osd/CONTRACT.md)과
[native 검증 기록](integration/fixture/OSD_MUTATIONS_VALIDATION.md)에 opcode·flag·완료
의미와 native 비교 범위를 기록했다. 20.2.4 IPv4·legacy CRUSH에서 single
BlueStore OSD/replica profile의 11개 시험이 통과했다. 이후 3-OSD BlueStore·
replicas 3/min_size 2의 전체 선택 시험 12개도 통과했다. 25개 mutation/guard
native 비교 사례에는 EOF 밖을 zero-extension으로 비교하는 CMPEXT 4개가
포함되며 reply length가 2 또는 0으로 줄어드는 metadata도 보존했다.
응답 손실 뒤 재실행 금지, 정상 재시작·SIGKILL 후 native/Go readback과
partial·EOF read/stat와 쓰기를 섞은 5개 vector를 검증했다.
3-replica profile에서는 ONDISK 완료 뒤
실제 primary를 SIGKILL하고 남은 두 replica에서 새 primary를 호출자가 지정했다.
새 Go 연결과 독립 native read가 binary bytes·version·mtime을 보존했고
추가 쓰기나 자동 mutation 재실행은 없었다. CRC/aes256k·single MemStore·Darwin
host relay에서도 25개 mutation/guard와 mixed vector 5개를 포함한 9개 시험이
통과했다(BlueStore 전용 3개 제외). CRC/AES client·service key·IPv4 host relay의
3-OSD BlueStore·replicas 3/min_size 2에서도 12개 시험 전체가 첫 실행에 통과했다.
동일한 25개 mutation/guard·mixed vector 5개와 실제 primary SIGKILL을 포함하며,
정리 후 소유 컨테이너 10개와 network 소멸을 독립 확인했다. 이 profile의 제품
소스는 `0943727781a4d41d5092d38ab600f03478dba6ef`다.
IPv6·secure·aes256k client/service key·Linux arm64 container의 같은 3복제본
BlueStore 구성도 전체 12개 시험, mutation/guard 25개 및 mixed vector 5개를
첫 native 실행에 통과했다. 모든 OSD의 실제 public endpoint가 IPv6임을
검사했으며, 정리 후 소유 컨테이너 10개와 network 소멸을 확인했다.
Host/device 전원 장애 검증은 아니다.

같은 객체·OSD 연결에 16개씩 동시 제출하는 별도 compound mutation 시험도
CRC/AES/AES·IPv4·Darwin arm64 host relay의 3-BlueStore 구성에서 통과했다.
독립 WRITE extent와 두 APPEND로 구성한 binary record의 원자성, 유일 TID·
version·ONDISK 및 native bytes/mtime을 비교했다. Append 완료 순서는
가정하지 않고 record 집합을 비교하며, 자동 재실행은 없다.

다만 nonempty WRITEFULL·APPEND·offset 0 WRITE 뒤 offset 0 READ/CMPEXT가
앞선 payload 크기 이상을 검사하는 조합은 `ErrUnsupportedOSDOperation`으로
송신 전에 거절한다. WRITEFULL 뒤 full READ는 실제 native checksum 실패를
관찰했다. WRITE/APPEND/CMPEXT의 같은 위험은 pinned 소스의 공통 read/write
경로에서 추론한 것으로 별도 실서버 실패 재현 주장은 아니다. Staged digest와
committed bytes 비교로 객체가 unreadable이 될 수 있어 보수적으로 제한하며
중간 operation이 있어도 유지한다.
그 밖의 partial·EOF mixed reads 통과를 일반적인 staged read 보장으로 확대하지 않는다.
O0 event 실검증, 넓은 replicated/주소·OS matrix, EC와
host/device 전원 장애 범위는 미완료다.

별도 [fresh native encoder fixture](internal/osd/testdata/mutation-native-README.md)는
고정 Ceph 소스에서 새 MOSDOp/MOSDOpReply 객체의 `encode_payload`를 직접
실행했다. Go의 519-byte request front와 11-byte data가 모두 일치했고,
네 native reply의 flags·union·result·version을 검증했다. Native envelope
설정과 명시적 offline seq/ACK/priority 입력의 41-byte header 직렬화도 Go
frame과 일치했다. 실제 ProtocolV2 송신 세션이나 서버 처리의 증거는 아니다.
재현 도구도 public Testcontainers의 소유 builder에서 실행한다.

[제한 xattr 검증](integration/fixture/OSD_XATTR_VALIDATION.md)은 fresh native
encoder·별도 decoder와 Go wire bytes를 비교했다. Secure/aes256k·IPv4
3-BlueStore의 Go/native client 대조에서는 42개 vector와 mixed GET/READ/STAT,
총 43개 하위 시험이 통과했다. Binary·빈 값·non-UTF8 이름, 여섯 STRING
비교·양수 per-op 결과·알려진 거절·FAILOK와 원본 bytes/mtime 복구를 확인했다.
이후 APPEND+xattr 응답 손실·Close·primary SIGKILL, namespace 격리,
서버 이름/값 제한과 readonly EPERM을 별도 시험했다. 이 검증을 포함한
`bc2b9df` CI는 42개 작업 모두 통과했고 9개 OSD profile의 실제 시험을 확인했다.
광범위한 pool/EC·snapshot·hardware 전원 장애와 미지원 metadata는 별도 범위다.

[제한 OMAP 읽기 검증](integration/fixture/OSD_OMAP_VALIDATION.md)은 fresh
native encoder/별도 decoder의 5개 message vector와 Go bytes를 대조하고,
CRC/AES host 3 BlueStore의 23개 실서버 Go/native 사례를 통과했다.
Secure/aes256k container 3 BlueStore의 CGO0 전체 OSD 시험도 22개 통과했다.
CI의 OMAP profile matrix는 해당 커밋의 독립 결과가 필요하다.

[OMAP header·쓰기 검증](integration/fixture/OSD_OMAP_WRITE_VALIDATION.md)은 fresh
native encoder/별도 decoder의 5개 message vector와 Go의 header/front/data를
대조했다. CRC/AES host 3 BlueStore에서는 native C API와 pinned rados CLI header
관찰을 쓴 17개 Go/native 사례가 통과했다. Binary·빈 키와 값, header 설정·읽기,
RMKEYS 뒤 header 유지, CLEAR의 header 삭제, 빈 mutation의 version·mtime 반영,
없는 객체 생성과 ENOENT, AssertVersion 실패 rollback, FAILOK을 확인했다.
Secure/aes256k container 3 BlueStore의 CGO0 전체 OSD 시험 23개도 통과했다.
쓰기 vector의 64-byte RETURNVEC 출력 한도 거절에서는 OMAP·header·bytes가
그대로였다. 다만 primary가 보고하는 user version과 mtime은 거절된 요청 값으로
앞서 나갔으며, 고정 소스의 `finish_ctx`가 cache에 먼저 반영한 결과로 기록했다.
쓰기 응답 손실·SIGKILL 복구, 권한 거절과 CI matrix는 별도 검증이 필요하다.

[OMAP 범위 삭제 검증](integration/fixture/OSD_OMAP_RANGE_VALIDATION.md)은 fresh
native encoder/별도 decoder의 5개 vector와 Go bytes를 대조했다. CRC/AES host 3
BlueStore의 8개 실서버 사례가 native C mirror와 일치했고, secure/aes256k
container의 CGO0 전체 OSD 시험 26개도 통과했다.

[object class CALL 검증](integration/fixture/OSD_CALL_VALIDATION.md)은 hello class로
18개 실서버 사례를 확인했고 secure/aes256k container의 CGO0 전체 OSD 시험 27개도
통과했다. 모든 사례에서 Go aggregate·per-op 결과·출력 bytes가
native C의 aggregate·prval·출력과 일치했다. 확인한 범위는 read/write method,
양수 결과 42, 쓰기 오류의 출력 반환, 520-byte 출력의 `-75`, 실행 전 거절인
`-95`·`-1`, read mode 요청의 쓰기 실행, readonly의 `-1`이다.

[allocation hint 검증](integration/fixture/OSD_ALLOC_HINT_VALIDATION.md)은 6개 실서버
사례에서 Go가 기록한 object_info hint를 고정 `ceph-dencoder`로 해석했고, native C
mirror와 같은 값임을 확인했다. 기존 객체의 bytes 유지, 최대값과 모든 flag, 0으로
초기화, 없는 객체 생성, FAILOK 뒤 쓰기를 포함한다. secure/aes256k container의
CGO0 전체 OSD 시험 28개도 통과했다.

[snapshot 검증](integration/fixture/OSD_SNAPSHOT_VALIDATION.md)은 fresh native
encoder/별도 decoder의 8개 vector 중 요청 4개(snap 읽기, ORDERSNAP 쓰기, ROLLBACK,
LIST_SNAPS)와 Go bytes가 일치함을 확인했다. CRC/AES host 3 BlueStore의 11개
실서버 사례에서는 native C로 할당한 self-managed snap으로 Go와 native twin을
비교했다. 두 번의 clone 생성, snap·head·snapset seq보다 큰 snap 읽기, rados CLI와
`ceph-dencoder`로 대조한 목록, ORDERSNAP `-85`, clone 복원·mtime 없는 rollback·seq
위 rollback, whiteout 삭제, 부재 객체 rollback, 덮는 clone이 없는 rollback의 head
삭제를 포함한다. secure/aes256k container의 CGO0 전체 OSD 시험 29개와 단일
MemStore 전체 OSD 시험도 통과했다.

[OMAP 비교 검증](integration/fixture/OSD_OMAP_COMPARE_VALIDATION.md)은 fresh
native encoder/별도 decoder의 5개 vector와 Go bytes를 대조했다. 정렬하지 않은
assertion 입력도 native map 순서로 인코딩함을 확인했다. CRC/AES host 3
BlueStore의 25개 실서버 사례와 secure/aes256k container의 CGO0 전체 OSD 시험
24개가 통과했다. 실서버 사례 중 단일 assertion 17개는 Go per-op 결과, native C
prval, test-only signed-char 모델이 모두 일치했다. arm64 OSD에서도 저장값
`0x80`이 `0x7f`보다 작다고 평가되어 signed 순서를 확인했다. 비교 뒤 쓰기의
commit·`-125` 취소·FAILOK과 없는 객체의 `-2`, 서버가 평가하지 않는 식별자의
native `-22`와 Go의 송신 전 거절도 확인했다. 한 operation에 여러 assertion을
담는 native 대조는 C API 제약으로 간접 증거이며 C++ oracle이 필요하다.

응답 대기 중 OSD 연결을 `Close`하는 별도 실서버 시험은 CRC/AES/AES·IPv4
3-BlueStore host relay에서 통과했다. Native read가 단일 append 실행을
먼저 확인한 뒤, caller context가 살아 있는 상태에서 `OutcomeUnknown`과
`ErrClosed`·local identity를 보존했다. 새 연결과 native read 모두 동일한
bytes/version/mtime을 확인했으며 append 재실행은 없었다.

```sh
CEPH_MSGR_TEST_OSD=1 CEPH_MSGR_TEST_OSD_STORE=bluestore \
  CEPH_MSGR_TEST_RUN='^TestCephOSD(Mutations|Compound|MutationLostReply|MutationRestart|MutationCrash|IO|Renewal)Integration$' \
  sh integration/run.sh

CEPH_MSGR_TEST_OSD=1 CEPH_MSGR_TEST_OSD_STORE=bluestore \
  CEPH_MSGR_TEST_OSD_REPLICAS=3 \
  CEPH_MSGR_TEST_RUN='^TestCephOSDReplicasIntegration$' \
  sh integration/run.sh

CEPH_MSGR_TEST_OSD=1 CEPH_MSGR_TEST_OSD_STORE=bluestore \
  CEPH_MSGR_TEST_OSD_REPLICAS=3 CEPH_MSGR_TEST_RUNTIME=host \
  CEPH_MSGR_TEST_CONNECTION_MODE=crc \
  CEPH_MSGR_TEST_KEY_TYPE=aes CEPH_MSGR_TEST_SERVICE_CIPHER=aes \
  CEPH_MSGR_TEST_RUN='^TestCeph(MonOSDMaps.*|OSD.*)Integration$' \
  sh integration/run.sh


CEPH_MSGR_TEST_OSD=1 CEPH_MSGR_TEST_OSD_STORE=bluestore \
  CEPH_MSGR_TEST_OSD_REPLICAS=3 CEPH_MSGR_TEST_IP_FAMILY=6 \
  CEPH_MSGR_TEST_KEY_TYPE=aes256k CEPH_MSGR_TEST_SERVICE_CIPHER=aes256k \
  sh integration/run.sh
```

`OSDConnection.NextMap(ctx)`는 그 연결에서 받은 다음 `OSDMapBatch`를 반환한다.
FSID·encoding version·trim/latest epoch와 `FullMaps`·`IncrementalMaps`의
epoch·원본 bytes 및 `RawFront`를 보존한다. 내포한 지도의 해석·FSID 검증·적용,
기준 full map 확보, 배치와 새 요청의 epoch 결정은 사용 레이어가 처리한다.
`NextMap` 자체는 MON 구독이나 지도 적용·조회·객체 재실행을 시작하지 않는다.
MON의 명시적인 조회·구독은 위의 API를 사용한다.

Tentacle도 현재 기능 협상에서는 envelope v1을 선택할 수 있으며 이 경우
trim/latest epoch는 wire에 없으므로 둘 다 0이다. `NewestMap`은 서버가 광고한
값으로, 전달한 마지막 blob이나 이 라이브러리가 적용한 epoch를 의미하지 않는다.
Current v4 envelope의 obsolete removed-snapshot map은 빈 count만 지원한다.

지도 큐는 OSD 연결별 FIFO다. Incremental chain을 버리거나 full map으로
합치지 않는다. `MaxBufferedOSDMapBytes`는 기본 `MaxFrameSize`, 1 KiB–1 GiB이며
보수적으로 batch당 512 bytes·원본 front·blob당 64 bytes를 계산한다. 최대 64개
batch, envelope당 full/incremental 합계 최대 4,096 blobs를 허용한다. 한도 초과는
`ErrOSDMapOverflow`로 해당 OSD 세션을 실패시키며 전송 중 요청의 결과 불명확
계약을 유지한다. 따라서 지도 큐를 소비하지 않는 사용에도 이 한도가 적용된다.
이미 접수한 batch는 종료·실패 후에도 먼저 drain하고 처음 기록된 원인을 반환한다.
`NextMap`의 context 취소는 그 대기만 끝내며 이미 취소된 context는 큐를 소비하지 않는다.
동시 호출은 각각 batch를 한 번만 받는다. 서버가 항상 모든 history를 보내는
것은 아니므로 raw 전달 자체를 lossless 구독이나 완전한 지도 chain으로 보지 않는다.

MON의 CRUSH admission 예외는 OSD에 적용하지 않는다. 구현하지 않은 필수
feature는 `ErrUnsupportedFeatures`로 거부한다. 현재 실제 검증은 legacy CRUSH,
HASHPSPOOL·upmap을 사용하지 않는 replicated pool에 한정한다. 일반적인 최신
CRUSH 설정의 OSD 접속 지원은 주장하지 않는다. 독립 EC shard 대상 지정,
snap id 할당·pool snapshot, 자동 배치·라우팅·재시도, RADOS/RBD/CephFS는
이 단계의 구현 범위 밖이다.

20.2.4·Darwin arm64·IPv4 host relay의 aes256k secure/CRC와 Linux arm64
container·IPv4의 aes secure에서 native client가 준비한 binary 객체 10,513
bytes와 stat2의 size·nanosecond mtime를 대조했다.
Partial·EOF·ENOENT, 8개 동시 요청, OSD ticket 갱신 후 재접속, 종료 및 현대
CRUSH 필수 feature의 접속 거부를 검증했다. 합성 peer는 전송 후 취소·늦은
응답·공유 handle·연결 상한·transaction ID·setup 정리 경합을 별도로 검증한다.
Windows OSD 상호운용과 OSD IPv6는 아직 검증하지 않았다.

Raw OSDMap은 같은 20.2.4의 Darwin arm64·IPv4 host relay·aes256k
secure/CRC와 Linux arm64·IPv4 container·aes secure에서 각각 실제 전체·증분
지도 10개를 받아 native decoder의 FSID·epoch와 협상 feature의 native
인코딩 bytes를 대조했다. Full map은 같은 epoch의 canonical map을 입력으로
사용한다. 지도 수신 뒤 명시적인 현재
epoch 읽기도 성공했다. fixture는 객체 생성 이전 epoch 5에서만 history를 trim해
full-map 응답을 검증하며, 지도 적용·배치 지원의 근거로 사용하지 않는다.

재현은 disposable memstore OSD 하나와 테스트용 pool을 만드는 opt-in profile이다.
Native `rados`·librados stat2 oracle은 개발 fixture 안에서만 사용한다.

```sh
CEPH_MSGR_TEST_OSD=1 CEPH_MSGR_TEST_RUNTIME=host integration/run.sh
CEPH_MSGR_TEST_OSD=1 CEPH_MSGR_TEST_RUNTIME=host \
  CEPH_MSGR_TEST_CONNECTION_MODE=crc integration/run.sh
```

### OSD·PG 대상 raw 명령

`OSDConnection.Command`는 명시적으로 연결한 OSD에 daemon-local 명령을 보내고,
`PGCommand`는 호출자가 primary로 고른 OSD에 PG 명령을 보낸다. 둘 다 객체 요청과
같은 lossy 세션과 client 전체 OSD TID를 쓰는 `MCommand`/`MCommandReply`이며
raw JSON·bulk 입력·출력 bytes·상태 문자열·signed 서버 코드를 그대로 보존한다.
`pgid`가 들어 있는 JSON은 `PGCommand`로만 보낼 수 있다. `PGCommand`는 JSON의
`pgid`가 `OSDCommandPG`의 `<pool>.<hex seed>`와 같고 `Primary`가 연결 대상 OSD
ID와 같아야 송신하며 JSON을 고치거나 끼워 넣지 않는다. `Seed`는 caller의
OSDMap에서 접은 실제 PG seed이고 `MapEpoch`는 wire에 없는 로컬 metadata다.

```go
pg := cephmsgr.OSDCommandPG{Pool: pool, Seed: seed, Primary: primaryID, MapEpoch: epoch}
result, err := primary.PGCommand(ctx, pg, cephmsgr.Command{
    JSON: []byte(`{"prefix":"query","pgid":"2.0"}`),
})
// result.Code·Message·Data와 result.Submission(TID·global ID·OSD·PG)을 보존한다.
```

OSD는 allow-all OSD capability가 없는 session의 명령을 `-13`으로 거절한다.
PG 명령은 OSD가 자기 지도로 pgid를 해석해 primary가 아니면 `-11`, PG가 없으면
`-2`를 반환하며 두 경우 모두 명령을 실행하지 않는다. Native librados는 `-11`에서
즉시 재전송하지만 이 client는 서버 결과를 그대로 반환하고 재전송·재연결·대상
변경을 하지 않는다. 취소·Close·연결 단절·잘못된 응답은 `OutcomeUnknownError`와
local submission으로 남긴다. 코드는 Linux wire errno이므로 호스트 `syscall`
상수와 비교하지 않는다. 같은 OSD의 명령은 서버에서 하나씩 실행되므로 긴 명령이
뒤 명령을 지연시킬 수 있다.

[OSD·PG 명령 검증](integration/fixture/OSD_COMMAND_VALIDATION.md)은 CRC/AES host
3 BlueStore에서 11개 실서버 사례를 통과했고 secure/aes256k container의 CGO0 전체
OSD 시험 25개도 통과했다. `version`(1 MiB bulk 입력 포함)과
`config get`, 33,895 bytes `get_command_descriptions`, unknown prefix·잘못된 인자의
`-22`, readonly의 `-13`이 native C API 결과와 byte 단위로 일치했다. Primary PG
query, replica의 `-11` 1회 반환(native는 timeout까지 재전송), 없는 PG의 `-2`,
동시 명령·객체 요청과 OSD 일시정지 중 취소·Close의 결과 불명확도 확인했다.

## Raw 명령 catalog

명령 목록도 일반 raw 명령으로 조회한다. 경로는 호출자가 선택하고 성공 출력의
JSON 해석은 사용 레이어에서 수행한다.

```go
result, err := client.MonCommand(ctx, cephmsgr.Command{
    JSON: []byte(`{"prefix":"get_command_descriptions","format":"json"}`),
})
// result.Data·Code·Message를 사용 레이어에 그대로 전달한다.
```

같은 payload를 `MgrCommand`·`MonTell`·`MgrTell`·`MonTellTo`에 전달할 수 있다.
MON/MGR 관리 catalog와 daemon-local admin catalog는 서로 다른 application
schema다. Catalog가 실행 경로·권한·모듈 활성화를 보장하지 않으며 해당 의미를
이 라이브러리에서 해석하지 않는다. 성공 응답은 JSON이 아니어도 그대로 반환한다.

20.2.4의 Linux arm64·IPv6·aes256k와 Darwin arm64·IPv4·aes/race 구성에서
raw 관리 catalog 및 daemon-local catalog를 독립 native client·CLI 출력과 대조했다.
비교용 JSON 해석은 시험 코드에만 두고 제품의 raw 응답은 변경하지 않는다.

## MON 로그

`WatchLogs(ctx, LogOptions)`는 현재 인증된 MON에서 cluster log를 받는
`LogStream`을 만든다. 성공은 로컬 worker 등록을 뜻한다. 서버 등록·첫 로그
수신·읽기 권한을 확인한 결과가 아니며, Ceph는 권한이 없는 구독을 조용히
거절할 수 있다. SubscribeAck에도 구독별 권한 결과가 없으므로 필요한
대기에는 context deadline을 둔다.

```go
watchCtx, stopWatch := context.WithTimeout(context.Background(), time.Minute)
defer stopWatch()
logs, err := client.WatchLogs(watchCtx, cephmsgr.LogOptions{
    Level: cephmsgr.LogInfo,
})
if err != nil {
    return err
}
defer logs.Close()

wait, stopWait := context.WithTimeout(context.Background(), 5*time.Second)
batch, err := logs.Next(wait)
stopWait()
if err != nil {
    return err
}
fmt.Printf("cluster=%s cursor=%d entries=%d\n", batch.FSID, batch.Version, len(batch.Entries))
```

Watch context는 복구를 포함한 전체 구독 수명을 제어하고, `Next(ctx)`의
취소는 그 대기만 끝낸다. 이미 취소된 Next context는 큐를 소비하지 않는다.
동시 Next 호출에는 각 batch를 한 호출에 한 번 전달한다. `LogStream.Close`
는 watch worker를 종료하고 구독 슬롯을 해제하며 공유 MON 연결은 유지한다.
살아 있는 Next context로 이미 접수한 batch를 읽은 뒤 최초 종료 원인을
계속 받는다. 명시적인 watch 종료는 `ErrLogStreamClosed`, client 종료는
`ErrClosed`이며 먼저 기록된 취소·overflow 등의 원인을 덮어쓰지 않는다.
수신 frame의 설정 상한이나 프로토콜·인증 검증이 실패하면 해당 원인을
로그 구독의 종료 오류로 보존한다. 빠른 MON 교체나 지연된 연결 정리가
그 원인을 대기 timeout으로 바꾸지 않으며, 이미 접수한 batch는 먼저 읽는다.
교체된 옛 세션의 뒤늦은 오류는 현재 구독을 종료하지 않는다.

`Level`은 서버에 요청하는 최소 우선순위다. 기본값은 `LogInfo`이며
`LogDebug`·`LogSec`·`LogWarn`·`LogError`도 제공한다. 전달받은 entry를 로컬에서
우선순위로 다시 거르지 않는다. Ceph가 history 누락을 알리는 낮은 우선순위의
WARN이나 알 수 없는 priority를 보내도 원문 그대로 보존한다.

`StartVersion`은 inclusive MON log-service cursor다. 0은 구독 처리 시점의
마지막 committed batch를 요청하며 전체 과거 이력을 뜻하지 않는다.
별도 client에서 이어 받으려면 이전 `LogBatch.Version + 1`을 지정한다.
재접속은 마지막으로 큐에 접수한 version 다음부터 요청한다. `Version`은
개별 entry의 `Sequence`나 Messenger 메시지 version과 다르다.
증가시킬 수 없는 최대 uint64 cursor는 `ErrLogCursorOverflow`로 거부한다.
`LogBatch.FSID`와 entry의 entity·rank·주소·원본 seconds/nanoseconds·priority·
message·channel을 보존하며 timestamp를 정규화하지 않는다.

client당 한 watch만 허용하며 중복 등록은 `ErrLogWatchActive`다. 구독은 관리
명령이나 `MaxInFlight` 슬롯을 사용하지 않는다. 큐는 최대 64 batch이고,
`MaxBufferedBytes`는 기본 `MaxFrameSize`, 허용 범위는 1 KiB–1 GiB다.
문자열·주소·entry metadata를 포함한 보수적인 보유 데이터 추정치이며 정확한
Go heap 상한은 아니다. 초과하면 해당 watch만 `ErrLogOverflow`로 끝내고
이미 접수한 큐와 일반 MON/MGR 명령 연결은 유지한다.

구독 종료는 로컬 처리이며 원격 unsubscribe나 무손실 전달을 보장하지 않는다.
서버의 history 누락 entry도 별도 신뢰 표시로 추측하지 않고 raw entry로
전달한다. MLog에는 구독 generation ID가 없어 같은 MON 세션의 이전 watch에서
늦게 온 batch가 새 watch에 도착할 수 있다. 현재 admission을 통과한 MON과
일치하는 FSID의 메시지만 받고 service version으로 이미 접수한 batch를 거른다.

## MON 설정

`WatchConfig(ctx, ConfigOptions)`는 인증된 client identity에 적용되는 설정을
전체 `map[string]string`으로 받는 `ConfigStream`을 만든다. 알려지지 않은
옵션·빈 값·raw 문자열을 그대로 전달하며 Go client의 `Options`에는 적용하지
않는다. Host는 `Dial`에 전달한 `Options.Hostname`이고 기본값은 빈 문자열이다.
모든 MON 구독과 `MGetConfig`에 같은 값을 client 수명 동안 그대로 보낸다.
Watch별로 바꾸지 않으며 재등록·인증 갱신·MON 복구에도 유지한다. Device class는
빈 값이다. 운영체제의 hostname·환경변수·로컬 Ceph 설정을 조회하거나 문자열을
정규화·축약하지 않는다. Hostname 선택은 사용 레이어가 맡으며 MON 접속 주소와
독립적이다. 서버가 선택한 설정 map이며 컴파일된 모든 기본값을 나열하는 API는 아니다.

Hostname과 identity를 포함한 제어 메시지가 `MaxFrameSize`를 넘으면 `Dial`은
인코딩·접속 전에 `ErrLimitExceeded`로 거절한다. 이 사전 검사는 서버의 인증·초기
지도 frame이 그 상한 안에 들어간다는 보장이 아니다.

```go
watchCtx, stopWatch := context.WithTimeout(context.Background(), time.Minute)
defer stopWatch()
configs, err := client.WatchConfig(watchCtx, cephmsgr.ConfigOptions{})
if err != nil {
    return err
}
defer configs.Close()

wait, stopWait := context.WithTimeout(context.Background(), 5*time.Second)
values, err := configs.Next(wait)
stopWait()
if err != nil {
    return err
}
fmt.Printf("configured options=%d\n", len(values))
```

하나의 unread map만 보유하며 후속 map이 전체를 교체한다. 이전에 있던 key가
없어지거나 빈 map이 오면 삭제도 반영한다. 중간 map은 합쳐 전달하지 않고
생략할 수 있다. 반환한 map은 호출자 소유이며 동시 Next 호출에 한 번만
전달한다. 이미 취소된 Next context는 map을 소비하지 않고, 개별 대기 취소는
watch를 종료하지 않는다. Watch context는 재접속을 포함한 전체 수명에 적용한다.

Client당 config watch 하나를 허용하며 중복은 `ErrConfigWatchActive`다.
로그 watch와 함께 사용할 수 있고 명령이나 `MaxInFlight` 슬롯, MGR 연결을
사용하지 않는다. 성공은 로컬 worker 등록이며 첫 map 전달의 확인은 아니다.
같은 MON에 watch를 다시 만들 때도 연속 구독과 `MGetConfig`로 전체 map을
요청한다. 서버가 같은 session의 변하지 않은 map을 다시 보내지 않을 수 있어
구독 메시지 단독으로 초기 값을 보장하지 않는다.

고정 20.2.4의 CephX 인증은 비어 있지 않은 MON cap을 요구한다. Config·monmap
구독의 일반 MON read 권한 예외와 별개다. 빈 MON cap 계정의 초기 인증은
`AuthenticationError(-13)`으로 거절됐으며 제품이 다른 인증으로 바꾸지 않는다.

`MaxBufferedBytes`는 기본 `MaxFrameSize`, 허용 범위는 1 KiB–1 GiB다.
512 bytes에 entry당 128 bytes와 key/value 길이를 더한 보수적 추정치이며
정확한 Go heap 상한은 아니다. 초과하면 해당 watch만 `ErrConfigOverflow`로
끝내고 이미 접수한 map과 공유 MON 연결을 유지한다. `ConfigStream.Close`는
worker를 기다리고 슬롯을 해제한다. 살아 있는 Next context는 접수한 map을
먼저 읽고 최초 종료 원인을 계속 받는다. 명시적 watch 종료는
`ErrConfigStreamClosed`, client 종료는 `ErrClosed`다.

두 watch의 최초 종료 원인은 해당 watch에 먼저 기록된 원인이다. Source의
오류가 watch에 기록되기 전에 수명 context의 취소를 먼저 관찰하면 context
오류로 종료할 수 있다. 이미 기록된 종료 원인과 접수한 데이터는 이후
취소나 Close가 덮어쓰지 않는다.

현재 admission을 통과한 MON의 메시지만 받는다. MConfig payload에는 FSID·설정
revision·cursor·연관된 request ID·구독 generation이 없으며, 같은 MON session의 이전 watch에서
늦게 온 응답은 구분할 수 없다. 재접속은 새 전체 map을 요청한다. 종료는 로컬
처리이고 원격 unsubscribe·중간 변경 이력·무손실 전달을 보장하지 않는다.
현재 source의 잘못된 map은 공유 session을 실패시키며 이미 전송한 명령의
결과 불명확 원인을 보존한다. 그 명령을 새 session에서 재실행하지 않는다.

## MON health·상태 구독

`WatchDigest`는 MON의 주기적인 `mgrdigest`를 받아 health detail과 그 MON의
상태 JSON을 한 쌍으로 전달한다. MGR 접속이나 조회 명령, 명령 슬롯을 사용하지
않는다. MON read 권한이 필요하며 등록 성공은 로컬 worker의 시작을 뜻한다.
실제 권한이나 첫 전달의 성공은 `Next`에서 확인한다.

```go
digests, err := client.WatchDigest(ctx, cephmsgr.DigestOptions{})
if err != nil {
    return err
}
defer digests.Close()
digest, err := digests.Next(ctx)
if err != nil {
    return err
}
fmt.Printf("health=%s\nMON=%s\n", digest.Health, digest.MonStatus)
```

`ClusterDigest.Health`와 `MonStatus`는 원본 `json.RawMessage`다. JSON을
해석하거나 알려진 필드만 남기지 않으며 반환한 bytes는 호출자가 소유한다.
아직 읽지 않은 한 쌍만 보관하고 새 전달로 교체하므로 중간 변경 이력이나
무손실 전달을 보장하지 않는다. `DigestOptions.MaxBufferedBytes`는 기본적으로
client의 `MaxFrameSize`이며 512 bytes와 두 JSON 길이의 합으로 보관 크기를
추정한다. 범위는 1 KiB–1 GiB이고 정확한 Go heap 상한은 아니다.

Client당 digest watch 하나를 허용하며 중복은 `ErrDigestWatchActive`다.
Config·log watch와 함께 사용할 수 있다. Watch context는 MON 복구를 포함한
전체 수명에, `Next(ctx)`의 context는 해당 대기에만 적용한다. 이미 취소된
Next는 보관된 값을 소비하지 않는다. MON 재접속·인증 갱신 후 다시 구독한다.
Tentacle 기본 주기는 5초이며 서버의 health 변경으로 더 일찍 전달될 수 있다.

`Close`는 로컬 watch를 종료한다. 보관된 값은 먼저 읽을 수 있고 이후 최초
종료 원인을 유지한다. 명시적인 종료는 `ErrDigestStreamClosed`, client 종료는
`ErrClosed`, 보관 크기 초과는 `ErrDigestOverflow`다. Overflow는 해당 watch만
종료한다. Wire payload에는 FSID·지도 epoch·cursor·구독 generation이 없고,
같은 MON session의 이전 watch에서 늦게 온 값은 구분할 수 없다.

Tentacle 20.2.4의 Linux IPv6/aes256k와 Darwin IPv4/AES race에서 health
mute·unmute, ticket 갱신 이후 새 전달, 유일한 seed의 접속 단절 후 다른 MON의
전달, watch 재등록과 Close를 검증했다. 각 단계의 전체 health detail과 같은
daemon의 MON 상태를 독립 native 클라이언트와 비교했으며 MGR 접속은 없었다.

## 운영 상태

`Snapshot()`은 네트워크 요청이나 재접속 대기 없이 현재 client 상태를 읽는다.
FSID와 client global ID, MON/MGR 지도 epoch·접속 주소, MON 이름·rank, active·standby MGR,
ticket 만료·갱신 예정 시각, 명시적인 MON 인증 거절을 확인할 수 있다.
키와 ticket의 암호 내용은 포함하지 않는다.

```go
state := client.Snapshot()
fmt.Printf("MON ready=%t MGR available=%t ready=%t\n",
    state.Monitor.Ready, state.Manager.Available, state.Manager.Ready)
for _, member := range state.Monitor.Members {
    fmt.Printf("MON name=%s rank=%d\n", member.Name, member.Rank)
}
for _, standby := range state.Manager.Standbys {
    fmt.Printf("standby MGR name=%s gid=%d\n", standby.Name, standby.GlobalID)
}
for module, uri := range state.Manager.Services {
    fmt.Printf("MGR service module=%s uri=%s\n", module, uri)
}
if state.AuthRejection != nil {
    fmt.Printf("authentication method=%d code=%d\n",
        state.AuthRejection.Method, state.AuthRejection.Code)
}
```

지도 변경을 기다릴 때는 `WaitMonMap(ctx, afterEpoch)`·`WaitMgrMap(ctx, afterEpoch)`를
사용한다. `afterEpoch`보다 큰 마지막 인증된 epoch의 독립 복사본을 반환하며
`0`은 이미 받은 nonzero 지도도 허용한다. 중간 epoch를 건너뛸 수 있고 준비 상태를
보장하지 않는다. 새 명령·구독·MGR 연결이나 명령 슬롯을 사용하지 않는다.

```go
manager, err := client.WaitMgrMap(ctx, 0)
if err != nil {
    return err
}
for {
    manager, err = client.WaitMgrMap(ctx, manager.MapEpoch)
    if err != nil {
        return err
    }
    fmt.Printf("MGR epoch=%d active=%s services=%v\n",
        manager.MapEpoch, manager.Name, manager.Services)
}
```

Context 종료는 해당 대기만 끝내며 다른 대기와 client는 유지한다. MON 재접속
중에도 기다리고, Close는 `ErrClosed`, 명시적인 인증 거절은 해당 오류로 반환한다.
Close 후 마지막 지도를 읽을 때는 `Snapshot`을 사용한다.

Linux IPv6/aes256k와 Darwin IPv4/AES race에서 실제 MON 추가·삭제의 새 epoch와
MGR 전환을 native 지도와 대조했다. 유일한 명령 슬롯 점유 중에도 진행했고,
MON 재접속을 잠시 막아 둔 대기가 학습한 두 번째 MON의 재구독으로 완료됐다.
로컬 대기 취소와 Close를 검증했으며 제품의 별도 MGR 접속은 발생하지 않았다.

`Manager.Standbys`는 인증된 MgrMap의 standby 이름과 daemon global ID를
`[]StandbyManager`로 반환한다. 별도 명령이나 MGR 접속 없이 조회하며 반환 slice를
수정해도 client 상태는 바뀌지 않는다. 이는 마지막 지도에 광고된 멤버십이며
준비 상태나 접속 대상으로 해석하지 않는다. Active MGR과 같은 `MapEpoch`의
정보이고, 새 지도에서 standby가 active로 바뀌면 목록도 갱신된다. Close 후에도
마지막 목록을 유지한다.

Tentacle 20.2.4의 Linux IPv6/aes256k와 Darwin IPv4/AES race 시험에서
전환 전후의 같은 epoch를 native `mgr dump`와 비교했다. Standby가 active로
승격된 뒤 재기동한 daemon이 새 ID로 복귀하는 것도 일치했으며, 조회 과정에서
제품의 MGR 접속은 발생하지 않았다.

`Manager.EnabledModules`는 같은 MgrMap의 명시적인 활성화 설정을 반환한다.
Always-on 모듈 전체 목록은 포함하지 않는다. `Manager.AvailableModules`는 active
MGR이 보고한 `ManagerModule{Name, CanRun, ErrorString}` 목록이다. `CanRun`과
원인 문자열은 해당 daemon의 마지막 load 가능 보고이며 실행 중 상태나 명령
권한을 의미하지 않는다. 두 목록 모두 별도
명령이나 MGR 접속 없이 조회하고 호출자가 소유하며 Close 후 마지막 값을 유지한다.

같은 Linux IPv6/aes256k와 Darwin IPv4/AES race 구성에서 명시적인 활성화 목록과
34개 모듈 보고를 같은 epoch의 native `mgr dump`와 대조했다. `iostat` 비활성화·
재활성화와 active MGR 전환 뒤에도 전체 목록과 로딩 실패 원인이 일치했다.
이 조회에서도 제품의 MGR 접속은 발생하지 않았다.

`Manager.AlwaysOnModules`는 MgrMap의 release 코드 → 상시 활성화 모듈 목록을
`map[uint32][]string`으로 보존한다. `Manager.ForceDisabledModules`는 상시 활성화
모듈 중 서버 설정으로 강제 비활성화한 목록이다. 명시적인 `EnabledModules`와
구분하며 현재 실행 목록이나 특정 release에 적용할 합집합을 계산하지 않는다.
Tentacle 지도가 포함한 이전 release 코드도 원본 metadata로 유지하며 해당
Ceph 계열에 접속하는 기능을 추가하지 않는다. 두 값은 같은 `MapEpoch`에서
조회하고, 중첩 slice도 호출자가 소유하며 새 지도는 전체 정책을 교체한다.
`WaitMgrMap`으로 정책 변경을 기다릴 수 있고 Close 후 마지막 값을 유지한다.

Linux IPv6/aes256k와 Darwin IPv4/AES race에서 6개 release 정책과 강제
비활성화 목록을 같은 epoch의 native `mgr dump`와 대조했다. `progress`·`status`
두 모듈의 강제 비활성화, active MGR 전환, 재활성화에 따른 목록 삭제가 일치했다.
Ceph formatter가 반복하는 `module` object key도 oracle에서 모두 보존했다.
제품 조회는 별도의 MGR 접속 없이 동작했다.

각 `ManagerModule.Options`는 서버가 보고한 option 이름 → `ManagerModuleOption`
map이다. 이름, 원문 타입·level 코드와 flags, 기본값·min/max·설명, enum·tag·관련
옵션 목록을 보존한다. 기본값과 범위는 schema metadata이며 현재 설정값이나
Go 인자를 검증·변환하는 규칙으로 적용하지 않는다. Map key와 descriptor의
`Name`을 각각 유지하고 중첩 map과 slice도 Snapshot마다 독립 복사한다.

Linux IPv6/aes256k와 Darwin IPv4/AES race에서 425개 descriptor 전체를 같은
epoch의 native `mgr dump`와 대조했다. Prometheus 설정값을 `9393`으로 바꿔도
보고된 기본값 `9283`과 구분됐고, active MGR 전환과 Close 뒤에도 보존됐다.
이 schema 조회에서도 제품의 MGR 접속은 발생하지 않았다.

`Manager.Services`는 같은 지도의 module 이름 → raw URI를 새
`map[string]string`으로 복사한다. Active MGR이 광고한 Dashboard·Prometheus 등
서비스 주소이며 별도 명령이나 MGR 접속 없이 읽는다. URL을 해석하거나 HTTP에
접속하지 않으며 서비스 응답·접근 권한을 확인한 값은 아니다. 새 지도에서 제거된
서비스는 다음 Snapshot에서도 사라지고 Close 후에는 마지막 map을 유지한다.

Linux IPv6/aes256k와 Darwin IPv4/AES race의 실제 Prometheus 광고를 같은
epoch의 native `mgr dump`와 비교했다. 별도 포트를 사용한 두 fixture MGR의
전환에서 URI가 새 active 주소로 바뀌고 모듈 비활성화 뒤 삭제되는 것이 일치했다.
제품에서 서비스 URI에 접속하거나 MGR session을 열지 않았다.

`Manager.Available`은 마지막으로 수신한 MgrMap 값이다. MGR 접속은
`MgrCommand`, `MgrTell` 또는 `WaitMgrReady`가 필요할 때 시작하므로 available이어도
ready는 아닐 수 있다.
MON 복구를 기다리거나 인증 갱신이 명시적으로 거절되면 MGR ready도 false다.
Ready는 현재 알려진 명령 접수 조건이며 이후 명령 성공을 보장하지 않는다.

`Monitor.Members`는 같은 `MapEpoch`의 마지막 인증된 MonMap을 rank 순서로
복사한다. `Name`은 `MonTellTo`가 받는 정확한 bare 이름이며, rank는 지도 변경에
따라 바뀔 수 있다. 목록은 각 daemon의 준비 상태를 의미하지 않는다.
이 목록을 읽은 뒤 대상 연관이 바뀌면 이름 지정 Tell의 독립 admission에서
검사한다. Snapshot 조회가 이후 호출의 대상을 예약하지는 않는다.

`Close` 후에는 `Closed=true`, MON/MGR ready=false를 반환하고 마지막으로
알아낸 지도·identity 정보는 남는다. 반환한 주소·member slice와 인증 거절 객체를
수정해도 client에는 영향을 주지 않는다. Snapshot은 한 시점의 관찰값이다.

서비스 시작 시 연결 준비를 기다리려면 `WaitMonReady(ctx)`와
`WaitMgrReady(ctx)`를 사용한다. MON 인증·MonMap 확인을 기다리며,
MGR 대기는 active MGR 발견과 별도 인증 연결까지 준비한다.
관리 명령을 전송하거나 `MaxInFlight` 명령 슬롯을 차지하지 않는다.

```go
prepare, stop := context.WithTimeout(context.Background(), 10*time.Second)
defer stop()
if err := client.WaitMgrReady(prepare); err != nil {
    return err
}
```

대기 context의 취소·만료는 해당 대기만 끝내고, 이미 준비된 연결은 client가
소유한다. `Close`는 대기 중인 호출을 `ErrClosed`로 끝내며, 명시적인 인증
거절은 `AuthenticationError`의 서버 코드와 인증 방식을 보존한다.
이 API는 관리 명령을 보내지 않으므로 `OutcomeUnknownError`를 만들지 않는다.
성공은 준비된 연결을 관찰했다는 뜻이며 이후 명령의 성공을 보장하지 않는다.
MGR이 없어도 MON 준비와 MON 명령은 별도로 사용할 수 있다.

## 수명과 복구

- 동시 호출을 지원한다. `MaxInFlight` 기본값은 64이며 슬롯 대기도 호출
  context에 따른다. `MaxFrameSize` 기본값은 논리 frame당 16 MiB다.
  초기 인증·지도·구독에도 적용하므로 서버가 보내는 초기 frame보다 작게
  설정하면 admission부터 실패할 수 있다.
- `ConnectTimeout` 기본값은 endpoint별 10초다. 요청 deadline은 공유
  연결에 적용하지 않는다. `Close`는 연결과 내부 worker를 종료하고 기다린다.
  MON의 MonMap 검증 대기는 protocol 실패와 context 종료가 겹쳐도 세션에
  먼저 기록된 종료 원인을 보존한다.
  MGR 핸드셰이크 중인 연결의 정리도 완료한 뒤 반환한다.
  종료 후 새 명령·준비 대기·`WatchLogs`·`WatchConfig` 호출은 입력 검증·복사 전에
  `ErrClosed`를 반환한다. 호출 context가
  이미 취소됐으면 해당 context 오류를 먼저 반환한다.
- `KeepaliveInterval` 기본값은 15초, `KeepaliveTimeout`은 45초이며 timeout은
  interval보다 커야 한다. TCP keepalive와 별도로 Messenger probe를 보내고,
  완전한 frame 수신이 끊기면 `ErrKeepaliveTimeout`으로 연결을 종료한다.
  timeout 검사는 probe 주기에 수행하므로 감지는 다음 주기까지 늦어질 수 있다.
  경과 시간은 Go의 monotonic clock을 사용한다. 이미 전송을 시작한 명령은
  이 원인을 가진 `OutcomeUnknownError`를 반환하며 자동 재실행하지 않는다.
- 모든 seed에서 일시적인 전송·연결 오류가 나면 bootstrap을 다시 시도한다.
  추가 시도는 `ConnectTimeout`과 Dial context로 제한한다. 명시적인 인증
  거절, wire 검증 실패, FSID·지원 계열 불일치는 이 재시도의 대상이 아니다.
  개별 endpoint의 setup timeout은 호출자 context가 살아 있을 때 재시도할
  수 있다. 호출자의 context 취소·만료 이후에는 새 시도를 시작하지 않는다.
- MON 단절 시 seed 및 MonMap 주소로 다시 인증한다. 이후 요청은 복구를
  기다린다. Ticket 수명의 약 75%인 AUTH/MGR의 `RenewAfter` 중 가장 이른
  시각에 새 MON 연결로 ticket을 갱신한다. 갱신 실패나
  연결 준비 중 갱신 시각이 지난 성공 응답은 250ms부터 5초까지 backoff한다.
  이전 MON의 진행 중 요청은 `ConnectTimeout` 동안 완료할 기회를 준다.
  교체된 MON은 새 요청을 받지 않는다. 전송 전에 거절된 요청만 새 MON으로
  보내며, 이전 연결에서 이미 전송을 시작한 요청을 다시 실행하지 않는다.
- 인증된 MonMap의 `auth_epoch`가 증가하면 새 접속에 사용할 ticket을
  조기 갱신한다. 기존 global ID와 갱신 검증용 이전 proof는 보존하며,
  이미 인증된 MGR 연결과 진행 중 명령을 유지한다. 갱신 중인 MON 후보가
  이전 epoch의 인증 결과를 뒤늦게 게시하지 못하도록 검사한다.
- MON 재인증을 서버가 명시적으로 거절하면 기존 MON/MGR 연결에서 새
  명령 접수를 중단한다. 새 호출에는 `AuthenticationError`, 이미 전송을
  시작한 호출에는 인증 거절을 원인으로 가진 `OutcomeUnknownError`를
  반환한다. 같은 키의 인증이 복원되면 background 재인증으로 복구한다.
  `auth rm`의 반영 시점은 Ceph에 따르며, client는 재인증 거절을 받은
  시점부터 요청을 차단한다.
  오래 단절되어 ticket 검증용 rotating secret까지 서버가 버린 경우에는
  유효한 장기 키가 있어도 기존 global ID의 재사용을 거부할 수 있다.
  이 거부를 받은 client가 스스로 새 ID로 인증하지 않는다. 호출자는 원인을
  확인한 뒤 새 `Dial`을 명시적으로 수행할 수 있다.
- MgrMap이 바뀌면 이전 MGR 연결을 종료하며 다음 호출은 새 active MGR로
  연결한다. 전송 중이던 요청은 `ErrManagerChanged`를 원인으로 보존한다.
  MGR 접속 중 client global ID가 바뀌면 이전 ID의 접속 결과를 폐기하고,
  아직 전송하지 않은 명령은 현재 ID로 인증한 연결에서 처리한다.
- active MGR이 없거나 service ticket을 갱신 중이면 `MgrCommand`는 호출
  context 안에서 대기한다. `MgrTell`도 같은 대기 규칙을 따른다. 아직 명령을 전송하지 않은 이 대기의 취소는
  결과 불명확 오류가 아니다. MGR을 기다리는 동안 MON 명령은 계속 사용할 수 있다.
- 이미 전송을 시작한 호출의 취소·단절은 `*OutcomeUnknownError`가 될 수 있다.
  `errors.As`로 이를 확인한다. `errors.Is`로 원인 context 오류도 확인할 수
  있다. 취소가 서버 실행의 취소나 rollback을 의미하지 않는다.
- 결과가 불명확한 명령은 자동 재실행하지 않는다. ACK도 명령 완료가 아니다.
  응답 메시지 종류나 본문이 손상된 경우에도 전송한 요청의 결과는 불명확하며
  해당 세션을 종료한다. 본문 해석 실패 시 수신한 raw data는 보존한다.
  복구는 fresh lossy session을 사용하며 Messenger cookie에 의한 기존
  session 재개는 구현하지 않는다. 서버가 reliable 방식이나 알 수 없는
  session flag를 선택하면 거부한다. 인증된 서버 식별 주소에도 연결 대상의
  주소·포트·nonce와 IPv6 scope·flow 정보가 일치해야 한다. 재접속할 때
  MonMap에서 받은 원본 주소를 유지하며 TCP 접속 문자열로 재파싱하지 않는다.

MON seed는 `host:port`, `v2:host:port/nonce` 형식이며 IPv6는 대괄호로 감싼다.
`%3`·`%en0` 같은 IPv6 zone seed는 연결 전에 거부한다. 로컬 routing zone은
원격 Ceph 주소의 wire `ScopeID`를 결정하지 못하므로 이를 추측하지 않는다.
MonMap에서 받은 scope·flow 보존과 실제 link-local 네트워크 지원은 구분한다.

연결 모드는 기본 `secure`와 명시적으로 선택한 `crc`를 지원하며 자동
fallback하지 않는다. 현재 Tentacle의 `aes256k`와 기존 `aes` CephX 키를
지원한다. 압축 협상은 압축을 끄는 데 사용한다.
FSID는 `ExpectedFSID`가 있으면 해당 값, 없으면 첫 인증 MonMap으로 고정한다.
MonMap의 `min_mon_release >= 20`을 요구한다. 이 설정은 daemon별 정확한
패치 버전을 증명하지 않으며 이전 최소 계열을 유지하는 업그레이드 클러스터는
거부한다.

MON이 명령 전용 CLIENT에도 요구하는 CRUSH 세대 비트는 MON 접속에 한정해
광고한다. 명시적 OSD stat/read 연결에서도 이 예외를 재사용하지 않는다.
CRUSH 계산·OSDMap 구독·RADOS/RBD/CephFS는 구현하지 않았으며 그 밖의
미지원 필수 기능은 명시적으로 거부한다.

## 검증 결과

2026-10-01–03에 다음 구성을 실제 Ceph daemon과 검증했다.

호출자가 지정한 hostname은 Linux arm64·CGO=0·aes256k·직접 IPv6와 Darwin
arm64·aes·IPv4 host relay의 race 시험에서 각각 19.95초·20.37초에 통과했다.
같은 identity로 두 hostname·빈 문자열·공백을 포함한 원문을 전송하고, 별도 native
client의 `NODE_NAME`으로 선택한 시험용 설정 한 개와 비교했다. Native client의
기본값·타입 변환이 있으므로 이 oracle은 전체 raw map의 동등성을 주장하지 않는다.
로그·digest 병행, 명령 슬롯 점유, watch 재등록, 실제 ticket 갱신과 학습 MON
복구 후에도 선택값을 유지했다. 서버 선택을 구분하는 빈 CRUSH host·상위 root는
disposable fixture에만 만들었으며 제품의 MGR·OSD 데이터 접속은 0회였다.
Go가 OS hostname을 조회하거나 CRUSH 배치를 계산하는 기능은 추가하지 않았다.

MON config 구독은 Linux arm64·CGO=0·aes256k·직접 IPv6의 전체 통합시험
38개와 Darwin arm64·aes·IPv4 host relay의 race 5개에서 통과했다.
Config 시험은 각각 29.25초·30.30초였다. Fresh native CLI의 전체 effective map과
Go 수신 map을 독립적인 raw byte 길이·key 순서의 SHA256 및 entry count로
17회 대조했다. 전체 설정은 진단에 저장하지 않고 시험용 두 key만 남겼다.
Global→client→정확한 identity의 우선순위, override 삭제·fallback,
raw newline·Unicode·NUL·공백 값과 caller의 map 소유권을 확인했다.

MON cap을 `allow command "fsid"`로 제한한 계정은 fsid 조회가 성공하고
status·일반 config get이 각각 서버 code -13으로 거절됐지만 config map과
변경을 받았다. 빈 MON caps 인증 거절과 구독의 일반 read 권한 예외를
구분했다. 명령 슬롯이 찬 동안 수신, 로그 병행, 같은 session 재등록,
watch만 끝내는 1 KiB overflow, 두 차례 실제 ticket 갱신,
client TCP만 끊은 학습 MON 복구와 Close도 검증했다. Config 시험 중
MGR 접속은 0회였고 변경·cleanup 명령은 불명확해도 자동 재실행하지 않았다.

지연된 Conn.Close와 아직 실패를 관찰하지 않은 worker 뒤에서 Client.Close·
Stream.Close가 최초 frame·제한·인증 원인을 덮는 경계를 합성 peer로 재현했다.
수정 전 overlay의 실패와 수정 후 CGO=0·race 100회를 확인했고, 이미 교체된
source·일반 전송 EOF는 구분했다. 공개된 MON 재인증 거절도 두 watch에 같은
경계에서 기록하며 먼저 접수한 데이터와 앞선 overflow·취소·Close 원인을
유지했다. 최종 전체 Go CGO=0·race 및 vet도 통과했다.

MON 이름 조회는 독립 native CLI MonMap의 name·rank·FSID·epoch와 대조했다.
Snapshot 100회가 새 접속을 만들지 않고 MGR를 lazy 상태로 유지하는 것도
확인했다. 조회한 이름으로 private Tell을 실행하고 실제 ticket 갱신 뒤와
Close 후에도 마지막 인증된 목록을 유지했다. Linux arm64·CGO=0·aes256k·
직접 IPv6의 관련 3개 시험과 Darwin arm64·aes·IPv4 host relay의 race
5개 시험이 통과했다. 지도 교체·거절·stale source·private map 격리와
동시 Snapshot·Close를 검사하는 member 단위 시험은 race 100회를 통과했다.

로그의 frame 크기 거절은 합성 peer의 인증된 secure frame으로 재현했다.
일반 종료·worker보다 빠른 MON 교체·지연된 Conn.Close의 세 경우에서
기존 코드는 제한 원인을 잃었고, 수정 후 CGO=0 및 race 100회를 통과했다.
이미 교체된 옛 source의 오류는 새 구독을 유지하는 대조군도 확인했다.
peer가 받은 명령은 제한 원인과 결과 불명확을 유지하며 재전송하지 않았다.
Session의 종료 callback과 정리 완료를 기다리는 순서는 바꾸지 않았다.
실제 Linux arm64·aes256k·직접 IPv6 로그 수신과 두 차례 ticket 갱신·
seed TCP 복구도 19.75초·3.04초의 두 시험에서 통과했다.

MON 로그 core `ff80b4c`를 포함한 20.2.4 시험은 Linux arm64·CGO=0·aes256k·
직접 IPv6와 Darwin arm64·aes·IPv4 host relay의 race 실행에서 통과했다.
독립 native CLI 로그와 entity·rank·주소·priority·text 및 microsecond 표시
시각을 대조했고 raw nanoseconds를 보존했다. 실제 entry sequence 0도
MON log-service cursor와 구분했다.
read-only 계정의 실제 로그 수신, Next 대기 취소·watch 취소와 재등록,
1 KiB 큐 overflow, 일반 MON/MGR 명령과 Close도 확인했다.
복구 시험은 AUTH·MGR ticket 만료 시각이 두 번 실제로 증가하는 동안 ID를
유지했다. Ceph quorum을 유지한 채 client TCP를 끊고 seed 33300 재접속을
차단해 학습한 MON 33301에서 로그를 이어 받았다. 이 시험은 서버 이력의
무손실 전달을 증명하지 않는다. 최종 `1a09b01`의 Linux arm64·CGO=0·aes256k·
직접 IPv6 전체 통합시험은 35개가 통과했다.

이름 지정 MON core `80d8b42`는 고정 20.2.4의 Linux arm64·CGO=0·aes256k·
직접 IPv6 시험을 0.18초에 통과했다. native CLI의 `mon.b mon_status`에서
얻은 name·rank와 대조하고 a/b/c 대상, 존재하지 않는 이름의 접속 전 거절,
지정 b의 TCP 접속 거절 시 다른 MON으로 바꾸지 않는 동작을 확인했다.
daemon의 `-22`와 read-only 계정의 `-13`은 raw 결과와 `CommandError`로
보존했고 주 MON a·MGR·로그 수신과 인증 identity는 유지했다.
Darwin arm64·aes·IPv4 host relay의 race 시험은 1.08초에 통과했으며,
b/c의 동시 Tell과 일반 MON/MGR 명령도 함께 검사했다.
같은 동시 호출을 포함한 최종 Linux arm64·CGO=0·aes256k·직접 IPv6 시험도
0.46초에 통과했다.
11개 synthetic API 시험은 새 인증의 global ID 0·빈 이전 ticket proof와
CLIENT_IDENT의 새 ID 84를 주 ID 42와 독립 비교한다. 지도 검증·원주소 보존·
전송 전후 취소·raw 결과·자동 재실행 금지·setup 종료 소유권을 포함한
CGO=0 및 race 20회와 Windows build가 통과했다.
추가 시험은 완전한 Tell 수신 뒤 대상 삭제·이름 변경·주소 재할당을 주입했다.
세 경우 모두 `ErrMonitorTargetChanged`를 원인으로 한 결과 불명확을 반환하고
명령을 한 번만 전송했으며 주 인증·지도·MON/MGR 명령을 유지했다.
이 경계 시험은 CGO=0 및 race 각 100회를 통과했다. 지도 parser fuzzing은
20초 동안 2,487,568개 입력을 처리해 통과했다.
이름 지정 Tell을 포함한 최종 Linux arm64·CGO=0·aes256k·직접 IPv6 전체
통합시험은 36개가 통과했고, 별도 fixture가 필요한 9개는 제외됐다.
기존 ticket 갱신·실제 MON/MGR 장애·불명확한 변경의 재실행 금지와
트래픽 중 12회 종료도 통과했으며 FD 수는 6에서 6으로 유지됐다.

Tell 제품 구현 `6ba0d88`은 20.2.4의 Linux arm64·CGO=0·aes256k·직접 IPv6와
Darwin arm64·aes·IPv4 host race에서 검증했다. native CLI의 MON/MGR 버전
JSON과 대조했고, 알 수 없는 명령 `-22`, read-only 계정의 `-13`, 거절 후
일반 조회, 일반 명령과 Tell의 동시 호출 및 Close가 통과했다.
전송 중 Tell 취소·Close·잘못된 응답의 raw data 보존·자동 재실행 금지는
별도의 synthetic API·session 시험으로 검증했다.
이를 포함한 `94bc9e3`의 Linux arm64·CGO=0·aes256k·직접 IPv6 전체 통합시험은
32개가 통과했다. 당시 daemon SIGSTOP 방식의 별도 Tell 복구 시험은 실제 AUTH·MGR ticket
갱신 2회, seed MON a를 멈춘 동안 학습한 b에서의 Tell, 한 번만 보낸
`mgr fail`에 따른 b→a 전환 뒤 lazy 접속·Tell·일반 조회·Close를 약 24초에
통과했다. client global ID는 유지했고 새 MGR의 name·ID·epoch는 별도
observer의 현재 `mgr dump` 응답과 대조했다.
같은 Tell 복구 시험은 Darwin arm64·aes·IPv4 host race에서도 약 24초에
통과했다. 이 실행에서는 MGR a→b 전환을 확인했으며 host TCP 경로는
개발용 relay를 사용했다.

| Ceph | 인증 키 / rotating service cipher | 주소 | 결과 |
| --- | --- | --- | --- |
| 20.2.4 | aes256k / aes256k | IPv4 | MON/MGR 명령, 동시 호출, ticket 갱신, MON 장애, MGR 전환 통과 |
| 20.2.4 | aes / aes | IPv4 | 동일 시험 통과 |
| 20.2.4 | aes256k / aes256k | IPv6 | 동일 시험 통과 |
| 20.2.4 | aes / aes256k | IPv4 | 동일 시험 통과, MGR session key는 aes |
| 20.2.4 | aes256k / aes | IPv4 | 동일 시험 통과, MGR session key는 aes |
| 20.2.3 | aes / aes | IPv4 | MON/MGR 명령, 동시 호출, ticket 갱신, MON 장애, MGR 전환 통과 |

모두 secure 모드이며 클라이언트는 Linux arm64 및 GitHub CI의 Linux amd64에서
CGO=0으로 실행했다.
혼합 구성에서 MGR session key는 [Tentacle KeyServer](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/auth/cephx/CephxKeyServer.cc#L591)가
클라이언트 키와 service secret 중 낮은 타입으로 선택한다. 해당 키 타입도
시험에서 확인했다.

2026-10-03에는 명시적으로 선택한 msgr2.1 CRC 연결도 Ceph 20.2.4와
검증했다. 아래 두 실행은 각각 공개 API·내부 통합시험 52개를 통과했고,
별도 fixture가 필요한 10개는 제외했다.

| Ceph | CephX 키 / service cipher | 클라이언트·주소 | 실행 |
| --- | --- | --- | --- |
| 20.2.4 | aes256k / aes256k | Darwin arm64·IPv4 host relay | CGO=0 |
| 20.2.4 | aes / aes | Darwin arm64·IPv4 host relay | 개발용 race detector |

CRC-only MON/MGR의 실제 AUTH_REQUEST에서 CephX와 mode `[1]`만 제안하는
것을 확인했다. 일반 명령·MON/MGR Tell·이름 지정 MON Tell, raw binary
입출력·서버 오류, ticket 갱신, MON/MGR 장애·구독 복구, 동시 요청·취소와
종료를 검증했다. 결과가 불명확한 변경 명령은 재실행하지 않았고,
트래픽 중 12회 종료 뒤 FD 수는 두 실행 모두 5에서 5로 유지됐다.
같은 CRC-only MON에 secure를 선택한 연결은 `AuthenticationError`의
서버 코드 `-95`로 거부됐다. 인증 transcript 서명과 인증 이후 CRC 손상
검출은 별도 synthetic peer에서도 확인했다.
CI에는 Linux IPv4 host의 두 키 타입과 IPv6 container의 aes256k CRC
프로필을 추가했다. 이전 Tentacle 패치·혼합 cipher 및 Windows의 실제
CRC 연결은 위 실행으로 검증한 구성이 아니다.

시험 구성은 3 MON·2 MGR, OSD 없음, 12초 ticket TTL이다. `status`와
`pg stat` 응답을 검증했고 MON 프로세스 종료 및 active MGR fail을 주입했다.
현재 fixture는 `auth_allow_insecure_global_id_reclaim=false`를 사용하며,
이 설정으로 아래의 전체 CI 시험을 통과했다.
잘못된 인증 키, 읽기 전용 계정의 쓰기 거절, 미지원 명령의 코드·상태 문자열,
`pg getmap` binary 출력과 40 KiB binary bulk 입력도 실제 서버로 확인했다.
설정 set/get/rm과 balancer·crash·iostat 모듈 명령을 검증했다. iostat을
비활성화했을 때 서버의 `-95` 코드와 상태 문자열을 보존했고, 재활성화 및
MGR 전환 후 모듈 명령이 정상 동작했다. 접속 중인 MON을 8회 재시작하는
동안 동시 조회를 수행했다. 클라이언트 12개를 차례로 생성해 동시 요청 중
종료했으며 worker, 파일 descriptor 및 GC 후 heap 정리를 확인했다.
수신을 차단한 변경 명령은 독립 클라이언트로 서버 적용을 먼저 확인한 뒤
로컬 대기를 취소해 `OutcomeUnknownError`가 반환되는지 검증했다.

네트워크 지연 시험에서는 40 KiB frame의 일부를 실제 서버에 보낸 뒤
전송을 멈췄다. 해당 호출을 취소하면 결과 불명확 오류, 전송 전에 대기하다
만료된 호출은 일반 context 오류를 반환했다. 이후 전송과 응답 수신을
재개하면 같은 MON/MGR 연결의 동시 요청 8개가 모두 완료됐다. JSON과
binary 응답을 각각 대조했고, 취소한 MON 변경이 나중에 실제 적용된 것도
독립 클라이언트로 확인했다. Go relay는 쓰기를 최대 257 bytes,
읽기를 최대 37 bytes로 나눈다. 이는 stream 분할 검사이며 TCP packet 수를
측정한 것은 아니다. 20.2.4·aes256k의 Darwin arm64·IPv4 및 Linux arm64의
직접 IPv6 연결에서 통과했다.

사용자 정의 context의 `Err`에서 `Snapshot`을 조회하는 MON/MGR 명령도
20.2.4·aes256k의 Darwin arm64·IPv4에서 CGO=0과 race 계측으로 통과했다.
JSON·binary 출력과 서버 `-22` 응답을 각각 확인했다. Context 검사는 세션
잠금 밖에서 실행하며, 검사 중 취소·종료된 전송 전 요청을 뒤늦게 보내지
않는 것도 단위 시험으로 검증했다.

같은 context 동작을 MGR 최초 연결, ticket 갱신, MON 연결 복구,
MGR 전환과 대기 취소·종료 중에도 검증했다. 복구 시험은 결과 불명확
오류에 포함된 원인을 모두 검사하며, 인증·프로토콜 오류가 일시적인
네트워크 오류로 취급되지 않는 것도 회귀 시험으로 확인했다.
현재 context·Tell 복구 시험은 client의 MON-a TCP만 끊고 재접속을 Close까지
차단한다. 학습한 b/c에서 복구하는 동안 native MON quorum과 daemon 인증은
유지한다. Tell은 장애 전 실제 ticket 갱신 2회를 확인하며, 두 시험 모두
실제 MGR 전환을 한 번만 요청한다. 별도의 실제 MON/MGR SIGSTOP·종료·
반복 재기동·전체 MON 중단 시험은 유지한다.
제품 소스 `c14f78f`는 20.2.4·aes256k의 Linux arm64·CGO=0·직접 IPv6에서
전체 통합시험과 3분 부하 시험을 통과했다. 부하 시험은 명령 63,278건,
ticket 갱신 18회, MGR 장애 주입 시도 6회와 context의 상태 조회 284,403회를
처리했다. 결과 불명확 응답은 없었고 관찰한 최대 세션 수는 2였다.

MON 후보에서 먼저 받은 MgrMap은 해당 후보의 MonMap이 FSID·최소 계열을
검증한 뒤에 반영한다. 후보를 거절하거나 검증 대기가 만료되면 기존 MGR와
진행 중 명령을 유지한다. 지도 메시지의 호환 header version과 인증 transcript
크기도 검증한다. 잘못된 명령 응답으로 같은 세션을 종료할 때는 영향을 받은
모든 요청에 `ErrMalformedMessage`와 해석 오류를 함께 보존한다.
`Close`는 세션 worker뿐 아니라 연결의 `Close`, 종료 callback, 취소된
handshake의 연결 정리까지 기다린다. 이 경합은 지연된 정리를 사용하는
단위·race 시험으로 확인했다.

이 보강을 포함한 제품 소스 `23148f5`도 같은 실제 IPv6 구성의 전체 통합시험과
3분 부하 시험을 통과했다. 명령 64,480건, ticket 갱신 18회, 서버가 성공 응답한
MGR 장애 주입 5회와 context 상태 조회 289,749회를 처리했다. 결과 불명확
2건은 MGR 교체 원인을 유지했으며 자동 재실행하지 않았다. 관찰한 최대
세션은 2개, goroutine은 19개였다. 반복 종료 12회에서 파일 descriptor는
6개로 돌아왔고, GC 후 heap은 268,056 bytes에서 273,336 bytes였다.

같은 `23148f5`의 직접 IPv6·aes256k·CGO=0
[1시간 연속 시험](https://github.com/JSYoo5B/ceph-msgr-go/tree/checkpoint/communication-lifecycle-1h)도
통과했다. 성공 호출 1,248,669건, ticket 갱신 368회, 서버가 성공 응답한
MGR 장애 주입 119회와 context 상태 조회 5,609,455회를 처리했다.
결과 불명확 14건은 모두 MGR 교체 원인을 보존했다. 샘플링한 최대 세션은
3개, goroutine은 24개였고, 이후 관측에서 각각 2개와 19개로 돌아왔다.
이 시험은 뒤에 추가한 인증 epoch 및 응답 decoder 변경을 포함하지 않는다.

인증 epoch 보강은 20.2.4의 별도 120초 ticket fixture에서 확인했다.
실제 `auth wipe-rotating-service-keys`를 두 번 실행해, 예정된 갱신보다
일찍 새 ticket을 받고 global ID와 기존 MGR 연결을 유지했다. 개발 fixture는
새 CLI process의 MGR 조회를 준비 확인으로 사용한 뒤,
Go client의 최초 MGR 연결을 검증한다. 이 Go 연결의 인증 거절을
재시도하지 않는다. Linux arm64·CGO=0의 aes256k·IPv6와 aes·IPv4에서 통과했다.
이전 제품 소스 `23148f5`는 같은 시험에서 조기 갱신을 하지 못해 실패했다.
이 두 차례 교체와 큰 MON 응답은 Darwin arm64·IPv4·aes256k의 실제 host
바이너리에서도 race 계측으로 통과했다. 계측은 개발 시험에만 사용한다.

`0134506`의 [GitHub CI](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/37020182074)는
25개 중 24개가 통과했고, aes·IPv4의 교체 시험은
cold MGR 인증 `-13`으로 실패했다. MGR 로그에는 MON 발급 ticket의 복호화
실패가 기록됐다. 같은 소스의 새 로컬 fixture는 두 교체 cycle을 통과했으며,
남은 로그로 과거 실패의 발급 MON이나 service-key 세대를 확정하지 못했다.
기존 준비 확인은 대상 MGR와 현재 auth epoch를 묶지 않았으므로 보강했다.
각 MON의 로컬 `mon_status`에서 새 map/auth epoch를 확인하고, 전체 초기
MonMap과 rank 지정으로 native CLI의 MON 경로를 고정한다. active MGR의
name·global ID를 대조한 뒤 같은 MON rank의 새 native CLI로 이름 지정
MGR Tell을 실행한다. 각 probe는 전체 15초 중 남은 시간으로 제한한다.
Ceph CLI 자체의 bootstrap·재연결은 native 정책을 따르며, 최종 인증된
읽기 응답을 준비 증거로 사용한다. 이는 Go cold client의 정확한 ticket이나
최초 인증 성공을 대신 증명하지 않는다. Go의 최초 MGR 인증 거절과 wipe
변경은 재시도하지 않는다. 최종 보강은 Linux arm64·CGO=0의 aes·IPv4
4.34초와 aes256k·직접 IPv6 8.68초에 각각 두 cycle을 통과했다.
별도 실험에서 요구 auth epoch를 실제 값보다 1,000 높게 지정하자 준비
확인이 종료됐고, native MON 응답의 실제 epoch는 3으로 남았다.

2026-10-15 기준 이 간헐 거절의 원인은 아직 확정하지 못했다.
`2d1cc46`의 [CI 첫 실패](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/38605086391/job/115993789742)에서는
첫 wipe 뒤에도 cold Go MGR 인증이 `-13`으로 거절됐다. 초기·갱신 MON
연결은 모두 mon.a였고, native 준비 probe 9개는 map epoch 4/auth epoch 10과
동일한 mgr.a/global ID 4167을 확인한 뒤 모두 통과했다. 직후 그 MGR는
ticket-info bad magic을 기록했다. 이 관측은 Go ticket 발급과 MGR 검증에
사용한 실제 service-key 세대의 일치를 증명하지 않는다.
[fixture 질문과 추가 근거](https://github.com/JSYoo5B/ceph-testcontainers-go/issues/71#issuecomment-6174955026)를
남겼으며, 이후 `d39cc93`의 CI 41개 통과도 원인이 해결됐다는 뜻으로
처리하지 않는다. 최초 인증이나 wipe를 재시도해 실패를 숨기지 않는다.

`b01a3cf`의 [CI 첫 실패](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/38739526554/job/116437288897)에서도
secure/AES256K·IPv6·Linux amd64의 두 번째 wipe 후 최초 cold MGR 인증이
`-13`으로 거절됐다. MGR의 ticket-info 복호화 실패와 Go의 발급·선택·송신
증거를 [별도 기록](integration/fixture/SERVICE_KEY_EPOCH_FAILURE_20261015.md)에
보존했다. SecretID와 epoch, native 준비 probe 성공으로 실제 키 세대의
일치를 단정하지 않으며, 이 실패는 아직 미해결이다.

시험은 `cephmsgr/integration_auth_epoch_test.go`에 두어 비공개 cached 상태를
함께 관측한다. 각 client의 MON dial 이력, 동일 lock 구간의 map/auth epoch와
MGR identity, service SecretID·validity 및 교체 전/후 blob 일치 여부를
기록한다. Blob·key·proof bytes는 출력하지 않으며 held/cold 이력도 분리한다.
이 정보는 관측 시점의 저장 상태이며 정확한 발급·설치 이벤트나 이후
authorizer가 선택한 ticket, native daemon key 세대의 증거는 아니다.
AES/AES·IPv4·120초 ticket·Linux arm64 container의 새 독립 fixture에서
두 cycle이 12.29초에 통과했다. 간헐 실패의 원인을 고쳤다는 주장은 아니다.

2026-10-15 후속 소비자 진단은 티켓의 로컬 parse 식별자와 실제 publication
순서로 인증된 MON 연결을 기록하고, MGR이 선택한 티켓과 새 authorizer에
인코딩한 blob을 run별 HMAC label로 연결한다. 그대로 유지한 티켓의 발급
연결은 새 MON 연결로 덮어쓰지 않는다. 설치 callback을 context 안에서
기다리며, 실제 인증 frame의 로컬 write 결과, AUTH_DONE 검증, 전체 handshake
완료를 구분한다. 원본 ticket·proof·key bytes는 기록하지 않는다.
이 기록도 native MGR이 실제 검증에 사용한 키 세대를 보여주지는 않는다.
관측 계약과 새 검증 범위는
[소비자 후속 검증 기록](integration/fixture/CONSUMER_QUESTIONS_VALIDATION.md)에 둔다.

준비 확인을 보강한 `1e4cfda`의
[GitHub CI](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/37024308483)는
auth epoch 두 구성을 포함해 25개 중 24개가 통과했다. aes256k/AES·IPv4의
context·Tell 복구는 MGR 준비 대기 timeout으로 실패했고, native MGR 두 개의
MON 접속에서 오래된 AUTH ticket의 `-13` 거절이 기록됐다. 장애 전 갱신·ACK
흐름을 복원할 로그가 부족해 근본 원인은 확정하지 못했다. 같은 커밋의 새
로컬 fixture에서 두 복구 시험은 14.83초·25.18초에 통과했다. 이 실패를
기존 cold MGR 교체 실패와 구분하며, 인증 오류를 재시도하거나 허용하지
않고 두 복구 시험의 client TCP 장애를 daemon 장애와 분리했다.
분리한 전체 Linux arm64·CGO=0·aes256k/AES·IPv4 실서버 시험은 36개가
통과했고 별도 fixture 대상 9개는 제외됐다. 실제 MON 재시작 8회·전체
MON 중단과 복구 3회·MGR 장애·불명확한 변경의 재실행 금지도 확인했다.
트래픽 중 종료 12회 후 FD는 6개로 돌아왔다.

큰 응답 시험에서는 `MaxFrameSize=32 MiB`로 유효한 status JSON 뒤에
16 MiB 공백을 붙였다. 실제 MON이 원래 명령을 응답 front에 그대로 포함했고,
서버 코드·raw JSON을 보존한 뒤 같은 연결의 다음 명령도 성공했다.
응답 본문 decoder에도 설정한 상한을 전달하며, 기본 상한은 16 MiB로 유지한다.

공개 limit 분류를 추가한 `a5edf75`는 실제 20.2.4에서 256 KiB 상한으로
다섯 명령 경로의 입력 초과 10건을 송신 전에 거절했다. 딱 한 번 보낸
261,888-byte read-only status 요청에는 서버가 정상적인 큰 응답을 반환했고,
client는 `ErrLimitExceeded`와 `OutcomeUnknownError`를 함께 보존했다.
`ErrMalformedMessage`나 알려진 서버 거절로 바꾸지 않았다. 이후 작은 status
조회가 같은 FSID·global ID로 복구됐고, 요청·복구 control을 합한 TCP 송신
263,464 bytes(Linux arm64·CGO=0·aes256k·IPv6)와
263,463 bytes(Darwin arm64·AES·IPv4 relay·race)는 큰 요청 재실행이 없음을
확인했다. 최초 64 KiB probe는 bootstrap부터 크기 제한으로 실패했다.
그 trace에서는 초과 frame 종류를 특정하지 못했고, 상한을 256 KiB로 올린
probe가 통과했다. 허용 Options 범위가 모든 서버의 admission을 보장하지 않는다.

같은 변경은 CephX의 네 encoded-length 경계에서 decoder 오류가 crypto·version
오류로 가려지던 문제를 수정했다. AES/AES256K의 독립된 회귀 입력 20개가
원래 코드에서 실패하고 수정 후 CGO=0·race 50회 검사에 통과했다. 유효 인증,
실제 암호문 변조·미지원 version의 오류, 실패 시 identity·ticket 미게시도
유지했다. 정상 길이 초과·잘못된 내부 길이·1,025개 echoed command의 count
제한은 secure 합성 peer로 구분해 raw 출력·결과 불명확·재실행 금지를 검사했다.
Darwin·AES의 실제 config 갱신·MON 복구·취소·tell·상태 조회를 포함한 6개
시험도 race로 통과했다. 이 malformed 입력 검증과 정상 실서버 상호운용을 구분한다.

후속 `6dba908`은 암호문 검증이 끝난 내부 plaintext의 필수 version·key·validity·
blob·challenge·authorizer nonce가 잘린 경우 원래 `io.ErrUnexpectedEOF`를
보존한다. AES/AES256K의 독립 회귀 입력 26개는 원래 코드에서 실패했고,
수정 후 50회 CGO=0·race 검사에 통과했다. 완전한 미지원 version·잘못된 nonce·
zero validity·암호문 변조는 기존 의미·암호 오류를 유지한다. 공개 MON/MGR
setup에서도 잘린 7개 원인은 `ErrMalformedMessage`와 EOF를 함께 보존하며,
client·명령·인증 상태를 게시하거나 setup을 재시도하지 않는다.

앞선 `f40c670`의 로컬 전체 Linux·aes256k·IPv6 실행은 38개 성공·1개 실패·
9개 별도 fixture 제외였다. MGR 자연 승계·ticket 갱신·새 MGR 명령은
40.694초에 성공했지만, 정리용 15초 context 안에 재시작한 standby를
확인하지 못했다. 로그에서 새 PID의 Python 모듈 로딩 13.092초와 마지막
조회보다 0.952초 늦은 정상 standby 수락·새 GID의 인증된 명령을 확인했다.
Native daemon의 rotating-key 경고도 있었으며 영구 인증 거절로 단정하지
않는다. 정리만 30초로 제한하고 원래 90초 승계 검증·새 nonzero GID 확인·
start 한 번·오류 즉시 실패를 유지했다. 수정한 Linux·aes256k·IPv6에서 config
갱신(41.03초)·공개 limit(0.05초)·MGR 자연 승계와 standby 복귀(42.60초)가
모두 통과했다. 인증 보안이나 명령 재실행 정책을 완화하지 않았다.

같은 product 변경의 Darwin arm64·AES·IPv4 relay 시험도 race로 config 갱신,
공개 limit, MGR 자연 승계와 standby 복귀를 통과했다. 이 시험에서 승계 중
ticket 갱신과 새 standby ID를 실제로 확인했다. 두 키 타입의 정상 실서버
검증과, 잘린 plaintext를 보내는 합성 peer의 공개 오류 회귀를 구분한다.

`FuzzAuthenticationPlaintextPublication`는 최대 8 KiB의 임의 내부 plaintext를
AES/AES256K로 암호화한 뒤 AUTH/MGR key·encrypted blob·challenge·authorizer
reply의 6개 경로에 전달한다. 원본 입력과 실패 시 ticket·credential·nonce·
base·부분 출력의 불변성, 성공한 독립 control의 ticket·secret을 검사한다.
20초 검사에서 533,819개 입력이 통과했고 CI에도 같은 20초 경로를 추가했다.
기존 ciphertext 변조 fuzz와 목적이 다르며 실제 Ceph 호환성 검증을 대신하지 않는다.

양의 소수 초 ticket 유효기간은 encrypted codec 단위 시험으로 확인했다.
이 codec 변경 자체는 소수 초 ticket의 실제 갱신 검증을 포함하지 않았다.

이 변경을 포함한 `7b7c307`은 같은 직접 IPv6·aes256k 구성에서 전체 통합시험과
3분 부하 시험을 통과했다. 성공 호출 62,700건, ticket 갱신 18회, 서버가 성공
응답한 MGR 장애 주입 5회와 context 상태 조회 281,761회를 처리했다.
결과 불명확 2건은 MGR 교체 원인을 유지했다. 관찰한 최대 세션은 2개,
goroutine은 19개였다. 반복 종료 12회에서 파일 descriptor는 6개로 돌아왔고,
GC 후 heap은 344,424 bytes에서 279,192 bytes였다.

후속 `649b45e`는 1초 간격의 갱신 검사를 실제 `RenewAfter`를 기다리는
타이머로 바꿨다. 암호화된 500ms AUTH/MGR ticket은 각각 만료 전에 갱신을
시작했고, 기존 세션에서 진행 중인 명령의 원본 응답도 유지했다.
실제 20.2.4에서는 AUTH ticket 1.5초·MGR service ticket 12초·MON tick 1초의
격리 fixture로 네 번 모두 만료 전에 갱신했다. Linux arm64·CGO=0·aes256k·
직접 IPv6와 Darwin arm64·aes·IPv4 host race에서 client ID와 기존 MGR 연결을
유지했다. 두 종류 ticket을 모두 1.5초로 줄인 별도 구성은 통과하지 못했으며
검증된 범위에 포함하지 않는다. Ceph는
[rotating key의 남은 수명으로 validity를 줄일 수 있다](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/auth/cephx/CephxKeyServer.cc#L123).

이 갱신 방식을 포함한 `47cec13`의 Linux arm64·CGO=0·20.2.4·aes256k·직접 IPv6
[1시간 시험](https://github.com/JSYoo5B/ceph-msgr-go/tree/checkpoint/auth-lifecycle-1h)도 통과했다.
성공 호출 1,263,576건, ticket 갱신 404회, 서버가 성공 응답한 MGR 장애 주입
119회와 context 상태 조회 5,676,627회를 처리했다. 결과 불명확 11건은 모두
MGR 교체 원인을 보존했다. 샘플링한 최대 세션은 2개, goroutine은 20개였다.
같은 실행에서 큰 MON 응답도 원본 JSON과 서버 코드를 유지했다.
이 시험은 이후 MON idle fixture, admission 오류 경합과 seed 주소 변경을
포함하지 않는다.

실제 CRC-only MON/MGR listener에서도 secure-only 제안과 인증 거절의
method `2`·서버 코드 `-95`, 전송 전 알려진 실패를 확인했다. 개발용 observer가
outbound AUTH_REQUEST의 mode `[secure]`를 직접 검사하며 credential bytes를
로그에 남기지 않는다. MGR 거절 중 MON 조회는 유지됐고, 두 MGR을 secure로 재시작한
뒤 같은 client가 새 MGR 지도·인증·조회를 완료했다. MON은 aes256k·Linux arm64·
직접 IPv6, MGR 거절·복구는 aes256k·Darwin arm64·IPv4 host race에서 통과했다.
CRC는 이 부정 시험의 서버와 독립 Ceph fixture 클라이언트에만 허용한다.

MON 재접속의 IPv6 scope·flow와 동일 endpoint의 서로 다른 식별 후보는
인증·지도·명령 응답까지 수행하는 synthetic peer 시험 4개로 확인했다.
실제 link-local 네트워크 시험은 아니다. 같은 제품 소스 `3cca408`은
20.2.4·aes256k의 Linux arm64·직접 IPv6 전체 통합시험 28개를 통과했다.

후속 `f17a0aa`는 명시적 seed와 인증된 mapped AF_INET6 주소의 family·scope·flow를
IPv4로 바꾸지 않고 보존한다. 이는 고정 Tentacle의
[원본 sockaddr encoding](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/msg/msg_types.h#L500)과
정확한 서버 식별 비교에 따른 수정이다. 직접 작성한 raw 주소 vector와
secure synthetic peer로 일치하는 주소의 성공, 같은 IP의 서로 다른 family 거부,
learned MON 복구를 확인했다. DNS seed에서 추론한 Go `TCPAddr`의 IPv4 표현만
별도로 정규화하며, custom dialer의 4·16-byte IPv4와 native IPv6도 검사했다.
이 raw vector·synthetic 시험 자체는 실제 mapped IPv6 Ceph 상대 검증이 아니다.

제품 소스 `7034227`은 별도 Linux arm64·20.2.4·aes256k fixture에서 실제 mapped
AF_INET6 MON 3개·MGR 2개와 기본 Go dialer의 secure 인증을 통과했다. 독립 Ceph
CLI의 `status`·`mon dump`·`mgr dump`·`pg stat` JSON과 인증된 지도 주소·MGR ID를
대조하고, Go의 raw MON/MGR JSON·서버 code `0`·Close를 확인했다. TCP peer는
IPv4로 표시됐지만 wire map은 `[::ffff:127.0.0.1]`을 유지했다. Ceph 설정 입력은
hex 형식 `[::ffff:7f00:1]`을 사용한다. 이 결과의 범위는 Linux `bindv6only=0`의
컨테이너 loopback이며, hostname 추론·host relay·mapped 주소의 장애 복구는
이 시험에 포함하지 않는다.

`8a827e2`의 mapped 시험은 AUTH·MGR ticket 갱신 2회, 단일 seed MON a를
SIGSTOP한 동안 지도에서 학습한 b로의 접속·조회, 별도 observer가 한 번 보낸
`mgr fail`에 따른 b→a 전환을 확인했다. 새 `mgr dump`의 name·ID·epoch와 상태를
대조한 뒤 lazy MGR 접속·양쪽 조회·Close도 통과했다. 포트 예약을 적용한
Linux arm64·20.2.4·aes256k 실행은 약 32초였다.
앞선 실행에서는 Ceph MGR a의 listener가 `Address already in use`로 실패했고,
당시 포트 소유자는 확인하지 못했다. 별도의 결정적인 Linux socket 대조군은
자동 client 포트가 향후 mapped listener를 막는 경로와 예약에 따른 성공을
확인했다. 개발 컨테이너는 MON/MGR 고정 포트를 자동 할당에서 제외한다.
이 대조군과 후속 성공만으로 앞선 충돌의 실제 소유자를 확정하지는 않는다.

후속 `e7f3a3b`의 원격 CI에서는 같은 pause/resume 뒤 두 native MGR가
없어진 AUTH service secret으로 이전 proof를 검증하지 못하며 복귀하지 않았다.
Go MON 인증·갱신은 유지됐고 같은 커밋의 로컬 재현은 통과했으므로 정확한
발생 원인은 확정하지 않았다. 현재 mapped 시험은 Ceph quorum을 유지한 채
client TCP를 끊고 seed 33300 경로를 Close까지 차단해 이 결합을 분리한다.
Linux arm64·20.2.4·aes256k에서 두 ticket 갱신·학습한 MON 33301 접속·
한 번의 `mgr fail`·새 지도 대조·lazy 접속·Close를 약 19초에 통과했다.
이는 client 경로 장애 시험이다. 실제 MON daemon 중단·복구는 별도 통합시험으로
계속 검증하며 앞선 SIGSTOP 시험 결과와 구분한다.

후속 `25496b0`는 hostname seed에서 custom dialer가 일반 `net.Addr`로 제공한
숫자형 IP:port도 MON 식별 주소로 사용한다. 명시적 seed와 learned wire 주소는
유지하며, 해석할 수 없는 RemoteAddr는 handshake를 시작하지 않고 연결 종료를
마친 뒤 로컬 설정 오류로 거부한다. IPv4·IPv6·mapped IPv6의 secure synthetic
peer와 종료 대조군으로 수정 전 실패 및 수정 후 결과를 확인했다.

`08f7d5e`의 공개 API 시험은 fixture용 hostname을 숫자형 MON 주소로 라우팅하고
일반 `net.Addr`의 논리 peer 주소를 사용한다. 같은 제품 `25496b0`에서 실제
20.2.4·aes256k·Linux arm64·직접 IPv6의 MON/MGR 명령과 Close가 통과했으며,
기존 기본 Go DNS dialer 시험도 통과했다. 20.2.4·aes·IPv4의 Darwin arm64
host race·relay 구성에서도 일반 peer 주소 경로가 통과했다.

`ac45f06`은 handshake가 이미 선택한 거부 원인을 연결 정리 중의 후행
context 종료에도 보존한다. 원래 코드에서 실패한 20개 거부 회귀와 대조군으로
인증 코드·framing·feature 거부 및 plain 정책 오류를 확인했다. transport 오류의
context 매핑과 성공 연결의 수명은 유지하며, 실제 endpoint deadline을 넘긴
cleanup에서도 malformed HELLO를 bootstrap 재시도로 바꾸지 않고 종료한다.

MGR의 `balancer mode` 변경에서도 서버 적용을 독립 클라이언트로 확인한 뒤
응답 수신을 막았다. 독립 클라이언트가 원래 설정을 다시 저장하고 MON에서
저장 결과를 확인한 다음 MGR을 전환했다. 진행 중 호출은
`ErrManagerChanged`를 원인으로 가진 `OutcomeUnknownError`를 반환했고,
새 MGR에는 원래 설정이 유지됐다. 변경 명령의 전송은 한 번뿐이었다.
이 시험은 20.2.4·aes256k의 Darwin arm64·IPv4 및 20.2.3·aes의
Linux arm64·IPv6에서 통과했다. 후자의 결과는 이 시험에 대한 검증이다.

MON의 `config-key set`도 적용된 변경의 응답을 막은 상태로 검증했다.
독립 클라이언트가 값을 다시 저장하고 읽어 확인한 뒤 실제 MON을 재기동했다.
원래 호출은 결과 불명확 오류로 끝났으며, 다른 MON에 인증한 기존 클라이언트와
독립 클라이언트 모두 덮어쓴 값을 읽었다. 변경 명령의 전송은 한 번뿐이었다.
이 시험은 CI의 Linux amd64·실제 Ceph 구성과 20.2.4·aes256k의
Darwin arm64·IPv4에서 통과했다.

MGR 핸드셰이크에서는 실제 서버의 수신 bytes를 차단한 상태로 `Close`를
호출했다. 연결 정리를 잠시 보류하면 `Close`도 기다렸으며, 정리를 끝내면
연결과 대기 호출이 모두 종료됐다. 명령을 전송하기 전이므로 호출은
결과 불명확 오류 없이 `ErrClosed`를 반환했다. 독립 클라이언트의 MGR
명령은 계속 성공했고 파일 descriptor 검사도 통과했다. 이 시험은 아래
CI의 Linux amd64·실제 Ceph 구성 및 20.2.4·aes256k의 Darwin arm64·IPv4에서
통과했다. Darwin에서는 해당 시험 전후 파일 descriptor가 모두 7개였고,
수정 전 제품 코드는 이 시험의 연결 정리 검사에서 실패했다.

MGR 응답 수신을 차단해 로컬 취소, keepalive timeout, 새 연결 복구를
확인했다. Go relay에서 MON bulk frame의 일부 쓰기를 멈춰 쓰기 timeout,
전송 전 대기 요청의 context 만료, 변경 명령의 자동 재실행 금지도 확인했다.
이 방향별 차단 시험은 Go 시험 전송 계층에서 수행한다. 별도로 실제 MON과
MGR 프로세스에 SIGSTOP을 적용해 TCP 연결을 열린 채로 유지했고, 무응답
감지 후 다른 MON과 standby MGR에서 조회가 성공했다.
active MGR 프로세스를 종료한 별도 시험에서는 `mgr fail` 없이 Ceph의 기본
beacon 설정으로 약 33초 뒤 standby가 자동 승격됐다. 그동안 인증 티켓이
갱신됐고, 승격된 MGR의 명령과 재기동한 daemon의 standby 복귀도 확인했다.
이 경로는 20.2.4·aes256k의 Linux arm64 및 20.2.3·aes의 Darwin arm64에서
통과했다. 기본 Go dialer의 `localhost` seed도 Linux arm64에서 IPv4·IPv6
각각 인증된 MON/MGR 명령을 완료했다.

[MGR 연결 준비 복구 체크포인트](https://github.com/JSYoo5B/ceph-msgr-go/tree/checkpoint/manager-setup-recovery)는
`321dd35`다. 연결 준비 중 active MGR이나 인증 ticket이 바뀌면 현재 상태를
다시 확인해 아직 전송하지 않은 명령을 처리한다. 실제 20.2.4·aes256k의
Darwin arm64 시험에서는 연결 준비와 MGR 전환을 겹치게 한 뒤 변경 명령의
단일 전송과 독립 클라이언트가 읽은 변경 결과를 확인했다. 제품 수정이 동일한
`d4e737a`의 전체 시험과 3분 부하 시험에서 63,504개 명령, ticket 갱신 18회, MGR 전환 시도
6회를 처리했다. 이 체크포인트의 CI 17개 작업도 모두 통과했다.

별도의 120초 ticket fixture에서 8초 동안 애플리케이션 명령 없이 MON/MGR
연결을 유지했다. 1초 keepalive와 4초 무응답 제한을 사용했고, ticket 갱신이나
MON 재연결 없이 수신 활동과 이후의 MgrMap 전달을 확인했다. 독립 클라이언트가
MGR을 전환한 뒤에도 기존 MON 구독으로 새 MGR을 찾아 명령을 완료했다.
이 시험은 20.2.4·aes256k의 Darwin arm64·IPv6 및 20.2.3·aes의 Linux arm64·IPv4에서
통과했다.

후속 `de18213`은 MON session timeout을 3초·tick을 1초로 줄인 별도 fixture에서
같은 8초 idle을 검증했다. 독립 client가 daemon의 실제 설정 7개를 읽고,
첫 keepalive가 10초 뒤인 대조군의 원래 MON 세션은 8초 안에 서버에서 닫혔다.
1초 keepalive를 쓰는 client는 원래 MON 세션·인증을 유지하고 이후 MGR 전환
지도를 받았다. 20.2.4·aes256k·Linux amd64·IPv6의 host 모드
[실제 CI 시험](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/36990152382)에서 통과했다.
같은 고정 소스는 20.2.4·aes·Darwin arm64·IPv4의 host race에서도 통과했다.
대조군 종료는 약 3.10초였고, 기존 client는 MON/auth와 양쪽 수신 활동을 유지했다.
서버가 먼저 보낸 keepalive의 과거·미래 timestamp 원본 echo와 로컬 수신 시각,
명령 sequence·ACK 유지는 별도의 secure synthetic peer 시험으로 확인했다.

MON 세 개를 모두 중단하고 재기동하는 과정을 3회 반복했다. 중단 중 전송을
기다리는 호출은 context 만료로 끝나며, 충분한 deadline을 가진 MON/MGR
조회는 quorum 복구 후 완료됐다. 이 시험은 프로세스 중단에 대한 검증이다.
별도의 계정을 실제 `auth rm`으로 제거하고 자연스러운 ticket 갱신에서
`AuthenticationError`의 method `2`, code `-13`을 확인했다. 이후 `auth import`로
같은 키를 복원해 기존 client의 MON/MGR 호출이 다시 성공하는지 검증했다.
키 회수·복원 시험은 Linux arm64에서 20.2.4의 aes256k·IPv4 및 aes·IPv6,
20.2.3의 aes·IPv4 구성으로도 통과했다.
인증 키 자체를 교체한 경우에도 기존 client의 갱신 거절을 확인했고,
호출자가 새 키로 명시적으로 만든 client에서 MON/MGR 명령이 성공했다.
쓰기 중인 연결 뒤에 대기하던 호출을 취소하거나 종료할 때 bulk 입력과
context 참조가 GC로 해제되는지도 별도의 단위 시험으로 확인했다.

별도 fixture에서 실제 ticket이 만료된 뒤 서버가 이전 ID의 암호 증거를
받아들이면 ID를 유지하고 ticket을 갱신했다. 그 증거의 rotating secret까지
폐기되도록 기다린 경우에는 MON/MGR의 새 명령을 `AuthenticationError(-13)`로
차단했고, 호출자가 명시적으로 수행한 새 `Dial`은 정상 접속했다.
폐기 시험은 보관한 이전 증명을 각 MON이 실제로 거절하는지 먼저 확인하며,
고정된 대기 시간만으로 rotating secret 폐기를 단정하지 않는다.

[1시간 부하 시험 체크포인트](https://github.com/JSYoo5B/ceph-msgr-go/tree/checkpoint/tentacle-soak-1h)는
`0e73e56`이다. Linux arm64·IPv4·aes256k 구성에서 성공 요청 1,244,441건,
ticket 갱신 366회, MGR fail 시도 119회를 수행했다. 장애 중 결과 불명확 15건은
해당 오류로 반환했다. 이후 복구 보강은 별도의 3분 부하 시험과 CI로 검증했다.
세션·goroutine 수는 샘플링한 값이며 순간 최대치를 보장하지 않는다.

수신 무응답 감지를 추가한 바이너리 `dd3daef`의
[추가 1시간 부하 시험](https://github.com/JSYoo5B/ceph-msgr-go/tree/checkpoint/stall-soak-1h)도
같은 Linux arm64·IPv4·aes256k 구성에서 통과했다. 성공 호출 1,248,430건,
ticket 갱신 361회, MGR fail 시도 120회, 결과 불명확 22건을 기록했다.
샘플링한 세션 최대는 3개, goroutine 최대는 24개였다. 두 1시간 시험의
fixture는 global ID reclaim에 Ceph의 기본 허용 설정을 사용했다.

`auth_allow_insecure_global_id_reclaim=false`를 적용한 바이너리 `cadd40d`의
[엄격한 인증 설정 1시간 시험](https://github.com/JSYoo5B/ceph-msgr-go/tree/checkpoint/strict-auth-soak-1h)도
Linux arm64·IPv4·aes256k 구성에서 통과했다. 성공 호출 1,246,654건,
ticket 갱신 367회, MGR fail 시도 119회, 결과 불명확 34건을 기록했다.
샘플링한 세션 최대는 3개, goroutine 최대는 24개였으며 종료 후 worker
정리 검사도 통과했다. 이후 변경은 각 실서버 시험과 CI로 따로 검증했다.

MGR 연결 준비 복구를 포함한 `965ee5b`의
[추가 1시간 시험](https://github.com/JSYoo5B/ceph-msgr-go/tree/checkpoint/mgr-recovery-soak-1h)도
동일한 엄격한 인증 설정의 Linux arm64·20.2.4·IPv4·aes256k 구성에서 통과했다.
성공 호출 1,256,578건, ticket 갱신 363회, MGR fail 시도 120회, 결과 불명확 39건을
기록했다. 샘플 최대는 세션 3개·goroutine 24개였으며 종료 후 worker 검사도
통과했다. 이 바이너리에는 이후 MGR 핸드셰이크 종료 수정과 부하 시험 오류
원인 검사가 포함돼 있지 않다. 이후 부하 시험은 연결 단절·세션 교체 등
허용한 복구 원인만 받아들이며, 결과 불명확이더라도 인증·암호·프로토콜
오류를 허용하지 않는다.

이 검사를 적용한 제품 소스 `5f36a9c`의 Darwin arm64·20.2.4·aes256k·IPv4
전체 시험과 3분 부하 시험도 통과했다. 성공 호출 50,532건, ticket 갱신
18회, MGR fail 시도 5회, 결과 불명확 0건을 기록했다. 샘플 최대는 세션 2개·
goroutine 19개였다. 이 시험에는 MGR 핸드셰이크 종료와 MON/MGR 변경 명령의
실제 적용·재실행 금지 검증도 포함한다.

앞선 별도 장시간 실행은 16분 31초에 실패했다. Go 호출은 결과 불명확 EOF를
반환했고 Ceph MGR·MON의 crash 로그도 있었다. 원인은 아직 확정하지 못했으며,
장시간 시험만 따로 실행한 위의 성공 결과가 그 실패 원인을 설명하지는 않는다.

MGR이 없는 상태에서도 MON 조회와 실제 ticket 갱신을 확인했다. MGR 대기
취소는 전송 전 오류로, 대기 중 `Close`는 `ErrClosed`로 반환했고, 이후
MGR을 기동하면 기존 대기 요청이 처리됐다. 이 시험은 Linux arm64에서
aes256k·IPv4 및 aes·IPv6 구성으로 수행했다.

실제 시험 클라이언트 바이너리는 Ceph 실행 파일이나 라이브러리를 호출하지
않는다. 1시간을 넘는 운영, 위에 명시하지 않은 모듈·클러스터 구성,
20.2.0–20.2.2 및 이후 Ceph 패치는 아직 검증하지 않았다.

Darwin arm64에서도 CGO=0 native 시험 바이너리를 직접 실행해 20.2.4의
aes256k 구성과 상호운용을 확인했다. Ceph는 Docker에 두고 개발용 Go TCP
relay로 접근했다. Ceph 주소는 IPv4와 IPv6를 각각 시험했으며 relay까지의
호스트 TCP 경로는 IPv4다. 이 결과를 Darwin에서의 직접 IPv6 접속 검증으로
해석하지 않는다. 반복 종료 시험의 파일 descriptor 수는 시작과 끝 모두 5였다.

공개 `Snapshot`의 FSID와 active MGR 정보는 독립 클라이언트의 `status`·
`mgr dump` 응답과 대조했다. 실제 ticket 갱신, MON 재기동, MGR 전환과
lazy 접속, 인증 키 회수·복원, 종료에 따른 상태 변화도 확인했다.
실제 MON을 SIGSTOP하고 재접속을 잠시 막은 시험에서는 MGR 연결이 살아
있어도 MON/MGR ready가 false였고, 다른 MON에서 복구한 뒤 둘 다 true로
돌아왔다. 이 시험들은 20.2.4·aes256k의 Darwin arm64·IPv4에서 통과했다.
같은 공개 상태 검사를 포함한 제품 소스 `9a3f5c0`의 전체 통합시험 25개도
20.2.4·aes256k의 Linux arm64·IPv6에서 통과했다.

Go 1.24.0과 1.27 계열에서 Linux·macOS·Windows의 CGO=0 unit/vet 검사를
통과했다. Linux와 Darwin arm64에서 race 검사, Darwin에서 parser fuzzing도 통과했다.
Windows amd64, Darwin amd64, Linux 386에서 CGO=0 빌드를 확인했다.
Windows amd64의 실제 Ceph 상대 실행도 소스 `37e2027`에서 확인했다.
[2026-10-03 CI](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/37121095318)의
26개 작업이 모두 통과했으며 Windows의 아래 선택 시험 10개도 실제 pass를
기록했다. Go 1.27.1·CGO=0·20.2.4·aes256k·secure·IPv4 relay 구성이다.

Windows 전용 CI는 `windows-2025`에서 CGO=0 Windows amd64 시험 실행 파일을
직접 실행한다. 개발용 Ceph 20.2.4·aes256k·secure·IPv4 fixture만 별도의 WSL2
배포판에 두고 기존 opaque TCP relay의 Windows loopback 경로로 접근한다.
Fixture 제어는 Go 1.26 이상으로 컴파일한 Linux 개발 실행기가 맡으며,
제품 시험 바이너리는 계속 Windows에서 직접 실행한다.
이는 Windows의 직접 IPv6 접속이나 다른 인증 구성의 검증을 대신하지 않는다.
준비·인증 갱신·MON/MGR 전환, raw 명령·Tell·이름 지정 MON Tell, 동시성,
context·Close와 log/config/digest 수신을 포함한 공개 시험 9개와 내부 통신
시험 1개를 선택한다. 모든 선택 시험의 실제 pass와 package pass를 요구해
전체 skip 또는 잘못된 selector를 성공으로 처리하지 않는다.
시험 동안 전용 foreground WSL 세션을 유지하고 fixture 정리 후 종료·대기한다.
Docker 서비스가 떠 있다는 사실만으로 WSL 배포판의 수명이 유지된다고 가정하지 않는다.
Fixture 제어 ACK는 임시 파일을 닫은 뒤 같은 디렉터리에서 rename으로 발행한다.
시험이 ACK를 확인하자마자 삭제해도 발행자가 해당 파일을 다시 수정하지 않는다.

[Windows 검증 드라이버](tools/windows_interop.py)는 실행마다 별도의 `run-*`
증거 폴더를 만들고 source SHA·Go 도구 체인·daemon 버전·시험 목록을
`verification.json`에 기록한다. 준비 실패도
`startup.json`·`bootstrap.log`·`failure.json`으로 남긴다. 전용 컨테이너와 임시
credential을 정리하고 CI가 자신이 만든 WSL 배포판만 삭제한다. 정리 실패는
`cleanup-failure.json`에 소유한 컨테이너·output 경로를 기록하고 output을
보존한다. Windows 시험의 시간 초과는 해당 PID의 자식 tree만 종료·대기한다. 이 도구와
WSL·Docker·Python·Ceph CLI는 개발 검증 도구이며 제품 의존성이 아니다.
준비한 WSL2 배포판에서는 PowerShell로 다음을 실행할 수 있다.

```powershell
python tools/windows_interop.py --distro CephMsgrFixture --diagnostics "$env:TEMP/ceph-windows-evidence"
```

WSL 구성은 [공식 runner image](https://github.com/actions/runner-images/blob/main/images/windows/Windows2025-Readme.md),
[배포판 import](https://learn.microsoft.com/en-us/windows/wsl/use-custom-distro),
[localhost 전달](https://learn.microsoft.com/en-us/windows/wsl/networking)을 기준으로 한다.
개발용 Ubuntu rootfs는 Canonical의 고정 20240423 archive와
[게시된 SHA256](https://cloud-images.ubuntu.com/wsl/releases/noble/20240423/SHA256SUMS)을 확인한다.

```sh
go test -race ./...
go vet ./...
go -C integration/fixture test ./...          # 개발 실행기, Go 1.26 이상
go -C integration/fixture vet ./...
sh integration/run.sh                         # Docker 필요, aes256k
CEPH_MSGR_TEST_KEY_TYPE=aes sh integration/run.sh
CEPH_MSGR_TEST_CONNECTION_MODE=crc sh integration/run.sh # CRC-only MON/MGR, CephX 유지
CEPH_MSGR_TEST_CONNECTION_MODE=crc CEPH_MSGR_TEST_KEY_TYPE=aes CEPH_MSGR_TEST_RUNTIME=host sh integration/run.sh
CEPH_MSGR_TEST_IP_FAMILY=6 sh integration/run.sh
CEPH_MSGR_TEST_IP_FAMILY=6 CEPH_MSGR_TEST_MAPPED_IPV6=1 sh integration/run.sh # 별도 mapped 주소·native CLI 대조
CEPH_MSGR_TEST_KEY_TYPE=aes CEPH_MSGR_TEST_SERVICE_CIPHER=aes256k sh integration/run.sh
CEPH_MSGR_STRESS_DURATION=3m sh integration/run.sh
CEPH_MSGR_TEST_MGR_COUNT=0 sh integration/run.sh # MGR 지연 기동
CEPH_MSGR_TEST_RUNTIME=host sh integration/run.sh # 호스트 native 바이너리
CEPH_MSGR_TEST_RUNTIME=host CEPH_MSGR_TEST_RACE=1 sh integration/run.sh # 실제 Ceph 상대 race 검사
CEPH_MSGR_TEST_EXPIRE_TICKETS=1 sh integration/run.sh # 별도 인증 만료 fixture
CEPH_MSGR_TEST_IDLE_SESSIONS=1 sh integration/run.sh # 별도 120초 ticket·idle 시험
CEPH_MSGR_TEST_AUTH_EPOCH=1 sh integration/run.sh # 별도 service-key 교체·조기 갱신 시험
CEPH_MSGR_TEST_SHORT_TICKETS=1 sh integration/run.sh # 별도 fractional AUTH ticket 시험
CEPH_MSGR_TEST_MODE_REJECTION=mon sh integration/run.sh # 실제 CRC-only MON 거절
CEPH_MSGR_TEST_MODE_REJECTION=mgr sh integration/run.sh # 실제 CRC-only MGR 거절·secure 복구
CEPH_MSGR_STRESS_DURATION=1h CEPH_MSGR_TEST_TIMEOUT=70m sh integration/run.sh
```

부하 시험 시간은 45초 이상으로 설정한다. 더 긴 시험에는
`CEPH_MSGR_TEST_TIMEOUT`도 늘린다. 기본 timeout은 시험 바이너리별로 10분이다.

[공개 API 통합시험](integration/)은 별도 Go 시험 패키지에서 제품을 import한다.
명령·권한·인증 키 회수와 교체·운영 상태·연결 준비 검사를 포함한다.
[클라이언트 패키지](cephmsgr/)에는 제품 구현과 내부 세션·인증 증거·
요청 대기열을 직접 검사하는 시험을 함께 둔다. 공유하는 relay
접속·장애 제어 함수는 [개발용 내부 패키지](internal/testcluster/)에 있으며
제품의 빌드·실행 의존성에는 포함되지 않는다.
분리한 시험 소스 `48e9054`는 20.2.4·aes256k·IPv4의 Darwin arm64 host 모드에서
공개 API 시험 10개와 내부 시험 14개를 연속 실행해 통과했다. hostname과
별도 fixture가 필요한 시험은 실행 조건에 따라 건너뛰었으며, 반복 종료
시험의 파일 descriptor 수는 시작과 끝 모두 5개였다.
제품 패키지까지 이동한 소스 `a402b03`에서는 같은 Darwin arm64·20.2.4·
aes256k·IPv4 구성에서 공개 상태 조회, 실제 MON 무응답 복구, MON/MGR 명령
시험 3개가 새 import 경로로 통과했다. 패키지 이동은 기존 Go 소스 33개의
내용을 그대로 유지했다.

연결 준비 API를 추가한 소스 `cc621d0`는 같은 Darwin arm64·20.2.4·aes256k·
IPv4 구성에서 관리 명령 없이 동시 8개 MGR 준비 호출, 실제 ticket 갱신,
active MGR 교체 후 별도 인증, 취소와 종료를 통과했다. MGR 지연 기동 fixture도
통과했다. 이 경우 MON 사용과 갱신을 유지하면서 MGR 명령·준비 대기를 취소하거나
종료했고, MGR을 기동하면 진행 중이던 두 대기가 모두 완료됐다.
단위 시험에서는 실제 전송 메시지를 세어 준비 API가 관리 명령을 보내지 않고,
명령 슬롯이 모두 사용 중이어도 완료되는 것을 확인했다.

개발 Harness는 [별도 Go 모듈](integration/fixture/go.mod)에 고정한 공개
`ceph-testcontainers-go v0.2.0`을 사용한다. 개발 실행기는 Go 1.26 이상을
요구하며 제품의 Go 1.24·표준 라이브러리 dependency graph에는 포함되지 않는다.
`integration/run.sh`의 환경변수 입력과 profile별 시험 selector는 유지한다.

`WithMonitorCount(3)`, `WithManagerCount(2)` 또는 `WithNoInitialManagers`,
`WithAuth`, `WithMessengerMode(MessengerV2Only)`, `WithMessengerPolicy`,
`WithAddressFamily`로 각 daemon을 라이브러리가 소유한 컨테이너에서 실행한다.
Ticket TTL과 aes/aes256k client·service cipher는 bootstrap 시 설정한다.
OSD는 기본 MemStore 또는 `CEPH_MSGR_TEST_OSD_STORE=bluestore`에 따라
`WithOSDObjectStore`로 초기화한다. Replica profile은 1 또는 3 OSD이며
3 OSD는 BlueStore·replicas 3/min_size 2를 사용한다. Native `crushtool`로 만든
legacy map과 `CreatePool(NoHashPSPool: true)`를 적용한다.
`CreateClient`가 생성한 시험용 keyring은 private IPC 디렉터리에만 보관한다.
OSD profile은 `[global] osd_pool_default_flag_hashpspool = false`를
`WithConfigFile`로 Ceph 시작 전에 전달해 MGR 모듈의 자동 `.mgr` 풀에도
적용한다. 시작 후 `.mgr` 플래그를 바꾸지 않으며, background 생성이 끝날
때까지 유한 context 안에서 읽기 전용 native OSDMap을 확인한다. 기본
`legacy-v6` fixture는 `.mgr`와 데이터 풀의 존재 및 모든 풀의 HASHPSPOOL
미사용을 검사한다. 별도 `replicated-pg-v8` fixture는 HASHPSPOOL·8 PG를 검증한다.
데이터 풀의 `NoHashPSPool`만으로 MON 요구 feature가 줄어들지는 않는다.
제품이 미구현 bit를 광고해 admission을 통과시키지 않으며,
bootstrap recipe와 실제 native 풀 정책은 `pool-policy.json`에 기록한다.
이 bootstrap 변경의 새 native 검증 상태는
[fixture validation](integration/fixture/VALIDATION.md)에 별도로 기록한다.

MON 주소·FSID는 실제 native map에서 읽는다. 장애 주입 대상은 고정 포트가
아닌 MON 이름과 MGR 컨테이너 주소로 식별한다. `cluster.sh`는 독립
CLI/librados oracle만 실행하며 daemon mkfs·기동·키 생성을 하지 않는다.
MON/MGR 정지·재기동·pause/resume와 일시적 mode 정책은 Go controller가
공개 daemon handle·`PauseContainer`·`TemporaryMessengerPolicy`로 실행한 뒤
기존 request ID의 acknowledgement를 게시한다. OSD 적용 epoch는
`AdminSocket`으로 독립 확인한다. Native 결과·byte 비교와 mutation의
once-only 전송 검증은 그대로 유지한다.

별도 `CEPH_MSGR_TEST_NATIVE_MGR_PREPARATION=1`은 native MGR 준비 시험 하나를
선택한다. Standby mgr.b를 공개 Pause/Resume로 45초 멈춘 뒤 native map의
standby 소멸과 최신 AUTH -13을 독립 확인했다. Conditional 준비 동작은
receipt 하나와 Stop/Start 한 번으로 새 global ID의 standby를 복원했고
active MGR는 유지했다. 정상 상태에서 두 번째 준비는 재시작·receipt를 추가하지
않았다(48.95초 통과). 이는 disposable native fixture의 사전조건 복구 검증이며
Go CephX protocol 수정이나 Go MGR 인증 재실행 검증은 아니다. 이 단독 AES/aes256k·IPv4 프로필을 CI에도 추가했다. Linux arm64의
로컬 통과와 Linux amd64 CI 통과는 별도로 확인한다. 상세 결과·독립 증거는
[검증 기록](integration/fixture/OSD_MUTATIONS_VALIDATION.md)에 둔다.

기본 모드는 cluster network에 붙인 별도 native oracle 컨테이너에서 CGO=0
Go 시험 바이너리를 실행한다. `host` 모드는 현재 OS·CPU의 시험 바이너리를
실행하며 `127.0.0.1`에 게시한 relay로 모든 광고된 daemon 주소에 접속한다.
Relay는 native topology의 endpoint만 허용한다. MON을 모두 정지할 때도
oracle 컨테이너는 계속 실행되며 복구 시 b/c부터 시작해 quorum을 만든다.
종료 시 oracle/relay 컨테이너를 먼저 정리하고 라이브러리의 daemon·network·
volume을 정리한다. Windows WSL controller도 같은 공개 fixture를 사용한다.

교체 검증의 실행 profile·제외 조건·최초 실패와 upstream 질문은
[fixture 검증 기록](integration/fixture/VALIDATION.md)에 구분해 둔다.

`CEPH_MSGR_TEST_RACE=1`은 `host` 모드에서만 지원한다. 이때 시험 바이너리는
Go race detector에 필요한 CGO=1 계측을 사용한다. 제품과 기본 fixture
바이너리는 계속 CGO=0이며, race 계측은 제품 의존성에 포함되지 않는다.
Darwin arm64·20.2.4·aes256k·IPv4에서 인증 키 회수·복원, 연결 준비,
상태 조회·실제 ticket 갱신·MGR 전환, 부분 전송·응답 지연, MGR 무응답,
MON 쓰기 timeout·실제 MON 정지 복구를 포함한 7개 시험이 race 검사로 통과했다.
Ceph CLI는 이 개발 fixture의 초기화와 daemon 준비 상태 확인에 사용한다.
Harness는 공개 API 시험과 클라이언트 패키지의 내부 시험을 각각 컴파일해
같은 fixture에서 차례로 실행한다. 마지막 내부 복구 시험이 MON 하나를
중단하므로 공개 API 시험을 먼저 실행한다. `CEPH_MSGR_TEST_RUN`은 두
바이너리 모두에 적용된다.
`auth_allow_insecure_global_id_reclaim=false`를 적용한다. 인증 만료 시험은
MON을 ticket과 rotating secret의 수명보다 오래 중단한다. Ceph daemon의
기존 인증에도 영향을 주므로 전용 fixture에서만 실행한다.
`CEPH_MSGR_TEST_DIAGNOSTICS`에 개발용 디렉터리를 지정하면 실패 시 daemon
텍스트 로그와 crash metadata를 보관한다. 키와 프로세스 메모리는 복사하지 않는다.
Metadata는 종료된 컨테이너에서도 회수하며, 이 개발용 수집에는 Python 3
표준 라이브러리를 사용한다. CI는 실패한 Ceph 시험의 로그와 metadata를
artifact로 7일간 보관하도록 설정했다.
완료된 Go 시험 실패가 없이 harness가 비정상 종료되면, CI 공개 annotation에
종료 코드와 마지막으로 시작한 시험, 최대 10개 진단을 남긴다. 마지막 시험이
종료 원인이라는 뜻은 아니다. 앞선 CI `57b8619`의 20.2.4·aes256k·IPv6 작업은
코드 137로 종료됐으며 원인은 아직 확인하지 못했다. 이후 동일한 Ceph·암호·
IP 구성의 Linux arm64 통합시험과 CI 성공이 이 종료 원인을 설명하지는 않는다.
20.2.3 시험은 `CEPH_MSGR_TEST_IMAGE=quay.io/ceph/ceph:v20.2.3`과
`CEPH_MSGR_TEST_KEY_TYPE=aes`를 함께 설정한다. 해당 이미지의 개발 도구에는
aes256k 옵션이 없으므로 이를 요청하면 fixture 준비를 명시적으로 실패시킨다.
기본 fixture는 digest를 고정한 20.2.4 이미지와 aes256k를 사용한다.

기존 클러스터에 읽기 명령만 시험하려면 `CEPH_MSGR_MONITORS`(쉼표 구분),
`CEPH_MSGR_IDENTITY`, `CEPH_MSGR_KEY_FILE`(base64 key 값이 든 파일), 선택적으로
`CEPH_MSGR_FSID`를 설정하고 `go test -run '^TestCephIntegration$' -v ./cephmsgr`를
실행한다. 장애 주입 시험은 격리 harness의 control directory가 있을 때만
실행한다.

[CI 설정](.github/workflows/ci.yml)은 Linux·macOS·Windows에서 Go 1.24.0과
1.27 계열의 CGO=0 unit/vet 검사, Linux에서 race 검사와 지정한 Tentacle 구성을
시험한다. Race 작업은 Linux amd64에서 실제 Ceph를 상대하는 공개 API와
클라이언트 내부 통합시험도 host 모드로 실행한다.
Frame·지도·인증 응답 parser fuzzing, 20.2.4·aes256k와 20.2.3·aes의
3분 부하 시험, MGR 지연 기동, host 모드, 별도 인증 만료, 긴 ticket·idle fixture와
service-key 교체 2개 구성을 포함한
[GitHub CI 20개 작업](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/36975839366)이
2026-10-02에 모두 통과했다. Actions 설정 lint와 개발용 진단 도구의 단위
시험 및 소스 비교 도구 시험 31개도 통과했다.
[서비스 키 교체·큰 응답 체크포인트](https://github.com/JSYoo5B/ceph-msgr-go/tree/checkpoint/service-key-epochs)는
이 실행의 `7b7c307`을 가리킨다.

단위시험은 `go test -json -fullpath`로 package·test별 기록을 수집하고,
[진단 도구](tools/go_test_annotations.py)가 실패 assertion의 실제 파일·줄 번호를
CI 화면에 표시한다. 병렬·하위 테스트와 imported helper의 위치를 구분하고,
원본 JSON은 실패 artifact로 보존한다. `-fullpath`는 최소 Go 버전의
[Go 1.24 testing 소스](https://github.com/golang/go/blob/go1.24.0/src/testing/testing.go#L469)에서도
확인했다. 독립 Go 실패·panic·빌드·성공 출력, 도구 시험 59개와 Linux·macOS·Windows의
[CI 20개 작업](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/36979376909)이 통과했다.
[단위시험 진단 체크포인트](https://github.com/JSYoo5B/ceph-msgr-go/tree/checkpoint/unit-failure-diagnostics)는
해당 CI의 `54d21b9`를 가리킨다.

기본 `net.Dialer`를 사용하는 실제 loopback TCP 시험은 IPv4·IPv6에서 raw 출력과
서버 오류 코드 보존, 전송 후 개별 취소와 후속 요청, `Close`의 요청·연결 종료를
검사한다. IPv6 listener가 없는 환경에서는 해당 경우만 이유를 남겨 건너뛴다.
Darwin arm64의 반복·race 검사와
[CI 20개 작업](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/36981511888)이 통과했다.
[기본 TCP 체크포인트](https://github.com/JSYoo5B/ceph-msgr-go/tree/checkpoint/native-tcp-dialer)는
이 실행의 `43433a1`을 가리킨다. 상대는 Go codec을 공유하는 synthetic peer이며,
이 시험 자체가 실제 Ceph나 Windows의 실제 Ceph 상대 지원을 입증하지는 않는다.
후속 `085b98f`는 서로 다른 MON/MGR loopback TCP 연결에서 지도 발견,
`WaitMgrReady`, 두 daemon의 동시 raw 응답과 양쪽 `Close`도 검사한다.

갱신 deadline, clock 기반 만료 시험, 실제 보안 모드 거절을 포함한
[CI 24개 작업](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/36986263689)이
2026-10-02에 모두 통과했다. Linux·macOS·Windows의 두 Go 버전,
실제 fractional AUTH ticket과 MON/MGR CRC-only 거절, race·fuzz 및 기존 Ceph
구성을 포함한다. [인증 갱신·모드 체크포인트](https://github.com/JSYoo5B/ceph-msgr-go/tree/checkpoint/auth-deadlines-and-modes)는
이 실행의 `47cec13`을 가리킨다. Windows의 1ns 시험은 wire TTL만으로 만료를
추정하지 않고 실제 clock이 시한을 넘은 뒤 publication을 허용하도록 보강했다.

MON idle 만료 대조군, secure keepalive echo, IPv6 zone seed 거부와 admission
종료 원인 보존을 포함한
[CI 24개 작업](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/36990152382)도 모두 통과했다.
[MON 세션 체크포인트](https://github.com/JSYoo5B/ceph-msgr-go/tree/checkpoint/mon-lifetime-boundaries)는
해당 `de18213`을 가리킨다. 오류가 먼저 기록돼도 deadline 분기가 이를 덮는
경합은 제어된 시험으로 재현했다. 일반 map 검증과 의도한 missing-map timeout의
시험 예산도 분리했다. 앞선 macOS CI 실패가 어느 경로였는지는 확정하지 못했다.

mapped wire family 보존과 custom `TCPAddr` 추론을 포함한
[CI 24개 작업](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/36994298657)이 모두 통과했다.
[주소 family 체크포인트](https://github.com/JSYoo5B/ceph-msgr-go/tree/checkpoint/wire-address-family)는
해당 `f17a0aa`를 가리킨다. 이 CI의 실제 Ceph 상대 주소는 기존 IPv4·IPv6 구성이며,
mapped AF_INET6 구성은 별도의 raw vector·synthetic peer 검증 범위다.

후속 `08f7d5e` CI는 23개 작업이 통과했고 macOS·Go 1.24의 malformed-MgrMap
admission 시험이 deadline으로 실패했다. 고정 소스의 제어된 경합에서는 시험의
private MON 후보와 시작 직후 supervisor 후보가 겹쳐 첫 후보의 map이 무시되는
동일 증상을 재현했다. `ab17b77`은 두 단위 fixture에서 명시적인 후보 소유권을
분리하고 시도·map 전달 진단을 추가했다. 실제 CI가 이 경로였는지는 미확정이며,
제품의 MON 연결 소유자와 재접속 동작은 변경하지 않았다.

generic peer 주소의 실제 Ceph 시험, handshake 거부 원인 보존과 fixture 소유권
분리를 포함한 `ab17b77`의
[CI 24개 작업](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/36999005901)이 모두 통과했다.
[peer 주소 체크포인트](https://github.com/JSYoo5B/ceph-msgr-go/tree/checkpoint/peer-addresses)는
이 커밋을 가리킨다.

독립 native CLI metadata oracle과 mapped secure 인증·명령·종료를 추가한
`0f87a0c`의 [CI 25개 작업](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/37002707754)도
모두 통과했다. [mapped 인증 체크포인트](https://github.com/JSYoo5B/ceph-msgr-go/tree/checkpoint/mapped-peer-interop)는
이 커밋을 가리킨다. 이후 mapped 갱신·failover 시험은 별도의 변경이다.

포트 예약과 mapped ticket 갱신·MON/MGR 장애 복구를 추가한 `8a827e2`의
[CI 25개 작업](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/37004993582)도
모두 통과했다. [mapped 복구 체크포인트](https://github.com/JSYoo5B/ceph-msgr-go/tree/checkpoint/mapped-session-recovery)는
이 커밋을 가리킨다.

`MonTell`·`MgrTell`과 실제 갱신·MON/MGR 전환 시험을 포함한 `fbf8ceb`의
[CI 25개 작업](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/37007686548)도
모두 통과했다. [daemon Tell 체크포인트](https://github.com/JSYoo5B/ceph-msgr-go/tree/checkpoint/daemon-tell)는
이 커밋을 가리킨다.

MON 로그 stream·native CLI oracle·cursor 복구·parser fuzz와 client TCP 장애를
격리한 mapped 시험을 포함한 `5573026`의
[CI 25개 작업](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/37016830915)도
모두 통과했다. [MON 로그 체크포인트](https://github.com/JSYoo5B/ceph-msgr-go/tree/checkpoint/mon-log-stream)는
이 커밋을 가리킨다.

독립 인증·원주소 검증을 사용하는 이름 지정 MON Tell과 준비 oracle 보강,
context·Tell의 client TCP 장애 격리를 포함한 `f685aa7`의
[CI 25개 작업](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/37027436808)이
모두 통과했다. [이름 지정 MON 체크포인트](https://github.com/JSYoo5B/ceph-msgr-go/tree/checkpoint/named-mon-tell)는
이 커밋을 가리킨다.

인증된 MON 이름·rank의 로컬 조회와 native map oracle을 추가한 `66ef78c`의
[CI 25개 작업](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/37077788952)도
모두 통과했다. [MON 이름 조회 체크포인트](https://github.com/JSYoo5B/ceph-msgr-go/tree/checkpoint/mon-member-discovery)는
이 커밋을 가리킨다.

등록된 MON 로그 source의 frame 제한 원인을 빠른 교체·지연된 정리 중에도
보존하는 `1082657`의
[CI 25개 작업](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/37079706985)도
모두 통과했다. [로그 frame 제한 체크포인트](https://github.com/JSYoo5B/ceph-msgr-go/tree/checkpoint/log-frame-limits)는
이 커밋을 가리킨다.

전체 config map 구독·제한된 MON capability의 native CLI oracle·ticket 갱신과
client TCP 복구·stream 종료 원인 보존을 포함한 `993001d`의
[CI 25개 작업](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/37083221718)도
모두 통과했다. [MON config 체크포인트](https://github.com/JSYoo5B/ceph-msgr-go/tree/checkpoint/mon-config-stream)는
이 커밋을 가리킨다.

공개 limit 오류·CephX 길이 원인 보존·native limit 실행 결과 검증을 포함한
`f40c670`의 [CI 25개 작업](https://github.com/JSYoo5B/ceph-msgr-go/actions/runs/37085465712)도
모두 통과했다. [공개 limit 체크포인트](https://github.com/JSYoo5B/ceph-msgr-go/tree/checkpoint/public-limit-errors)는
이 커밋을 가리킨다. 별도 로컬 실행의 MGR 정리 timeout과 후속 보정은 위에 기록했다.

[MON 후보·종료 검증 체크포인트](https://github.com/JSYoo5B/ceph-msgr-go/tree/checkpoint/monitor-admission)는
앞서 CI 18개 작업을 통과한 `23148f5`를 가리킨다.
[복구 중 Context 검증 체크포인트](https://github.com/JSYoo5B/ceph-msgr-go/tree/checkpoint/recovery-contexts)는
앞서 CI 18개 작업을 통과한 `c14f78f`를 가리킨다.
[MON 주소 보존 체크포인트](https://github.com/JSYoo5B/ceph-msgr-go/tree/checkpoint/monitor-wire-address)는
앞서 CI 18개 작업을 통과한 `3cca408`을 가리킨다.
[Context 콜백 체크포인트](https://github.com/JSYoo5B/ceph-msgr-go/tree/checkpoint/context-admission)는
앞서 CI 18개 작업을 통과한 `580ea2a`를 가리킨다.
[네트워크 취소·실서버 race 체크포인트](https://github.com/JSYoo5B/ceph-msgr-go/tree/checkpoint/network-cancellation)는
앞서 CI 18개 작업을 통과한 `836fdb2`를 가리킨다.
[연결 준비 API 체크포인트](https://github.com/JSYoo5B/ceph-msgr-go/tree/checkpoint/readiness-waits)는
앞서 CI 18개 작업을 통과한 `cc621d0`를 가리킨다. 실제 Ceph에서 인증 키 회수·복원과 MON
무응답 복구를 검사할 때 두 준비 API의 인증 오류·대기·복구도 확인했다.
[클라이언트 패키지 이동 체크포인트](https://github.com/JSYoo5B/ceph-msgr-go/tree/checkpoint/client-package-layout)는
앞서 CI 18개 작업을 통과한 `a402b03`를 가리킨다.
[통합시험 패키지 분리 체크포인트](https://github.com/JSYoo5B/ceph-msgr-go/tree/checkpoint/integration-test-layout)는
앞서 CI 18개 작업을 통과한 `48e9054`를 가리킨다.
[운영 상태 API 체크포인트](https://github.com/JSYoo5B/ceph-msgr-go/tree/checkpoint/client-state-snapshots)는
앞서 CI 18개 작업을 통과한 `d27732d`를 가리킨다.
[MON/MGR 복구 오류 검사 체크포인트](https://github.com/JSYoo5B/ceph-msgr-go/tree/checkpoint/mon-mgr-recovery-causes)는
앞서 CI 18개 작업을 통과한 `5f36a9c`를 가리킨다.
[MGR 결과·종료 검증 체크포인트](https://github.com/JSYoo5B/ceph-msgr-go/tree/checkpoint/mgr-outcomes-and-close)는
앞서 CI 18개 작업을 통과한 `72c76a5`를 가리킨다.
[idle·인증 증명 검증 체크포인트](https://github.com/JSYoo5B/ceph-msgr-go/tree/checkpoint/idle-and-proof-verification)는
앞서 CI 18개 작업을 통과한 `5f4e1b7`를 가리킨다.
[서버 식별·MGR 자동 승계 체크포인트](https://github.com/JSYoo5B/ceph-msgr-go/tree/checkpoint/server-identity-and-mgr-recovery)는
앞서 CI 17개 작업을 통과한 `a65c54a`를 가리킨다.

[Ceph 변경 비교 도구](tools/ceph_diff.py)는 Python 표준 라이브러리로
upstream ref를 commit SHA로 고정한 후 Messenger, CephX, 지도·복구,
MON/MGR 서버의 인증·접속 정책, 메시지와 명령 schema 등 89개 경로를
비교한다. AuthRegistry와 global·MON 옵션, 시험에서 사용하는 balancer·crash·
iostat 모듈도 포함한다.
소스는 메모리에서만 읽고 결과를
stdout으로 출력한다. 파일 변화가 wire 변경이나 호환성 판정을 의미하지는
않으며 지정된 경로 밖의 변경은 검사하지 않는다.

2026-10-02에 `tentacle`을 커밋
`7411a08041185df39dbb166f983a6b0be7af0811`로 고정해 참조와 비교했다.
56개 경로에서 8개 파일이 달랐으며 자동 조회·비교는 8.59초였다. 이 시간은
수동 diff 검토와 실서버 시험 시간을 포함하지 않는다. 검사한 경로의
frame·CephX·메시지 인코딩은 그대로였고, 연결 재사용 판단·서버 내부 처리와
일부 MON 명령 schema가 바뀌었다. Go client의 fresh lossy 연결과 raw 명령
API를 변경할 필요는 발견하지 못했다. 이는 소스 검토 결과이며 해당 가변
브랜치를 빌드한 실서버의 호환성 검증은 아니다.
추가 감시한 `global.yaml.in`에는 BlueFS·BlueStore·block-device 옵션 8개 추가와
storage profile 허용값 변경이 있었다. 이 추가 diff에서 Messenger·인증 옵션은
변하지 않았다. raw MON config 명령의 옵션 목록이나 서버 허용값 변화는
원래 명령·응답 계약으로 노출하며 API 호환 wrapper를 추가하지 않는다.
도구 시험은 추가·삭제·변경 분류, commit SHA 고정, 응답 크기 제한,
HTTP 오류 구분과 오류 응답 자원 정리를 검사한다.

Tell 추가 후 같은 고정 base와 Tentacle HEAD를 59개 경로로 다시 비교한
2026-10-02의 실행은 8.78초였으며 변경 파일 8개는 동일했다. 새로 감시한
`MCommand.h`·`MCommandReply.h`·`admin_socket.cc`는 byte-identical이었다.
이는 소스 비교 결과이며 가변 브랜치의 실서버 검증을 의미하지 않는다.

MON 로그 추가 후 같은 고정 base와 HEAD를 66개 경로로 비교한 실행은
11.79초였으며 9개 파일이 달랐다. 새 로그 감시 경로 7개 중 6개는
byte-identical이었다. 나머지
[`entity_name.cc` 변경](https://github.com/ceph/ceph/blob/7411a08041185df39dbb166f983a6b0be7af0811/src/common/entity_name.cc#L145-L155)은
유효 타입명 목록의 표시 수정이었다. EntityName 인코딩·해석과 LogEntry v5의
wire 의미는 그대로였고 Go 변경이 필요한 차이를 발견하지 못했다. 비교 시간은
수동 검토·실서버 시험을 포함하지 않으며 해당 HEAD의 런타임 지원 주장이 아니다.

이름 지정 MON Tell 추가 후 같은 base와 HEAD를 67개 경로로 비교한 실행은
9.25초였으며 변경 파일 9개는 동일했다. 추가한 `MMonGetMap.h`는
byte-identical이었다. 이는 지정 소스의 비교 결과다.

MON config 추가 후 같은 base와 고정 HEAD를 73개 경로로 비교한 실행은
10.39초였으며 변경 파일 9개는 동일했다. 추가한 MConfig·MGetConfig와
ConfigMonitor·ConfigMap의 6개 경로는 모두 byte-identical이었다.
이는 지정 소스의 비교 결과이며 해당 HEAD의 런타임 지원 주장이 아니다.

```sh
python3 tools/ceph_diff.py tentacle           # 고정 v20.2.4 참조와 비교
python3 tools/ceph_diff.py v20.2.4 --base v20.2.3 --json
```

## 소스 및 fixture 출처

규약은 [Tentacle Messenger 문서](https://docs.ceph.com/en/tentacle/dev/msgr2/)와
고정 Ceph 소스로 확인했다. Ceph 소스에는 LGPL-2.1 등의 개별 라이선스가
명시되어 있으며 해당 소스를 제품에 vendor하거나 링크하지 않았다.

- MON 설정은 고정 [MConfig](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/messages/MConfig.h),
  [MGetConfig](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/messages/MGetConfig.h),
  [ConfigMonitor](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/mon/ConfigMonitor.cc),
  [ConfigMap](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/mon/ConfigMap.cc)의
  wire 의미를 독립 Go 코드로 작성했다. LGPL-2.1 또는 LGPL-3 선택 고지를
  확인했으며 C++ 코드를 복사하지 않았다. Native CLI와 설정 변경은 개발
  fixture에만 두며 제품 의존성이 아니다.

- MON 로그는 고정 [MLog](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/messages/MLog.h),
  [LogEntry v5](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/common/LogEntry.cc#L203),
  [EntityName](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/common/entity_name.h),
  [LogMonitor 구독 처리](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/mon/LogMonitor.cc#L1090)의
  wire 의미를 독립 Go 코드로 작성했다. 관련 header와 LogMonitor의 LGPL-2.1
  고지를 확인했으며 C++ 코드를 복사하지 않았다. 독립 raw wire 시험과 native
  CLI 로그 oracle은 개발 검증에만 사용한다.
- 이름 지정 MON Tell은 고정 [MonClient의 독립 연결 경로](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/mon/MonClient.cc#L1238)와
  [MMonGetMap](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/messages/MMonGetMap.h)의
  wire 의미를 독립 구현했다. LGPL-2.1 고지를 확인했으며 C++ 코드를 복사하지
  않았다. native client의 Tell 재전송을 제품 동작에 포함하지 않는다.
- AES256K 암호 검증은 [RFC 8009](https://www.rfc-editor.org/rfc/rfc8009.html)의
  공식 vector를 사용한다.
- Frame vector는 별도의 Python CRC/AES-GCM 구현으로 생성했다. 이는
  [규약 기반 vector](internal/msgr/testdata/generate.py)이며 Ceph traffic
  capture가 아니다.
- [실제 Ceph banner·HELLO capture](internal/msgr/testdata/README.md)는 인증 전
  CRC framing 회귀·fuzz 입력으로 사용한다. Secure reader도 독립 AES-GCM
  vector를 seed로 별도 fuzz 검사한다.
- [MON/MGR 지도 fixture](internal/maps/testdata/README.md)는 실제 Ceph가 생성했다.
  Fixture의 SHA-256과 생성 환경을 함께 기록했다.
- [OSD 요청·응답 fixture](internal/osd/testdata/README.md)는 고정 Tentacle의
  `ceph-dencoder`의 native decoder로 검증했다. Python이 field 규약에서 독립
  envelope를 구성하며 Go codec의 round trip으로 생성하지 않았다. Message
  재출력은 기존 payload를 보존하므로 native payload encoder oracle로 주장하지
  않는다. [Mutation decoder vector 5개](internal/osd/testdata/mutation-README.md)도
  같은 한계를 유지하며 9개 mutation/guard opcode의 union·input segmentation과
  success/EEXIST/extent mismatch/zero→truncate reply를 native decoder로 대조했다.
  LGPL-2.1 고지를 확인했고 C++ 코드는 포함하지 않았다. 실제 OSD의 위치·
  응답 의미는 별도 native client oracle과 실서버 통신으로 검증한다.
- [OSDMap envelope fixture](internal/maps/testdata/osd-message/README.md)는
  독립 구성한 v1/v4 입력을 고정 Tentacle의 native message decoder로 검증했다.
  실제 OSD가 보낸 내포 full/incremental blob은 별도 native class decoder와
  현재 fixture의 협상 feature encoder로 대조한다. 내포 지도 codec을 제품에
  구현했다는 의미가 아니다.
- Synthetic peer 테스트는 취소·오류·경합을 검증하는 용도이며 실제 Ceph
  상호운용 시험을 대신하지 않는다.
