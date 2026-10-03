# ceph-msgr-go

Ceph Tentacle의 MON/MGR와 msgr2.1로 통신하는 native Go 라이브러리다.
Messenger·CephX·세션·raw 명령 송수신과 wire 메시지·지도·구독을 구현한다.
제품은 Go 표준 라이브러리만 사용하며 CGO, go-ceph, librados, Ceph CLI를
요구하지 않는다. 최소 Ceph 계열은 Tentacle(20.2)이며 객체 I/O는 후속 업무다.

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

인증은 `secure`만 허용하며 자동 downgrade하지 않는다. 현재 Tentacle의
`aes256k`와 기존 `aes` 키를 지원한다. 압축 협상은 압축을 끄는 데 사용한다.
FSID는 `ExpectedFSID`가 있으면 해당 값, 없으면 첫 인증 MonMap으로 고정한다.
MonMap의 `min_mon_release >= 20`을 요구한다. 이 설정은 daemon별 정확한
패치 버전을 증명하지 않으며 이전 최소 계열을 유지하는 업그레이드 클러스터는
거부한다.

MON이 명령 전용 CLIENT에도 요구하는 CRUSH 세대 비트는 MON 접속에 한정해
광고한다. CRUSH 계산, OSDMap 구독, OSD 연결, RADOS/RBD/CephFS는 구현하지
않았다. 그 밖의 미지원 필수 기능은 명시적으로 거부한다.

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
native client의 최초 MGR 연결을 검증한다. 이 native 연결의 인증 거절을
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
Windows의 실제 Ceph 상대 실행은 아직 검증하지 않았다.

Windows 전용 CI는 `windows-2025`에서 CGO=0 Windows amd64 시험 실행 파일을
직접 실행한다. 개발용 Ceph 20.2.4·aes256k·secure·IPv4 fixture만 별도의 WSL2
배포판에 두고 기존 opaque TCP relay의 Windows loopback 경로로 접근한다.
이는 Windows의 직접 IPv6 접속이나 다른 인증 구성의 검증을 대신하지 않는다.
준비·인증 갱신·MON/MGR 전환, raw 명령·Tell·이름 지정 MON Tell, 동시성,
context·Close와 log/config/digest 수신을 포함한 공개 시험 9개와 내부 통신
시험 1개를 선택한다. 모든 선택 시험의 실제 pass와 package pass를 요구해
전체 skip 또는 잘못된 selector를 성공으로 처리하지 않는다.

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
sh integration/run.sh                         # Docker 필요, aes256k
CEPH_MSGR_TEST_KEY_TYPE=aes sh integration/run.sh
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

기본 Harness는 격리된 컨테이너 안에서 Ceph 클러스터와 CGO=0 Go 테스트 바이너리를
실행하고 종료 시 컨테이너·임시 키를 삭제한다. 기본 모드는 호스트 포트를
공개하지 않는다. `host` 모드는 현재 OS·CPU의 시험 바이너리를 실행하며
개발용 relay 하나를 `127.0.0.1`의 임시 포트에 공개한다.
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
MON/MGR 서버의 인증·접속 정책, 메시지와 명령 schema 등 82개 경로를
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
- Synthetic peer 테스트는 취소·오류·경합을 검증하는 용도이며 실제 Ceph
  상호운용 시험을 대신하지 않는다.
