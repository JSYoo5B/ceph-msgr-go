# ceph-msgr-go

Ceph MON/MGR 관리 명령을 msgr2.1로 직접 호출하는 native Go 라이브러리다.
제품은 Go 표준 라이브러리만 사용하며 CGO, go-ceph, librados, Ceph CLI를
요구하지 않는다. 최소 Ceph 계열은 Tentacle(20.2)이며 객체 I/O는 후속 업무다.

고정 wire 참조는 Ceph `v20.2.4`, 커밋
`7f793731f1b39eb4f465e960113d2363c311b964`다.
프로젝트 정책과 변경 빈도 조사는 [SPEC.md](SPEC.md)에 기록했다.
공개 API 변경 시 이전 API를 위한 호환 wrapper를 추가하지 않는다.

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
result, err := client.MonCommand(ctx, cephmsgr.Command{
    JSON: json.RawMessage(`{"prefix":"status","format":"json"}`),
})
if err != nil {
    return err
}
fmt.Printf("%s\n", result.Data)
```

`MgrCommand`는 active MGR에 별도로 인증해 명령을 보낸다. 예를 들어
`{"prefix":"pg stat","format":"json"}`을 사용할 수 있다. MON/MGR 경로는
호출자가 선택한다. 각 명령은 해당 identity의 Ceph 권한에 따라 처리된다.

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

## 운영 상태

`Snapshot()`은 네트워크 요청이나 재접속 대기 없이 현재 client 상태를 읽는다.
FSID와 client global ID, MON/MGR 지도 epoch·접속 주소, active MGR,
ticket 만료·갱신 예정 시각, 명시적인 MON 인증 거절을 확인할 수 있다.
키와 ticket의 암호 내용은 포함하지 않는다.

```go
state := client.Snapshot()
fmt.Printf("MON ready=%t MGR available=%t ready=%t\n",
    state.Monitor.Ready, state.Manager.Available, state.Manager.Ready)
if state.AuthRejection != nil {
    fmt.Printf("authentication method=%d code=%d\n",
        state.AuthRejection.Method, state.AuthRejection.Code)
}
```

`Manager.Available`은 마지막으로 수신한 MgrMap 값이다. MGR 접속은
`MgrCommand` 또는 `WaitMgrReady`가 필요할 때 시작하므로 available이어도
ready는 아닐 수 있다.
MON 복구를 기다리거나 인증 갱신이 명시적으로 거절되면 MGR ready도 false다.
Ready는 현재 알려진 명령 접수 조건이며 이후 명령 성공을 보장하지 않는다.

`Close` 후에는 `Closed=true`, MON/MGR ready=false를 반환하고 마지막으로
알아낸 지도·identity 정보는 남는다. 반환한 주소 slice와 인증 거절 객체를
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
- `ConnectTimeout` 기본값은 endpoint별 10초다. 요청 deadline은 공유
  연결에 적용하지 않는다. `Close`는 연결과 내부 worker를 종료하고 기다린다.
  MGR 핸드셰이크 중인 연결의 정리도 완료한 뒤 반환한다.
  종료 후 호출은 입력 검증·복사 전에 `ErrClosed`를 반환한다. 호출 context가
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
  기다린다. Ticket 수명의 약 75%에서 새 MON 연결로 ticket을 갱신하고,
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
  context 안에서 대기한다. 아직 명령을 전송하지 않은 이 대기의 취소는
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

2026-10-01–02에 다음 구성을 실제 Ceph daemon과 검증했다.

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

같은 context 동작을 MGR 최초 연결, ticket 갱신, MON 무응답 복구,
MGR 전환과 대기 취소·종료 중에도 검증했다. 복구 시험은 결과 불명확
오류에 포함된 원인을 모두 검사하며, 인증·프로토콜 오류가 일시적인
네트워크 오류로 취급되지 않는 것도 회귀 시험으로 확인했다.
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
새 CLI process로 MGR에 교체된 service key가 전달됐는지 먼저 확인한 뒤,
native client의 최초 MGR 연결을 검증한다. 이 native 연결의 인증 거절을
재시도하지 않는다. Linux arm64·CGO=0의 aes256k·IPv6와 aes·IPv4에서 통과했다.
이전 제품 소스 `23148f5`는 같은 시험에서 조기 갱신을 하지 못해 실패했다.
이 두 차례 교체와 큰 MON 응답은 Darwin arm64·IPv4·aes256k의 실제 host
바이너리에서도 race 계측으로 통과했다. 계측은 개발 시험에만 사용한다.

큰 응답 시험에서는 `MaxFrameSize=32 MiB`로 유효한 status JSON 뒤에
16 MiB 공백을 붙였다. 실제 MON이 원래 명령을 응답 front에 그대로 포함했고,
서버 코드·raw JSON을 보존한 뒤 같은 연결의 다음 명령도 성공했다.
응답 본문 decoder에도 설정한 상한을 전달하며, 기본 상한은 16 MiB로 유지한다.
양의 소수 초 ticket 유효기간은 encrypted codec 단위 시험으로 확인했다.
소수 초 단위 ticket 갱신의 실제 동작을 검증한 것은 아니다.

이 변경을 포함한 `7b7c307`은 같은 직접 IPv6·aes256k 구성에서 전체 통합시험과
3분 부하 시험을 통과했다. 성공 호출 62,700건, ticket 갱신 18회, 서버가 성공
응답한 MGR 장애 주입 5회와 context 상태 조회 281,761회를 처리했다.
결과 불명확 2건은 MGR 교체 원인을 유지했다. 관찰한 최대 세션은 2개,
goroutine은 19개였다. 반복 종료 12회에서 파일 descriptor는 6개로 돌아왔고,
GC 후 heap은 344,424 bytes에서 279,192 bytes였다.

MON 재접속의 IPv6 scope·flow와 동일 endpoint의 서로 다른 식별 후보는
인증·지도·명령 응답까지 수행하는 synthetic peer 시험 4개로 확인했다.
실제 link-local 네트워크 시험은 아니다. 같은 제품 소스 `3cca408`은
20.2.4·aes256k의 Linux arm64·직접 IPv6 전체 통합시험 28개를 통과했다.

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

```sh
go test -race ./...
go vet ./...
sh integration/run.sh                         # Docker 필요, aes256k
CEPH_MSGR_TEST_KEY_TYPE=aes sh integration/run.sh
CEPH_MSGR_TEST_IP_FAMILY=6 sh integration/run.sh
CEPH_MSGR_TEST_KEY_TYPE=aes CEPH_MSGR_TEST_SERVICE_CIPHER=aes256k sh integration/run.sh
CEPH_MSGR_STRESS_DURATION=3m sh integration/run.sh
CEPH_MSGR_TEST_MGR_COUNT=0 sh integration/run.sh # MGR 지연 기동
CEPH_MSGR_TEST_RUNTIME=host sh integration/run.sh # 호스트 native 바이너리
CEPH_MSGR_TEST_RUNTIME=host CEPH_MSGR_TEST_RACE=1 sh integration/run.sh # 실제 Ceph 상대 race 검사
CEPH_MSGR_TEST_EXPIRE_TICKETS=1 sh integration/run.sh # 별도 인증 만료 fixture
CEPH_MSGR_TEST_IDLE_SESSIONS=1 sh integration/run.sh # 별도 120초 ticket·idle 시험
CEPH_MSGR_TEST_AUTH_EPOCH=1 sh integration/run.sh # 별도 service-key 교체·조기 갱신 시험
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
1.27 계열의 CGO=0 unit/vet 검사, Linux에서 race 검사와 위 6개 Ceph 구성을
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
MON/MGR 서버의 인증·접속 정책, 메시지와 명령 schema 등 52개 경로를
비교한다. 시험에서 사용하는 balancer·crash·iostat 모듈도 포함한다.
소스는 메모리에서만 읽고 결과를
stdout으로 출력한다. 파일 변화가 wire 변경이나 호환성 판정을 의미하지는
않으며 지정된 경로 밖의 변경은 검사하지 않는다.

2026-10-02에 `tentacle`을 커밋
`7411a08041185df39dbb166f983a6b0be7af0811`로 고정해 참조와 비교했다.
52개 경로에서 7개 파일이 달랐으며 자동 조회·비교는 7.95초였다. 이 시간은
수동 diff 검토와 실서버 시험 시간을 포함하지 않는다. 검사한 경로의
frame·CephX·메시지 인코딩은 그대로였고, 연결 재사용 판단·서버 내부 처리와
일부 MON 명령 schema가 바뀌었다. Go client의 fresh lossy 연결과 raw 명령
API를 변경할 필요는 발견하지 못했다. 이는 소스 검토 결과이며 해당 가변
브랜치를 빌드한 실서버의 호환성 검증은 아니다.
도구 시험은 추가·삭제·변경 분류, commit SHA 고정, 응답 크기 제한,
HTTP 오류 구분과 오류 응답 자원 정리를 검사한다.

```sh
python3 tools/ceph_diff.py tentacle           # 고정 v20.2.4 참조와 비교
python3 tools/ceph_diff.py v20.2.4 --base v20.2.3 --json
```

## 소스 및 fixture 출처

규약은 [Tentacle Messenger 문서](https://docs.ceph.com/en/tentacle/dev/msgr2/)와
고정 Ceph 소스로 확인했다. Ceph 소스에는 LGPL-2.1 등의 개별 라이선스가
명시되어 있으며 해당 소스를 제품에 vendor하거나 링크하지 않았다.

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
