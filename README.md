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
`github.com/jsyoo5b/ceph-msgr-go`이며 패키지 이름은 `cephmsgr`다.
배포된 module tag는 아직 없다.

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

## 수명과 복구

- 동시 호출을 지원한다. `MaxInFlight` 기본값은 64이며 슬롯 대기도 호출
  context에 따른다. `MaxFrameSize` 기본값은 논리 frame당 16 MiB다.
- `ConnectTimeout` 기본값은 endpoint별 10초다. 요청 deadline은 공유
  연결에 적용하지 않는다. `Close`는 연결과 내부 worker를 종료하고 기다린다.
- MON 단절 시 seed 및 MonMap 주소로 다시 인증한다. 이후 요청은 복구를
  기다린다. Ticket 수명의 약 75%에서 새 MON 연결로 ticket을 갱신하고,
  이전 MON의 진행 중 요청은 `ConnectTimeout` 동안 완료할 기회를 준다.
- MgrMap이 바뀌면 이전 MGR 연결을 종료하며 다음 호출은 새 active MGR로
  연결한다. 전송 중이던 요청은 `ErrManagerChanged`를 원인으로 보존한다.
- 이미 전송을 시작한 호출의 취소·단절은 `*OutcomeUnknownError`가 될 수 있다.
  `errors.As`로 이를 확인한다. `errors.Is`로 원인 context 오류도 확인할 수
  있다. 취소가 서버 실행의 취소나 rollback을 의미하지 않는다.
- 결과가 불명확한 명령은 자동 재실행하지 않는다. ACK도 명령 완료가 아니다.
  응답 메시지 종류나 본문이 손상된 경우에도 전송한 요청의 결과는 불명확하며
  해당 세션을 종료한다. 본문 해석 실패 시 수신한 raw data는 보존한다.
  복구는 fresh lossy session을 사용하며 Messenger cookie에 의한 기존
  session 재개는 구현하지 않는다.

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

2026-10-01에 다음 구성을 실제 Ceph daemon과 검증했다.

| Ceph | 인증 키 / rotating service cipher | 주소 | 결과 |
| --- | --- | --- | --- |
| 20.2.4 | aes256k / aes256k | IPv4 | MON/MGR 명령, 동시 호출, ticket 갱신, MON 장애, MGR 전환 통과 |
| 20.2.4 | aes / aes | IPv4 | 동일 시험 통과 |
| 20.2.4 | aes256k / aes256k | IPv6 | 동일 시험 통과 |
| 20.2.4 | aes / aes256k | IPv4 | 동일 시험 통과, MGR session key는 aes |
| 20.2.4 | aes256k / aes | IPv4 | 동일 시험 통과, MGR session key는 aes |

모두 secure 모드이며 클라이언트는 Linux arm64, CGO=0으로 실행했다.
혼합 구성에서 MGR session key는 [Tentacle KeyServer](https://github.com/ceph/ceph/blob/7f793731f1b39eb4f465e960113d2363c311b964/src/auth/cephx/CephxKeyServer.cc#L591)가
클라이언트 키와 service secret 중 낮은 타입으로 선택한다. 해당 키 타입도
시험에서 확인했다.

시험 구성은 3 MON·2 MGR, OSD 없음, 12초 ticket TTL이다. `status`와
`pg stat` 응답을 검증했고 MON 프로세스 종료 및 active MGR fail을 주입했다.
잘못된 인증 키, 읽기 전용 계정의 쓰기 거절, 미지원 명령의 코드·상태 문자열,
`pg getmap` binary 출력과 40 KiB binary bulk 입력도 실제 서버로 확인했다.
IPv4·aes256k 구성의 3분 부하 시험에서는 동시 요청 62,362건, ticket 갱신
17회, MGR fail 5회를 수행했고 세션 수와 `Close` 후 worker 정리를 확인했다.
실제 시험 클라이언트 바이너리는 Ceph 실행 파일이나 라이브러리를 호출하지
않는다. 장시간 운영, 다양한 모듈·CRUSH 설정, 20.2.0–20.2.3 및 이후
Ceph 패치는 아직 검증하지 않았다.

Go 1.24.0과 1.27.1에서 전체 unit 검사를 통과했다. Darwin arm64에서
race/vet 검사와 parser fuzzing도 통과했다.
Windows amd64, Darwin amd64, Linux 386에서 CGO=0 빌드를 확인했다.
이 결과는 각 OS에서 실제 Ceph 상대 실행을 검증했다는 의미가 아니다.

```sh
go test -race ./...
go vet ./...
sh integration/run.sh                         # Docker 필요, aes256k
CEPH_MSGR_TEST_KEY_TYPE=aes sh integration/run.sh
CEPH_MSGR_TEST_IP_FAMILY=6 sh integration/run.sh
CEPH_MSGR_TEST_KEY_TYPE=aes CEPH_MSGR_TEST_SERVICE_CIPHER=aes256k sh integration/run.sh
CEPH_MSGR_STRESS_DURATION=3m sh integration/run.sh
```

부하 시험 시간은 45초 이상으로 설정한다. 더 긴 시험에는
`CEPH_MSGR_TEST_TIMEOUT`도 늘린다. 기본 timeout은 10분이다.

Harness는 격리된 컨테이너 안에서 Ceph 클러스터와 CGO=0 Go 테스트 바이너리를
실행하고 종료 시 컨테이너·임시 키를 삭제한다. 호스트 포트를 공개하지 않는다.
Ceph CLI는 이 개발 fixture의 초기화에만 사용한다.

기존 클러스터에 읽기 명령만 시험하려면 `CEPH_MSGR_MONITORS`(쉼표 구분),
`CEPH_MSGR_IDENTITY`, `CEPH_MSGR_KEY_FILE`(base64 key 값이 든 파일), 선택적으로
`CEPH_MSGR_FSID`를 설정하고 `go test -run '^TestCephIntegration$' -v .`를
실행한다. 장애 주입 시험은 격리 harness의 control directory가 있을 때만
실행한다.

[CI 설정](.github/workflows/ci.yml)은 Linux·macOS·Windows에서 Go 1.24.0과
1.27 계열의 CGO=0 unit/vet 검사, Linux에서 race 검사와 위 5개 Ceph 구성을
시험하도록 구성했다. Actions 설정 lint는 통과했으며 GitHub에서의 실행
결과는 아직 없다.

[Ceph 변경 비교 도구](tools/ceph_diff.py)는 Python 표준 라이브러리로
upstream ref를 commit SHA로 고정한 후 Messenger, CephX, 지도·복구,
메시지와 명령 schema 파일을 비교한다. 소스는 메모리에서만 읽고 결과를
stdout으로 출력한다. 파일 변화가 wire 변경이나 호환성 판정을 의미하지는
않으며 지정된 경로 밖의 변경은 검사하지 않는다.

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
- [MonMap fixture](internal/maps/testdata/README.md)는 실제 Ceph가 생성했다.
  Fixture의 SHA-256과 생성 환경을 함께 기록했다.
- Synthetic peer 테스트는 취소·오류·경합을 검증하는 용도이며 실제 Ceph
  상호운용 시험을 대신하지 않는다.
