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

| Ceph | 키와 모드 | 클라이언트 실행 | 결과 |
| --- | --- | --- | --- |
| 20.2.4 | aes256k / secure | Linux arm64, CGO=0 | MON/MGR 명령, 동시 호출, ticket 갱신, MON 장애, MGR 전환 통과 |
| 20.2.4 | aes / secure | Linux arm64, CGO=0 | 동일 시험 통과 |

시험 구성은 3 MON·2 MGR, OSD 없음, 12초 ticket TTL이다. `status`와
`pg stat` 응답을 검증했고 MON 프로세스 종료 및 active MGR fail을 주입했다.
실제 시험 클라이언트 바이너리는 Ceph 실행 파일이나 라이브러리를 호출하지
않는다. 장시간 운영, 다양한 명령·모듈·CRUSH 설정, 20.2.0–20.2.3 및 이후
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
```

Harness는 격리된 컨테이너 안에서 Ceph 클러스터와 CGO=0 Go 테스트 바이너리를
실행하고 종료 시 컨테이너·임시 키를 삭제한다. 호스트 포트를 공개하지 않는다.
Ceph CLI는 이 개발 fixture의 초기화에만 사용한다.

기존 클러스터에 읽기 명령만 시험하려면 `CEPH_MSGR_MONITORS`(쉼표 구분),
`CEPH_MSGR_IDENTITY`, `CEPH_MSGR_KEY_FILE`(base64 key 값이 든 파일), 선택적으로
`CEPH_MSGR_FSID`를 설정하고 `go test -run '^TestCephIntegration$' -v .`를
실행한다. 장애 주입 시험은 격리 harness의 control directory가 있을 때만
실행한다.

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
