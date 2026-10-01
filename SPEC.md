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
- MGR 발견에 필요한 MgrMap 구독 및 갱신.

MGR 기능:

- MgrMap의 active MGR 주소를 사용한 별도 연결과 service 인증.
- MGR 명령과 응답, 모듈 미지원·권한 오류의 구분.
- active MGR 변경 시 연결 갱신과 진행 중 요청의 결과 처리.

MON 명령과 MGR 명령은 명시적인 API로 구분한다. 명령 prefix만 보고 임의로 경로를 추측하지 않는다. 명령의 JSON, bulk 입력, binary 출력, 상태 문자열과 서버 오류 코드를 각각 보존한다.

참조: [CephX 보안 수정](https://docs.ceph.com/en/latest/security/CVE-2025-30156/), [CephX 개요](https://docs.ceph.com/en/tentacle/dev/cephx/), [librados 명령 입출력 계약](https://docs.ceph.com/en/tentacle/rados/api/librados/).

## Go API 설계 기준

공개 API는 `ParseKey`, `Dial`, `MonCommand`, `MgrCommand`, `Close`와 `Options`, `Command`, `Result`를 중심으로 한다. go-ceph 및 C API의 함수 이름·타입과 호환시키는 것을 목표로 하지 않는다.

- `Dial(ctx, options)`는 bootstrap과 초기 인증을 취소할 수 있어야 한다. Dial context의 종료가 성공적으로 생성된 client의 전체 수명을 자동으로 종료하지 않도록 한다.
- `MonCommand(ctx, command)`와 `MgrCommand(ctx, command)`는 요청별 취소와 deadline을 지원한다. 먼저 raw command API를 구현하고 필요한 typed API만 추가한다.
- 응답은 원본 data bytes와 상태 문자열을 보존한다. 모든 응답이 JSON이라고 가정하지 않는다.
- 서버 오류 코드와 Go의 context·네트워크·프로토콜 오류를 구분한다. 서버 코드는 호스트 OS의 errno 숫자로 재해석하지 않는다.
- client에서 동시 명령 호출을 허용한다. 요청 하나의 deadline을 공유 TCP 연결에 직접 적용하지 않는다.
- `MaxFrameSize`는 frame 논리 크기, `MaxInFlight`는 동시 명령 수를 제한한다. 대기 슬롯을 얻기 전에는 bulk 입력을 복사하지 않는다. 기본값은 각각 16 MiB와 64다.
- 취소 시 pending 요청과 자원을 정리한다. 늦은 응답을 안전하게 소비한다. 일부 전송한 frame을 방치해 연결 framing을 깨뜨리지 않는다.
- context 취소는 서버에서 명령 실행을 취소하거나 이미 적용된 변경을 되돌린다는 보장이 아니다.
- transport ACK는 관리 명령 완료 응답과 다르다. 연결 단절 후 변경 명령의 실행 결과가 불명확하면 이를 명시적으로 반환한다.
- Messenger가 보장하는 세션 재개와 애플리케이션 명령 재실행을 구분한다. 새 세션에서 결과가 불명확한 변경 명령을 자동으로 다시 실행하지 않는다.
- `Close`는 내부 작업과 연결을 종료하고 대기 중 호출을 해제해야 한다. 종료·취소·재연결의 경합을 검증한다.

## 구현 및 검증 순서

1. 고정 소스에서 codec, frame, feature, CephX 인증 흐름의 최소 집합을 추출한다. 암호 및 frame의 독립적인 test vector를 확보한다.
2. Go만으로 msgr2.1 secure 및 `aes256k`를 사용하는 MON 인증과 관리 조회 명령을 수행한다.
3. MgrMap을 받아 active MGR에 연결하고 실제 MGR 명령을 수행한다.
4. ticket 갱신, MON/MGR failover, 연결 단절과 session reset을 검증한다.
5. 동시 요청 중 개별 context 취소, 늦은 응답, 결과 불명확, Close 경합을 검증한다.
6. 제품의 전이 의존성을 포함해 CGO·네이티브 라이브러리·CLI 의존성이 없는지 확인하고 대상 OS에서 빌드·실행을 검증한다.

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
