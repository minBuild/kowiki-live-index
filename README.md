# kowiki-live-index

한국어 위키백과를 실시간으로 색인하는 RAG 데이터 파이프라인.
덤프 백필과 Wikimedia 편집 스트림을 같은 Kafka 파이프라인으로 흘려서 Qdrant 인덱스를 최신 상태로 유지한다.

## 구성

```
[수집]   덤프 백필(backfill) + 실시간 편집 스트림(live) + API 샘플(seed)
           │  원본 HTML은 S3에 두고 Kafka에는 참조만 보낸다 (claim-check)
           ▼
[Kafka]  doc.events → parse → doc.parsed → chunk → doc.chunks
           → embed → chunk.embeddings → index → Qdrant → search-api
           ↳ 처리 실패는 <topic>.dlq로, 원인을 고친 뒤 kowiki replay로 되돌린다
```

| 단계 | 입력 → 출력 | 하는 일 |
|---|---|---|
| live | `mediawiki.page_change.v1` → doc.events | 한국어 위키 본문 이벤트만 골라 해당 판의 HTML을 받아 S3에 올린다. 마지막 이벤트 ID를 체크포인트로 남긴다 |
| backfill | Enterprise HTML 덤프 → doc.events | 8GB짜리 tar.gz를 디스크에 풀지 않고 스트리밍으로 읽는다 |
| parse | doc.events → doc.parsed | Parsoid HTML을 제목 경로가 붙은 문단 목록으로 바꾼다. 각주, 둘러보기 틀, 사이드바는 버린다 |
| chunk | doc.parsed → doc.chunks | 섹션 경계를 넘지 않게 문단을 묶는다. 청크 ID는 내용 해시 |
| embed | doc.chunks → chunk.embeddings | 이미 색인된 청크는 벡터를 재사용하고 새 청크만 임베딩한다 |
| index | chunk.embeddings → Qdrant | 버전을 비교해 반영하고 사라진 청크를 지운다 |

기술 스택: Python 3.12, confluent-kafka, Kafka 4.3 (KRaft), Qdrant, Ollama 또는 TEI(bge-m3), SeaweedFS(S3), FastAPI, Strimzi, KEDA, Helm

## 로컬 실행 (docker compose)

Docker와 Compose v2, 메모리 12GB 이상이 필요하다. colima라면 `colima start --cpu 8 --memory 16`으로 띄우고, compose·buildx 플러그인은 `brew install docker-compose docker-buildx`로 따로 설치한다.

```bash
make up                         # 처음에는 bge-m3(1.2GB)를 받느라 몇 분 걸린다
make seed-top N=30              # 지난달 조회수 상위 30개 문서
make search Q="대한민국의 수도"
make logs
```

실시간 수집(live)은 `make up`과 함께 바로 돈다. 덤프 백필은 이렇게 한다.

```bash
make backfill LIMIT=10000       # 덤프 앞에서부터 1만 건
```

Kafka UI가 필요하면 `docker compose --profile tools up -d kafka-ui` 후 http://localhost:8080.

## 개발

```bash
uv sync
make test    # 단위 테스트
make e2e     # compose의 Kafka·Qdrant에 붙는 통합 테스트
make lint
```

## 쿠버네티스 (kind)

```bash
make kind-up          # control-plane 1 + worker 3
make k8s-deps         # Strimzi(Kafka 3대, KRaft), KEDA, Qdrant
make k8s-deploy       # 이미지 빌드 → kind에 로드 → Helm 배포
curl -G localhost:30080/search --data-urlencode "q=대한민국의 수도"
make k8s-monitoring   # kube-prometheus-stack + PodMonitor
```

- 워커(parse/chunk/embed/index)는 KEDA ScaledObject가 컨슈머 lag을 보고 늘리고 줄인다. 최대 개수는 파티션 수(12)를 넘지 않게 잡았다.
- 토픽은 Strimzi `KafkaTopic`으로 관리하고(`deploy/kafka/topics.yaml`), 앱의 bootstrap Job은 버킷과 컬렉션만 만든다.
- amd64 노드라면 `-f deploy/helm/kowiki/values-tei.yaml`로 임베딩 서버를 TEI로 바꿀 수 있다.

## 설계

- **키와 순서**: 모든 토픽의 키는 `doc_id`(`kowiki:<page_id>`)라서 한 문서의 이벤트는 단계마다 같은 파티션에서 순서대로 처리된다. 삭제도 같은 토픽을 타고 내려가므로 앞서 들어온 수정보다 먼저 반영되지 않는다.
- **claim-check**: 원본 HTML은 큰 문서가 1MB를 넘는다. S3에 두고 참조만 보낸다. 중간 단계 메시지도 512KB를 넘으면 같은 방식으로 보낸다.
- **멱등성과 버전 비교**: at-least-once로 처리한다. 인덱서가 문서별 `(rev_id, event_ts)`를 레지스트리 컬렉션에 남기고, 같거나 오래된 버전은 건너뛴다. 예전 덤프로 백필하면서 실시간 스트림을 같이 돌려도 최신 판이 덮어써지지 않고, 삭제된 문서가 되살아나지도 않는다.
- **청크 ID와 증분 임베딩**: 청크 ID는 `doc_id`와 임베딩 입력의 해시다. 이미 색인된 청크는 Qdrant에서 벡터를 가져다 쓴다. 섹션 단위로 자르기 때문에 한 섹션을 고쳐도 다른 섹션의 청크는 그대로다(고정 길이로 자르면 앞부분만 고쳐도 전부 바뀐다).
- **실패 처리**: 임베딩 서버·Qdrant·S3 연결 오류나 5xx는 파티션을 멈춘 채 백오프로 재시도한다. 그동안에도 poll을 계속해서 컨슈머 그룹에서 빠지지 않는다. 메시지 자체가 문제면 배치를 한 건씩 다시 돌려 원인 메시지만 DLQ로 보낸다.
- **재색인**: 파서나 모델을 바꾸면 `KOWIKI_COLLECTION_VERSION`을 올려 새 컬렉션을 채우고, 확인한 뒤 `kowiki switch-alias <version>`으로 검색을 넘긴다.

결정 배경은 [docs/adr](docs/adr)에 정리했다.

## 지표

| 이름 | 설명 |
|---|---|
| `kowiki_freshness_seconds{source}` | 원본이 바뀐 시각부터 인덱스에 반영될 때까지. live의 p95를 SLO로 본다 |
| `kowiki_messages_total{stage,result}` | 단계별 처리 건수 (ok, dlq) |
| `kowiki_chunks_total{result}` | 새로 임베딩한 청크와 재사용한 청크 |
| `kowiki_index_ops_total{result}` | upsert, delete, stale |
| `kowiki_transient_retries_total{stage}` | 일시 오류 재시도 횟수 |
| `kowiki_search_seconds` | 검색 지연 |

워커는 `:9100/metrics`, `:9100/healthz`(루프가 멈추면 503), 검색 API는 `:8000/metrics/`로 노출한다.

## 측정 기록

| 항목 | 환경 | 값 |
|---|---|---|
| 백필 처리량 (docs/s) | | |
| 실시간 반영 지연 p95 | | |
| 검색 지연 p95 @ RPS | | |
| 재임베딩 절감률 | | |

## 로드맵

- [x] 수집(덤프·실시간) → 파싱 → 청킹 → 임베딩 → 색인 → 검색
- [x] claim-check, DLQ와 replay, 버전 비교, 증분 임베딩
- [x] docker compose, kind + Strimzi + KEDA + Helm
- [ ] Grafana 대시보드와 알람 규칙 (lag, DLQ 증가, freshness)
- [ ] OpenTelemetry로 문서 한 건의 단계별 트레이스
- [ ] 장애 재현과 포스트모템 ([템플릿](docs/postmortems/TEMPLATE.md)): 브로커 kill, 리밸런스 폭풍, 대용량 문서 OOM, 롤링 배포
- [ ] KorQuAD로 청킹 전략과 임베딩 모델의 Recall@k 비교
- [ ] 하이브리드 검색(dense + sparse), 양자화
- [ ] 인덱스 변경 로그 토픽(compact)으로 재색인을 리플레이만으로 끝내기

## 데이터

본문은 위키백과(CC BY-SA)에서 가져온다. Enterprise HTML 덤프는 2025년 3월 이후 갱신되지 않아서, 백필한 뒤 그 사이에 바뀐 문서는 다시 편집될 때 실시간 스트림으로 따라잡는다.
요청에는 프로젝트 주소를 담은 User-Agent를 붙인다.

## 라이선스

MIT
