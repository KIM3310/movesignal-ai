# MoveSignal AI — Codex Handoff Document
> Snowflake Hackathon Project: 서초·영등포·중구 렌탈/마케팅 배분 의사결정 엔진
> Last updated: 2026-04-07 KST

---

## 1. 프로젝트 현재 상태 (완료된 것)

### Snowflake 오브젝트 (MOVESIGNAL_AI.ANALYTICS)
| 오브젝트 | 타입 | 상태 | 설명 |
|---------|------|------|------|
| STG_POP | Table | ✅ | SPH 유동인구 (CITY_KOR_NAME 기준 구별 집계) |
| STG_CARD | Table | ✅ | SPH 카드소비 (8개 카테고리) |
| STG_ASSET | Table | ✅ | SPH 자산 |
| STG_PRICE | Table | ✅ | Richgo 부동산 시세 |
| STG_MOVE | Table | ✅ | Richgo 인구이동 (전입/전출/순이동) |
| FEATURE_MART_FINAL | Table | ✅ | 통합 Feature Mart (3구 × 60개월) |
| FORECAST_INPUT | Table | ✅ | ML 입력 (DS, DISTRICT, Y) |
| MOVESIGNAL_FORECAST | ML Model | ✅ | Snowflake ML FORECAST 모델 |
| FORECAST_RESULTS | Table | ✅ | 예측 결과 (3구 × 3개월, 따옴표 제거 완료) |
| ACTUAL_VS_FORECAST | Table | ✅ | 실제 vs 예측 비교 |
| RECOMMEND_ALLOCATION | Procedure | ✅ | 예산 배분 추천 |
| SIMULATE_WHATIF | Procedure | ✅ | What-if 시뮬레이션 |
| STREAMLIT_STAGE | Stage | ✅ | Streamlit 앱 파일 스테이지 |
| MOVESIGNAL_APP | Streamlit | ✅ | 배포 완료, Active 상태 |

### Streamlit 앱 현재 상태
- URL: `https://app.snowflake.com/ligiqmy/mm49381/#/streamlit-apps/MOVESIGNAL_AI.ANALYTICS.MOVESIGNAL_APP`
- **5개 탭**: Allocation | Analysis | AI Agent | Simulation | Ops/Trust
- 버전: **streamlit_app_v7.py** (1,142줄)
- 배분: 서초구 16% / 영등포구 17% / 중구 67% (따옴표 제거 완료)
- Actual vs Forecast 시계열 차트 정상
- Cortex LLM: mistral-large2 한국어 완벽 작동 확인
- Ablation Study A→E 차트 + 평가 지표 (MAPE/SMAPE/MAE)
- What-if 시뮬레이션 + AI 코멘트
- V_APP_HEALTH 운영 모니터링 패널

### 확인된 데이터 매핑
- SPH: `CITY_KOR_NAME` = 구 이름 (서초구/영등포구/중구), `DISTRICT_KOR_NAME` = 동 이름
- Richgo: `SGG` = 서초구/영등포구/중구, `REGION_LEVEL` = 'sgg' (소문자)
- Richgo: `MOVEMENT_TYPE` = '전입'/'전출'/'순이동' (한국어)
- AJD: `TELECOM_INSIGHTS` 스키마, 11개 뷰 (V01~V11), 시/군 단위

---

## 2. 남은 작업

### 2-1. 데모 영상 녹화 (10분, 한국어, MP4)
DEMO_SCRIPT.md 참조. 8개 세그먼트:
```
[0:00-0:40] 문제 정의
[0:40-1:30] 데이터 소스 설명
[1:30-3:00] Forecast 검증 (Ablation + 평가 지표)
[3:00-6:00] 라이브 데모 (5탭 순회)
[6:00-7:10] Snowflake 아키텍처
[7:10-8:10] 운영 가능성
[8:10-9:00] 민간 + 공공 듀얼 유스케이스
[9:00-10:00] 다음 단계 + 마무리
```

### 2-2. 제출본 업로드 (해커톤 플랫폼)

---

## 3. 핵심 기술 정보

### Snowflake 계정
- Account: `ligiqmy-mm49381`
- Region: ap-northeast-2 (서울)
- User: DOEON_KIM (ACCOUNTADMIN)
- Warehouse: COMPUTE_WH (X-Small)
- Credits: $374 remaining ($400 trial)

### 데이터 소스
| 소스 | DB.SCHEMA | 설명 |
|------|-----------|------|
| SPH | SEOUL_DISTRICTLEVEL_DATA_FLOATING...GRANDATA | 유동인구, 카드소비, 자산 (동 단위) |
| Richgo | KOREA_REAL_ESTATE_APARTMENT_MAR...RICHGO | 부동산 시세, 인구이동 (구 단위) |
| AJD | SOUTH_KOREA_TELECOM_SUBSCRIPTIO...TELECOM_INSIGHTS | 통신 가입 (시/군 단위) |

### Cortex LLM
- 모델: `mistral-large2` (한국어 완벽 지원)
- 응답 시간: ~6초
- 호출 방법: `SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large2', '질문')`

### 주요 디버깅 이력
1. SPH `DISTRICT_KOR_NAME` → `CITY_KOR_NAME` (동 vs 구)
2. MOVEMENT_TYPE: 'IN'/'OUT' → '전입'/'전출'
3. REGION_LEVEL: 'SGG' → 'sgg' (소문자)
4. ML FORECAST: exogenous NULL → Y만 모델
5. f-string SyntaxError → 변수 추출
6. FORECAST_RESULTS SERIES 컬럼: VARIANT 타입 + 따옴표 → CTAS로 VARCHAR 변환 + 따옴표 제거

---

## 4. 로컬 파일 구조
```
/Users/dolphin/Downloads/Claude/movesignal-ai/
├── README.md                   # 프로젝트 설명 (배지, Prerequisites, Troubleshooting)
├── CODEX_HANDOFF.md            # 이 문서
├── DEMO_SCRIPT.md              # 10분 데모 스크립트
├── SUBMISSION_CHECKLIST.md     # 제출 체크리스트
├── LICENSE                     # MIT License
│
├── 02_feature_mart_v4.sql      # Feature Mart SQL
├── 03_ml_and_cortex_v2.sql     # ML Forecast + Cortex LLM
├── 06_semantic_view.sql        # Semantic View (Cortex Analyst)
├── 07_dynamic_tables_tasks.sql # Dynamic Tables + Tasks + V_APP_HEALTH
├── 08_ajd_integration.sql      # AJD 통신 데이터 통합
├── 09_cortex_search_agent.sql  # Cortex Search + Agent
├── 10_external_data.sql        # 4개 외부 데이터 + FEATURE_MART_V2
├── 11_ablation_study.sql       # Ablation A→E (5 모델 비교)
│
├── streamlit_app_v7.py         # 최종 앱 (5탭, 1,142줄, 경쟁용)
├── snowflake.yml               # Snow CLI 배포 설정
│
├── MoveSignal_AI_Hackathon.pptx # 발표 PPT (15슬라이드)
├── MoveSignal_AI_Submission.zip # 제출용 ZIP
│
├── 04_databricks_integration.py    # (Portfolio) Databricks pipeline
├── 05_databricks_sql_analytics.sql # (Portfolio) Databricks SQL
├── 06_palantir_foundry_integration.py # (Portfolio) Palantir Foundry
├── databricks_notebook.py          # (Portfolio) Databricks notebook
└── generate_pptx.py                # PPT 자동 생성 스크립트
```

---

## 5. 중구 67% 배분에 대한 해커톤 설명 전략
중구(명동/을지로)는 실제 카드 소비 규모가 서초/영등포의 4~5배.
→ "데이터 기반 합리적 결과"로 설명
→ "What-if 시뮬레이션으로 배분 비율 조절 가능"
→ "AI 에이전트에게 '영등포구에 예산을 늘려야 할까?' 질문 시연"
