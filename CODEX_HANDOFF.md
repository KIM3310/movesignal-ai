# MoveSignal AI — Codex Handoff Document
> Snowflake Hackathon Project: 서초·영등포·중구 렌탈/마케팅 배분 의사결정 엔진
> Last updated: 2026-04-05 15:00 KST

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
- 3개 탭: Allocation | Analysis | AI
- 배분: 서초구 16% / 영등포구 17% / 중구 67% (따옴표 제거 완료)
- Actual vs Forecast 시계열 차트 정상
- Cortex LLM: mistral-large2 한국어 완벽 작동 확인

### 확인된 데이터 매핑
- SPH: `CITY_KOR_NAME` = 구 이름 (서초구/영등포구/중구), `DISTRICT_KOR_NAME` = 동 이름
- Richgo: `SGG` = 서초구/영등포구/중구, `REGION_LEVEL` = 'sgg' (소문자)
- Richgo: `MOVEMENT_TYPE` = '전입'/'전출'/'순이동' (한국어)
- AJD: `TELECOM_INSIGHTS` 스키마, 11개 뷰 (V01~V11), 시/군 단위

---

## 2. 남은 작업

### 2-1. Streamlit 앱 한국어 풀버전 업그레이드 (선택)
현재 스테이지에 올라간 코드는 영문 미니멀 버전 (UPLOAD_APP2 프로시저로 생성).
로컬에 `streamlit_app_v3.py` (337줄) 한국어 풀버전이 준비되어 있음.

**풀버전 기능:**
- 사이드바: 예산 입력, 메뉴 라디오 버튼
- 4개 페이지: 배분 추천 / 지역 분석 / 시뮬레이션 / AI 에이전트
- 소비 체질 분석 (카테고리별 비율 바 차트)
- 자산 대비 시세 비율 분석
- AI 에이전트: 빠른 질문 버튼 + 자유 입력 + 대화 이력

**업그레이드 방법:**
Snowflake Streamlit 에디터에서 직접 코드를 교체하거나,
UPLOAD_APP 프로시저를 수정하여 스테이지에 업로드.

> ⚠️ 주의: Snowflake Streamlit 에디터는 자동 들여쓰기가 Python 코드를 깨뜨림.
> 해결법: Python 프로시저(UPLOAD_APP)로 `session.file.put_stream()`을 사용하여 스테이지에 업로드.
> 또는 Snowflake SQL에서 `$$` 대신 작은따옴표 + `\n` 줄바꿈으로 한 줄 형식 사용.

### 2-2. ACTUAL_VS_FORECAST 테이블 DISTRICT 따옴표 제거
FORECAST_RESULTS는 수정 완료. ACTUAL_VS_FORECAST는 아직 남아있을 수 있음.
```sql
UPDATE MOVESIGNAL_AI.ANALYTICS.ACTUAL_VS_FORECAST
SET DISTRICT = REPLACE(DISTRICT, '"', '');
```
DISTRICT 컬럼이 VARIANT 타입이면:
```sql
CREATE OR REPLACE TABLE MOVESIGNAL_AI.ANALYTICS.ACTUAL_VS_FORECAST AS
SELECT REPLACE(DISTRICT::VARCHAR, '"', '') AS DISTRICT, DS, ACTUAL, FORECAST_VAL
FROM MOVESIGNAL_AI.ANALYTICS.ACTUAL_VS_FORECAST;
```

### 2-3. 10분 데모 시나리오 준비
```
[0:00-1:00] 프로젝트 소개: MoveSignal AI란?
[1:00-3:00] 데이터 파이프라인: SPH + Richgo + AJD → Feature Mart
[3:00-5:00] ML Forecast: Snowflake ML FORECAST → 3개월 예측
[5:00-7:00] Streamlit 앱 라이브 데모: 배분 추천 / 지역 분석
[7:00-9:00] Cortex AI 에이전트: 한국어 Q&A 라이브
[9:00-10:00] 운영 비용 / 확장 계획 / Q&A
```

### 2-4. PPT 제작 (10~15슬라이드)
1. 표지: MoveSignal AI
2. 문제 정의: 렌탈/마케팅 예산 배분의 비효율
3. 해결 방안: 데이터 기반 의사결정 엔진
4. 아키텍처: SPH + Richgo + AJD → Feature Mart → ML → Streamlit
5. Feature Mart 설계: 5개 STG 테이블 → 통합
6. ML Forecast: 모델 학습 및 예측 결과
7. Streamlit 앱 스크린샷: 배분 추천
8. Streamlit 앱 스크린샷: 지역 분석
9. Cortex AI 에이전트 데모
10. 비용 분석: ~$80/월 (Compute WH + Cortex LLM)
11. 확장 계획: 전국 확대, 실시간 데이터
12. 기술 스택 요약
13. Q&A

### 2-5. 데모 영상 녹화 (선택)
Streamlit 앱 라이브 데모 + Cortex AI 질문 시연

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
├── SESSION_HANDOFF.md          # 최초 핸드오프 문서
├── CODEX_HANDOFF.md            # 이 문서
├── 02_feature_mart_v4.sql      # Feature Mart SQL (최종)
├── 03_ml_and_cortex_v2.sql     # ML Forecast SQL (최종)
├── 04_fix_streamlit.sql        # Streamlit 배포 SQL
├── 05_final_fix.sql            # 따옴표 수정 프로시저
├── streamlit_app_v3.py         # 풀버전 Streamlit (337줄, 한국어)
└── deploy_streamlit.py         # Python 배포 스크립트 (SSO 필요)
```

---

## 5. 중구 67% 배분에 대한 해커톤 설명 전략
중구(명동/을지로)는 실제 카드 소비 규모가 서초/영등포의 4~5배.
→ "데이터 기반 합리적 결과"로 설명
→ "What-if 시뮬레이션으로 배분 비율 조절 가능"
→ "AI 에이전트에게 '영등포구에 예산을 늘려야 할까?' 질문 시연"
