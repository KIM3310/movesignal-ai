# DistrictPilot AI

[![Snowflake](https://img.shields.io/badge/Snowflake-Native_App-29B5E8?logo=snowflake&logoColor=white)](https://www.snowflake.com/)
[![Python](https://img.shields.io/badge/Python-3.8+-3776AB?logo=python&logoColor=white)](https://python.org)
[![Streamlit](https://img.shields.io/badge/Streamlit-in_Snowflake-FF4B4B?logo=streamlit&logoColor=white)](https://streamlit.io/)
[![Status](https://img.shields.io/badge/Status-Competition_Ready-brightgreen)]()

> **Snowflake Korea Hackathon 2026 - Tech Track**
> 서울 서초/영등포/중구 렌탈 마케팅 예산 배분 의사결정 에이전트

## Problem

"다음 달 서초/영등포/중구 중 어디에 어떤 렌탈 상품과 마케팅 예산을 얼마나 넣어야 하는가?"

## Solution

DistrictPilot AI는 **100% Snowflake Native** 의사결정 에이전트입니다.

- **Semantic View + Cortex Analyst** -- 자연어 질문을 정확한 SQL로 변환 (VQR 10개, SQL 규칙 13개)
- **ML FORECAST + Ablation** -- 외생변수 효과를 정량 증명한 수요 예측
- **AI_COMPLETE Structured Output** -- JSON 액션 카드로 배분 의사결정 자동화
- **Dynamic Tables + Tasks** -- 운영 가능성 증명 (1h lag, daily/weekly refresh)

외부 서비스 없이 Snowflake 안에서 데이터 수집부터 의사결정까지 완결됩니다.

## Architecture

```
Data Sources
  Marketplace: SPH (유동인구/카드소비) + Richgo (부동산/인구이동)
  Public: 공휴일 + 연령구조 + 관광수요 + 상권변화
                         |
                         v
DT_FEATURE_MART_V2 (Dynamic Table, 1h lag)
  3구 x 60개월, 50+ features
         |                    |
         v                    v
  ML FORECAST            Semantic View (DISTRICTPILOT_SV)
  Ablation A->E          Cortex Analyst (VQR 10개)
  Feature Importance     Cortex Search (정책 문서)
         |                    |
         v                    v
  Streamlit in Snowflake (5 tabs)
  Allocation | Analysis | AI Agent | Simulation | Ops/Trust
```

## Evidence Chain

| Step | Snowflake Object | Evidence |
|------|------------------|----------|
| Forecast | `DISTRICTPILOT_FORECAST` | 3구 x 3개월 예측, MAPE < 10%, 95% CI |
| Feature Importance | `FEATURE_IMPORTANCE` + `ABLATION_RESULTS` | Top-5 기여도, ablation 전후 MAPE 개선 |
| Semantic View | `DISTRICTPILOT_SV` | VQR 10개, SQL 규칙 13개, synonym 100+, 카테고리 7개 |
| Search Grounding | `CORTEX.SEARCH()` | 외부 컨텍스트로 hallucination 방지 |
| AI Structured Output | `AI_COMPLETE` | JSON action card (priority/budget_pct/confidence) |
| Refresh State | `DYNAMIC_TABLE_REFRESH_HISTORY` | 실시간 LAG_SEC, 갱신 이력, query_tag 감사 |

## Scoring Coverage

| Criteria | Max | How We Cover It |
|----------|-----|-----------------|
| Creativity 25 | Forecast + Allocation + Simulation + Dual use case | Ablation, What-if, public+private |
| Snowflake expertise 25 | Semantic View, Analyst, Search, DT, Tasks, AI_COMPLETE | Full native stack |
| AI expertise 25 | Exogenous Forecast + Eval metrics + Feature importance + Grounded AI | Evidence-based agent |
| Realism 15 | V_APP_HEALTH, target lag, retry, RBAC, query tags | Ops/Trust panel |
| Presentation 10 | 10-min demo, ROI, execution roadmap | Demo script + PPT |

## Data Sources

### Sponsor (Snowflake Marketplace)
| Source | Data | Level |
|--------|------|-------|
| **SPH** | 유동인구 (거주/직장/방문), 카드소비 (8 카테고리), 자산/소득 | 법정동 -> 구 집계 |
| **Richgo** | 아파트 매매/전세 시세, 인구이동 (전입/전출/순이동) | 시군구 |

### External (Public Open Data)
| Source | Data | License |
|--------|------|---------|
| 한국천문연구원 | 공휴일/특일 캘린더 | 공공누리 1유형 |
| 행정안전부 | 연령/성별 주민등록 인구 | 공공누리 1유형 |
| 한국관광 데이터랩 | 관광수요/외래객 지수 | 공공누리 1유형 |
| 서울시 상권분석서비스 | 상권변화지표 | 공공누리 1유형 |

## Project Structure

```
districtpilot-ai/
|-- README.md
|
|-- Snowflake SQL Pipeline --
|-- 00_rename_database.sql          # DB rename (MOVESIGNAL -> DISTRICTPILOT)
|-- 02_feature_mart_v4.sql          # Feature Mart (5 STG -> FEATURE_MART_FINAL)
|-- 03_ml_and_cortex_v2.sql         # ML Forecast + Cortex LLM
|-- 06_semantic_view.sql            # Semantic View + VQR 10개 (Cortex Analyst)
|-- 07_dynamic_tables_tasks.sql     # Dynamic Tables + Tasks + V_APP_HEALTH
|-- 08_ajd_integration.sql          # AJD 통신 데이터 통합 (Optional)
|-- 09_cortex_search_agent.sql      # Cortex Search + Agent
|-- 10_external_data.sql            # 4개 외부 데이터 + FEATURE_MART_V2
|-- 11_ablation_study.sql           # Ablation A->E (5 모델 비교)
|
|-- Streamlit App --
|-- streamlit_app_v7.py             # 최종 앱 (5탭)
|-- snowflake.yml                   # SiS 배포 설정
|
|-- Docs --
|-- DEMO_SCRIPT.md                  # 10분 데모 스크립트
|-- evidence_chain_slide.md         # Evidence Chain 슬라이드 + 30초 멘트
|-- DATA_SOURCES_AND_LICENSES.md    # 데이터 소스 + 라이선스
|-- SUBMISSION_CHECKLIST.md         # 제출 체크리스트
```

## Snowflake Objects

| Object | Type | Description |
|--------|------|-------------|
| `FEATURE_MART_V2` | Table | 확장 Feature Mart (50+ features) |
| `DT_FEATURE_MART` | Dynamic Table | 자동 갱신 (1h lag) |
| `DISTRICTPILOT_FORECAST` | ML Model | Ablation 최종 모델 (외생변수 포함) |
| `ABLATION_RESULTS` | Table | 5 모델 MAPE/SMAPE/MAE 비교 |
| `DISTRICTPILOT_SV` | Semantic View | Cortex Analyst용 비즈니스 메트릭 (VQR 10개) |
| `DISTRICTPILOT_SEARCH_SVC` | Cortex Search | 정책/룰북 문서 검색 |
| `V_APP_HEALTH` | View | 운영 상태 모니터링 |

## Prerequisites

- Snowflake account with **ACCOUNTADMIN** role
- Marketplace datasets: **SPH**, **Richgo**
- Warehouse: `COMPUTE_WH` (X-Small)
- Streamlit in Snowflake enabled

```sql
CREATE DATABASE IF NOT EXISTS DISTRICTPILOT_AI;
CREATE SCHEMA IF NOT EXISTS DISTRICTPILOT_AI.ANALYTICS;
CREATE STAGE IF NOT EXISTS DISTRICTPILOT_AI.ANALYTICS.STREAMLIT_STAGE
  DIRECTORY = (ENABLE = TRUE);
```

## Execution Order

```sql
-- 1. Base pipeline
02_feature_mart_v4.sql
03_ml_and_cortex_v2.sql

-- 2. External data + extended features
10_external_data.sql

-- 3. Ablation study
11_ablation_study.sql

-- 4. AI layer
06_semantic_view.sql
09_cortex_search_agent.sql

-- 5. Operations
07_dynamic_tables_tasks.sql

-- 6. Deploy Streamlit
streamlit_app_v7.py
```

## Key Differentiators

- **Ablation Study**: Sponsor-only -> +Holiday -> +Age -> +Tourism -> +Commercial, MAPE 개선 정량화
- **Evidence Chain**: Forecast -> Feature Importance -> Semantic View -> Search Grounding -> Structured Output -> Refresh State
- **Dual Use Case**: 동일 데이터/모델로 민간 (렌탈/마케팅 배분)과 공공 (상권 활성화/행정 배분) 모두 적용
- **Production-Ready**: Dynamic Tables (1h lag) + Tasks (daily/weekly) + Health monitoring + Query tags

## Cost

~**$80/month** (Compute WH X-Small + Cortex LLM + Streamlit + Dynamic Tables)

## Author

**Doeon Kim** - [GitHub](https://github.com/KIM3310)

---

*Snowflake Korea Hackathon 2026 Tech Track*
*Submission deadline: 2026-04-12 23:59 KST*
*Finals: 2026-04-29, Seoul (10min presentation + 3min Q&A)*
