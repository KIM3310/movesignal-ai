# MoveSignal AI

> **Snowflake Korea Hackathon 2026 — Tech Track**
> 서초·영등포·중구 렌탈/마케팅 예산 배분 의사결정 엔진

## Overview

MoveSignal AI는 서울 3개 핵심 상권(서초구, 영등포구, 중구)의 렌탈 및 마케팅 예산을 **데이터 기반으로 최적 배분**하는 의사결정 엔진입니다.

**100% Snowflake 네이티브** — 데이터 수집부터 ML, AI, 시각화까지 외부 서비스 없이 Snowflake 안에서 동작합니다.

## Architecture

```
┌─────────────────────────────────────────────────┐
│              Snowflake Marketplace               │
│  SPH (유동인구/카드소비/자산)                      │
│  Richgo (부동산시세/인구이동)                      │
│  AJD (통신가입)                                   │
└──────────────┬──────────────────────────────────┘
               ▼
┌─────────────────────────────────────────────────┐
│          Feature Mart (3구 × 60개월)              │
│  STG_POP → STG_CARD → STG_ASSET                  │
│  STG_PRICE → STG_MOVE → FEATURE_MART_FINAL       │
└──────────────┬──────────────────────────────────┘
               ▼
┌──────────────┴──────────────────────────────────┐
│  Snowflake ML FORECAST    Snowflake Cortex LLM   │
│  (3개월 수요 예측)         (mistral-large2 한국어)  │
└──────────────┬──────────────────────────────────┘
               ▼
┌─────────────────────────────────────────────────┐
│         Streamlit in Snowflake                    │
│  배분 추천 | 지역 분석 | 시뮬레이션 | AI 에이전트    │
└─────────────────────────────────────────────────┘
```

## Key Features

| Feature | Description |
|---------|------------|
| **ML Forecast** | Snowflake ML FORECAST로 3개월 수요 예측 (95% 신뢰구간) |
| **AI Agent** | Cortex mistral-large2 기반 한국어 데이터 분석 에이전트 |
| **Budget Allocation** | 예측 기반 자동 예산 배분 추천 |
| **What-if Simulation** | 시나리오별 배분 시뮬레이션 |
| **One Engine, Two Impacts** | 동일 엔진으로 민간(렌탈/마케팅) + 공공(상권활성화) 활용 |

## Data Sources (Snowflake Marketplace)

- **SPH**: 유동인구 (거주/직장/방문), 카드소비 (8개 카테고리), 자산소득
- **Richgo**: 아파트 매매/전세 시세, 인구이동 (전입/전출/순이동)
- **AJD**: 통신 가입 데이터 (시/군 단위)

## Project Structure

```
movesignal-ai/
├── README.md
├── 02_feature_mart_v4.sql      # Feature Mart SQL (5개 STG → 통합)
├── 03_ml_and_cortex_v2.sql     # ML Forecast + Cortex SQL
├── streamlit_app_v4.py         # Streamlit 앱 (최신, GPT Pro 리뷰 반영)
├── streamlit_app_v3.py         # Streamlit 앱 (이전 버전)
├── DEMO_SCRIPT.md              # 10분 데모 발표 스크립트
├── CODEX_HANDOFF.md            # 프로젝트 문서
└── MoveSignal_AI_Hackathon.pptx # 발표 PPT (13슬라이드)
```

## Snowflake Objects

| Object | Type | Description |
|--------|------|-------------|
| `FEATURE_MART_FINAL` | Table | 통합 Feature Mart (3구 × 60개월) |
| `MOVESIGNAL_FORECAST` | ML Model | Snowflake ML FORECAST 모델 |
| `FORECAST_RESULTS` | Table | 3개월 예측 결과 |
| `RECOMMEND_ALLOCATION` | Procedure | 예산 배분 추천 |
| `SIMULATE_WHATIF` | Procedure | What-if 시뮬레이션 |
| `MOVESIGNAL_APP` | Streamlit | 배포된 Streamlit 앱 |

## Cost

~**$80/month** (Compute WH X-Small + Cortex LLM + Streamlit hosting)

## Author

**Doeon Kim** — [GitHub](https://github.com/KIM3310)
