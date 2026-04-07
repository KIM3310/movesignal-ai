# MoveSignal AI — Case Study

## 한 줄 요약

Snowflake Marketplace 데이터와 공공 데이터를 결합하여, 외부 서비스 없이 Snowflake 안에서 수요 예측부터 예산 배분 액션까지 완결되는 의사결정 에이전트를 구축한 사례.

---

## 고객 문제

통신/렌탈 기업의 지역 마케팅 팀은 매월 서초·영등포·중구에 예산을 배분해야 합니다.
기존 방식은 전년 동기 대비 비율이나 담당자 감에 의존했고, 유동인구·부동산·인구이동 같은 외부 변수를 반영하지 못했습니다.

**핵심 질문**: "다음 달 어느 구에 얼마를 넣어야 ROI가 최대인가?"

## 해결 접근

| 단계 | Snowflake 기능 | 역할 |
|------|---------------|------|
| 데이터 통합 | Marketplace (SPH, Richgo) + 공공 API 4종 | 유동인구, 카드소비, 부동산, 인구이동, 공휴일, 연령, 관광, 상권 |
| Feature Store | Dynamic Table (1h lag) | 50+ Feature 자동 갱신 |
| 예측 | ML FORECAST + Ablation Study (5 모델) | 외생변수 기여도 정량화, MAPE 개선 입증 |
| 의미 계층 | Semantic View + Cortex Analyst | 비즈니스 메트릭 정의, 자연어 SQL 생성 |
| 근거 검색 | Cortex Search Service | 정책 문서 기반 답변 보강 |
| 액션 생성 | AI_COMPLETE Structured Output | 구별 배분 비율, 근거, 리스크, 다음 액션을 JSON으로 출력 |
| 운영 | Tasks + V_APP_HEALTH | 일/주 단위 자동 갱신, 상태 모니터링 |
| 인터페이스 | Streamlit in Snowflake (5탭) | 배분·분석·AI·시뮬레이션·운영 대시보드 |

## 핵심 차별점

1. **Ablation Study로 근거 확보**: Holiday → Age → Tourism → Commercial 순으로 변수를 추가하며 MAPE 개선을 정량 측정. "이 변수를 왜 넣었는가"에 숫자로 답할 수 있음.

2. **100% Snowflake Native**: 데이터 적재부터 예측, AI 액션, 대시보드까지 Snowflake 밖으로 데이터가 나가지 않음. 보안 심사와 거버넌스 요구사항을 구조적으로 충족.

3. **액션까지 완결**: 단순 예측이 아니라 "서초 16%, 영등포 17%, 중구 67%" 같은 배분 액션을 structured output으로 생성하고, What-if 시뮬레이션으로 사용자가 즉시 검증 가능.

## 비즈니스 임팩트

| 지표 | Before | After |
|------|--------|-------|
| 배분 결정 소요 | 2-3일 (수동 분석) | 즉시 (AI 추천 + 시뮬레이션) |
| 근거 투명성 | 감 기반 | Feature Importance + Ablation 수치 |
| 운영 비용 | 별도 ML 인프라 필요 | ~$80/월 (Snowflake 내장) |
| 데이터 거버넌스 | 외부 전송 필요 | Zero data movement |

## 기술 스택

```
Snowflake ML FORECAST · Semantic View · Cortex Analyst · Cortex Search
AI_COMPLETE (Structured Output) · Dynamic Tables · Tasks · Streamlit in Snowflake
```

## 적용 확장성

- **민간**: 렌탈/마케팅 예산 배분, 매장 출점, 재고 배치
- **공공**: 구청 상권 활성화 예산, 소상공인 지원금, 관광 인프라 배치

---

**Author**: Doeon Kim — [GitHub](https://github.com/KIM3310)
