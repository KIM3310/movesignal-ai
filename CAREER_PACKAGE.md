# MoveSignal AI - Career Transition Package

**Doeon Kim** | GitHub: [KIM3310](https://github.com/KIM3310)
Target Roles: Snowflake Korea - Solution Engineer / Account Engineer / Associate SE

---

## 1. 1-Page Case Study (한국어)

### MoveSignal AI: Snowflake-Native 의사결정 에이전트

**문제**: 서울 3개 핵심 상권(서초/영등포/중구)의 렌탈·마케팅 예산을 데이터 없이 감으로 배분하고 있음

**솔루션**: 100% Snowflake 플랫폼에서 동작하는 데이터 기반 배분 엔진

| 레이어 | 구현 |
|---------|------|
| 데이터 수집 | Marketplace 데이터 + 공개 데이터 → Feature Mart (Dynamic Table, 1h lag) |
| 수요 예측 | ML FORECAST + Ablation study → 3개월 수요 예측 (MAPE 정량 검증) |
| 자연어 인터페이스 | Semantic View + Cortex Analyst → 자연어 SQL 생성 |
| AI 의사결정 | AI_COMPLETE Structured Output → 액션 카드 (추천 지역/비율/근거/리스크/실행) |
| 문서 기반 검증 | Cortex Search → 정책 문서 기반 grounding |
| 사용자 앱 | Streamlit in Snowflake → 5탭 앱 (배분/분석/AI/시뮬레이션/운영) |

**기술 스택**: Snowflake ML FORECAST, Cortex Analyst, Cortex Search, AI_COMPLETE, Semantic Views, Dynamic Tables, Tasks, Streamlit in Snowflake

**결과**:
- **민간**: 렌탈/마케팅 예산 최적 배분 → ROI 측정 가능
- **공공**: 같은 엔진으로 상권 활성화/행정 배분 지원
- **비용**: 월 운영비 ~$80, 엔지니어 1명으로 운영 가능

**왜 Snowflake인가**:
- 데이터 수집에서 AI, 시각화까지 하나의 플랫폼에서 완결
- 데이터가 밖으로 나가지 않는 보안 모델 (Owner's Rights)
- Marketplace 데이터 활용으로 수집 비용 제로
- 별도 인프라 관리 없이 서버리스로 운영

---

## 2. LinkedIn/이메일 아웃리치 문구 (영문)

### Version A: Cold Outreach to Snowflake Korea Recruiter/Hiring Manager

> **Subject: Solution Engineer Candidate -- Snowflake-Native AI Project (Hackathon)**
>
> Hi [Name],
>
> I'm Doeon Kim, a Solutions Architect and full-stack engineer based in Seoul. I recently built **MoveSignal AI**, a Snowflake-native decision agent for the Snowflake Hackathon that uses ML FORECAST, Cortex Analyst, Cortex Search, and AI_COMPLETE to optimize budget allocation across Seoul's commercial districts -- all running entirely within Snowflake with zero external dependencies.
>
> I'm reaching out because I'm deeply interested in the **Solution Engineer** (or Account Engineer / Associate SE) role at Snowflake Korea. My hands-on experience building end-to-end on the Snowflake platform, combined with my background in ML, data engineering, and customer-facing solution design, aligns well with what the SE team delivers.
>
> I'd welcome the chance to walk you through the architecture and demo.
>
> - GitHub: https://github.com/KIM3310/movesignal-ai
> - Demo Video: [PLACEHOLDER_DEMO_VIDEO_URL]
>
> Best regards,
> Doeon Kim

### Version B: Follow-Up After Hackathon Results

> **Subject: Hackathon Update + Continued Interest in Snowflake Korea SE Role**
>
> Hi [Name],
>
> Following up on my earlier note -- I wanted to share that **MoveSignal AI**, my Snowflake Hackathon submission, [achieved / was recognized for] [PLACEHOLDER_RESULT]. The project demonstrates a fully Snowflake-native architecture: Dynamic Tables for feature engineering, ML FORECAST for demand prediction, Cortex Analyst for natural language querying, and a Streamlit in Snowflake front-end -- all governed under Snowflake's security model.
>
> Building this reinforced my conviction that Snowflake's platform is uniquely positioned to deliver end-to-end AI solutions without data leaving the account. I'd love to bring this perspective to the **Solution Engineer** team at Snowflake Korea.
>
> Happy to share a live demo or discuss the technical architecture in detail.
>
> - GitHub: https://github.com/KIM3310/movesignal-ai
> - Demo Video: [PLACEHOLDER_DEMO_VIDEO_URL]
>
> Best regards,
> Doeon Kim

### Version C: Talent Community Registration Cover Note

> **Subject: Talent Community -- Solution Engineer, Snowflake Korea**
>
> Hello,
>
> I'm registering my interest in Solution Engineering roles at Snowflake Korea. My background spans solutions architecture, machine learning, full-stack development, and data/DevOps engineering.
>
> Most recently, I built **MoveSignal AI** for the Snowflake Hackathon -- a decision-support agent that runs 100% natively on Snowflake. The project leverages ML FORECAST, Cortex Analyst, Cortex Search, AI_COMPLETE, Dynamic Tables, and Streamlit in Snowflake to optimize budget allocation for Seoul commercial districts. Monthly operating cost is approximately $80 with no external infrastructure.
>
> I'm passionate about helping customers realize the full potential of Snowflake's platform and would welcome any opportunity to contribute to the Korea team.
>
> - GitHub: https://github.com/KIM3310/movesignal-ai
> - Demo Video: [PLACEHOLDER_DEMO_VIDEO_URL]
>
> Best regards,
> Doeon Kim

---

## 3. README 히어로 섹션 (for GitHub)

```markdown
# MoveSignal AI

**Snowflake-Native Decision Agent for Commercial District Budget Optimization**

---

### Problem

Rental and marketing budgets across Seoul's key commercial districts (Seocho, Yeongdeungpo, Jung-gu) are allocated by intuition, not data. There is no systematic way to forecast demand, compare districts, or justify spend.

### Solution

MoveSignal AI is a **100% Snowflake-native** decision engine that transforms raw marketplace and public data into actionable budget allocation recommendations -- without any data leaving the Snowflake account.

### Architecture

```
Marketplace Data + Public Data
        |
        v
  Dynamic Tables (Feature Mart, 1h refresh)
        |
        v
  ML FORECAST (3-month demand prediction, MAPE-validated)
        |
        v
  Cortex Analyst + Semantic Views (Natural language SQL)
        |
        v
  AI_COMPLETE (Structured action cards: region / ratio / rationale / risk)
        |
        v
  Cortex Search (Policy document grounding)
        |
        v
  Streamlit in Snowflake (5-tab app)
```

### Demo

![Demo GIF](assets/demo.gif)
<!-- TODO: Replace with actual demo GIF -->

### Key Metrics

| Metric | Value |
|--------|-------|
| External dependencies | 0 |
| Monthly operating cost | ~$80 |
| Required headcount | 1 engineer |
| Data freshness | 1-hour lag (Dynamic Tables) |
| Forecast horizon | 3 months |
| Validation method | MAPE + Ablation study |
| Application sectors | Private (rental/marketing) + Public (district revitalization) |

### Tech Stack

`ML FORECAST` `Cortex Analyst` `Cortex Search` `AI_COMPLETE` `Semantic Views` `Dynamic Tables` `Tasks` `Streamlit in Snowflake`

---

> Built as a portfolio project demonstrating end-to-end Snowflake platform capabilities for the Snowflake Hackathon.
```

---

## 4. 자격증 준비 메모

### 목표 자격증

| 자격증 | 상태 | 비고 |
|--------|------|------|
| **SnowPro Core (C03)** | 준비 예정 | 2026.02.16 출시, 최신 버전 |
| **SnowPro Associate: Platform** | 다음 순서 | Core 합격 후 진행 |

### 우선순위

1. 해커톤 제출 완료 (최우선)
2. SnowPro Core C03 준비 및 응시
3. SnowPro Associate: Platform 준비 및 응시

### 학습 리소스

- 공식 학습 경로: [https://learn.snowflake.com](https://learn.snowflake.com)
- SnowPro Core C03 Study Guide (Snowflake 공식 문서 내)
- Snowflake Documentation: [https://docs.snowflake.com](https://docs.snowflake.com)
- MoveSignal AI 프로젝트 구축 과정에서 이미 다룬 영역:
  - Data Loading & Transformation (Dynamic Tables)
  - Data Sharing & Marketplace
  - Security & Governance (Owner's Rights)
  - Snowflake Cortex (ML, AI Functions)
  - Streamlit in Snowflake

### 전략 노트

- Core C03는 최신 출시이므로 기출 문제 풀이보다 공식 문서 + 실습 중심으로 준비
- MoveSignal AI 프로젝트 경험이 실무 문제 이해에 직접적 도움
- 면접 시 "자격증 준비 중"이라도 프로젝트 기반 역량 증명이 더 강력

---

## 5. 주요 Q&A 답변 (면접 대비)

### Q1. "이 프로젝트에서 가장 어려웠던 기술적 문제는?"

Cortex Analyst의 Semantic View 설계가 가장 도전적이었습니다. 자연어 질의를 정확한 SQL로 변환하려면 테이블 간 관계, 컬럼 설명, 비즈니스 용어 매핑을 Semantic View에 정밀하게 정의해야 합니다. 초기에는 "서초구 렌탈 수요"같은 질의가 엉뚱한 컬럼을 참조하는 문제가 있었고, 이를 해결하기 위해 Semantic View의 description과 synonym을 반복적으로 튜닝했습니다.

또한 ML FORECAST의 예측 정확도를 검증하기 위해 Ablation Study를 설계했는데, 어떤 Feature 조합이 MAPE를 개선하는지 체계적으로 실험하는 과정에서 Dynamic Table의 refresh 주기와 Feature Mart 구조를 여러 차례 재설계해야 했습니다. 이 경험을 통해 "데이터 품질과 메타데이터 설계가 AI 성능의 80%를 결정한다"는 점을 체감했습니다.

### Q2. "왜 Snowflake를 선택했나?"

세 가지 이유입니다.

첫째, **플랫폼 완결성**입니다. 데이터 수집(Marketplace), 변환(Dynamic Tables), ML(FORECAST), AI(Cortex Analyst, AI_COMPLETE), 검색(Cortex Search), 시각화(Streamlit)까지 전부 하나의 플랫폼에서 해결됩니다. 별도의 ETL 파이프라인, ML 서빙 인프라, 프론트엔드 호스팅이 불필요합니다.

둘째, **보안 모델**입니다. Owner's Rights 기반으로 데이터가 계정 밖으로 나가지 않습니다. 특히 한국 금융/공공 고객에게 이 점은 도입 장벽을 크게 낮춥니다.

셋째, **운영 효율**입니다. 서버리스 아키텍처 덕분에 월 ~$80로 운영 가능하고, 인프라 관리 인력이 필요 없습니다. 스타트업이든 대기업이든 바로 적용할 수 있는 구조입니다.

### Q3. "이 엔진을 실제 고객에게 제안한다면 어떻게 할 것인가?"

**3단계 접근법**을 사용하겠습니다.

**1단계 - Discovery (1~2주)**: 고객의 현재 예산 배분 프로세스를 파악합니다. 어떤 데이터를 이미 갖고 있는지, 의사결정 주기는 어떤지, 현재 pain point는 무엇인지 인터뷰합니다.

**2단계 - Proof of Value (2~4주)**: MoveSignal AI 아키텍처를 고객 데이터에 맞게 커스터마이징하여 PoV를 실행합니다. 고객의 실제 데이터로 3개월 예측을 수행하고, 기존 방식 대비 예측 정확도(MAPE)를 정량 비교합니다. Streamlit 앱으로 즉시 사용 가능한 대시보드를 제공합니다.

**3단계 - Production Rollout**: Dynamic Table 기반 자동 갱신, Task 스케줄링, 역할 기반 접근 제어(RBAC)를 설정하여 운영 환경으로 전환합니다. 월 운영비와 ROI를 명확히 산출하여 경영진 보고 자료를 함께 준비합니다.

핵심은 "기술 데모"가 아니라 "비즈니스 임팩트 증명"에 초점을 맞추는 것입니다.

### Q4. "Snowflake의 경쟁 우위는 뭐라고 보나?"

**데이터 클라우드로서의 네트워크 효과**가 가장 큰 차별점이라고 봅니다.

Databricks나 BigQuery도 강력한 플랫폼이지만, Snowflake는 Marketplace를 통해 데이터 공급자와 소비자를 연결하는 양면 네트워크를 구축하고 있습니다. MoveSignal AI에서도 Marketplace 데이터를 활용해 수집 비용을 제로로 만들었는데, 이것이 고객에게는 "Time to Value"를 극적으로 단축시킵니다.

두 번째는 **거버넌스와 보안**입니다. 특히 한국 시장에서 데이터 주권, 개인정보보호법(PIPA) 준수가 중요한데, Snowflake의 Owner's Rights 모델과 Cross-Region Replication은 이 요구사항에 정확히 부합합니다.

세 번째는 **Cortex AI의 통합성**입니다. LLM 기반 기능(Analyst, Search, AI_COMPLETE)이 SQL 인터페이스 안에서 네이티브로 동작하기 때문에, 데이터 팀이 별도의 ML 인프라 없이도 AI 애플리케이션을 구축할 수 있습니다. 이 진입 장벽 낮춤이 엔터프라이즈 AI 도입을 가속화합니다.

### Q5. "Solution Engineer로서 어떤 가치를 줄 수 있나?"

세 가지 가치를 제공할 수 있습니다.

**첫째, 실전 기반 기술 깊이**입니다. MoveSignal AI를 통해 Snowflake의 ML, Cortex AI, Dynamic Tables, Streamlit을 실제로 구축해 본 경험이 있습니다. 고객 앞에서 "이론적으로 가능합니다"가 아니라 "제가 직접 만들어 봤는데, 이런 점을 주의하셔야 합니다"라고 말할 수 있습니다.

**둘째, 멀티 롤 경험에서 오는 고객 공감 능력**입니다. Solutions Architect, ML Engineer, Full-stack Developer, Data Engineer, DevOps -- 다양한 역할을 수행해 왔기 때문에 고객사의 어떤 팀과 대화하든 그들의 언어로 소통할 수 있습니다. CTO에게는 아키텍처를, 데이터 팀에게는 파이프라인을, 경영진에게는 ROI를 이야기할 수 있습니다.

**셋째, 한국 시장에 대한 이해**입니다. 한국 상권 데이터, 공공 데이터, 규제 환경을 이해하고 있으며, 이를 Snowflake 플랫폼과 연결하는 실제 사례(MoveSignal AI)를 보유하고 있습니다. 한국 고객에게 Snowflake의 가치를 그들의 비즈니스 맥락에서 설명할 수 있다는 점이 가장 큰 강점입니다.
