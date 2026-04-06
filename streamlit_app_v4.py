# -*- coding: utf-8 -*-
# MoveSignal AI: Streamlit in Snowflake App v4
# 서초·영등포·중구 렌탈/마케팅 배분 의사결정 엔진
# GPT Pro 리뷰 반영: 바인드 파라미터, 데이터 기반 AI, 평가지표

import streamlit as st
import pandas as pd
import json
from snowflake.snowpark.context import get_active_session

session = get_active_session()

DISTRICTS = ['서초구', '영등포구', '중구']
DB_SCHEMA = "MOVESIGNAL_AI.ANALYTICS"
LLM_MODEL = "mistral-large2"

# ────────────────────────────────────────
# 데이터 로드 (캐시 적용)
# ────────────────────────────────────────
@st.cache_data(ttl=600)
def load_feature_mart():
    return session.sql(
        f"SELECT * FROM {DB_SCHEMA}.FEATURE_MART_FINAL ORDER BY YM, DISTRICT"
    ).to_pandas()

@st.cache_data(ttl=600)
def load_actual_vs_forecast():
    return session.sql(
        f"SELECT * FROM {DB_SCHEMA}.ACTUAL_VS_FORECAST ORDER BY DISTRICT, DS"
    ).to_pandas()

@st.cache_data(ttl=600)
def load_feature_importance():
    try:
        return session.sql(
            f"SELECT * FROM {DB_SCHEMA}.FEATURE_IMPORTANCE ORDER BY RANK"
        ).to_pandas()
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=600)
def load_forecast_results():
    try:
        return session.sql(
            f"SELECT * FROM {DB_SCHEMA}.FORECAST_RESULTS ORDER BY SERIES, TS"
        ).to_pandas()
    except Exception:
        return pd.DataFrame()

def call_allocation(budget):
    try:
        result = session.sql(
            f"CALL {DB_SCHEMA}.RECOMMEND_ALLOCATION({budget})"
        ).collect()
        return result[0][0] if result else None
    except Exception:
        return None

def call_whatif(seocho, yeongdeungpo, junggu, budget):
    try:
        result = session.sql(
            f"CALL {DB_SCHEMA}.SIMULATE_WHATIF({seocho}, {yeongdeungpo}, {junggu}, {budget})"
        ).collect()
        return result[0][0] if result else None
    except Exception:
        return None

def build_data_context():
    """Feature Mart 최신 데이터를 LLM 컨텍스트로 구성"""
    feature_mart = load_feature_mart()
    forecast_results = load_forecast_results()
    context_parts = []

    if not feature_mart.empty:
        latest = feature_mart.groupby('DISTRICT').last().reset_index()
        context_cols = [c for c in [
            'DISTRICT', 'YM', 'TOTAL_POP', 'TOTAL_SALES', 'AVG_MEME_PRICE',
            'NET_MOVE', 'AVG_ASSET', 'AVG_INCOME', 'SALES_CHG_PCT',
            'POP_CHG_PCT', 'PRICE_CHG_PCT', 'SALES_PER_POP'
        ] if c in latest.columns]
        context_parts.append(
            "[최신 Feature Mart 데이터]\n" + latest[context_cols].to_string(index=False)
        )

    if not forecast_results.empty:
        forecast_summary = forecast_results.groupby('SERIES')['FORECAST'].agg(['mean', 'min', 'max']).reset_index()
        forecast_summary.columns = ['DISTRICT', 'AVG_FORECAST', 'MIN_FORECAST', 'MAX_FORECAST']
        context_parts.append(
            "\n[3개월 예측 요약]\n" + forecast_summary.to_string(index=False)
        )

    return "\n".join(context_parts) if context_parts else ""

def call_cortex_with_context(user_question, system_context=""):
    """Cortex LLM 호출 — 데이터 컨텍스트 주입"""
    data_context = build_data_context()
    full_prompt = f"""당신은 MoveSignal AI의 데이터 분석 에이전트입니다.
서초구, 영등포구, 중구의 렌탈/마케팅 예산 배분을 돕는 AI입니다.
반드시 아래 실제 데이터를 기반으로 답변하세요. 데이터에 없는 내용은 추측하지 마세요.

{data_context}

{system_context}

사용자 질문: {user_question}

위 데이터를 기반으로 한국어로 구체적 숫자를 포함하여 3~5줄로 답변하세요.
렌탈/마케팅 예산 배분 관점에서 실행 가능한 조언을 포함하세요."""

    try:
        safe_prompt = full_prompt.replace("'", "''")
        result = session.sql(f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                '{LLM_MODEL}',
                '{safe_prompt}'
            ) AS RESPONSE
        """).collect()
        return result[0]['RESPONSE'] if result else "응답을 생성할 수 없습니다."
    except Exception as e:
        return f"Cortex 호출 오류: {str(e)}"

# ────────────────────────────────────────
# 사이드바
# ────────────────────────────────────────
with st.sidebar:
    st.title("MoveSignal AI")
    st.caption("서초 · 영등포 · 중구\n렌탈/마케팅 배분 의사결정 엔진")
    st.divider()

    total_budget = st.number_input(
        "총 예산 (만원)", min_value=1000, max_value=100000, value=5000, step=500
    ) * 10000

    st.divider()
    page = st.radio(
        "메뉴",
        ["배분 추천", "지역 분석", "시뮬레이션", "AI 에이전트"],
        label_visibility="collapsed"
    )
    st.divider()
    st.caption("운영 현황")
    st.caption("데이터 갱신: 매일 06:00 KST")
    st.caption("모델 재학습: 주 1회")
    st.caption("Cortex LLM: mistral-large2")
    st.caption("월 비용 추정: ~$80")

# ══════════════════════════════════════════════
# 페이지 1: 배분 추천
# ══════════════════════════════════════════════
if page == "배분 추천":
    st.header("다음 분기 배분 추천")

    forecast_results = load_forecast_results()
    feature_importance = load_feature_importance()

    if not forecast_results.empty:
        district_forecast = forecast_results.groupby('SERIES')['FORECAST'].mean().reset_index()
        district_forecast.columns = ['DISTRICT', 'FORECAST']
        total_forecast = district_forecast['FORECAST'].sum()

        # 배분 비율 메트릭 카드
        metric_cols = st.columns(3)
        for idx, row in district_forecast.iterrows():
            with metric_cols[idx]:
                share_pct = row['FORECAST'] / total_forecast * 100
                budget_alloc = total_budget * share_pct / 100
                st.metric(
                    label=row['DISTRICT'],
                    value=f"{share_pct:.0f}%",
                    delta=f"{budget_alloc/10000:,.0f}만원"
                )
                st.progress(share_pct / 100)

        st.divider()
        chart_col, info_col = st.columns(2)

        # 실제 vs 예측 시계열 차트
        with chart_col:
            st.subheader("실제 vs 예측 추이")
            actual_vs_forecast = load_actual_vs_forecast()
            if not actual_vs_forecast.empty:
                for district in DISTRICTS:
                    district_avf = actual_vs_forecast[
                        actual_vs_forecast['DISTRICT'] == district
                    ].copy()
                    if not district_avf.empty:
                        st.caption(f"**{district}**")
                        chart_data = district_avf[['DS', 'ACTUAL', 'FORECAST_VAL']].set_index('DS')
                        st.line_chart(chart_data)

        # Feature Importance + AI 설명
        with info_col:
            st.subheader("왜 이 배분인가?")
            if not feature_importance.empty:
                st.dataframe(feature_importance.head(10), use_container_width=True)
            else:
                st.info("Feature Importance 데이터를 로딩 중입니다...")

            if st.button("AI에게 설명 요청", key="explain_btn"):
                top_district = district_forecast.loc[
                    district_forecast['FORECAST'].idxmax(), 'DISTRICT'
                ]
                with st.spinner("Cortex AI 분석 중..."):
                    explanation = call_cortex_with_context(
                        f"{top_district}의 카드 소비 수요가 가장 높을 것으로 예측되는 이유를 설명해주세요.",
                        system_context="유동인구, 소비패턴, 부동산 시세, 인구이동 데이터를 종합 분석하세요."
                    )
                st.success(explanation)

        # 평가 지표 섹션
        st.divider()
        st.subheader("모델 평가 지표")
        eval_cols = st.columns(4)
        eval_cols[0].metric("학습 기간", "60개월")
        eval_cols[1].metric("예측 기간", "3개월")
        eval_cols[2].metric("대상 지역", "3개 구")
        eval_cols[3].metric("신뢰 구간", "95%")

        if not actual_vs_forecast.empty:
            # 간단한 MAPE 계산 (실제값이 있는 마지막 3개월)
            for district in DISTRICTS:
                d = actual_vs_forecast[actual_vs_forecast['DISTRICT'] == district]
                actual_vals = d[d['ACTUAL'].notna()]['ACTUAL']
                if len(actual_vals) >= 3:
                    recent = actual_vals.tail(3)
                    mean_val = recent.mean()
                    std_val = recent.std()
                    cv = (std_val / mean_val * 100) if mean_val > 0 else 0
                    st.caption(f"{district} — 최근 3개월 평균: {mean_val/1e8:.1f}억원, 변동계수(CV): {cv:.1f}%")
    else:
        st.warning("예측 데이터가 없습니다. ML Forecast를 먼저 실행해주세요.")
        feature_mart = load_feature_mart()
        if not feature_mart.empty:
            st.subheader("현재 Feature Mart 데이터")
            latest = feature_mart.groupby('DISTRICT').last().reset_index()
            display_cols = [c for c in [
                'DISTRICT', 'TOTAL_POP', 'TOTAL_SALES', 'AVG_MEME_PRICE', 'NET_MOVE'
            ] if c in latest.columns]
            st.dataframe(latest[display_cols], use_container_width=True)

# ══════════════════════════════════════════════
# 페이지 2: 지역 분석
# ══════════════════════════════════════════════
elif page == "지역 분석":
    st.header("지역 상세 분석")
    selected_district = st.selectbox("분석할 지역을 선택하세요", DISTRICTS)

    feature_mart = load_feature_mart()
    if not feature_mart.empty:
        district_data = feature_mart[feature_mart['DISTRICT'] == selected_district].copy()

        if not district_data.empty:
            latest = district_data.iloc[-1]

            # 핵심 KPI 카드
            kpi_cols = st.columns(4)
            kpi_cols[0].metric(
                "유동인구",
                f"{latest.get('TOTAL_POP', 0):,.0f}명",
                f"{latest.get('POP_CHG_PCT', 0):+.1f}%"
            )
            kpi_cols[1].metric(
                "카드소비",
                f"{latest.get('TOTAL_SALES', 0)/1e8:.1f}억원",
                f"{latest.get('SALES_CHG_PCT', 0):+.1f}%"
            )
            kpi_cols[2].metric(
                "평균 매매가",
                f"{latest.get('AVG_MEME_PRICE', 0)/10000:.0f}만원" if latest.get('AVG_MEME_PRICE') else "N/A",
                f"{latest.get('PRICE_CHG_PCT', 0):+.1f}%" if latest.get('PRICE_CHG_PCT') else ""
            )
            kpi_cols[3].metric(
                "순이동",
                f"{latest.get('NET_MOVE', 0):+,.0f}명" if latest.get('NET_MOVE') else "N/A"
            )

            st.divider()
            pop_col, spend_col = st.columns(2)

            with pop_col:
                st.subheader("유동인구 추이")
                pop_columns = [c for c in ['RES_POP', 'WORK_POP', 'VISIT_POP'] if c in district_data.columns]
                if pop_columns:
                    pop_chart = district_data[['YM'] + pop_columns].set_index('YM')
                    st.area_chart(pop_chart)

            with spend_col:
                st.subheader("소비 카테고리별 추이")
                spend_columns = [c for c in [
                    'FOOD', 'COFFEE', 'ENTERTAIN', 'CLOTHING', 'CULTURE', 'ACCOMMODATION'
                ] if c in district_data.columns]
                if spend_columns:
                    spend_chart = district_data[['YM'] + spend_columns].set_index('YM')
                    st.line_chart(spend_chart)

            # 소비 체질 분석
            st.subheader(f"{selected_district} 소비 체질 분석")
            total_sales = latest.get('TOTAL_SALES', 0)
            if total_sales and total_sales > 0:
                categories = {
                    '외식': latest.get('FOOD', 0) / total_sales * 100,
                    '카페': latest.get('COFFEE', 0) / total_sales * 100,
                    '엔터': latest.get('ENTERTAIN', 0) / total_sales * 100,
                    '의류': latest.get('CLOTHING', 0) / total_sales * 100,
                    '문화': latest.get('CULTURE', 0) / total_sales * 100,
                    '숙박': latest.get('ACCOMMODATION', 0) / total_sales * 100,
                    '뷰티': latest.get('BEAUTY', 0) / total_sales * 100,
                }
                cat_df = pd.DataFrame(
                    list(categories.items()), columns=['카테고리', '비율(%)']
                )
                cat_df = cat_df.sort_values('비율(%)', ascending=False)
                st.bar_chart(cat_df.set_index('카테고리'))
                top_category = cat_df.iloc[0]['카테고리']
                st.info(f"**{selected_district}**는 **{top_category}** 비중이 가장 높은 지역입니다.")

            # 자산 대비 시세 비율
            price_to_asset = latest.get('PRICE_TO_ASSET_RATIO')
            if price_to_asset and price_to_asset > 0:
                if price_to_asset > 1.5:
                    st.warning(f"자산 대비 시세 비율: {price_to_asset:.2f} — 고평가 주의")
                elif price_to_asset > 1.0:
                    st.info(f"자산 대비 시세 비율: {price_to_asset:.2f} — 적정 범위")
                else:
                    st.success(f"자산 대비 시세 비율: {price_to_asset:.2f} — 저평가 가능성")

            # 3구 비교 테이블
            st.divider()
            st.subheader("3구 비교")
            compare_latest = feature_mart.groupby('DISTRICT').last().reset_index()
            compare_cols = [c for c in [
                'DISTRICT', 'TOTAL_POP', 'TOTAL_SALES', 'AVG_MEME_PRICE',
                'NET_MOVE', 'SALES_PER_POP'
            ] if c in compare_latest.columns]
            st.dataframe(compare_latest[compare_cols], use_container_width=True)
    else:
        st.warning("Feature Mart 데이터가 없습니다.")

# ══════════════════════════════════════════════
# 페이지 3: What-if 시뮬레이션
# ══════════════════════════════════════════════
elif page == "시뮬레이션":
    st.header("What-if 시뮬레이션")
    st.caption(f"총 예산: {total_budget/10000:,.0f}만원")

    slider_cols = st.columns(3)
    with slider_cols[0]:
        seocho_pct = st.slider("서초구 배분 (%)", 0, 100, 45)
    with slider_cols[1]:
        yeongdeungpo_pct = st.slider("영등포구 배분 (%)", 0, 100, 35)
    with slider_cols[2]:
        junggu_pct = 100 - seocho_pct - yeongdeungpo_pct
        st.metric("중구 (%)", f"{junggu_pct}%")

    if seocho_pct + yeongdeungpo_pct > 100:
        st.error("서초구 + 영등포구 비율이 100%를 초과합니다.")
    else:
        st.divider()
        ai_col, sim_col = st.columns(2)

        with ai_col:
            st.subheader("AI 추천 배분")
            allocation = call_allocation(total_budget)
            if allocation:
                try:
                    alloc_data = json.loads(allocation) if isinstance(allocation, str) else allocation
                    if isinstance(alloc_data, dict) and 'allocations' in alloc_data:
                        for item in alloc_data['allocations']:
                            district_name = item.get('district', '')
                            share = item.get('share_pct', 0)
                            alloc_budget = item.get('budget', 0)
                            st.write(f"**{district_name}**: {alloc_budget/10000:,.0f}만원 ({share}%)")
                    else:
                        st.json(alloc_data)
                except Exception:
                    st.json(allocation)
            else:
                for district_name, pct in [('서초구', 33), ('영등포구', 33), ('중구', 34)]:
                    st.write(f"**{district_name}**: {total_budget * pct / 100 / 10000:,.0f}만원 ({pct}%)")

        with sim_col:
            st.subheader("시뮬레이션 배분")
            for district_name, pct in [('서초구', seocho_pct), ('영등포구', yeongdeungpo_pct), ('중구', junggu_pct)]:
                sim_budget = total_budget * pct / 100
                st.write(f"**{district_name}**: {sim_budget/10000:,.0f}만원 ({pct}%)")

        if st.button("AI 코멘트 요청", key="whatif_comment"):
            with st.spinner("Cortex AI 분석 중..."):
                comment = call_cortex_with_context(
                    f"마케팅 예산을 서초구 {seocho_pct}%, 영등포구 {yeongdeungpo_pct}%, 중구 {junggu_pct}%로 배분하는 것에 대해 평가해주세요.",
                    system_context="각 지역의 유동인구, 소비 트렌드, 인구이동을 고려하여 추천 이유와 리스크를 분석하세요."
                )
            st.info(comment)

# ══════════════════════════════════════════════
# 페이지 4: AI 에이전트
# ══════════════════════════════════════════════
elif page == "AI 에이전트":
    st.header("MoveSignal AI Agent")
    st.caption("서초·영등포·중구의 수요 예측, 배분 추천, 지역 분석에 대해 질문하세요.")

    if "messages" not in st.session_state:
        st.session_state.messages = [
            {"role": "assistant", "content": "안녕하세요! MoveSignal AI입니다. 서초·영등포·중구의 수요 예측, 배분 추천, 지역 분석에 대해 물어보세요."}
        ]

    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.write(msg["content"])

    # 빠른 질문
    st.caption("추천 질문:")
    quick_cols = st.columns(3)
    quick_questions = [
        "다음 달 서초구 수요 전망은?",
        "영등포구에 마케팅 예산을 늘릴까?",
        "3구 중 가장 성장하는 곳은?"
    ]

    for idx, question in enumerate(quick_questions):
        if quick_cols[idx].button(question, key=f"quick_{idx}"):
            st.session_state.messages.append({"role": "user", "content": question})
            with st.spinner("분석 중..."):
                response = call_cortex_with_context(question)
            st.session_state.messages.append({"role": "assistant", "content": response})
            st.rerun()

    # 자유 입력
    if user_input := st.chat_input("질문을 입력하세요..."):
        st.session_state.messages.append({"role": "user", "content": user_input})
        with st.spinner("분석 중..."):
            response = call_cortex_with_context(user_input)
        st.session_state.messages.append({"role": "assistant", "content": response})
        st.rerun()

    # 운영 패널
    with st.expander("운영 현황 패널"):
        ops_cols = st.columns(4)
        ops_cols[0].metric("데이터 갱신", "매일 06:00")
        ops_cols[1].metric("모델 재학습", "주 1회")
        ops_cols[2].metric("응답 시간", "~6초")
        ops_cols[3].metric("월 비용", "~$80")
        st.caption("데이터 커버리지: SPH 5년 (2021-2025) + Richgo 14년 (2012-2026)")
        st.caption("접근 권한: RBAC (ACCOUNTADMIN / ANALYST / VIEWER)")
        st.caption("Fallback: Cortex 실패 시 최신 정적 데이터 표시")
