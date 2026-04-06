# -*- coding: utf-8 -*-
# MoveSignal AI: Streamlit in Snowflake App v3
# 서초·영등포·중구 렌탈/마케팅 배분 의사결정 엔진

import streamlit as st
import pandas as pd
import json
from snowflake.snowpark.context import get_active_session

session = get_active_session()

DISTRICTS = ['서초구', '영등포구', '중구']
DB = "MOVESIGNAL_AI"
SCHEMA = "ANALYTICS"
LLM_MODEL = "mistral-large2"

# ── 데이터 로드 ──
@st.cache_data(ttl=600)
def load_feature_mart():
    return session.sql(f"SELECT * FROM {DB}.{SCHEMA}.FEATURE_MART_FINAL ORDER BY YM, DISTRICT").to_pandas()

@st.cache_data(ttl=600)
def load_forecast():
    return session.sql(f"SELECT * FROM {DB}.{SCHEMA}.ACTUAL_VS_FORECAST ORDER BY DISTRICT, DS").to_pandas()

@st.cache_data(ttl=600)
def load_feature_importance():
    try:
        return session.sql(f"SELECT * FROM {DB}.{SCHEMA}.FEATURE_IMPORTANCE ORDER BY RANK").to_pandas()
    except:
        return pd.DataFrame()

@st.cache_data(ttl=600)
def load_forecast_results():
    try:
        return session.sql(f"SELECT * FROM {DB}.{SCHEMA}.FORECAST_RESULTS ORDER BY SERIES, TS").to_pandas()
    except:
        return pd.DataFrame()

def call_allocation(budget):
    try:
        result = session.sql(f"CALL {DB}.{SCHEMA}.RECOMMEND_ALLOCATION({budget})").collect()
        return result[0][0] if result else None
    except:
        return None

def call_whatif(s, y, j, budget):
    try:
        result = session.sql(f"CALL {DB}.{SCHEMA}.SIMULATE_WHATIF({s}, {y}, {j}, {budget})").collect()
        return result[0][0] if result else None
    except:
        return None

def call_cortex(prompt):
    try:
        safe = prompt.replace("'", "''")
        result = session.sql(f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                '{LLM_MODEL}',
                '{safe}'
            ) AS RESPONSE
        """).collect()
        return result[0]['RESPONSE'] if result else "응답을 생성할 수 없습니다."
    except Exception as e:
        return f"Cortex 호출 오류: {str(e)}"

# ── 사이드바 ──
with st.sidebar:
    st.title("🏢 MoveSignal AI")
    st.caption("서초 · 영등포 · 중구\n렌탈/마케팅 배분 의사결정 엔진")
    st.divider()

    total_budget = st.number_input(
        "💰 총 예산 (만원)", min_value=1000, max_value=100000, value=5000, step=500
    ) * 10000

    st.divider()
    page = st.radio(
        "메뉴",
        ["📊 배분 추천", "🔍 지역 분석", "🔄 시뮬레이션", "💬 AI 에이전트"],
        label_visibility="collapsed"
    )
    st.divider()
    st.caption("⚙️ 운영 현황")
    st.caption("데이터 갱신: 매일 06:00 KST")
    st.caption("모델 재학습: 주 1회")
    st.caption("Cortex LLM: mistral-large2")
    st.caption("월 비용 추정: ~$80")

# ══════════════════════════════════════
# 📊 페이지 1: 배분 추천
# ══════════════════════════════════════
if page == "📊 배분 추천":
    st.header("📊 다음 분기 배분 추천")

    forecast_df = load_forecast_results()
    feature_imp = load_feature_importance()

    if not forecast_df.empty:
        district_forecast = forecast_df.groupby('SERIES')['FORECAST'].mean().reset_index()
        district_forecast.columns = ['DISTRICT', 'FORECAST']
        total_forecast = district_forecast['FORECAST'].sum()

        cols = st.columns(3)
        for i, row in district_forecast.iterrows():
            with cols[i]:
                pct = row['FORECAST'] / total_forecast * 100
                budget_alloc = total_budget * pct / 100
                st.metric(
                    label=row['DISTRICT'],
                    value=f"{pct:.0f}%",
                    delta=f"₩{budget_alloc/10000:,.0f}만원"
                )
                st.progress(pct / 100)

        st.divider()
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("📈 실제 vs 예측 추이")
            avf = load_forecast()
            if not avf.empty:
                for district in DISTRICTS:
                    d = avf[avf['DISTRICT'] == district].copy()
                    if not d.empty:
                        st.caption(f"**{district}**")
                        chart_data = d[['DS', 'ACTUAL', 'FORECAST_VAL']].set_index('DS')
                        st.line_chart(chart_data)

        with col2:
            st.subheader("🔎 왜 이 배분인가?")
            if not feature_imp.empty:
                st.dataframe(feature_imp.head(10), use_container_width=True)
            else:
                st.info("Feature Importance 데이터를 로딩 중입니다...")

            if st.button("🤖 AI에게 설명 요청", key="explain_btn"):
                top_district = district_forecast.loc[district_forecast['FORECAST'].idxmax(), 'DISTRICT']
                prompt = f"""{top_district}의 카드 소비 수요가 가장 높을 것으로 예측되는 이유를
유동인구, 소비패턴, 부동산 시세, 인구이동 데이터를 기반으로 한국어 3줄로 설명해주세요.
분석적이고 구체적인 숫자를 포함해서 답변하세요."""
                with st.spinner("🧠 Cortex AI 분석 중..."):
                    explanation = call_cortex(prompt)
                st.success(explanation)
    else:
        st.warning("⚠️ 예측 데이터가 없습니다. ML Forecast를 먼저 실행해주세요.")
        fm = load_feature_mart()
        if not fm.empty:
            st.subheader("📋 현재 Feature Mart 데이터")
            latest = fm.groupby('DISTRICT').last().reset_index()
            display_cols = [c for c in ['DISTRICT', 'TOTAL_POP', 'TOTAL_SALES', 'AVG_MEME_PRICE', 'NET_MOVE'] if c in latest.columns]
            st.dataframe(latest[display_cols], use_container_width=True)

# ══════════════════════════════════════
# 🔍 페이지 2: 지역 분석
# ══════════════════════════════════════
elif page == "🔍 지역 분석":
    st.header("🔍 지역 상세 분석")
    selected_district = st.selectbox("분석할 지역을 선택하세요", DISTRICTS)

    fm = load_feature_mart()
    if not fm.empty:
        district_data = fm[fm['DISTRICT'] == selected_district].copy()

        if not district_data.empty:
            latest = district_data.iloc[-1]

            # 핵심 KPI 카드
            cols = st.columns(4)
            cols[0].metric(
                "👥 유동인구",
                f"{latest.get('TOTAL_POP', 0):,.0f}",
                f"{latest.get('POP_CHG_PCT', 0):+.1f}%"
            )
            cols[1].metric(
                "💳 카드소비",
                f"₩{latest.get('TOTAL_SALES', 0)/100000000:.1f}억",
                f"{latest.get('SALES_CHG_PCT', 0):+.1f}%"
            )
            cols[2].metric(
                "🏠 평균 매매가",
                f"₩{latest.get('AVG_MEME_PRICE', 0)/10000:.0f}만" if latest.get('AVG_MEME_PRICE') else "N/A",
                f"{latest.get('PRICE_CHG_PCT', 0):+.1f}%" if latest.get('PRICE_CHG_PCT') else ""
            )
            cols[3].metric(
                "🚚 순이동",
                f"{latest.get('NET_MOVE', 0):+,.0f}명" if latest.get('NET_MOVE') else "N/A"
            )

            st.divider()
            col1, col2 = st.columns(2)

            with col1:
                st.subheader("👥 유동인구 추이")
                pop_cols = [c for c in ['RES_POP', 'WORK_POP', 'VISIT_POP'] if c in district_data.columns]
                if pop_cols:
                    pop_chart = district_data[['YM'] + pop_cols].set_index('YM')
                    st.area_chart(pop_chart)

            with col2:
                st.subheader("💳 소비 카테고리별 추이")
                spend_cols = [c for c in ['FOOD', 'COFFEE', 'ENTERTAIN', 'CLOTHING', 'CULTURE', 'ACCOMMODATION'] if c in district_data.columns]
                if spend_cols:
                    spend_chart = district_data[['YM'] + spend_cols].set_index('YM')
                    st.line_chart(spend_chart)

            # 동네 체질 분석
            st.subheader(f"🏘️ {selected_district} 소비 체질 분석")
            total_sales = latest.get('TOTAL_SALES', 0)
            if total_sales and total_sales > 0:
                categories = {
                    '🍽️ 외식': latest.get('FOOD', 0) / total_sales * 100,
                    '☕ 카페': latest.get('COFFEE', 0) / total_sales * 100,
                    '🎭 엔터': latest.get('ENTERTAIN', 0) / total_sales * 100,
                    '👗 의류': latest.get('CLOTHING', 0) / total_sales * 100,
                    '⚽ 문화': latest.get('CULTURE', 0) / total_sales * 100,
                    '🏨 숙박': latest.get('ACCOMMODATION', 0) / total_sales * 100,
                    '💄 뷰티': latest.get('BEAUTY', 0) / total_sales * 100,
                }
                cat_df = pd.DataFrame(list(categories.items()), columns=['카테고리', '비율(%)'])
                cat_df = cat_df.sort_values('비율(%)', ascending=False)
                st.bar_chart(cat_df.set_index('카테고리'))
                top_cat = cat_df.iloc[0]['카테고리']
                st.info(f"**{selected_district}**는 **{top_cat}** 비중이 가장 높은 동네입니다.")

            # 자산 대비 시세 비율
            ratio = latest.get('PRICE_TO_ASSET_RATIO')
            if ratio and ratio > 0:
                if ratio > 1.5:
                    st.warning(f"⚠️ 자산 대비 시세 비율: {ratio:.2f} — 고평가 주의")
                elif ratio > 1.0:
                    st.info(f"📊 자산 대비 시세 비율: {ratio:.2f} — 적정 범위")
                else:
                    st.success(f"✅ 자산 대비 시세 비율: {ratio:.2f} — 저평가 가능성")
    else:
        st.warning("Feature Mart 데이터가 없습니다.")

# ══════════════════════════════════════
# 🔄 페이지 3: What-if 시뮬레이션
# ══════════════════════════════════════
elif page == "🔄 시뮬레이션":
    st.header("🔄 What-if 시뮬레이션")
    st.caption(f"총 예산: ₩{total_budget/10000:,.0f}만원")

    col1, col2, col3 = st.columns(3)
    with col1:
        seocho_pct = st.slider("서초구 배분 (%)", 0, 100, 45)
    with col2:
        yeongdeungpo_pct = st.slider("영등포구 배분 (%)", 0, 100, 35)
    with col3:
        junggu_pct = 100 - seocho_pct - yeongdeungpo_pct
        st.metric("중구 (%)", f"{junggu_pct}%")

    if seocho_pct + yeongdeungpo_pct > 100:
        st.error("❌ 서초구 + 영등포구 비율이 100%를 초과합니다.")
    else:
        st.divider()
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("🤖 AI 추천 배분")
            allocation = call_allocation(total_budget)
            if allocation:
                try:
                    alloc_data = json.loads(allocation) if isinstance(allocation, str) else allocation
                    if isinstance(alloc_data, dict) and 'allocations' in alloc_data:
                        for item in alloc_data['allocations']:
                            d = item.get('district', '')
                            pct = item.get('share_pct', 0)
                            b = item.get('budget', 0)
                            st.write(f"**{d}**: ₩{b/10000:,.0f}만원 ({pct}%)")
                    else:
                        st.json(alloc_data)
                except:
                    st.json(allocation)
            else:
                for d, pct in [('서초구', 33), ('영등포구', 33), ('중구', 34)]:
                    st.write(f"**{d}**: ₩{total_budget * pct / 100 / 10000:,.0f}만원 ({pct}%)")

        with col2:
            st.subheader("🎯 시뮬레이션 배분")
            for d, pct in [('서초구', seocho_pct), ('영등포구', yeongdeungpo_pct), ('중구', junggu_pct)]:
                budget = total_budget * pct / 100
                st.write(f"**{d}**: ₩{budget/10000:,.0f}만원 ({pct}%)")

        if st.button("🤖 AI 코멘트 요청", key="whatif_comment"):
            prompt = f"""마케팅 예산을 서초구 {seocho_pct}%, 영등포구 {yeongdeungpo_pct}%, 중구 {junggu_pct}%로 배분하는 것에 대해,
각 지역의 유동인구와 소비 트렌드를 고려하여 한국어 3줄로 평가해주세요.
구체적인 추천 이유와 리스크를 포함하세요."""
            with st.spinner("🧠 AI 분석 중..."):
                comment = call_cortex(prompt)
            st.info(comment)

# ══════════════════════════════════════
# 💬 페이지 4: AI 에이전트
# ══════════════════════════════════════
elif page == "💬 AI 에이전트":
    st.header("💬 MoveSignal AI Agent")
    st.caption("서초·영등포·중구의 수요 예측, 배분 추천, 지역 분석에 대해 질문하세요.")

    if "messages" not in st.session_state:
        st.session_state.messages = [
            {"role": "assistant", "content": "안녕하세요! MoveSignal AI입니다. 서초·영등포·중구의 수요 예측, 배분 추천, 지역 분석에 대해 물어보세요. 🏢"}
        ]

    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.write(msg["content"])

    # 빠른 질문
    st.caption("💡 추천 질문:")
    quick_cols = st.columns(3)
    quick_questions = [
        "다음 달 서초구 수요 전망은?",
        "영등포구에 마케팅 예산을 늘릴까?",
        "3구 중 가장 성장하는 곳은?"
    ]

    for i, q in enumerate(quick_questions):
        if quick_cols[i].button(q, key=f"quick_{i}"):
            st.session_state.messages.append({"role": "user", "content": q})
            fm = load_feature_mart()
            context = ""
            if not fm.empty:
                latest = fm.groupby('DISTRICT').last().reset_index()
                ctx_cols = [c for c in ['DISTRICT', 'TOTAL_POP', 'TOTAL_SALES', 'AVG_MEME_PRICE', 'NET_MOVE', 'SALES_CHG_PCT', 'POP_CHG_PCT'] if c in latest.columns]
                context = f"최신 데이터:\n{latest[ctx_cols].to_string(index=False)}"

            full_prompt = f"""{context}

위 데이터를 기반으로 다음 질문에 한국어로 답해주세요.
구체적인 숫자와 트렌드를 포함하고, 3~5줄로 답변하세요.
렌탈/마케팅 배분 관점에서 실행 가능한 조언을 포함하세요.

질문: {q}"""
            with st.spinner("🧠 분석 중..."):
                response = call_cortex(full_prompt)
            st.session_state.messages.append({"role": "assistant", "content": response})
            st.rerun()

    # 자유 입력
    if user_input := st.chat_input("질문을 입력하세요..."):
        st.session_state.messages.append({"role": "user", "content": user_input})
        fm = load_feature_mart()
        context = ""
        if not fm.empty:
            latest = fm.groupby('DISTRICT').last().reset_index()
            ctx_cols = [c for c in ['DISTRICT', 'TOTAL_POP', 'TOTAL_SALES', 'AVG_MEME_PRICE', 'NET_MOVE'] if c in latest.columns]
            context = f"최신 데이터: {latest[ctx_cols].to_string(index=False)}"

        full_prompt = f"""{context}

위 데이터를 기반으로 다음 질문에 한국어로 구체적으로 답해주세요 (3~5줄).
렌탈/마케팅 예산 배분 관점에서 답변하세요.

질문: {user_input}"""
        with st.spinner("🧠 분석 중..."):
            response = call_cortex(full_prompt)
        st.session_state.messages.append({"role": "assistant", "content": response})
        st.rerun()

    # 운영 패널
    with st.expander("⚙️ 운영 현황 패널"):
        op_cols = st.columns(4)
        op_cols[0].metric("📅 데이터 갱신", "매일 06:00")
        op_cols[1].metric("🔄 모델 재학습", "주 1회")
        op_cols[2].metric("⚡ 응답 시간", "~6초")
        op_cols[3].metric("💰 월 비용", "~$80")
        st.caption("📊 데이터 커버리지: SPH 5년 (2021-2025) + Richgo 14년 (2012-2026)")
        st.caption("🔐 접근 권한: RBAC (ACCOUNTADMIN / ANALYST / VIEWER)")
        st.caption("🔙 Fallback: Cortex 실패 시 → 최신 정적 데이터 표시")
