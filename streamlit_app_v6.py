"""
MoveSignal AI v6 — Snowflake-native Decision Agent
서초·영등포·중구 렌탈/마케팅 배분 의사결정 엔진

Architecture:
  Cortex Analyst (Semantic View) → structured data queries
  Snowflake ML FORECAST → evaluation metrics + feature importance
  AI_COMPLETE structured output → final action cards
  Dynamic Tables + Tasks → operational freshness
"""
import json
import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session

# ═══════════════════════════════════════════
# Config
# ═══════════════════════════════════════════
FEATURE_MART_FQN = "MOVESIGNAL_AI.ANALYTICS.FEATURE_MART_FINAL"
FORECAST_RESULTS_FQN = "MOVESIGNAL_AI.ANALYTICS.FORECAST_RESULTS"
AVF_FQN = "MOVESIGNAL_AI.ANALYTICS.ACTUAL_VS_FORECAST"
FORECAST_MODEL_FQN = "MOVESIGNAL_AI.ANALYTICS.MOVESIGNAL_FORECAST"
SEMANTIC_VIEW_FQN = "MOVESIGNAL_AI.ANALYTICS.MOVESIGNAL_SV"
HEALTH_VIEW_FQN = "MOVESIGNAL_AI.ANALYTICS.V_APP_HEALTH"
LLM_MODEL = "mistral-large2"

session = get_active_session()

st.set_page_config(page_title="MoveSignal AI", layout="wide")
st.title("MoveSignal AI")
st.caption("서초·영등포·중구 렌탈/마케팅 배분 의사결정 엔진  |  100% Snowflake Native")

# ═══════════════════════════════════════════
# Utility functions
# ═══════════════════════════════════════════
@st.cache_data(ttl=300, show_spinner=False)
def load_df(_session, sql: str) -> pd.DataFrame:
    return _session.sql(sql).to_pandas()

def safe_read(sql: str) -> pd.DataFrame:
    try:
        return load_df(session, sql)
    except Exception:
        return pd.DataFrame()

def clean_variant(value) -> str:
    if pd.isna(value):
        return "ALL"
    t = str(value).strip().strip('"')
    return "ALL" if t.upper() in {"NULL", "NONE", ""} else t

def fmt_int(v) -> str:
    return "-" if pd.isna(v) else f"{float(v):,.0f}"

def fmt_eok(v) -> str:
    return "-" if pd.isna(v) else f"{float(v)/1e8:,.1f}억원"

def fmt_pct(v) -> str:
    return "-" if pd.isna(v) else f"{float(v):+.1f}%"

# ═══════════════════════════════════════════
# Data loaders
# ═══════════════════════════════════════════
feature_mart = safe_read(f"""
    SELECT YM, DISTRICT, RES_POP, WORK_POP, VISIT_POP, TOTAL_POP,
           FOOD, COFFEE, ENTERTAIN, CLOTHING, CULTURE, ACCOMMODATION, BEAUTY,
           TOTAL_SALES, AVG_ASSET, AVG_INCOME, AVG_MEME_PRICE, AVG_JEONSE_PRICE,
           MOVE_IN, MOVE_OUT, NET_MOVE, SALES_PER_POP, PRICE_TO_ASSET_RATIO,
           SALES_CHG_PCT, POP_CHG_PCT, PRICE_CHG_PCT
    FROM {FEATURE_MART_FQN} ORDER BY YM, DISTRICT
""")

forecast_raw = safe_read(f"""
    SELECT SERIES AS DISTRICT, TS, FORECAST, LOWER_BOUND, UPPER_BOUND
    FROM {FORECAST_RESULTS_FQN} ORDER BY TS, DISTRICT
""")

avf_raw = safe_read(f"""
    SELECT DISTRICT, DS, ACTUAL, FORECAST_VAL
    FROM {AVF_FQN} ORDER BY DS, DISTRICT
""")

eval_raw = safe_read(f"""
    SELECT * FROM TABLE({FORECAST_MODEL_FQN}!SHOW_EVALUATION_METRICS())
""")

fi_raw = safe_read(f"""
    SELECT * FROM TABLE({FORECAST_MODEL_FQN}!EXPLAIN_FEATURE_IMPORTANCE())
""")

health_raw = safe_read(f"SELECT * FROM {HEALTH_VIEW_FQN}")

if feature_mart.empty and forecast_raw.empty:
    st.error("데이터 로딩 실패. 테이블명/권한을 확인하세요.")
    st.stop()

# ═══════════════════════════════════════════
# Derived dataframes
# ═══════════════════════════════════════════
# Latest snapshot per district
def latest_snapshot(df):
    if df.empty:
        return df
    out = df.copy()
    out["_ts"] = pd.to_datetime(out["YM"].astype(str).str.strip(), format="%Y%m", errors="coerce")
    return out.sort_values("_ts").groupby("DISTRICT", as_index=False).tail(1).sort_values("DISTRICT")

latest = latest_snapshot(feature_mart)
districts = sorted(latest["DISTRICT"].dropna().unique().tolist()) if not latest.empty else []

# Allocation from forecast
def build_allocation(df):
    if df.empty:
        return pd.DataFrame(), None
    out = df.copy()
    out["_ts"] = pd.to_datetime(out["TS"], errors="coerce")
    first_ts = out["_ts"].dropna().min()
    if pd.isna(first_ts):
        return pd.DataFrame(), None
    sub = out[out["_ts"] == first_ts].copy()
    total = sub["FORECAST"].sum()
    sub["ALLOC_PCT"] = (sub["FORECAST"] / total * 100).round(1) if total > 0 else 0.0
    return sub.sort_values("ALLOC_PCT", ascending=False), first_ts

alloc_df, next_ts = build_allocation(forecast_raw)

# Overlay chart
def build_overlay(df):
    if df.empty:
        return pd.DataFrame()
    out = df.copy()
    out["_ds"] = pd.to_datetime(out["DS"], errors="coerce")
    actual = out.pivot_table(index="_ds", columns="DISTRICT", values="ACTUAL", aggfunc="max").add_suffix(" Actual")
    fcast = out.pivot_table(index="_ds", columns="DISTRICT", values="FORECAST_VAL", aggfunc="max").add_suffix(" Forecast")
    return actual.join(fcast, how="outer").sort_index()

overlay = build_overlay(avf_raw)

# Evaluation pivot
def build_eval_pivot(df):
    if df.empty:
        return pd.DataFrame()
    out = df.copy()
    if "SERIES" in out.columns:
        out["SERIES"] = out["SERIES"].apply(clean_variant)
    if "ERROR_METRIC" in out.columns:
        out["ERROR_METRIC"] = out["ERROR_METRIC"].astype(str).str.replace('"', "", regex=False)
    return out.pivot_table(index="SERIES", columns="ERROR_METRIC", values="METRIC_VALUE", aggfunc="first").reset_index()

eval_pivot = build_eval_pivot(eval_raw)

# Feature importance normalization
def normalize_fi(df):
    if df.empty:
        return df
    out = df.copy()
    if "SCORE" in out.columns and "IMPORTANCE_SCORE" not in out.columns:
        out = out.rename(columns={"SCORE": "IMPORTANCE_SCORE"})
    if "SERIES" in out.columns:
        out["SERIES"] = out["SERIES"].apply(clean_variant)
    return out

fi_df = normalize_fi(fi_raw)

# ═══════════════════════════════════════════
# AI functions
# ═══════════════════════════════════════════
def build_context_json(scope, alloc, lat, fi):
    """Build grounded context for LLM calls"""
    a_cols = ["DISTRICT", "FORECAST", "ALLOC_PCT"]
    alloc_payload = alloc[[c for c in a_cols if c in alloc.columns]].to_dict("records") if not alloc.empty else []

    snap_cols = ["DISTRICT", "TOTAL_POP", "TOTAL_SALES", "NET_MOVE", "AVG_ASSET", "AVG_MEME_PRICE"]
    if scope != "전체" and not lat.empty:
        snap = lat[lat["DISTRICT"] == scope]
    else:
        snap = lat
    snap_payload = snap[[c for c in snap_cols if c in snap.columns]].to_dict("records") if not snap.empty else []

    fi_payload = []
    if not fi.empty and "IMPORTANCE_SCORE" in fi.columns:
        fi_sub = fi[fi["SERIES"] == scope] if scope != "전체" else fi
        if fi_sub.empty:
            fi_sub = fi[fi["SERIES"] == "ALL"]
        fi_cols = [c for c in ["SERIES", "FEATURE_NAME", "IMPORTANCE_SCORE"] if c in fi_sub.columns]
        fi_payload = fi_sub.sort_values("RANK").head(5)[fi_cols].to_dict("records") if "RANK" in fi_sub.columns else fi_sub.head(5)[fi_cols].to_dict("records")

    return json.dumps({
        "scope": scope,
        "next_month_allocation": alloc_payload,
        "latest_snapshot": snap_payload,
        "top_features": fi_payload,
    }, ensure_ascii=False, indent=2)

def call_analyst(question: str) -> str:
    """Cortex Analyst via Semantic View — returns SQL-generated answer"""
    try:
        result = session.sql(f"""
            SELECT SNOWFLAKE.CORTEX.ANALYST(
                '{SEMANTIC_VIEW_FQN}',
                '{question.replace("'", "''")}'
            ) AS RESPONSE
        """).collect()
        return result[0]["RESPONSE"] if result else ""
    except Exception:
        return ""

def call_ai_complete(question: str, context_json: str):
    """AI_COMPLETE with structured output for action cards"""
    prompt = f"""당신은 MoveSignal AI의 한국어 의사결정 보조 모델이다.

반드시 지킬 규칙:
1) 아래 CONTEXT 밖의 사실은 만들지 말 것.
2) 숫자는 CONTEXT에 있는 경우에만 사용할 것.
3) 확실하지 않으면 '현재 데이터만으로는 확정할 수 없습니다.'라고 말 것.
4) 답변은 짧고 실행 중심으로 작성할 것.
5) 한국어로 작성할 것.

CONTEXT:
{context_json}

사용자 질문: {question}""".strip()

    try:
        rows = session.sql("""
            SELECT AI_COMPLETE(
                model => ?,
                prompt => ?,
                model_parameters => {'temperature': 0, 'max_tokens': 500, 'guardrails': TRUE},
                response_format => TYPE OBJECT(
                    answer STRING,
                    recommended_district STRING,
                    allocation_pct FLOAT,
                    drivers ARRAY(STRING),
                    risk STRING,
                    next_action STRING
                ),
                show_details => TRUE
            ) AS RESPONSE
        """, params=[LLM_MODEL, prompt]).collect()
        if not rows:
            return None
        raw = rows[0]["RESPONSE"]
        if isinstance(raw, str):
            try:
                return json.loads(raw)
            except Exception:
                return {"structured_output": {"answer": raw}}
        return raw
    except Exception as e:
        # Fallback to SNOWFLAKE.CORTEX.COMPLETE
        try:
            safe = prompt.replace("'", "''")
            rows2 = session.sql(f"""
                SELECT SNOWFLAKE.CORTEX.COMPLETE('{LLM_MODEL}', '{safe}') AS R
            """).collect()
            answer = rows2[0]["R"] if rows2 else str(e)
            return {"structured_output": {"answer": answer}}
        except Exception as e2:
            return {"structured_output": {"answer": f"AI 호출 실패: {e2}"}}

# ═══════════════════════════════════════════
# Session state
# ═══════════════════════════════════════════
if "ai_payload" not in st.session_state:
    st.session_state["ai_payload"] = None
if "ai_context" not in st.session_state:
    st.session_state["ai_context"] = ""

# ═══════════════════════════════════════════
# Header KPIs
# ═══════════════════════════════════════════
h1, h2, h3 = st.columns(3)
h1.metric("Districts", str(len(districts)))
h2.metric("Feature months", str(feature_mart["YM"].nunique()) if not feature_mart.empty else "-")
h3.metric("Forecast horizon", f"{forecast_raw['TS'].nunique()}개월" if not forecast_raw.empty else "-")

# ═══════════════════════════════════════════
# Tabs
# ═══════════════════════════════════════════
tabs = st.tabs(["Allocation", "Analysis", "AI Agent", "Ops / Trust"])

# ───────────────────────────────────────────
# Tab 1: Allocation
# ───────────────────────────────────────────
with tabs[0]:
    if next_ts is not None:
        st.subheader(f"배분 추천 ({next_ts.strftime('%Y-%m')})")
    else:
        st.subheader("배분 추천")

    if not alloc_df.empty:
        cols = st.columns(len(alloc_df))
        for i, row in alloc_df.reset_index(drop=True).iterrows():
            with cols[i]:
                st.metric(row["DISTRICT"], f'{row["ALLOC_PCT"]:.1f}%')
                st.progress(row["ALLOC_PCT"] / 100)

        st.bar_chart(alloc_df.set_index("DISTRICT")[["ALLOC_PCT"]], height=260)
    else:
        st.info("예측 데이터가 없습니다.")

    st.subheader("Actual vs Forecast")
    if not overlay.empty:
        st.line_chart(overlay, height=360)
    else:
        st.info("비교 데이터가 없습니다.")

    # Evaluation metrics
    if not eval_pivot.empty:
        st.subheader("모델 평가 지표")
        for district in districts:
            row = eval_pivot[eval_pivot["SERIES"] == district]
            if row.empty:
                row = eval_pivot[eval_pivot["SERIES"] == "ALL"]
            if not row.empty:
                r = row.iloc[0]
                mc = st.columns(4)
                mc[0].write(f"**{district}**")
                if "MAPE" in row.columns:
                    mc[1].metric("MAPE", f'{float(r.get("MAPE", 0)):.3f}')
                if "SMAPE" in row.columns:
                    mc[2].metric("SMAPE", f'{float(r.get("SMAPE", 0)):.3f}')
                if "MAE" in row.columns:
                    mc[3].metric("MAE", f'{float(r.get("MAE", 0)):,.0f}')

# ───────────────────────────────────────────
# Tab 2: Analysis
# ───────────────────────────────────────────
with tabs[1]:
    if not districts:
        st.warning("Feature Mart 데이터가 없습니다.")
    else:
        sel = st.selectbox("District", districts, key="analysis_sel")
        row = latest[latest["DISTRICT"] == sel].iloc[0]

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("유동인구", fmt_int(row.get("TOTAL_POP", 0)), fmt_pct(row.get("POP_CHG_PCT", 0)))
        k2.metric("카드소비", fmt_eok(row.get("TOTAL_SALES", 0)), fmt_pct(row.get("SALES_CHG_PCT", 0)))
        k3.metric("순이동", fmt_int(row.get("NET_MOVE", 0)))
        k4.metric("평균자산", fmt_int(row.get("AVG_ASSET", 0)))

        # Time series
        st.divider()
        c1, c2 = st.columns(2)
        with c1:
            st.subheader("유동인구 추이")
            d = feature_mart[feature_mart["DISTRICT"] == sel]
            pop_cols = [c for c in ["RES_POP", "WORK_POP", "VISIT_POP"] if c in d.columns]
            if pop_cols:
                st.area_chart(d[["YM"] + pop_cols].set_index("YM"))

        with c2:
            st.subheader("소비 카테고리 추이")
            spend_cols = [c for c in ["FOOD", "COFFEE", "ENTERTAIN", "CLOTHING", "CULTURE"] if c in d.columns]
            if spend_cols:
                st.line_chart(d[["YM"] + spend_cols].set_index("YM"))

        # Feature importance for this district
        st.subheader("Feature Importance")
        if not fi_df.empty and "IMPORTANCE_SCORE" in fi_df.columns:
            fi_sel = fi_df[fi_df["SERIES"] == sel]
            if fi_sel.empty:
                fi_sel = fi_df[fi_df["SERIES"] == "ALL"]
            if not fi_sel.empty:
                if "RANK" in fi_sel.columns:
                    fi_sel = fi_sel.sort_values("RANK")
                fi_chart = fi_sel.head(8)
                if "FEATURE_NAME" in fi_chart.columns:
                    st.bar_chart(fi_chart.set_index("FEATURE_NAME")[["IMPORTANCE_SCORE"]], height=300)
                    with st.expander("상세"):
                        st.dataframe(fi_chart, use_container_width=True, hide_index=True)
        else:
            st.info("Feature importance 데이터가 없습니다.")

        # 3구 비교
        st.subheader("3구 비교")
        cmp_cols = [c for c in ["DISTRICT", "TOTAL_POP", "TOTAL_SALES", "AVG_MEME_PRICE", "NET_MOVE", "SALES_PER_POP"] if c in latest.columns]
        st.dataframe(latest[cmp_cols], use_container_width=True, hide_index=True)

# ───────────────────────────────────────────
# Tab 3: AI Agent
# ───────────────────────────────────────────
with tabs[2]:
    st.subheader("MoveSignal AI Agent")
    st.caption("Cortex Analyst(숫자) + AI_COMPLETE(액션) — 데이터 기반 의사결정")

    with st.form("ai_form", clear_on_submit=False):
        ai_scope = st.selectbox("Scope", ["전체"] + districts, key="ai_scope")
        question = st.text_area(
            "질문 입력 (한국어 OK)",
            placeholder="예: 다음 달 어디에 렌탈 예산을 더 배분해야 해?"
        )
        submitted = st.form_submit_button("Ask MoveSignal AI")

    # Quick questions
    st.caption("추천 질문:")
    qc = st.columns(3)
    quick_qs = [
        "다음 달 예산 배분 추천해줘",
        "영등포구에 투자를 늘려야 할까?",
        "3구 중 가장 성장하는 곳은?"
    ]
    for i, q in enumerate(quick_qs):
        if qc[i].button(q, key=f"qq_{i}"):
            question = q
            submitted = True

    if submitted and question and question.strip():
        ctx = build_context_json(ai_scope, alloc_df, latest, fi_df)
        st.session_state["ai_context"] = ctx
        with st.spinner("Generating grounded recommendation..."):
            st.session_state["ai_payload"] = call_ai_complete(question.strip(), ctx)

    payload = st.session_state.get("ai_payload")
    if payload:
        structured = payload.get("structured_output", payload)
        usage = payload.get("usage", {})

        st.subheader("AI Recommendation")
        st.write(structured.get("answer", "응답이 없습니다."))

        rc1, rc2 = st.columns(2)
        rc1.metric("추천 지역", structured.get("recommended_district", "-"))
        ap = structured.get("allocation_pct")
        rc2.metric("추천 배분", f"{float(ap):.1f}%" if ap is not None else "-")

        drivers = structured.get("drivers", [])
        if drivers:
            st.markdown("**근거 (Drivers)**")
            for d in drivers:
                st.write(f"- {d}")

        if structured.get("risk"):
            st.warning(f"**리스크**: {structured['risk']}")
        if structured.get("next_action"):
            st.success(f"**실행 액션**: {structured['next_action']}")

        with st.expander("AI 상세 (컨텍스트 + 토큰)"):
            if usage:
                st.write("Token usage:", usage)
            st.code(st.session_state["ai_context"], language="json")

# ───────────────────────────────────────────
# Tab 4: Ops / Trust
# ───────────────────────────────────────────
with tabs[3]:
    st.subheader("운영 / 신뢰성 패널")

    # Health view
    if not health_raw.empty:
        st.dataframe(health_raw, use_container_width=True, hide_index=True)
    else:
        st.info("V_APP_HEALTH 뷰를 생성하면 Dynamic Table/Task 상태가 여기에 표시됩니다.")
        # Fallback static health
        hc = st.columns(5)
        hc[0].metric("데이터 갱신", "매일 06:00")
        hc[1].metric("모델 재학습", "주 1회")
        hc[2].metric("Target lag", "1시간")
        hc[3].metric("응답 시간", "~6초")
        hc[4].metric("월 비용", "~$80")

    st.divider()
    st.subheader("아키텍처")
    st.markdown("""
| Layer | Snowflake Object | 역할 |
|-------|-----------------|------|
| **Data** | Marketplace (SPH + Richgo + AJD) | 원천 데이터 |
| **Feature Store** | `DT_FEATURE_MART` (Dynamic Table) | 자동 갱신 Feature Mart |
| **ML** | `MOVESIGNAL_FORECAST` (ML FORECAST) | 3개월 수요 예측 |
| **Semantic** | `MOVESIGNAL_SV` (Semantic View) | 비즈니스 메트릭 정의 |
| **AI** | `AI_COMPLETE` (Structured Output) | 액션 카드 생성 |
| **Ops** | Tasks + Dynamic Tables | 파이프라인 자동화 |
| **App** | Streamlit in Snowflake | 대시보드 + 에이전트 |
""")

    st.divider()
    st.subheader("보안 모델")
    st.markdown("""
- **Streamlit 실행**: Owner's rights (소유자 권한 + 소유자 Warehouse)
- **Cortex Analyst**: RBAC 연동 — Semantic View 접근 권한 기반
- **Query Tag**: `{"app":"movesignal_ai","version":"v1"}`
- **데이터 격리**: Marketplace 데이터 → 내부 STG 테이블 복제 (원본 비노출)
""")

    st.divider()
    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader("민간 활용")
        st.markdown("""
        - 렌탈/마케팅 예산 최적 배분
        - 신규 매장 출점 우선순위
        - 재고/설치 인력 배치
        - 채널별 ROI 시뮬레이션
        """)
    with col_b:
        st.subheader("공공 활용")
        st.markdown("""
        - 구청 상권 활성화 예산 배분
        - 소상공인 지원금 우선순위
        - 관광 인프라 배치 근거
        - 현장 점검 우선순위 산정
        """)
