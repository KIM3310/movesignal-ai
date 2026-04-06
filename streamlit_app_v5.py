import json
import pandas as pd
import streamlit as st
from snowflake.snowpark.context import get_active_session

# =========================
# Config
# =========================
FEATURE_MART_FQN = "MOVESIGNAL_AI.ANALYTICS.FEATURE_MART_FINAL"
FORECAST_RESULTS_FQN = "MOVESIGNAL_AI.ANALYTICS.FORECAST_RESULTS"
ACTUAL_VS_FORECAST_FQN = "MOVESIGNAL_AI.ANALYTICS.ACTUAL_VS_FORECAST"
FORECAST_MODEL_FQN = "MOVESIGNAL_AI.ANALYTICS.MOVESIGNAL_FORECAST"
LLM_MODEL = "mistral-large2"

session = get_active_session()

st.set_page_config(page_title="MoveSignal AI", layout="wide")
st.title("MoveSignal AI")
st.caption("서초·영등포·중구 렌탈/마케팅 배분 의사결정 엔진")

# =========================
# Helpers
# =========================
@st.cache_data(ttl=300, show_spinner=False)
def load_df(_session, sql: str) -> pd.DataFrame:
    return _session.sql(sql).to_pandas()

def safe_read(sql: str) -> pd.DataFrame:
    try:
        return load_df(session, sql)
    except Exception:
        return pd.DataFrame()

def parse_month_col(series: pd.Series) -> pd.Series:
    if series.empty:
        return pd.Series(dtype="datetime64[ns]")
    text = series.astype(str).str.strip()
    for fmt in ("%Y-%m", "%Y%m", "%Y.%m", "%Y/%m"):
        parsed = pd.to_datetime(text, format=fmt, errors="coerce")
        if parsed.notna().any():
            return parsed
    return pd.to_datetime(text, errors="coerce")

def clean_variant_text(value) -> str:
    if pd.isna(value):
        return "ALL"
    text = str(value).strip()
    if text.startswith('"') and text.endswith('"'):
        text = text[1:-1]
    if text.upper() in {"NULL", "NONE", ""}:
        return "ALL"
    return text

def normalize_eval_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if "SERIES" in out.columns:
        out["SERIES"] = out["SERIES"].apply(clean_variant_text)
    if "ERROR_METRIC" in out.columns:
        out["ERROR_METRIC"] = out["ERROR_METRIC"].astype(str).str.replace('"', "", regex=False)
    return out

def normalize_feature_importance_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if "SCORE" in out.columns and "IMPORTANCE_SCORE" not in out.columns:
        out = out.rename(columns={"SCORE": "IMPORTANCE_SCORE"})
    if "SERIES" in out.columns:
        out["SERIES"] = out["SERIES"].apply(clean_variant_text)
    return out

def latest_snapshot(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    out["YM_TS"] = parse_month_col(out["YM"])
    out = out.sort_values(["YM_TS", "DISTRICT", "YM"])
    return out.groupby("DISTRICT", as_index=False).tail(1).sort_values("DISTRICT")

def build_allocation(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Timestamp | None]:
    if df.empty:
        return pd.DataFrame(), None
    out = df.copy()
    out["TS_TS"] = pd.to_datetime(out["TS"], errors="coerce")
    target_ts = out["TS_TS"].dropna().min()
    if pd.isna(target_ts):
        return pd.DataFrame(), None
    next_df = out[out["TS_TS"] == target_ts].copy()
    total = next_df["FORECAST"].sum()
    next_df["ALLOCATION_PCT"] = 0.0 if total == 0 else (next_df["FORECAST"] / total * 100).round(1)
    return next_df.sort_values("ALLOCATION_PCT", ascending=False), target_ts

def build_overlay_chart_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    out = df.copy()
    out["DS_TS"] = pd.to_datetime(out["DS"], errors="coerce")
    actual = (
        out.pivot_table(index="DS_TS", columns="DISTRICT", values="ACTUAL", aggfunc="max")
        .add_suffix(" Actual")
    )
    forecast = (
        out.pivot_table(index="DS_TS", columns="DISTRICT", values="FORECAST_VAL", aggfunc="max")
        .add_suffix(" Forecast")
    )
    return actual.join(forecast, how="outer").sort_index()

def build_eval_pivot(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = normalize_eval_df(df)
    return (
        out.pivot_table(index="SERIES", columns="ERROR_METRIC", values="METRIC_VALUE", aggfunc="first")
        .reset_index()
    )

def top_features_for_district(df: pd.DataFrame, district: str, top_n: int = 8) -> pd.DataFrame:
    if df.empty:
        return df
    out = normalize_feature_importance_df(df)
    selected = out[out["SERIES"] == district].copy()
    if selected.empty:
        selected = out[out["SERIES"] == "ALL"].copy()
    if "RANK" in selected.columns:
        selected = selected.sort_values("RANK")
    return selected.head(top_n)

def build_ai_context(
    selected_district: str,
    allocation_df: pd.DataFrame,
    latest_df: pd.DataFrame,
    fi_df: pd.DataFrame,
) -> str:
    alloc_payload = allocation_df[["DISTRICT", "FORECAST", "ALLOCATION_PCT"]].to_dict("records")

    if selected_district == "전체":
        snapshot_payload = latest_df[
            ["DISTRICT", "TOTAL_POP", "TOTAL_SALES", "NET_MOVE", "AVG_ASSET", "AVG_MEME_PRICE", "AVG_JEONSE_PRICE"]
        ].to_dict("records")
        fi_payload = (
            normalize_feature_importance_df(fi_df)
            .sort_values(["SERIES", "RANK"])
            .groupby("SERIES", as_index=False)
            .head(5)[["SERIES", "FEATURE_NAME", "IMPORTANCE_SCORE", "FEATURE_TYPE"]]
            .to_dict("records")
            if not fi_df.empty
            else []
        )
    else:
        snapshot_payload = latest_df[
            latest_df["DISTRICT"] == selected_district
        ][["DISTRICT", "TOTAL_POP", "TOTAL_SALES", "NET_MOVE", "AVG_ASSET", "AVG_MEME_PRICE", "AVG_JEONSE_PRICE"]].to_dict("records")
        fi_payload = top_features_for_district(fi_df, selected_district, 5)[
            ["SERIES", "FEATURE_NAME", "IMPORTANCE_SCORE", "FEATURE_TYPE"]
        ].to_dict("records") if not fi_df.empty else []

    payload = {
        "scope": selected_district,
        "next_month_allocation": alloc_payload,
        "latest_snapshot": snapshot_payload,
        "top_feature_importance": fi_payload,
    }
    return json.dumps(payload, ensure_ascii=False, indent=2)

def ask_ai(question: str, selected_district: str, context_json: str) -> dict | None:
    prompt = f"""
당신은 MoveSignal AI의 한국어 의사결정 보조 모델이다.

반드시 지킬 규칙:
1) 아래 CONTEXT 밖의 사실은 만들지 말 것.
2) 숫자는 CONTEXT에 있는 경우에만 사용할 것.
3) 확실하지 않으면 '현재 제공된 데이터만으로는 확정할 수 없습니다.'라고 말할 것.
4) 답변은 짧고 실행 중심으로 작성할 것.
5) 출력은 한국어로 작성할 것.

CONTEXT:
{context_json}

사용자 질문:
{question}
""".strip()

    sql = """
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
    """

    rows = session.sql(sql, params=[LLM_MODEL, prompt]).collect()
    if not rows:
        return None

    raw = rows[0]["RESPONSE"]
    if raw is None:
        return None

    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except Exception:
            return {"structured_output": {"answer": raw}}

    return raw

def fmt_int(v) -> str:
    if pd.isna(v):
        return "-"
    return f"{float(v):,.0f}"

def fmt_eok(v) -> str:
    if pd.isna(v):
        return "-"
    return f"{float(v) / 1e8:,.1f}억원"

# =========================
# Load data
# =========================
feature_mart_df = safe_read(f"""
SELECT
    YM, DISTRICT, RES_POP, WORK_POP, VISIT_POP, TOTAL_POP,
    FOOD, COFFEE, ENTERTAIN, CLOTHING, CULTURE, ACCOMMODATION, BEAUTY,
    TOTAL_SALES, AVG_ASSET, AVG_MEME_PRICE, AVG_JEONSE_PRICE,
    MOVE_IN, MOVE_OUT, NET_MOVE
FROM {FEATURE_MART_FQN}
ORDER BY YM, DISTRICT
""")

forecast_df = safe_read(f"""
SELECT
    SERIES AS DISTRICT, TS, FORECAST
FROM {FORECAST_RESULTS_FQN}
ORDER BY TS, DISTRICT
""")

actual_vs_forecast_df = safe_read(f"""
SELECT
    DISTRICT, DS, ACTUAL, FORECAST_VAL
FROM {ACTUAL_VS_FORECAST_FQN}
ORDER BY DS, DISTRICT
""")

evaluation_df = normalize_eval_df(safe_read(f"""
SELECT *
FROM TABLE({FORECAST_MODEL_FQN}!SHOW_EVALUATION_METRICS())
"""))

feature_importance_df = normalize_feature_importance_df(safe_read(f"""
SELECT *
FROM TABLE({FORECAST_MODEL_FQN}!EXPLAIN_FEATURE_IMPORTANCE())
"""))

if feature_mart_df.empty and forecast_df.empty:
    st.error("기본 데이터 로딩에 실패했습니다. 테이블명/권한을 확인하세요.")
    st.stop()

latest_df = latest_snapshot(feature_mart_df)
allocation_df, next_ts = build_allocation(forecast_df)
overlay_df = build_overlay_chart_df(actual_vs_forecast_df)
eval_pivot_df = build_eval_pivot(evaluation_df)

district_options = []
if not latest_df.empty:
    district_options = sorted(latest_df["DISTRICT"].dropna().astype(str).unique().tolist())

if "ai_payload" not in st.session_state:
    st.session_state["ai_payload"] = None
if "ai_context" not in st.session_state:
    st.session_state["ai_context"] = ""

# =========================
# Header KPIs
# =========================
top_left, top_mid, top_right = st.columns(3)
top_left.metric("Districts", str(len(district_options)) if district_options else "-")
top_mid.metric("Feature months", str(feature_mart_df["YM"].nunique()) if not feature_mart_df.empty else "-")
top_right.metric("Forecast horizon", str(forecast_df["TS"].nunique()) if not forecast_df.empty else "-")

tabs = st.tabs(["Allocation", "Analysis", "AI"])

# =========================
# Tab 1: Allocation
# =========================
with tabs[0]:
    if next_ts is not None:
        st.subheader(f"Next-month allocation recommendation ({next_ts.strftime('%Y-%m')})")
    else:
        st.subheader("Next-month allocation recommendation")

    if not allocation_df.empty:
        cols = st.columns(len(allocation_df))
        for i, row in allocation_df.reset_index(drop=True).iterrows():
            cols[i].metric(
                row["DISTRICT"],
                f'{row["ALLOCATION_PCT"]:.1f}%'
            )
        st.bar_chart(
            allocation_df.set_index("DISTRICT")[["ALLOCATION_PCT"]],
            height=280,
        )
    else:
        st.info("배분 비율을 계산할 예측 데이터가 없습니다.")

    st.subheader("Actual vs Forecast")
    if not overlay_df.empty:
        st.line_chart(overlay_df, height=360)
    else:
        st.info("Actual/Forecast 비교 데이터가 없습니다.")

# =========================
# Tab 2: Analysis
# =========================
with tabs[1]:
    if not district_options:
        st.warning("지구별 분석을 위한 feature mart 데이터가 없습니다.")
    else:
        selected_district = st.selectbox("District", district_options, key="analysis_district")
        row = latest_df[latest_df["DISTRICT"] == selected_district].iloc[0]

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Population", fmt_int(row.get("TOTAL_POP", 0)))
        k2.metric("Card sales", fmt_eok(row.get("TOTAL_SALES", 0)))
        k3.metric("Net migration", fmt_int(row.get("NET_MOVE", 0)))
        k4.metric("Avg asset", fmt_int(row.get("AVG_ASSET", 0)))

        metric_row = pd.DataFrame()
        if not eval_pivot_df.empty:
            metric_row = eval_pivot_df[eval_pivot_df["SERIES"] == selected_district]
            if metric_row.empty:
                metric_row = eval_pivot_df[eval_pivot_df["SERIES"] == "ALL"]

        if not metric_row.empty:
            st.subheader("Forecast evaluation")
            m1, m2, m3 = st.columns(3)
            if "MAPE" in metric_row.columns:
                m1.metric("MAPE", f'{float(metric_row.iloc[0]["MAPE"]):.3f}')
            if "SMAPE" in metric_row.columns:
                m2.metric("SMAPE", f'{float(metric_row.iloc[0]["SMAPE"]):.3f}')
            if "MAE" in metric_row.columns:
                m3.metric("MAE", f'{float(metric_row.iloc[0]["MAE"]):.3f}')
            with st.expander("See all evaluation metrics"):
                st.dataframe(metric_row, use_container_width=True, hide_index=True)
        else:
            st.info("평가 지표를 표시하려면 FORECAST_MODEL_FQN을 실제 모델 오브젝트명으로 연결하세요.")

        st.subheader("Top feature importance")
        district_fi = top_features_for_district(feature_importance_df, selected_district, 8)
        if not district_fi.empty and "IMPORTANCE_SCORE" in district_fi.columns:
            fi_chart = district_fi.set_index("FEATURE_NAME")[["IMPORTANCE_SCORE"]]
            st.bar_chart(fi_chart, height=320)
            with st.expander("Feature importance detail"):
                st.dataframe(district_fi, use_container_width=True, hide_index=True)
        else:
            st.info("Feature importance를 표시하려면 Forecast 모델 메서드 연결이 필요합니다.")

# =========================
# Tab 3: AI
# =========================
with tabs[2]:
    if not district_options:
        st.warning("AI 탭을 쓰려면 기본 district 데이터가 필요합니다.")
    else:
        with st.form("ai_form", clear_on_submit=False):
            ai_scope = st.selectbox("Scope", ["전체"] + district_options, key="ai_scope")
            question = st.text_area(
                "Ask about 3 districts (Korean OK)",
                placeholder="예: 다음 달 어디에 렌탈 예산을 더 배분해야 해?"
            )
            submitted = st.form_submit_button("Ask MoveSignal AI")

        if submitted:
            if not question or not question.strip():
                st.warning("질문을 입력하세요.")
            else:
                context_json = build_ai_context(ai_scope, allocation_df, latest_df, feature_importance_df)
                st.session_state["ai_context"] = context_json
                with st.spinner("Generating grounded recommendation..."):
                    try:
                        st.session_state["ai_payload"] = ask_ai(question.strip(), ai_scope, context_json)
                    except Exception as e:
                        st.session_state["ai_payload"] = None
                        st.error(f"AI 요청 실패: {e}")

        payload = st.session_state.get("ai_payload")
        if payload:
            structured = payload.get("structured_output", payload)
            usage = payload.get("usage", {})

            st.subheader("AI Recommendation")
            st.write(structured.get("answer", "응답이 없습니다."))

            c1, c2 = st.columns(2)
            c1.metric("Recommended district", structured.get("recommended_district", "-"))
            alloc_pct = structured.get("allocation_pct")
            c2.metric(
                "Suggested allocation",
                f"{float(alloc_pct):.1f}%" if alloc_pct is not None else "-"
            )

            drivers = structured.get("drivers", [])
            if drivers:
                st.markdown("**Drivers**")
                for item in drivers:
                    st.write(f"- {item}")

            if structured.get("risk"):
                st.warning(structured["risk"])

            if structured.get("next_action"):
                st.success(structured["next_action"])

            with st.expander("AI details"):
                if usage:
                    st.write("Token usage:", usage)
                st.code(st.session_state["ai_context"], language="json")
