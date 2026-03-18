# Streamlit frontend for the Text-to-SQL Agent, Calls the FastAPI backend at localhost:8000.

import pandas as pd
import plotly.express as px
import requests
import streamlit as st
from datetime import datetime

API_BASE = "http://localhost:8000"

st.set_page_config(
    page_title="Text-to-SQL Agent",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    .sql-block {
        background: #1e1e1e;
        color: #d4d4d4;
        padding: 1rem;
        border-radius: 8px;
        font-family: 'Courier New', monospace;
        font-size: 0.85rem;
    }
    .error-block {
        background: #fff5f5;
        border-left: 4px solid #e53e3e;
        padding: 1rem;
        border-radius: 8px;
        color: #c53030;
    }
    .chat-user {
        background: #ebf4ff;
        padding: 0.75rem 1rem;
        border-radius: 12px 12px 2px 12px;
        margin: 0.5rem 0;
        border-left: 3px solid #4299e1;
    }
    .stTextInput input {
        color: #000000 !important;
    }
</style>
""", unsafe_allow_html=True)


# ── Session state ─────────────────────────────────────────────────────────────

def _init():
    defaults = {
        "messages": [],
        "selected_dataset": None,
        "datasets": [],
        "schema_tables": [],
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

_init()


# ── API helpers ───────────────────────────────────────────────────────────────

def api_get(path: str):
    try:
        r = requests.get(f"{API_BASE}{path}", timeout=120)
        r.raise_for_status()
        return r.json()
    except requests.ConnectionError:
        st.error("Cannot connect to backend — is `uvicorn backend.main:app` running on port 8000?")
        return None
    except Exception as e:
        st.error(f"API error: {e}")
        return None


def api_post(path: str, data: dict):
    try:
        r = requests.post(f"{API_BASE}{path}", json=data, timeout=180)
        r.raise_for_status()
        return r.json()
    except requests.ConnectionError:
        st.error("Cannot connect to backend — is `uvicorn backend.main:app` running on port 8000?")
        return None
    except Exception as e:
        st.error(f"API error: {e}")
        return None


# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("## Dataset")

    if st.button("Refresh datasets"):
        data = api_get("/datasets")
        if data:
            st.session_state.datasets = [d["id"] for d in data]

    if not st.session_state.datasets:
        data = api_get("/datasets")
        if data:
            st.session_state.datasets = [d["id"] for d in data]

    if st.session_state.datasets:
        selected = st.selectbox("Select dataset", st.session_state.datasets)
        if selected != st.session_state.selected_dataset:
            st.session_state.selected_dataset = selected
            st.session_state.schema_tables = []
    else:
        manual = st.text_input("Or enter dataset ID manually:")
        if manual:
            st.session_state.selected_dataset = manual

    # Schema explorer
    st.markdown("---")
    st.markdown("## Schema")

    if st.session_state.selected_dataset:
        if st.button("Load schema"):
            data = api_get(f"/schema/{st.session_state.selected_dataset}")
            if data:
                st.session_state.schema_tables = data

        for tbl in st.session_state.schema_tables:
            with st.expander(f"{tbl['table']} ({tbl['columns']} cols)"):
                for col in tbl.get("schema", []):
                    st.markdown(f"- **{col['name']}** `{col['type']}`")

    # Query history
    st.markdown("---")
    st.markdown("## History")

    history = api_get("/history?limit=10")
    if history and history.get("queries"):
        for item in reversed(history["queries"]):
            icon = "✅" if not item.get("error") else "❌"
            with st.expander(f"{icon} {item['question'][:45]}"):
                st.code(item.get("sql") or "", language="sql")
                st.caption(f"Rows: {item.get('row_count', 0)}")
    else:
        st.caption("No queries yet.")

    if st.button("Clear history"):
        requests.delete(f"{API_BASE}/history")
        st.success("Cleared!")


# ── Main ──────────────────────────────────────────────────────────────────────

st.markdown("# 🤖 Text-to-SQL Agent")
st.markdown("Ask questions about your data in plain English.")

# Example queries
st.markdown("**Sample questions:**")
examples = [
    "What are the top 10 products by total revenue?",
    "Show daily net revenue for the last 30 days",
    "Which product categories have the highest return rate?",
    "How many unique customers ordered each month this year?",
]
cols = st.columns(len(examples))
for i, ex in enumerate(examples):
    if cols[i].button(ex[:40] + "...", key=f"ex_{i}"):
        st.session_state["prefill"] = ex

st.markdown("---")


def _auto_chart(df: pd.DataFrame):
    if df.empty:
        st.info("No data.")
        return
    numeric = df.select_dtypes(include="number").columns.tolist()
    categorical = df.select_dtypes(exclude="number").columns.tolist()
    if categorical and numeric:
        fig = px.bar(df.head(30), x=categorical[0], y=numeric[0])
        st.plotly_chart(fig, use_container_width=True)
    elif len(numeric) >= 2:
        fig = px.scatter(df, x=numeric[0], y=numeric[1])
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Not enough data variety to auto-render a chart.")


def _render_result(result: dict):
    if result.get("error"):
        st.markdown(f'<div class="error-block">⚠️ {result["error"]}</div>', unsafe_allow_html=True)

    with st.expander("Generated SQL", expanded=True):
        st.code(result.get("sql") or "", language="sql")

    rows = result.get("results")
    if rows:
        df = pd.DataFrame(rows)
        tab_table, tab_chart, tab_stats = st.tabs(["Table", "Chart", "Stats"])

        with tab_table:
            st.dataframe(df, use_container_width=True, height=380)
            st.download_button(
                "Download CSV",
                df.to_csv(index=False),
                file_name=f"query_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
            )

        with tab_chart:
            _auto_chart(df)

        with tab_stats:
            st.dataframe(df.describe(include="all"), use_container_width=True)

    st.caption(f"Rows returned: {result.get('row_count', 0)}")


# Chat history
for msg in st.session_state.messages:
    if msg["role"] == "user":
        st.markdown(f'<div class="chat-user"><strong>You:</strong> {msg["content"]}</div>',
                    unsafe_allow_html=True)
    else:
        if msg.get("result"):
            _render_result(msg["result"])


# Input
prefill = st.session_state.pop("prefill", "")
question = st.text_input(
    "Your question:",
    value=prefill,
    placeholder="e.g. What were the top 5 products by revenue last month?",
)

col_ask, col_clear = st.columns([1, 5])
submit = col_ask.button("Ask", type="primary")
col_clear.button("Clear chat", on_click=lambda: st.session_state.update(messages=[]))

if submit and question:
    if not st.session_state.selected_dataset:
        st.warning("Select a dataset in the sidebar first.")
    else:
        st.session_state.messages.append({"role": "user", "content": question})

        with st.spinner("Agent working…"):
            result = api_post("/query", {
                "question": question,
                "dataset_id": st.session_state.selected_dataset,
            })

        if result:
            summary = result.get("result_summary") or "Done."
            error = result.get("error")
            st.session_state.messages.append({
                "role": "assistant",
                "content": f"⚠️ {error}" if error else summary,
                "result": result,
            })

        st.rerun()

st.markdown("---")
st.caption("Powered by OpenAI + BigQuery + dbt | FastAPI + Streamlit")
