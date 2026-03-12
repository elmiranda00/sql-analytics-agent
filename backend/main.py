# FastAPI backend link to expose the text-to-SQL agent through an API

from __future__ import annotations

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from backend.agent import run_agent
from backend.bigquery_utils import BigQueryClient

import time
import uuid
from datetime import datetime
from typing import Optional

from dotenv import load_dotenv
load_dotenv(override=True)

# Create the FastAPI server
app = FastAPI(title="Text-to-SQL Agent API")


# Enable CORS for Streamlit -> Fast API link
# Note: the frontend runs on
# Streamlit → localhost:8501
# FastAPI → localhost:8000
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8501", "http://127.0.0.1:8501"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory query history (max last 100 queries)
_history = []
MAX_HISTORY = 100

# Define the input/output (i.e. Request/Response) structure to the front end

class QueryRequest(BaseModel):
    question: str 
    dataset_id: str

class QueryResponse(BaseModel):
    question: str
    sql: Optional[str]
    results: Optional[list[dict]]
    row_count: int
    result_summary: str
    error: Optional[str]

##### API ENDPOINTS #####

@app.post("/query", response_model=QueryResponse)
def query(req: QueryRequest):
    
    # Build BigQuery schema context for the agent
    bq = BigQueryClient()
    schema_context = bq.get_schema_context(req.dataset_id)

    # Get result from the agent
    _result = run_agent(req.question, schema_context=schema_context)
    _results = _result.get("results") or []

    response = QueryResponse(
        question = req.question,
        sql = _result.get("sql"),
        results = _results,
        row_count = len(_results),
        result_summary= _result.get("summary") or "",
        error= _result.get("error"),
    )

    # Store in history
    query_history_entry = response.model_dump()
    _history.append(query_history_entry)
    if len(_history) > MAX_HISTORY:
        _history.pop(0)

    return response


@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/datasets")
def list_datasets():
    bq = BigQueryClient()
    try:
        datasets = bq.list_datasets()
        return [{"id": d} for d in datasets]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/schema/{dataset_id}")
def get_schema(dataset_id: str):
    """Returns tables with their columns for the Streamlit schema explorer."""
    bq = BigQueryClient()
    try:
        tables = bq.list_tables(dataset_id)
        result = []
        for table_id in tables:
            try:
                schema = bq.get_table_schema(dataset_id, table_id)
                result.append({"table": table_id, "columns": len(schema), "schema": schema})
            except Exception:
                result.append({"table": table_id, "columns": 0, "schema": []})
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/history")
def get_history(limit: int = 20):
    return {"queries": _history[-limit:]}

@app.delete("/history")
def clear_history():
    _history.clear()
    return {"status": "cleared"}
