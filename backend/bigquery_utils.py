# Utilities for Big Query: query execution, get schemas

from __future__ import annotations

import os
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
from google.cloud import bigquery

# Define query limits
MAX_GB = 30
MAX_ROWS = 10_000

# Set up schema cache (to avoid requerying schemas)
#   _SCHEMA_CACHE = {"project.database_name": ("<schema string>",timestamp)}
CACHE_TTL_MINUTES = 30 # time limit (schemas older than this time will be re-queried)
_SCHEMA_CACHE: dict[str, tuple[str, datetime]] = {}


class BigQueryClient:

    def __init__(self):
        
        self.project_id = os.environ["BIGQUERY_PROJECT_ID"]
        self.location = os.environ.get("BIGQUERY_LOCATION", "US")
        # Initialize BigQuery client
        self.client = bigquery.Client(project=self.project_id,location=self.location)

    #@property
    #def client(self) -> bigquery.Client:
    #    if self._client is None:
    #        self._client = bigquery.Client(project=self.project_id, location=self.location,)
    #    return self._client

    # Function to dry-run the query and then execute it
    def execute_query(self, query: str) -> dict:
        
        # Step 1: Dry run to validate query and check cost (skip if too costly > max_gb)
        try:

            job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
            dry_job = self.client.query(query, job_config=job_config)
            # Check if query is too expensive
            gb = dry_job.total_bytes_processed / (1024 ** 3)
            if gb > MAX_GB:
                return {
                    "success": False,
                    "error": f"Query cost {gb:.2f} GB exceeds limit of {MAX_GB} GB",
                    "data": None,
                }
            
        except Exception as e:
            print('Query failed, check the query syntax')
            return {
                'success': False, 'error': f'Query validation failed: {str(e)}', 'data': None}

        # Step 2: Execute the query
        try:
            job = self.client.query(query)
            df = job.result().to_dataframe()

            # Enforce max row limit
            if len(df) > MAX_ROWS:
                df = df.head(MAX_ROWS)

            # Tidy up datatypes
            for col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = df[col].astype(str)
                elif pd.api.types.is_object_dtype(df[col]):
                    df[col] = df[col].where(df[col].notna(), None)
            
            df2 = df.to_dict(orient="records")

            return {"success": True, "error": None, "data": df2}

        except Exception as e:
            return {"success": False, "error": f"Execution error: {str(e)}", "data": None}

    # Get list of dataset IDs
    def list_datasets(self) -> list[str]:
        return [ds.dataset_id for ds in self.client.list_datasets()]

    # Get list of table IDs
    def list_tables(self, dataset_id: str) -> list[str]:
        dataset_ref = self.client.dataset(dataset_id)
        return [t.table_id for t in self.client.list_tables(dataset_ref)]

    # Get schema of a single table
    def get_table_schema(self, dataset_id: str, table_id: str) -> list[dict]:

        table = self.client.get_table(f"{self.project_id}.{dataset_id}.{table_id}")
        
        return [{"name": col.name, "type": col.field_type} for col in table.schema]

    # Get the schema context as a string to be passed to the LLM prompt, cache the resulting schema obtained
    def get_schema_context(self, dataset_id: str, max_tables: int = 20) -> str:
        
        cache_key = f"{self.project_id}.{dataset_id}"

        # If schema already in cache (and saved recently), return it
        if cache_key in _SCHEMA_CACHE:
            context, cached_at = _SCHEMA_CACHE[cache_key]
            if datetime.utcnow() - cached_at < timedelta(minutes=CACHE_TTL_MINUTES):
                return context
        
        tables = sorted(self.list_tables(dataset_id))[:max_tables]

        output = [
            f"-- BigQuery Project: {self.project_id}",
            f"-- Dataset: {dataset_id}",
            f"-- Available Tables ({len(tables)}):",
            "",
        ]
        
        # Compile schemas for output and update cache
        for table_id in tables:

            schema = self.get_table_schema(dataset_id, table_id)
            columns = ", ".join(f"{col['name']} {col['type']}" for col in schema)
            #output.append(_format_table_schema(self.project_id, dataset_id, table_id, schema))
        
            output.append(
                f"-- Table: `{self.project_id}.{dataset_id}.{table_id}`\n"
                f"-- Columns: {columns}\n"
            )

        context = "\n".join(output)
        _SCHEMA_CACHE[cache_key] = (context, datetime.utcnow())

        return context

    
    #def get_dataset_tables(self, dataset_id: str) -> list[dict]:
    #    """Returns table list with schema for the Streamlit sidebar."""
    #    tables = self.list_tables(dataset_id)
    #    result = []
    #    for table_id in tables:
    #        try:
    #            schema = self.get_table_schema(dataset_id, table_id)
    #            result.append({"table": table_id, "columns": len(schema), "schema": schema})
    #        except Exception:
    #            result.append({"table": table_id, "columns": 0, "schema": []})
    #    return result

    #def invalidate_cache(self, dataset_id: Optional[str] = None):
    #    if dataset_id:
    #        _SCHEMA_CACHE.pop(f"{self.project_id}.{dataset_id}", None)
    #    else:
    #        _SCHEMA_CACHE.clear()


#def _format_table_schema(project: str, dataset: str, table: str, schema: list[dict]) -> str:
#    lines = [
#        f"-- Table: `{project}.{dataset}.{table}`",
#        "-- Columns:",
#    ]
#    for col in schema:
#        desc = f"  -- {col['description']}" if col.get("description") else ""
#        nullable = "NULLABLE" if col["mode"] == "NULLABLE" else col["mode"]
#        lines.append(f"--   {col['name']} {col['type']} ({nullable}){desc}")
#    lines.append("")
#    return "\n".join(lines)
