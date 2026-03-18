# Text-to-SQL Agent

A natural language to SQL agent that queries BigQuery, with a dbt transformation layer and a Streamlit UI backed by a FastAPI endpoint.

## What it does

1. Type in a question in natural language (e.g. *"What are the top 10 products by revenue?"*)
2. The agent uses an LLM to generate a BigQuery SQL query based on the schema
3. The query is validated,executed and the results are returned to the Streamlit UI

## Data Used

For the purpose of this project, I have used the free public dataset **`bigquery-public-data.thelook_ecommerce`** containing data from an e-commerce store and its orders, products, and customers.

dbt reads from the public dataset and writes cleaned models into the BigQuery project:

| Layer | Dataset (within the BigQuery project) | What it contains |
|---|---|---|
| Staging (views) | `dbt_sql_llm_staging` | Cleaned orders, order items, users |
| Marts (tables) | `dbt_sql_llm_marts` | Daily revenue, top products |

The agent queries the **marts** dataset.

---

## Project structure

```
├── backend/
│   ├── main.py            # FastAPI
│   ├── agent.py           # Agentic loop (uses OpenAI)
│   └── bigquery_utils.py  # BigQuery utilities
├── frontend/
│   └── app.py             # Streamlit UI
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml       # BigQuery connection (uses gcloud ADC)
│   └── models/
│       ├── sources.yml    # Points to the public dataset
│       ├── staging/       # stg_orders, stg_order_items, stg_users
│       └── marts/         # mart_revenue_by_day, mart_top_products
└── requirements.txt
```

---

## Setup & run

### Prerequisites
- Python 3.10+
- [Google Cloud CLI](https://cloud.google.com/sdk/docs/install) (`gcloud`)
- A GCP project with **billing enabled** (required to query public datasets)

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Create and configure your `.env` file
```
GROQ_API_KEY=your_key_here
BIGQUERY_PROJECT_ID=your-gcp-project-id
BIGQUERY_LOCATION=US
OPENAI_MODEL=gpt-4o
```

### 3. Authenticate with Google Cloud
```bash
gcloud auth application-default login
```

### 4. Run dbt (creates tables in your BQ project)
```bash
cd dbt
dbt run
cd ..
```

### 5. Start FastAPI
```bash
uvicorn backend.main:app --reload --port 8000
```

### 6. Start Streamlit (new terminal)
```bash
streamlit run frontend/app.py
```

Open `http://localhost:8501`, select `dbt_sql_llm_marts` from the sidebar, and start asking questions.
