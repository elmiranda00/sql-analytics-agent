# Agentic Loop to translate a natural language question to a BigQuery SQL query and returns the results

from __future__ import annotations

from openai import OpenAI
from backend.bigquery_utils import BigQueryClient

import json
import os
from typing import Optional

###### PROMPTS #####

# Set up system prompt
SYSTEM_PROMPT = """Your role is to translate requests to SQL queries for BigQuery.

IMPORTANT: You have access to tools to help you.

IMPORTANT BEHAVIOR RULES:
Always explain with one short sentence what you're going to do before using any tools
Don't just call tools without explanation."""

# Optional user rules to apply to the output
USER_RULES = "Use USING for joins when possible. Put SQL keywords in uppercase."


##### TOOLS #####

OPENAI_TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "execute_query",
            "description": "Execute a SQL query on BigQuery. Returns the query results.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "BigQuery SQL query.",
                    }
                },
                "required": ["query"],
                "additionalProperties": False,
            },
            "strict": True,
        },
    }
]

# Convert the tool description to a long string
TOOLS_DESCRIPTION = f"Available tools:\n{json.dumps(OPENAI_TOOLS, indent=2)}"


##### AGENTIC LOOP #####

def run_agent(user_prompt: str, schema_context: str = "", privacy_mode = True) -> dict:
    
    bq = BigQueryClient()

    def execute_query(query):
        return bq.execute_query(query)

    tool_registry = {"execute_query": execute_query}

    api_key = os.environ["GROQ_API_KEY"]
    model_name = os.environ["LLM_MODEL"]
    base_url = os.environ["GROQ_BASE_URL"]

    context_block = f"\n\nSchema context:\n{schema_context}" if schema_context else ""

    # 0. Compile the system prompt + user prompt to the AI agent (Concatenate all prompts)
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT + "\n" + TOOLS_DESCRIPTION},
        {"role": "user", "content": USER_RULES},
        {"role": "user", "content": user_prompt + context_block},
    ]

    llm = OpenAI(api_key=api_key, base_url=base_url)

    last_sql = None
    last_results = None
    last_error = None
    
    running = True

    while running:
        
        # 1. Send compiled prompt and get the first response from the agent
        response = llm.chat.completions.create(
            messages=messages,
            model=model_name,
            tools=OPENAI_TOOLS,
            parallel_tool_calls=False,
            stream=False,
        )

        message = response.choices[0].message
        
        # 2. Get the list of tools to execute; add assistant + tools messages to the stream of messages with the agent
        tool_calls = []
        if message.tool_calls:
            for tc in message.tool_calls:
                tool_calls.append({
                    "name": tc.function.name,
                    "args": tc.function.arguments,
                    "id": tc.id,
                })

            # Add tool calls to messages
            messages.append({
                "role": "assistant",
                "content": message.content,
                "tool_calls": [
                    {
                        "id": tc.id,
                        "type": "function",
                        "function": {
                            "name": tc.function.name,
                            "arguments": tc.function.arguments,
                        },
                    }
                    for tc in message.tool_calls
                ],
            })
        else:
            # Add the assistant's message to messages
            messages.append({"role": "assistant", "content": message.content})


        # 3. Execute tools
        for tc in tool_calls:
            try:
                args = json.loads(tc["args"])
                result = tool_registry[tc["name"]](**args)

                # Track the last SQL and results
                if tc["name"] == "execute_query":
                    last_sql = args.get("query")
                    if result.get("success"):
                        last_results = result.get("data")
                    else:
                        last_error = result.get("error")

                # Send tool results back to the LLM
                messages.append({
                    "role": "tool",
                    "content": json.dumps(result),
                    "tool_call_id": tc["id"],
                })

            except Exception as e:
                last_error = str(e)
                messages.append({
                    "role": "tool",
                    "content": f"Error: {e}",
                    "tool_call_id": tc["id"],
                })

        # 4. Running check (to stop the loop if no tools are to be executed)
        running = len(tool_calls) > 0

    # Final assistant summary (last non-tool message content)
    summary = messages[-1].get("content") or ""

    return {
        "sql": last_sql,
        "results": last_results,
        "summary": summary,
        "error": last_error,
    }


