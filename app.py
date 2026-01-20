import json
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------

SERVING_ENDPOINT_NAME = "databricks-claude-sonnet-4"

# Hardcoded job catalog (SAFE starting point)
JOB_CATALOG = {
    "daily sales etl": {
        "job_name": "daily_sales_etl",
        "job_id": 123456789012345  # <-- replace with real job ID
    },
    "inventory etl": {
        "job_name": "inventory_etl",
        "job_id": 987654321098765
    }
}

JOB_NAME_TO_ID = {
    v["job_name"]: v["job_id"]
    for v in JOB_CATALOG.values()
}

SYSTEM_PROMPT = f"""
You are a Databricks AI Agent.

Known Databricks jobs:
{chr(10).join([f"- {k} → job_name: {v['job_name']}" for k, v in JOB_CATALOG.items()])}

If the user asks to run one of these jobs, respond ONLY with JSON.

JSON format:
{{
  "action": "run_databricks_job",
  "arguments": {{
    "job_name": "<job_name>",
    "parameters": {{ ... }}
  }}
}}

Rules:
- Do NOT invent job names
- If the job is not listed, ask for clarification
- If information is missing, ask a question
- Do NOT execute anything
This is DRY-RUN mode only.
"""

ROLE_MAP = {
    "user": ChatMessageRole.USER,
    "assistant": ChatMessageRole.ASSISTANT,
}

w = WorkspaceClient()

# ------------------------------------------------------------------------------
# Agent Call
# ------------------------------------------------------------------------------

def call_agent(messages):
    sdk_messages = [
        ChatMessage(
            role=ChatMessageRole.SYSTEM,
            content=SYSTEM_PROMPT
        )
    ]

    for m in messages:
        if m["role"] in ROLE_MAP:
            sdk_messages.append(
                ChatMessage(
                    role=ROLE_MAP[m["role"]],
                    content=m["content"]
                )
            )

    response = w.serving_endpoints.query(
        name=SERVING_ENDPOINT_NAME,
        messages=sdk_messages,
        temperature=0.2,
        max_tokens=500
    )

    return response.choices[0].message.content


# ------------------------------------------------------------------------------
# Execution (GATED)
# ------------------------------------------------------------------------------

def execute_databricks_job(args):
    job_name = args["job_name"]
    parameters = args.get("parameters", {})

    job_id = JOB_NAME_TO_ID[job_name]

    run = w.jobs.run_now(
        job_id=job_id,
        notebook_params=parameters
    )

    return f"✅ Job `{job_name}` started successfully.\nRun ID: `{run.run_id}`"


def execute_plan(plan):
    action = plan["action"]
    args = plan["arguments"]

    if action == "run_databricks_job":
        return execute_databricks_job(args)

    else:
        raise ValueError(f"Unknown action: {action}")


# ------------------------------------------------------------------------------
# Streamlit UI
# ------------------------------------------------------------------------------

st.set_page_config(page_title="Databricks AI Agent", layout="centered")
st.title("🤖 Databricks AI Agent")
st.warning("🧪 Dry-Run Mode Enabled — actions require approval")

if "messages" not in st.session_state:
    st.session_state.messages = []

if "pending_plan" not in st.session_state:
    st.session_state.pending_plan = None

# Display chat history
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# User input
user_input = st.chat_input("Ask me to run a job or ask a question")

if user_input:
    st.session_state.messages.append({"role": "user", "content": user_input})

    with st.chat_message("user"):
        st.markdown(user_input)

    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            reply = call_agent(st.session_state.messages)

            # Attempt JSON dry-run parsing
            try:
                plan = json.loads(reply)

                st.session_state.pending_plan = plan

                dry_run_md = (
                    "### 🧪 Dry-Run Plan\n\n"
                    f"**Action:** `{plan['action']}`\n\n"
                    "**Arguments:**\n"
                    "```json\n"
                    f"{json.dumps(plan['arguments'], indent=2)}\n"
                    "```\n\n"
                    "⚠️ **Nothing has been executed yet.**"
                )

                st.markdown(dry_run_md)

                st.session_state.messages.append({
                    "role": "assistant",
                    "content": dry_run_md
                })

            except json.JSONDecodeError:
                st.markdown(reply)
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": reply
                })

# ------------------------------------------------------------------------------
# Approval Controls
# ------------------------------------------------------------------------------

if st.session_state.pending_plan:
    st.divider()
    st.warning("⚠️ Approving will execute this action in Databricks.")

    col1, col2 = st.columns(2)

    with col1:
        approve = st.button("✅ Approve")

    with col2:
        cancel = st.button("❌ Cancel")

    if cancel:
        st.session_state.pending_plan = None
        st.success("Action canceled. Nothing was executed.")

    if approve:
        with st.spinner("Executing..."):
            result = execute_plan(st.session_state.pending_plan)

        st.session_state.pending_plan = None
        st.success(result)