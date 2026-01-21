import json
import re
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

# ==============================================================================
# CONFIGURATION
# ==============================================================================

SERVING_ENDPOINT_NAME = "databricks-claude-sonnet-4"

# Replace with REAL job IDs
JOB_CATALOG = {
    "test db job": {
        "job_name": "Test_DB_Job1",
        "job_id": 1234567890
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

When a user requests a job:
- Return a JSON object describing the plan
- Do NOT explain execution status
- Do NOT say "dry run" in natural language

JSON format:
{{
  "action": "run_databricks_job",
  "arguments": {{
    "job_name": "<job_name>",
    "parameters": {{ }}
  }}
}}

Rules:
- Never invent job names
- Ask questions if information is missing
- Never execute anything
"""

ROLE_MAP = {
    "user": ChatMessageRole.USER,
    "assistant": ChatMessageRole.ASSISTANT,
}

w = WorkspaceClient()

# ==============================================================================
# UTILITIES
# ==============================================================================

def extract_json(text: str):
    """Extract first JSON object from text"""
    match = re.search(r"\{[\s\S]*\}", text)
    return match.group(0) if match else None

# ==============================================================================
# AGENT CALL
# ==============================================================================

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
        temperature=0.1,
        max_tokens=500
    )

    return response.choices[0].message.content

# ==============================================================================
# EXECUTION (GATED)
# ==============================================================================

def execute_databricks_job(args):
    job_name = args["job_name"]
    parameters = args.get("parameters", {})

    job_id = JOB_NAME_TO_ID[job_name]

    run = w.jobs.run_now(
        job_id=job_id,
        notebook_params=parameters
    )

    return f"🚀 Job `{job_name}` started\nRun ID: `{run.run_id}`"

def execute_plan(plan):
    if plan["action"] == "run_databricks_job":
        return execute_databricks_job(plan["arguments"])
    raise ValueError("Unknown action")

# ==============================================================================
# STREAMLIT UI
# ==============================================================================

st.set_page_config(page_title="Databricks AI Agent", layout="centered")
st.title("🤖 Databricks AI Agent")
st.warning("🧪 Dry-Run first → Approval required to execute")

if "messages" not in st.session_state:
    st.session_state.messages = []

if "pending_plan" not in st.session_state:
    st.session_state.pending_plan = None

# ------------------------------------------------------------------------------
# Chat history
# ------------------------------------------------------------------------------

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# ------------------------------------------------------------------------------
# User input
# ------------------------------------------------------------------------------

user_input = st.chat_input("Ask me to run a Databricks job")

if user_input:
    st.session_state.messages.append({"role": "user", "content": user_input})

    with st.chat_message("user"):
        st.markdown(user_input)

    with st.chat_message("assistant"):
        with st.spinner("Thinking..."):
            reply = call_agent(st.session_state.messages)

            json_text = extract_json(reply)

            if json_text:
                try:
                    plan = json.loads(json_text)

                    st.session_state.pending_plan = plan

                    dry_run_md = (
                        "### 🧪 Dry-Run Plan\n\n"
                        f"**Action:** `{plan['action']}`\n\n"
                        "**Arguments:**\n"
                        "```json\n"
                        f"{json.dumps(plan['arguments'], indent=2)}\n"
                        "```\n\n"
                        "⚠️ **Waiting for approval**"
                    )

                    st.markdown(dry_run_md)

                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": dry_run_md
                    })

                    st.rerun()

                except Exception as e:
                    st.error(f"Failed to parse plan: {e}")
                    st.markdown(reply)
            else:
                st.markdown(reply)
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": reply
                })

# ------------------------------------------------------------------------------
# APPROVAL CONTROLS (TOP-LEVEL)
# ------------------------------------------------------------------------------

if st.session_state.pending_plan:
    st.divider()
    st.warning("⚠️ Approving will execute this Databricks job")

    col1, col2 = st.columns(2)

    with col1:
        approve = st.button("✅ Approve", key="approve")

    with col2:
        cancel = st.button("❌ Cancel", key="cancel")

    if cancel:
        st.session_state.pending_plan = None
        st.success("Action canceled")
        st.rerun()

    if approve:
        with st.spinner("Executing job..."):
            result = execute_plan(st.session_state.pending_plan)

        st.session_state.pending_plan = None

        st.session_state.messages.append({
            "role": "assistant",
            "content": f"🚀 **Execution confirmed**\n\n{result}"
        })

        st.success(result)
        st.rerun()
