"""
SSD Agent Assist — Slack Bot
=============================
Responds when someone @mentions @SSD_Bot in any Slack channel.

Capabilities:
  1. Answer any SSD-related question (fetches live Confluence runbook)
  2. Update the Confluence runbook — bot proposes the change, human confirms
  3. Sync information — summarise Slack discussion and write it to Confluence

Usage examples:
  @SSD_Bot how do I fix BlobAlreadyExists?
  @SSD_Bot update runbook: when HDFS has no data, the job now skips instead of failing
  @SSD_Bot sync this thread to the runbook

Requirements:
    pip install slack-bolt anthropic python-dotenv requests beautifulsoup4 apscheduler

Environment variables (see .env.example):
    SLACK_BOT_TOKEN       xoxb-...
    SLACK_APP_TOKEN       xapp-...
    ANTHROPIC_API_KEY     sk-ant-...
    CONFLUENCE_EMAIL      you@conviva.com
    CONFLUENCE_API_TOKEN  (from id.atlassian.com/manage-profile/security/api-tokens)
"""

import os
import re
import json
import logging
import math
import threading
import requests
import urllib3
from datetime import datetime, timedelta, timezone
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from anthropic import Anthropic
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ─── Slack message length guard ────────────────────────────────────────────────
# Slack's chat.update rejects messages that are too long.  The documented limit
# is 40 000 chars but in practice msg_too_long fires well before that (around
# 4 000–6 000 chars depending on workspace tier and payload encoding overhead).
# Keep chunks small so every chunk is safe for both chat.update and postMessage.
SLACK_MAX_LEN = 3_500

def _slack_reply(client, *, channel: str, thread_ts: str,
                 text: str, update_ts=None) -> None:
    """Post (or update) a possibly-long message, splitting into chunks if needed.

    If update_ts is given, the first chunk replaces that message via chat_update.
    Subsequent chunks are posted as new thread replies.
    """
    # Split into chunks that respect the limit
    chunks = []
    while len(text) > SLACK_MAX_LEN:
        # Try to cut at a newline to avoid mid-word breaks
        cut = text.rfind("\n", 0, SLACK_MAX_LEN)
        if cut == -1:
            cut = SLACK_MAX_LEN
        chunks.append(text[:cut])
        text = text[cut:].lstrip("\n")
    chunks.append(text)

    total = len(chunks)
    for i, chunk in enumerate(chunks):
        if total > 1:
            chunk = f"_{i+1}/{total}_\n{chunk}"
        if i == 0 and update_ts:
            client.chat_update(channel=channel, ts=update_ts, text=chunk)
        else:
            client.chat_postMessage(channel=channel, thread_ts=thread_ts, text=chunk)

# ─── Persistent Memory ─────────────────────────────────────────────────────────
# Stores facts learned from past conversations (survives bot restarts).
# Each entry: { "value": "...", "updated": "YYYY-MM-DD" }

MEMORY_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "memory.json")

def _load_memory() -> dict:
    try:
        with open(MEMORY_FILE, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def _save_memory(memory: dict):
    with open(MEMORY_FILE, "w") as f:
        json.dump(memory, f, indent=2)

def _memory_context_string() -> str:
    """Return memory as a formatted string to inject into the system prompt."""
    memory = _load_memory()
    if not memory:
        return ""
    lines = ["━━━ BOT MEMORY (learned from past conversations — treat as reliable context) ━━━"]
    for key, entry in memory.items():
        value = entry.get("value", "") if isinstance(entry, dict) else str(entry)
        updated = entry.get("updated", "") if isinstance(entry, dict) else ""
        date_str = f" (learned {updated})" if updated else ""
        lines.append(f"• {key}{date_str}: {value}")
    return "\n".join(lines)

# ─── Clients ───────────────────────────────────────────────────────────────────

slack_app = App(token=os.environ["SLACK_BOT_TOKEN"])
anthropic = Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

# ─── Confluence Config ─────────────────────────────────────────────────────────

CONFLUENCE_BASE  = "https://conviva.atlassian.net/wiki"
CONFLUENCE_EMAIL = os.environ["CONFLUENCE_EMAIL"]
CONFLUENCE_TOKEN = os.environ["CONFLUENCE_API_TOKEN"]

CONFLUENCE_PAGES = {
    "SSD Playbook for Support": "2584412251",
    # Add more page IDs here as needed
}
PRIMARY_PAGE_ID  = "2584412251"
CONFLUENCE_SPACE = "CSS"          # Space key (visible in Confluence URLs: /spaces/CSS/)

# Pending updates awaiting human confirmation
# Keyed by (channel, user) so confirm/cancel works whether the user replies
# in the thread, outside the thread, or after a bot restart within the same session.
pending_updates: dict = {}

# Pending new page creations awaiting human confirmation (same keying pattern)
pending_pages: dict = {}

# Pending bulk DAG run state changes awaiting human confirmation
pending_mark_runs: dict = {}

# Pending bulk DAG run clears (re-run/backfill) awaiting human confirmation
pending_rerun_runs: dict = {}

# Queue of rerun proposals that are waiting behind a pending upstream trigger or
# another active rerun proposal. Keyed by (channel, user) → list[rerun_dict].
# Each entry is popped and posted to Slack only after the current action is confirmed.
pending_rerun_queue: dict = {}

# Pending upstream minute DAG trigger awaiting human confirmation
pending_trigger_upstream: dict = {}

# Pending HDFS → S3 repair copy awaiting human confirmation
pending_hdfs_s3_copy: dict = {}

# Pending batch flow-feed rerun with checkbox selection awaiting confirmation
# Value: {"dag_ids": [...], "instances_map": {dag_id: [(label, base)]},
#         "start_dt": str, "end_dt": str, "thread_ts": str, "message_ts": str}
pending_flow_feed_batch: dict = {}

# Tracks when a user said "force trigger" but the bot needed to ask a follow-up question.
# Keyed by (channel, user) → True.  Cleared as soon as the follow-up message is processed
# so the force=True flag is injected into handle_answer's question string.
pending_force_context: dict = {}

# AWS profile map: S3 bucket prefix → credentials profile in ~/.aws/credentials
AWS_S3_PROFILE_MAP = {
    "conviva-daas-dev": "daas-dev",
    "p-conviva-v2":       "sling",   # EchoStar-SlingTV DPI Event pipelines
    "p-conviva-onstream": "sling",
    "p-conviva-slingtv":  "sling",
    "p-conviva-echostar": "sling",
}

# S3 buckets where Conviva does NOT have delete permission (SlingTV / EchoStar-OnStream).
# Pipelines writing to these buckets cannot be rerun to redeliver — a _repair copy is needed.
NO_DELETE_S3_BUCKETS = {
    "p-conviva-v2",
    "p-conviva-onstream",
    "p-conviva-slingtv",
    "p-conviva-echostar",
}

# Pending backfill-mark-success (bypasses max_active_runs) awaiting human confirmation
pending_backfill: dict = {}

# Pending interleaved rerun (fair-queue between backfill and forward pipeline)
pending_interleaved_rerun: dict = {}

# Active interleaved rerun background threads, keyed by (channel, user)
# Each value: {"stop_event": threading.Event, "thread": threading.Thread}
active_interleaved_reruns: dict = {}

# ─── Upstream Minute DAG Config ────────────────────────────────────────────────
# Playbook: https://conviva.atlassian.net/wiki/spaces/CSS/pages/4192337966
# When DPI Flow Feed fails/pends at the sensor, re-trigger the upstream minute DAG
# with the exact logical date (failed_minute − 2 mins) and the HDFS output paths.

UPSTREAM_MINUTE_DAG_ID   = "ECO_CROSS_PAGE_EVENT_SUMMARY_SSD_MINUTE_DAG"
UPSTREAM_MINUTE_INSTANCE = "streamnew"  # moved to streamnew (conviva-airflow.prod.conviva.com); old streamid (rke-shared-1) is PAUSED
DPI_FLOW_UPSTREAM_OFFSET_MINS = 2       # failed minute X → upstream logical date = X − 2 mins
# NOTE: HDFS paths are NOT hardcoded here. The bot reads them live from the Confluence
# playbook (page 4192337966) each time it handles a DPI Flow Feed issue, so path
# changes are picked up automatically without a code deploy.

# Threads where the bot has already replied — follow-ups don't need @mention
active_threads: set = set()

# Per-thread conversation history — keyed by thread_ts
# Stores list of {"role": "user"/"assistant", "content": "..."}
# So the bot remembers everything said in the thread (resets on restart)
thread_history: dict = {}
THREAD_HISTORY_MAX = 20  # max messages to keep per thread (10 exchanges)

# ─── Confluence: Read ──────────────────────────────────────────────────────────

def fetch_confluence_page(page_id: str) -> dict:
    """Return {title, version, body_text, body_storage} for a page."""
    url = f"{CONFLUENCE_BASE}/rest/api/content/{page_id}"
    params = {"expand": "body.storage,version"}
    auth = (CONFLUENCE_EMAIL, CONFLUENCE_TOKEN)
    try:
        resp = requests.get(url, params=params, auth=auth, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        html = data["body"]["storage"]["value"]
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup.find_all(["ac:image", "ac:structured-macro"]):
            tag.decompose()
        plain = soup.get_text(separator="\n", strip=True)
        return {
            "title":        data.get("title", ""),
            "version":      data["version"]["number"],
            "body_text":    plain,
            "body_storage": html,
        }
    except Exception as e:
        logger.error(f"Failed to fetch page {page_id}: {e}")
        return {}


def build_runbook_context() -> str:
    """Fetch all configured pages and return combined plain-text context."""
    parts = []
    for name, pid in CONFLUENCE_PAGES.items():
        page = fetch_confluence_page(pid)
        if page:
            parts.append(f"# {page['title']} (v{page['version']})\n\n{page['body_text']}")
    return "\n\n---\n\n".join(parts) if parts else "(Confluence unavailable)"

# ─── Confluence: Write ─────────────────────────────────────────────────────────

def update_confluence_page(page_id: str, new_storage_html: str, version: int, title: str) -> bool:
    """Overwrite a Confluence page with new storage-format HTML."""
    url = f"{CONFLUENCE_BASE}/rest/api/content/{page_id}"
    auth = (CONFLUENCE_EMAIL, CONFLUENCE_TOKEN)
    payload = {
        "version": {"number": version + 1},
        "title":   title,
        "type":    "page",
        "body": {
            "storage": {
                "value":          new_storage_html,
                "representation": "storage",
            }
        },
    }
    try:
        resp = requests.put(url, json=payload, auth=auth, timeout=20)
        resp.raise_for_status()
        logger.info(f"Confluence page {page_id} updated to v{version + 1}")
        return True
    except Exception as e:
        logger.error(f"Failed to update Confluence page {page_id}: {e}")
        return False

def create_confluence_page(title: str, storage_html: str, parent_id: str = PRIMARY_PAGE_ID) -> dict:
    """Create a brand-new Confluence page as a child of parent_id. Returns {ok, url, page_id}."""
    url  = f"{CONFLUENCE_BASE}/rest/api/content"
    auth = (CONFLUENCE_EMAIL, CONFLUENCE_TOKEN)
    payload = {
        "type":  "page",
        "title": title,
        "ancestors": [{"id": parent_id}],
        "space": {"key": CONFLUENCE_SPACE},
        "body": {
            "storage": {
                "value": storage_html,
                "representation": "storage",
            }
        },
    }
    try:
        resp = requests.post(url, json=payload, auth=auth, timeout=20)
        resp.raise_for_status()
        data    = resp.json()
        page_id = data.get("id", "")
        page_url = f"{CONFLUENCE_BASE}/spaces/{CONFLUENCE_SPACE}/pages/{page_id}"
        logger.info(f"Confluence page created: {page_id} — {title}")
        return {"ok": True, "page_id": page_id, "url": page_url}
    except Exception as e:
        logger.error(f"Failed to create Confluence page '{title}': {e}")
        return {"ok": False, "error": str(e)}


# ─── Intent Detection ──────────────────────────────────────────────────────────

INTENT_SYSTEM = """You are an intent classifier for the SSD Agent Assist Slack bot.
Given a user message, return a JSON object with a single key "intent" set to one of:
  "answer"  — user wants an answer to a question or help debugging
  "update"  — user wants to update / add to the Confluence runbook
  "sync"    — user wants to sync a Slack thread or discussion into Confluence
  "confirm" — user is confirming a pending runbook update (e.g. "yes", "confirm", "looks good")
  "cancel"  — user is cancelling a pending update (e.g. "no", "cancel", "nevermind")
Return only the raw JSON, nothing else."""

def _parse_pd_alert(text: str):
    """Extract structured fields from a PagerDuty / Airflow alert pasted into Slack.

    Handles the two common formats engineers paste:

    Format A — plain text:
        Airflow alert: <TaskInstance: dag_id.task_id scheduled__2026-06-15T12:03:00+00:00 [failed]>

    Format B — Slack hyperlink wrapping the alert:
        <https://conviva.pagerduty.com/incidents/XXXXX|Airflow alert: <TaskInstance: dag_id.task_id ...>>

    Returns a dict with keys: dag_id, task_id, execution_date, state, pd_url (optional)
    or None if the text doesn't look like a PD alert.
    """
    # Extract optional PagerDuty URL
    pd_url = None
    pd_url_match = re.search(r"https://conviva\.pagerduty\.com/incidents/([A-Z0-9]+)", text)
    if pd_url_match:
        pd_url = f"https://conviva.pagerduty.com/incidents/{pd_url_match.group(1)}"

    # Core pattern: TaskInstance: dag_id.task_id run_type__execution_date [state]
    ti_pattern = re.search(
        r"TaskInstance:\s+"
        r"([\w\-\.]+)\."          # dag_id  (word chars, hyphens, dots)
        r"([\w\-\.]+)\s+"         # task_id
        r"(?:scheduled|manual|backfill)__"
        r"([\dT:\+\-Z]+)"         # execution_date
        r"\s+\[([\w]+)\]",        # state
        text,
    )
    if not ti_pattern:
        return None

    dag_id, task_id, exec_date, state = ti_pattern.groups()

    # Normalise execution_date to a plain UTC string Claude can work with
    exec_date_clean = exec_date.replace("%3A", ":").replace("%2B", "+")
    # Strip URL-encoding that sometimes appears in Slack-formatted links
    exec_date_clean = exec_date_clean.split("|")[0]  # drop any Slack link tail

    return {
        "dag_id":         dag_id,
        "task_id":        task_id,
        "execution_date": exec_date_clean,
        "state":          state,
        "pd_url":         pd_url,
    }


def _pd_alert_to_question(parsed: dict) -> str:
    """Convert a parsed PD alert into a rich investigation prompt for the agent."""
    dag_id    = parsed["dag_id"]
    task_id   = parsed["task_id"]
    exec_date = parsed["execution_date"]
    state     = parsed["state"]
    pd_url    = parsed.get("pd_url", "")

    pd_ref = f"\nPagerDuty incident: {pd_url}" if pd_url else ""

    # ── DPI Flow Feed sensor failure — specialized fast-path ──
    if "sensor_eco_cross_page_event_summary_ssd_minute" in task_id:
        return (
            f"DPI Flow Feed sensor failure alert:{pd_ref}\n"
            f"- Stuck flow feed DAG: `{dag_id}`\n"
            f"- Failing sensor task: `{task_id}`\n"
            f"- Stuck minute: `{exec_date}`\n"
            f"- Task state: `{state}`\n\n"
            f"This is a DPI Flow Feed stuck-at-sensor issue. Follow the workflow exactly:\n"
            f"1. Call get_flow_feed_failures_at_minute(stuck_minute=`{exec_date}`) — find ALL "
            f"pipelines stuck at this minute from #piccolo-daas-alert.\n"
            f"2. Call get_airflow_task_log for sensor task `{task_id}` (use highest try_number). "
            f"Read the failure reason:\n"
            f"   BRANCH B: If NOT a sensor-timeout/upstream-data-not-ready error → STOP. Tell user: "
            f"'❌ Failure not covered in runbook. Failure: [error]. Fix first, then ask me: "
            f"rerun {dag_id} for {exec_date}'. Do NOT proceed.\n"
            f"   BRANCH A: If sensor timed out waiting for upstream → continue.\n"
            f"3. Read Confluence page 4192337966 for HDFS paths and upstream Airflow base URL.\n"
            f"4. Check upstream DAG status: call get_airflow_dag_runs for "
            f"ECO_CROSS_PAGE_EVENT_SUMMARY_SSD_MINUTE_DAG at upstream_dt (={exec_date} − 2 min):\n"
            f"   BRANCH aa (no run): check HDFS data → if ready, propose trigger + batch rerun.\n"
            f"   BRANCH ab (run failed): get failed task log, summarise error, tell user: "
            f"'Fix then ask me: rerun ECO_CROSS_PAGE_EVENT_SUMMARY_SSD_MINUTE_DAG for [upstream_dt]'.\n"
            f"   BRANCH ac (running/queued): tell user to wait, no action needed.\n"
            f"   BRANCH ad (success): check HDFS — if ready, skip trigger, just propose batch rerun.\n"
            f"5. Use propose_flow_feed_reruns_batch(dag_ids=[all DAGs from step 1], "
            f"start_date=`{exec_date}`, end_date=`{exec_date}`) for the rerun proposal — "
            f"NOT propose_rerun_dag_runs. This gives the user a checkbox UI to select pipelines."
        )

    return (
        f"PagerDuty alert received for pipeline `{dag_id}`:{pd_ref}\n"
        f"- Failing task: `{task_id}`\n"
        f"- Execution time: `{exec_date}`\n"
        f"- Task state: `{state}`\n\n"
        f"Please investigate why this pipeline failed. "
        f"Check Confluence for known issues first, then check the failed runs using "
        f"state='failed', read the task log for `{task_id}` at this execution time, "
        f"check upstream dependencies, and provide a structured RCA with recommended fix steps."
    )


def classify_intent(text: str) -> str:
    low = text.lower().strip()

    # ── Fast keyword shortcuts (more reliable than LLM for obvious phrases) ──
    update_kw = ["update runbook", "add to runbook", "update the runbook",
                 "add this to runbook", "add to the runbook", "runbook update",
                 "update confluence", "add to confluence"]
    sync_kw   = ["sync this thread", "sync thread", "sync to runbook",
                 "sync to confluence", "sync this to confluence"]
    confirm_kw = {"yes", "confirm", "looks good", "apply it", "do it",
                  "approve", "go ahead", "apply", "yep", "yeah", "sure"}
    cancel_kw  = {"no", "cancel", "nevermind", "never mind", "stop", "discard", "nope",
                  "cancel rerun", "stop rerun", "cancel interleaved", "stop interleaved"}

    remember_kw = ["remember:", "remember this:", "bot remember", "save to memory", "save this to memory"]
    if any(kw in low for kw in remember_kw):
        return "remember"
    if any(kw in low for kw in update_kw):
        return "update"
    if any(kw in low for kw in sync_kw):
        return "sync"
    if low in confirm_kw:
        return "confirm"
    if low in cancel_kw:
        return "cancel"

    # ── Fall back to LLM for ambiguous messages ──
    try:
        resp = anthropic.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=20,
            system=INTENT_SYSTEM,
            messages=[{"role": "user", "content": text}],
        )
        raw = resp.content[0].text.strip()
        return json.loads(raw).get("intent", "answer")
    except Exception:
        return "answer"

# ─── Agent Tools (callable by Claude during reasoning) ────────────────────────

from urllib.parse import urlparse as _urlparse

def _airflow_api_base(raw_url: str) -> str:
    """Normalise an Airflow URL to an API base.
    Strips trailing UI page segments (/home, /dags, /graph …) from any path depth.
    E.g. https://host/datafeeds-airflow/airflow/home  →  https://host/datafeeds-airflow/airflow
    """
    p = _urlparse(raw_url)
    ui_suffixes = {"/home", "/dags", "/graph", "/tree", "/grid", "/log", "/gantt",
                   "/dag_runs", "/tasks", "/duration", "/landing_times", "/tries"}
    path = p.path.rstrip("/")
    # Strip trailing UI segment if present
    for suffix in ui_suffixes:
        if path.endswith(suffix):
            path = path[: -len(suffix)]
            break
    return f"{p.scheme}://{p.netloc}{path}"

# Both Airflow instances share the same credentials
AIRFLOW_USER     = os.environ.get("AIRFLOW_USERNAME", "")
AIRFLOW_PASSWORD = os.environ.get("AIRFLOW_PASSWORD", "")

# Build normalised API bases for both instances
# Auto-discover all Airflow instances from env vars matching AIRFLOW_<label>_URL
# To add a new instance, just add a line to .env:
#   AIRFLOW_streamid_URL=https://streamid-airflow.prod.conviva.com/...
#   AIRFLOW_mds_URL=http://mds-airflow.prod.conviva.com:8080/home
# No code changes needed — the bot picks them up automatically on restart.

AIRFLOW_INSTANCES = {}   # label → api_base
for _key, _val in os.environ.items():
    if _key.startswith("AIRFLOW_") and _key.endswith("_URL") and _val.strip():
        # Extract label: AIRFLOW_connect_URL → "connect", AIRFLOW_streamid_URL → "streamid"
        _label = _key[len("AIRFLOW_"):-len("_URL")].lower()
        AIRFLOW_INSTANCES[_label] = _airflow_api_base(_val.strip())

logger.info(f"Airflow instances loaded: {list(AIRFLOW_INSTANCES.keys())}")

# Slack channels the agent can read for context
SEARCHABLE_SLACK_CHANNELS = {
    "ces-internal-ssd":   "C07KV3PB79C",
    "wendy-sssd-test":    "C0ARYHQ727P",
    "piccolo-daas-alert": "C03KA6FQR1C",
}

def tool_search_confluence(query: str, max_results: int = 5) -> str:
    """Search Confluence for pages matching a query. Returns page titles + excerpts."""
    try:
        url = f"{CONFLUENCE_BASE}/rest/api/content/search"
        params = {
            "cql": f'type=page AND space="CSS" AND text~"{query}"',
            "limit": max_results,
            "expand": "body.storage",
        }
        resp = requests.get(url, params=params, auth=(CONFLUENCE_EMAIL, CONFLUENCE_TOKEN), timeout=15)
        resp.raise_for_status()
        results = resp.json().get("results", [])
        if not results:
            return f"No Confluence pages found for: {query}"
        parts = []
        for r in results:
            html  = r.get("body", {}).get("storage", {}).get("value", "")
            soup  = BeautifulSoup(html, "html.parser")
            text  = soup.get_text(separator=" ", strip=True)[:1500]
            title = r.get("title", "Untitled")
            pid   = r.get("id", "")
            parts.append(f"### {title}\nURL: {CONFLUENCE_BASE}/spaces/CSS/pages/{pid}\n{text}")
        return "\n\n---\n\n".join(parts)
    except Exception as e:
        logger.error(f"tool_search_confluence error: {e}")
        return f"Confluence search failed: {e}"


def tool_read_confluence_page(page_ref: str) -> str:
    """Read a specific Confluence page by URL or page ID and return its full text content."""
    # Extract page ID from a URL like:
    #   https://conviva.atlassian.net/wiki/spaces/CSS/pages/2584412251/...
    #   https://conviva.atlassian.net/wiki/spaces/CSS/pages/2584412251
    page_id = page_ref.strip()
    url_match = re.search(r"/pages/(\d+)", page_ref)
    if url_match:
        page_id = url_match.group(1)
    elif not page_id.isdigit():
        return f"Could not extract a page ID from: {page_ref}. Please provide a Confluence URL or numeric page ID."

    page = fetch_confluence_page(page_id)
    if not page:
        return f"Could not fetch Confluence page {page_id}. Check that the page exists and the bot has access."

    return (
        f"### {page['title']} (v{page['version']})\n"
        f"URL: {CONFLUENCE_BASE}/spaces/CSS/pages/{page_id}\n\n"
        f"{page['body_text']}"
    )


# Cache JWT tokens per Airflow instance so we don't re-login every call
_airflow_tokens: dict = {}

def _get_airflow_headers(base: str) -> dict:
    """Return auth headers for an Airflow instance.
    Tries JWT token auth first (Airflow 2.x), falls back to basic auth.
    Caches whichever method works so we don't retry JWT on every call."""
    if base in _airflow_tokens:
        return {"Accept": "application/json", "Authorization": _airflow_tokens[base]}
    if not AIRFLOW_USER:
        return {"Accept": "application/json"}

    import base64 as _b64
    basic_value = "Basic " + _b64.b64encode(f"{AIRFLOW_USER}:{AIRFLOW_PASSWORD}".encode()).decode()

    # Try JWT login (Airflow 2.x REST API)
    try:
        login_url = f"{base}/api/v1/security/login"
        r = requests.post(login_url, json={
            "username": AIRFLOW_USER,
            "password": AIRFLOW_PASSWORD,
            "provider": "db",
        }, verify=False, timeout=10, headers={"Content-Type": "application/json"})
        if r.status_code == 200:
            token = r.json().get("access_token", "")
            if token:
                logger.info(f"Airflow JWT login succeeded for {base}")
                _airflow_tokens[base] = f"Bearer {token}"
                return {"Accept": "application/json", "Authorization": f"Bearer {token}"}
        logger.info(f"Airflow JWT login failed ({r.status_code}) for {base}, using basic auth")
    except Exception as e:
        logger.warning(f"Airflow JWT login error for {base}: {e}")

    # Cache basic auth so we don't retry JWT on every subsequent call
    _airflow_tokens[base] = basic_value
    return {"Accept": "application/json", "Authorization": basic_value}


def _airflow_request(base: str, path: str, params: dict = None) -> dict:
    """Try Airflow 2.x REST API, then fall back to 1.x experimental API."""
    headers = _get_airflow_headers(base)
    # Connect Airflow (airflow-prod.mds.conviva.com) is significantly slower than
    # the other instances — use a longer timeout to avoid spurious read timeouts.
    _timeout = 45 if "mds.conviva.com" in base else 15
    # Try v2
    url = f"{base}/api/v1/{path}"
    logger.info(f"Airflow GET {url}")
    resp = requests.get(url, params=params, headers=headers, verify=False, timeout=_timeout)
    logger.info(f"  → status={resp.status_code} body={resp.text[:300]!r}")

    if resp.status_code == 401:
        # Token may have expired — clear cache and retry once
        _airflow_tokens.pop(base, None)
        headers = _get_airflow_headers(base)
        resp = requests.get(url, params=params, headers=headers, verify=False, timeout=_timeout)

    if resp.status_code == 404:
        # Try Airflow 1.x experimental API
        url_exp = f"{base}/api/experimental/{path}"
        logger.info(f"Airflow v2 404 — trying experimental {url_exp}")
        resp = requests.get(url_exp, params=params, headers=headers, verify=False, timeout=_timeout)
        logger.info(f"  → status={resp.status_code} body={resp.text[:300]!r}")
        if resp.ok:
            data = resp.json()
            return {"dags": data} if isinstance(data, list) else data

    if not resp.ok:
        return {"error": f"HTTP {resp.status_code}: {resp.text[:200]}"}

    return resp.json()


def _airflow_get(path: str, params: dict = None) -> dict:
    """Query all configured Airflow instances, return {label: response_or_error}."""
    results = {}
    for label, base in AIRFLOW_INSTANCES.items():
        try:
            results[label] = _airflow_request(base, path, params)
        except Exception as e:
            logger.error(f"Airflow {label} exception: {e}", exc_info=False)
            results[label] = {"error": str(e)}
    return results


def _airflow_get_all_dags(label: str, base: str) -> dict:
    """Fetch ALL DAGs from an Airflow instance, paginating if needed."""
    all_dags = []
    offset   = 0
    page_size = 100
    while True:
        data = _airflow_request(base, "dags", params={"limit": page_size, "offset": offset})
        if "error" in data:
            return data
        batch = data.get("dags", [])
        all_dags.extend(batch)
        total = data.get("total_entries", len(all_dags))
        logger.info(f"Airflow {label}: fetched {len(all_dags)}/{total} DAGs (offset={offset})")
        if len(all_dags) >= total or len(batch) < page_size:
            break
        offset += page_size
    return {"dags": all_dags, "total_entries": len(all_dags)}


def tool_get_airflow_dags(name_filters: list = None, instance: str = None) -> str:
    """List DAGs from Airflow instances, with optional name filter and instance filter.

    instance: restrict to a specific Airflow instance by label (e.g. 'legacy', 'connect', 'streamid').
              Leave empty to query all instances.
    name_filters: filter DAGs whose ID contains any of these substrings (OR logic).
                  Leave empty to list ALL DAGs from the selected instance(s).
    """
    if not AIRFLOW_INSTANCES:
        return "No Airflow URLs configured. Add AIRFLOW_<label>_URL entries to .env (e.g. AIRFLOW_connect_URL, AIRFLOW_streamid_URL)"

    # Normalise filters
    if isinstance(name_filters, str):
        name_filters = [name_filters]
    filters = [f.lower() for f in (name_filters or []) if f]

    # Restrict to a specific instance if requested
    instances_to_query = {}
    if instance:
        label = instance.lower().strip()
        if label not in AIRFLOW_INSTANCES:
            available = ", ".join(AIRFLOW_INSTANCES.keys())
            return f"Unknown instance '{instance}'. Available: {available}"
        instances_to_query[label] = AIRFLOW_INSTANCES[label]
    else:
        instances_to_query = AIRFLOW_INSTANCES

    sections = []
    for label, base in instances_to_query.items():
        data = _airflow_get_all_dags(label, base)
        if "error" in data:
            sections.append(f"*{label} Airflow* — ❌ unreachable: {data['error']}")
            continue
        dags = data.get("dags", [])
        total_before_filter = len(dags)
        logger.info(f"Airflow {label}: got {total_before_filter} total DAGs before filtering")
        if filters:
            dags = [d for d in dags
                    if any(f in d.get("dag_id", "").lower() for f in filters)]
        if not dags:
            msg = f"No DAGs matching {filters} (out of {total_before_filter} total)" if filters else "No DAGs found"
            sections.append(f"*{label} Airflow* — {msg}")
            continue

        active = [d for d in dags if not d.get("is_paused")]
        paused = [d for d in dags if d.get("is_paused")]
        filter_desc = f" matching {filters}" if filters else ""
        lines = [f"*{label} Airflow* — {len(dags)} DAG(s){filter_desc}: {len(active)} active, {len(paused)} paused"]
        for d in dags[:50]:
            status = "paused" if d.get("is_paused") else "active"
            lines.append(f"  - {d['dag_id']} ({status})")
        if len(dags) > 50:
            lines.append(f"  ... and {len(dags) - 50} more")
        sections.append("\n".join(lines))

    return "\n\n".join(sections)


def tool_list_all_dag_ids(sample_size: int = 30) -> str:
    """Return a sample of actual DAG IDs from all Airflow instances so we can identify naming patterns."""
    if not AIRFLOW_INSTANCES:
        return "No Airflow URLs configured."
    sections = []
    for label, base in AIRFLOW_INSTANCES.items():
        data = _airflow_get_all_dags(label, base)
        if "error" in data:
            sections.append(f"*{label} Airflow* — ❌ {data['error']}")
            continue
        dags  = data.get("dags", [])
        total = len(dags)
        sample = [d.get("dag_id", "") for d in dags[:sample_size]]
        sections.append(f"*{label} Airflow* — {total} total DAGs, first {len(sample)}:\n" +
                        "\n".join(f"  - {d}" for d in sample))
    return "\n\n".join(sections)


def _airflow_get_dag_runs_in_range(base: str, dag_id: str, start_dt: str, end_dt: str) -> list:
    """Fetch ALL DAG runs for a DAG within an execution date range, paginated."""
    all_runs = []
    offset = 0
    page_size = 500
    while True:
        data = _airflow_request(base, f"dags/{dag_id}/dagRuns", params={
            "execution_date_gte": start_dt,
            "execution_date_lte": end_dt,
            "limit":  page_size,
            "offset": offset,
            "order_by": "execution_date",
        })
        if "error" in data:
            logger.warning(f"_airflow_get_dag_runs_in_range error: {data['error']}")
            return []
        batch = data.get("dag_runs", [])
        all_runs.extend(batch)
        total = data.get("total_entries", len(all_runs))
        if len(all_runs) >= total or len(batch) < page_size:
            break
        offset += page_size
    return all_runs


def _airflow_get_max_active_runs(base: str, dag_id: str, default: int = 10) -> int:
    """Return the max_active_runs setting for a DAG. Falls back to `default` if unreadable."""
    data = _airflow_request(base, f"dags/{dag_id}")
    mar = data.get("max_active_runs")
    if isinstance(mar, int) and mar > 0:
        return mar
    return default


def _airflow_count_active_in_range(base: str, dag_id: str, start_dt: str, end_dt: str) -> int:
    """Count DAG runs in running or queued state within an execution date window."""
    total = 0
    for state in ("running", "queued"):
        data = _airflow_request(base, f"dags/{dag_id}/dagRuns", params={
            "execution_date_gte": start_dt,
            "execution_date_lte": end_dt,
            "state": state,
            "limit": 500,
        })
        total += len(data.get("dag_runs", []))
    return total


def _airflow_mark_dag_run(base: str, dag_id: str, dag_run_id: str, state: str) -> bool:
    """PATCH a single DAG run's state via the Airflow REST API."""
    headers = _get_airflow_headers(base)
    url = f"{base}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}"
    try:
        resp = requests.patch(url, headers=headers, json={"state": state}, verify=False, timeout=30)
        if not resp.ok:
            logger.warning(f"mark_dag_run {dag_run_id} → {resp.status_code}: {resp.text[:200]}")
        return resp.ok
    except Exception as e:
        logger.error(f"_airflow_mark_dag_run error: {e}")
        return False


def _airflow_sweep_running_runs(base: str, dag_id: str, start_dt: str, end_dt: str) -> dict:
    """Find any DAG runs still in 'running' state within the window and PATCH them to success.
    Used as a post-mark sweep to catch runs that were active during the main pass.
    Returns {"swept": <count>, "errors": [list of str]}"""
    headers = _get_airflow_headers(base)
    url = f"{base}/api/v1/dags/{dag_id}/dagRuns"
    swept  = 0
    errors = []
    try:
        resp = requests.get(url, headers=headers, params={
            "state":                "running",
            "execution_date_gte":   start_dt,
            "execution_date_lte":   end_dt,
            "limit":                500,
        }, verify=False, timeout=30)
        if not resp.ok:
            return {"swept": 0, "errors": [f"GET running runs failed: {resp.status_code}"]}
        runs = resp.json().get("dag_runs", [])
        for r in runs:
            run_id    = r.get("dag_run_id", "")
            patch_url = f"{base}/api/v1/dags/{dag_id}/dagRuns/{run_id}"
            try:
                pr = requests.patch(patch_url, headers=headers,
                                    json={"state": "success"}, verify=False, timeout=30)
                if pr.ok:
                    swept += 1
                else:
                    errors.append(f"{run_id}: PATCH {pr.status_code}")
            except Exception as e:
                errors.append(f"{run_id}: {e}")
    except Exception as e:
        return {"swept": 0, "errors": [str(e)]}
    return {"swept": swept, "errors": errors}


def _airflow_kill_running_tasks(base: str, dag_id: str, start_dt: str, end_dt: str) -> dict:
    """Clear (interrupt + reset) all currently-running task instances in the given
    execution-date window.  Uses clearTaskInstances with only_running=True so only
    active workers are affected.  reset_dag_runs=False keeps the DAG run states we
    already set via PATCH.  Returns {"killed": <count>, "error": <str or None>}."""
    headers = _get_airflow_headers(base)
    url = f"{base}/api/v1/dags/{dag_id}/clearTaskInstances"
    payload = {
        "dry_run":        False,
        "start_date":     start_dt,
        "end_date":       end_dt,
        "only_running":   True,
        "only_failed":    False,
        "reset_dag_runs": False,
    }
    try:
        resp = requests.post(url, headers=headers, json=payload, verify=False, timeout=60)
        if not resp.ok:
            logger.warning(f"kill_running_tasks {dag_id} → {resp.status_code}: {resp.text[:200]}")
            return {"killed": 0, "error": f"{resp.status_code}: {resp.text[:200]}"}
        killed = len(resp.json().get("task_instances", []))
        logger.info(f"kill_running_tasks {dag_id} killed {killed} task instance(s)")
        return {"killed": killed, "error": None}
    except Exception as e:
        logger.error(f"_airflow_kill_running_tasks error: {e}")
        return {"killed": 0, "error": str(e)}


def _airflow_get_dag_schedule(base: str, dag_id: str):
    """Return the schedule_interval object for a DAG from the Airflow API.
    Returns the raw dict/string from the API, or None on failure."""
    headers = _get_airflow_headers(base)
    url = f"{base}/api/v1/dags/{dag_id}"
    try:
        resp = requests.get(url, headers=headers, verify=False, timeout=30)
        if resp.ok:
            data = resp.json()
            return data.get("schedule_interval") or data.get("timetable_description")
    except Exception as e:
        logger.error(f"_airflow_get_dag_schedule error: {e}")
    return None


def _schedule_to_timedelta(schedule):
    """Parse an Airflow schedule_interval value to a timedelta.
    Handles:
      - {"__type": "TimeDelta", "days": N, "seconds": N, ...}
      - {"__type": "CronExpression", "value": "<cron>"}
      - plain strings like "* * * * *", "@hourly", "@daily"
    Returns a timedelta or None if unrecognised."""
    import re as _re
    from datetime import timedelta as _td

    if isinstance(schedule, dict):
        t = schedule.get("__type", "")
        if t == "TimeDelta":
            return _td(
                days=schedule.get("days", 0),
                seconds=schedule.get("seconds", 0),
                microseconds=schedule.get("microseconds", 0),
            )
        if t == "CronExpression":
            schedule = schedule.get("value", "")

    if not isinstance(schedule, str):
        return None

    schedule = schedule.strip()
    # Named shortcuts
    if schedule in ("@minutely", "* * * * *", "*/1 * * * *"):
        return _td(minutes=1)
    if schedule == "@hourly" or _re.fullmatch(r"0 \* \* \* \*", schedule):
        return _td(hours=1)
    if schedule == "@daily" or _re.fullmatch(r"0 0 \* \* \*", schedule):
        return _td(days=1)
    # */N * * * *  (every N minutes)
    m = _re.fullmatch(r"\*/(\d+) \* \* \* \*", schedule)
    if m:
        return _td(minutes=int(m.group(1)))
    # 0 */N * * *  (every N hours)
    m = _re.fullmatch(r"0 \*/(\d+) \* \* \*", schedule)
    if m:
        return _td(hours=int(m.group(1)))
    return None


def _airflow_find_run_id(base: str, dag_id: str, logical_date: str):
    """Look up the actual dag_run_id for a given logical_date.
    The run may exist as 'scheduled__...', 'manual__...', etc. — not just 'backfill__...'."""
    headers = _get_airflow_headers(base)
    url = f"{base}/api/v1/dags/{dag_id}/dagRuns"
    try:
        resp = requests.get(url, headers=headers, params={
            "execution_date_gte": logical_date,
            "execution_date_lte": logical_date,
            "limit": 1,
        }, verify=False, timeout=30)
        if resp.ok:
            runs = resp.json().get("dag_runs", [])
            if runs:
                return runs[0].get("dag_run_id")
    except Exception as e:
        logger.error(f"_airflow_find_run_id error: {e}")
    return None


def _airflow_backfill_create_or_patch(base: str, dag_id: str, logical_date: str) -> dict:
    """Ensure a dag run exists for logical_date and is marked success.
    1. Look up actual run_id by logical_date (handles scheduled__/manual__/etc.)
    2. If found: PATCH existing run to success.
    3. If not found: POST to create with backfill__ run_id, then PATCH to success.
    Retries up to 3 times on timeout or 5xx errors.
    Returns {"ok": bool, "action": "patched"|"created"|"error", "detail": str}"""
    import time as _time

    max_attempts = 3
    last_error   = ""

    for attempt in range(1, max_attempts + 1):
        try:
            headers = _get_airflow_headers(base)

            # Step 1: find the real run_id (may differ from backfill__ prefix)
            existing_run_id = _airflow_find_run_id(base, dag_id, logical_date)

            if existing_run_id:
                patch_url = f"{base}/api/v1/dags/{dag_id}/dagRuns/{existing_run_id}"
                resp = requests.patch(patch_url, headers=headers,
                                      json={"state": "success"}, verify=False, timeout=30)
                if resp.ok:
                    return {"ok": True, "action": "patched", "detail": ""}
                if resp.status_code < 500:
                    # Client error — no point retrying
                    return {"ok": False, "action": "error",
                            "detail": f"PATCH {resp.status_code}: {resp.text[:100]}"}
                last_error = f"PATCH {resp.status_code}: {resp.text[:100]}"

            else:
                # Step 2: run doesn't exist — create it (state is read-only on POST)
                run_id    = f"backfill__{logical_date}"
                post_url  = f"{base}/api/v1/dags/{dag_id}/dagRuns"
                patch_url = f"{base}/api/v1/dags/{dag_id}/dagRuns/{run_id}"
                resp = requests.post(post_url, headers=headers,
                                     json={"dag_run_id": run_id, "logical_date": logical_date},
                                     verify=False, timeout=30)
                if not resp.ok:
                    if resp.status_code < 500:
                        return {"ok": False, "action": "error",
                                "detail": f"POST {resp.status_code}: {resp.text[:100]}"}
                    last_error = f"POST {resp.status_code}: {resp.text[:100]}"
                else:
                    patch_resp = requests.patch(patch_url, headers=headers,
                                                json={"state": "success"}, verify=False, timeout=30)
                    if patch_resp.ok:
                        return {"ok": True, "action": "created", "detail": ""}
                    if patch_resp.status_code < 500:
                        return {"ok": False, "action": "error",
                                "detail": f"created but PATCH failed {patch_resp.status_code}: {patch_resp.text[:100]}"}
                    last_error = f"created but PATCH failed {patch_resp.status_code}: {patch_resp.text[:100]}"

        except requests.exceptions.Timeout:
            last_error = f"Read timed out (attempt {attempt})"
        except Exception as e:
            last_error = str(e)
            if attempt == max_attempts:
                break

        if attempt < max_attempts:
            _time.sleep(3 * attempt)  # 3s, then 6s before retrying

    return {"ok": False, "action": "error", "detail": last_error}


def tool_propose_backfill_dag_runs(dag_id: str, start_date: str, end_date: str,
                                   instance: str = None, kill_running: bool = False,
                                   channel: str = "", thread_ts: str = "",
                                   user: str = "", client=None) -> str:
    """Propose a backfill-mark-success over a time window.
    Unlike propose_mark_dag_runs, this creates missing dag run records so it works
    even when max_active_runs prevented the scheduler from creating those runs.
    If kill_running=True, also kills any currently-running tasks before marking."""
    if not AIRFLOW_INSTANCES:
        return "No Airflow URLs configured."

    from datetime import datetime as _dt, timedelta as _td, timezone as _tz

    start_dt_str = _normalise_dt(start_date)
    end_dt_str   = _normalise_dt(end_date)

    try:
        start_dt = _dt.fromisoformat(start_dt_str)
        end_dt   = _dt.fromisoformat(end_dt_str)
    except ValueError as e:
        return f"Could not parse dates: {e}"

    # Determine target instance
    if instance:
        label = instance.lower().strip()
        if label not in AIRFLOW_INSTANCES:
            return f"Unknown instance '{instance}'. Available: {', '.join(AIRFLOW_INSTANCES.keys())}"
        target_instances = {label: AIRFLOW_INSTANCES[label]}
    else:
        # Try all instances; use first one that knows the DAG
        target_instances = AIRFLOW_INSTANCES

    # Find which instance owns this DAG and get its schedule
    chosen_label = None
    chosen_base  = None
    interval     = None
    for label, base in target_instances.items():
        sched = _airflow_get_dag_schedule(base, dag_id)
        if sched is not None:
            td = _schedule_to_timedelta(sched)
            if td:
                chosen_label = label
                chosen_base  = base
                interval     = td
                break
            else:
                return (
                    f"Found `{dag_id}` on instance `{label}` but couldn't parse its schedule "
                    f"(`{sched}`). Please specify the interval manually or use the Airflow CLI:\n"
                    f"`airflow dags backfill --mark-success -d {dag_id} "
                    f"--start-date {start_dt_str} --end-date {end_dt_str}`"
                )

    if not chosen_base:
        return (
            f"Could not find `{dag_id}` on any Airflow instance, or schedule unreadable. "
            f"Check the DAG ID is correct."
        )

    # Generate all logical dates
    dates = []
    cur = start_dt
    while cur <= end_dt:
        dates.append(cur.strftime("%Y-%m-%dT%H:%M:%S+00:00"))
        cur += interval

    if not dates:
        return "No dates generated — check that start_date is before end_date."

    # Store for confirmation
    pending_backfill[(channel, user)] = {
        "dag_id":       dag_id,
        "base":         chosen_base,
        "label":        chosen_label,
        "dates":        dates,
        "start_dt":     start_dt_str,
        "end_dt":       end_dt_str,
        "kill_running": kill_running,
        "thread_ts":    thread_ts,
    }

    interval_label = (
        f"{int(interval.total_seconds() // 60)} min" if interval.total_seconds() < 3600
        else f"{int(interval.total_seconds() // 3600)} hour"
    )

    kill_note = "\n⚡ *Kill running tasks:* Yes — active tasks will be interrupted before marking." if kill_running else ""

    if client:
        client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=(
                f"⚠️ *Proposed: Backfill-Mark-Success*\n\n"
                f"*DAG:* `{dag_id}` (instance: `{chosen_label}`)\n"
                f"*Schedule:* every {interval_label}\n"
                f"*Range:* `{start_dt_str}` → `{end_dt_str}`\n"
                f"*Runs to create/mark:* {len(dates)}\n"
                f"*First:* `{dates[0]}` UTC\n"
                f"*Last:* `{dates[-1]}` UTC"
                f"{kill_note}\n\n"
                f"This will create any missing dag run records and mark all of them as *success*, "
                f"bypassing `max_active_runs`. No tasks will execute.\n\n"
                f"Reply *yes* to confirm, or *no* to cancel."
            ),
        )
    return f"✅ Preview posted. Waiting for yes/no to backfill {len(dates)} run(s) as success."


def _normalise_dt(dt_str: str) -> str:
    """Normalise a user-provided date string to ISO 8601 UTC for Airflow API.
    Accepts: '2026-05-22 05:04:00', '2026-05-22T05:04:00', '2026-05-22T05:04:00Z'
    Returns: '2026-05-22T05:04:00+00:00'
    """
    dt_str = dt_str.strip().replace(" ", "T")
    if dt_str.endswith("Z"):
        dt_str = dt_str[:-1] + "+00:00"
    elif "+" not in dt_str and not dt_str.endswith("+00:00"):
        dt_str += "+00:00"
    return dt_str


def tool_propose_mark_dag_runs(dag_id: str, start_date: str, end_date: str, state: str,
                                instance: str = None, kill_running: bool = False,
                                channel: str = "", thread_ts: str = "",
                                user: str = "", client=None) -> str:
    """Stage a bulk DAG run state change for human confirmation.
    Looks up all runs in the given window, shows a preview, and waits for yes/no.
    If kill_running=True, also interrupts any currently-running task instances."""
    if not AIRFLOW_INSTANCES:
        return "No Airflow URLs configured."

    state = state.lower().strip()
    if state not in {"success", "failed"}:
        return f"Invalid state '{state}'. Must be 'success' or 'failed'."

    start_dt = _normalise_dt(start_date)
    end_dt   = _normalise_dt(end_date)

    # Determine which instances to search
    instances_to_search = {}
    if instance:
        label = instance.lower().strip()
        if label not in AIRFLOW_INSTANCES:
            return f"Unknown instance '{instance}'. Available: {', '.join(AIRFLOW_INSTANCES.keys())}"
        instances_to_search[label] = AIRFLOW_INSTANCES[label]
    else:
        instances_to_search = AIRFLOW_INSTANCES

    # Collect all matching runs
    found = []
    for label, base in instances_to_search.items():
        runs = _airflow_get_dag_runs_in_range(base, dag_id, start_dt, end_dt)
        for r in runs:
            found.append({
                "label":         label,
                "base":          base,
                "dag_run_id":    r.get("dag_run_id", ""),
                "execution_date": r.get("execution_date", "")[:19],
                "current_state": r.get("state", "?"),
            })

    if not found:
        return (
            f"No runs found for `{dag_id}` between `{start_dt}` and `{end_dt}`. "
            f"Check the DAG ID and date range are correct, or try specifying an instance."
        )

    # Summarise current states
    state_counts: dict = {}
    for r in found:
        s = r["current_state"]
        state_counts[s] = state_counts.get(s, 0) + 1
    state_summary = ", ".join(f"{v} {k}" for k, v in sorted(state_counts.items()))

    first_run = found[0]["execution_date"]
    last_run  = found[-1]["execution_date"]

    # Count currently-running runs for kill_running preview
    running_count = sum(1 for r in found if r["current_state"] == "running")

    # Store for confirmation
    pending_mark_runs[(channel, user)] = {
        "dag_id":       dag_id,
        "state":        state,
        "start_dt":     start_dt,
        "end_dt":       end_dt,
        "runs":         found,
        "kill_running": kill_running,
        "thread_ts":    thread_ts,
    }

    kill_note = ""
    if kill_running:
        if running_count > 0:
            kill_note = f"\n⚡ *Kill running tasks:* Yes — {running_count} run(s) currently `running`, their active tasks will be interrupted."
        else:
            kill_note = "\n⚡ *Kill running tasks:* Yes (no runs currently `running` — no tasks to interrupt)."

    if client:
        client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=(
                f"⚠️ *Proposed: Mark DAG Runs as `{state}`*\n\n"
                f"*DAG:* `{dag_id}`\n"
                f"*Range:* `{start_dt}` → `{end_dt}`\n"
                f"*Runs found:* {len(found)} ({state_summary})\n"
                f"*First run:* `{first_run}` UTC\n"
                f"*Last run:* `{last_run}` UTC"
                f"{kill_note}\n\n"
                f"Marking as *{state}* will skip these runs — they will *not* be re-executed.\n\n"
                f"Reply *yes* to confirm, or *no* to cancel."
            ),
        )
    return f"✅ Preview posted. Waiting for yes/no to mark {len(found)} run(s) as '{state}'."


def tool_propose_rerun_dag_runs(dag_id: str, start_date: str, end_date: str,
                                instance: str = None,
                                channel: str = "", thread_ts: str = "",
                                user: str = "", client=None) -> str:
    """Stage a bulk DAG run clear (re-run/backfill) for human confirmation.
    Clears task instances in the window so Airflow re-executes them from scratch.
    max_active_runs throttling still applies — Airflow paces the backfill automatically."""
    if not AIRFLOW_INSTANCES:
        return "No Airflow URLs configured."

    start_dt = _normalise_dt(start_date)
    end_dt   = _normalise_dt(end_date)

    # Determine which instances to search
    instances_to_search = {}
    if instance:
        label = instance.lower().strip()
        if label not in AIRFLOW_INSTANCES:
            return f"Unknown instance '{instance}'. Available: {', '.join(AIRFLOW_INSTANCES.keys())}"
        instances_to_search[label] = AIRFLOW_INSTANCES[label]
    else:
        instances_to_search = AIRFLOW_INSTANCES

    # Look up runs in the range so we can show an accurate count in the preview
    found = []
    for label, base in instances_to_search.items():
        runs = _airflow_get_dag_runs_in_range(base, dag_id, start_dt, end_dt)
        for r in runs:
            found.append({
                "label":         label,
                "base":          base,
                "execution_date": r.get("execution_date", "")[:19],
                "current_state": r.get("state", "?"),
            })

    if not found:
        return (
            f"No runs found for `{dag_id}` between `{start_dt}` and `{end_dt}`. "
            f"Check the DAG ID and date range are correct."
        )

    # Summarise current states
    state_counts: dict = {}
    for r in found:
        s = r["current_state"]
        state_counts[s] = state_counts.get(s, 0) + 1
    state_summary = ", ".join(f"{v} {k}" for k, v in sorted(state_counts.items()))

    first_run = found[0]["execution_date"]
    last_run  = found[-1]["execution_date"]

    # Group instances that actually have runs (for the actual clear call)
    instances_with_runs = list({r["label"]: r["base"] for r in found}.items())

    rerun_data = {
        "dag_id":               dag_id,
        "start_dt":             start_dt,
        "end_dt":               end_dt,
        "instances_with_runs":  instances_with_runs,
        "run_count":            len(found),
        "thread_ts":            thread_ts,
        "state_summary":        state_summary,
        "first_run":            first_run,
        "last_run":             last_run,
    }

    key = (channel, user)

    # If an upstream trigger OR another rerun is already awaiting confirmation,
    # queue this rerun instead of posting it now — prevents ambiguous "yes" responses.
    if key in pending_trigger_upstream or key in pending_rerun_runs:
        if key not in pending_rerun_queue:
            pending_rerun_queue[key] = []
        pending_rerun_queue[key].append(rerun_data)
        if client:
            queue_len = len(pending_rerun_queue[key])
            client.chat_postMessage(
                channel=channel, thread_ts=thread_ts,
                text=(
                    f"📋 *Queued: Re-run `{dag_id}`* ({len(found)} run(s))\n"
                    f"Will be proposed after the current action is confirmed."
                    f" (Queue position: {queue_len})"
                ),
            )
        return f"Queued rerun for {dag_id} ({len(found)} runs) — will propose after current action confirmed."

    # No conflict — post proposal immediately and make it active
    pending_rerun_runs[key] = rerun_data

    if client:
        _post_rerun_proposal(client, channel, thread_ts, rerun_data)
    return f"✅ Preview posted. Waiting for yes/no to clear {len(found)} run(s) for re-execution."


def _post_rerun_proposal(client, channel: str, thread_ts: str, rerun_data: dict):
    """Post the rerun confirmation message to Slack."""
    client.chat_postMessage(
        channel=channel,
        thread_ts=thread_ts,
        text=(
            f"♻️ *Proposed: Re-run / Backfill DAG Runs*\n\n"
            f"*DAG:* `{rerun_data['dag_id']}`\n"
            f"*Range:* `{rerun_data['start_dt']}` → `{rerun_data['end_dt']}`\n"
            f"*Runs found:* {rerun_data['run_count']} ({rerun_data['state_summary']})\n"
            f"*First run:* `{rerun_data['first_run']}` UTC\n"
            f"*Last run:*  `{rerun_data['last_run']}` UTC\n\n"
            f"This will *clear* all {rerun_data['run_count']} run(s) so Airflow re-executes them. "
            f"`max_active_runs` throttling still applies — Airflow paces the backfill automatically.\n\n"
            f"Reply *yes* to confirm, or *no* to cancel."
        ),
    )


def _dequeue_next_rerun(client, channel: str, thread_ts: str, user: str):
    """After an action is confirmed, pop the next queued rerun and post its proposal."""
    key = (channel, user)
    queue = pending_rerun_queue.get(key, [])
    if not queue:
        return
    next_rerun = queue.pop(0)
    if not queue:
        pending_rerun_queue.pop(key, None)
    pending_rerun_runs[key] = next_rerun
    if client:
        _post_rerun_proposal(client, channel, next_rerun.get("thread_ts", thread_ts), next_rerun)


def tool_propose_flow_feed_reruns_batch(
    dag_ids: list, start_date: str, end_date: str,
    channel: str = "", thread_ts: str = "", user: str = "", client=None,
) -> str:
    """Propose rerunning multiple flow-feed DAGs in one Slack Block Kit message with checkboxes.

    The user can check/uncheck individual DAGs before clicking "Rerun Selected".
    All DAGs are pre-selected by default. Falls back gracefully if block_kit fails.

    Args:
        dag_ids:    List of DAG IDs to propose for rerun.
        start_date: Execution date/time window start (same as end for a single minute).
        end_date:   Execution date/time window end.
    """
    if not dag_ids:
        return "No DAG IDs provided."

    # Discover runs per DAG across all Airflow instances; capture states
    instances_map = {}  # dag_id → [(label, base)]
    run_counts    = {}  # dag_id → int
    run_states    = {}  # dag_id → set of state strings
    for dag_id in dag_ids:
        runs_found   = []
        states_seen  = set()
        for label, base in AIRFLOW_INSTANCES.items():
            runs = _airflow_get_dag_runs_in_range(base, dag_id, start_date, end_date)
            if runs:
                runs_found.append((label, base))
                run_counts[dag_id] = run_counts.get(dag_id, 0) + len(runs)
                for r in runs:
                    states_seen.add(r.get("state", "unknown"))
        instances_map[dag_id] = runs_found
        run_states[dag_id]    = states_seen

    # Classify each DAG by current Airflow run state.
    # Piccolo is the source of truth for who failed — if it's in the list, it failed.
    # We exclude only those already fixed (success) or actively being fixed (running/queued).
    # "no run found" = Airflow run missing (possibly on a different instance or execution_date
    # mismatch) — include it since piccolo confirmed the failure.
    eligible_dag_ids = []   # failed or no-run-found → propose rerun
    skipped_success  = []   # all runs success → already done
    skipped_running  = []   # running/queued → already being fixed

    for dag_id in dag_ids:
        states = run_states.get(dag_id, set())
        if not states:
            # No run found in Airflow; piccolo said it failed → still propose
            eligible_dag_ids.append(dag_id)
        elif any(s == "success" for s in states) and not any(s == "failed" for s in states):
            skipped_success.append(dag_id)
        elif any(s in {"running", "queued", "up_for_retry"} for s in states) and not any(s == "failed" for s in states):
            skipped_running.append((dag_id, ", ".join(sorted(states))))
        else:
            # Has at least one failed run (or unknown state) → propose
            eligible_dag_ids.append(dag_id)

    # If nothing left to propose, report and stop
    if not eligible_dag_ids:
        lines = ["ℹ️ *No failed flow feed pipelines to rerun:*\n"]
        if skipped_success:
            lines.append("✅ *Already succeeded:*\n" +
                         "\n".join(f"  • `{d}`" for d in skipped_success))
        if skipped_running:
            lines.append("⏳ *Currently running/queued (wait for completion):*\n" +
                         "\n".join(f"  • `{d}` ({s})" for d, s in skipped_running))
        msg = "\n".join(lines)
        if client:
            client.chat_postMessage(channel=channel, thread_ts=thread_ts, text=msg)
        return msg

    # Note about what was skipped (shown at top of proposal message)
    skip_parts = []
    if skipped_success:
        skip_parts.append(
            f"{len(skipped_success)} already ✅ success: "
            + ", ".join(f"`{d}`" for d in skipped_success)
        )
    if skipped_running:
        skip_parts.append(
            f"{len(skipped_running)} ⏳ running/queued: "
            + ", ".join(f"`{d}`" for d, _ in skipped_running)
        )
    skipped_note = ("_(Skipping — " + "; ".join(skip_parts) + ")_\n\n") if skip_parts else ""

    # Build Block Kit checkbox options (all pre-selected)
    options = []
    for dag_id in eligible_dag_ids:
        count  = run_counts.get(dag_id, 0)
        states = run_states.get(dag_id, set())
        state_str = ", ".join(sorted(states)) if states else "no run in Airflow"
        label = f"`{dag_id}` — {count} run(s) ({state_str})"
        options.append({
            "text":  {"type": "mrkdwn", "text": label},
            "value": dag_id,
        })

    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    f"♻️ *Proposed: Rerun Flow Feed Pipelines*\n"
                    f"*Minute:* `{start_date}` → `{end_date}`\n\n"
                    f"{skipped_note}"
                    f"Select pipelines to rerun (all pre-selected):"
                ),
            },
        },
        {
            "type": "actions",
            "block_id": "flow_feed_checkboxes",
            "elements": [{
                "type":            "checkboxes",
                "action_id":       "flow_feed_select",
                "options":         options,
                "initial_options": options,   # all checked by default
            }],
        },
        {
            "type": "actions",
            "block_id": "flow_feed_buttons",
            "elements": [
                {
                    "type":      "button",
                    "text":      {"type": "plain_text", "text": "✅ Rerun Selected"},
                    "style":     "primary",
                    "action_id": "flow_feed_confirm_rerun",
                    "value":     "confirm",
                },
                {
                    "type":      "button",
                    "text":      {"type": "plain_text", "text": "❌ Cancel"},
                    "style":     "danger",
                    "action_id": "flow_feed_cancel_rerun",
                    "value":     "cancel",
                },
            ],
        },
    ]

    key = (channel, user)
    pending_flow_feed_batch[key] = {
        "dag_ids":       eligible_dag_ids,
        "instances_map": instances_map,
        "start_dt":      start_date,
        "end_dt":        end_date,
        "thread_ts":     thread_ts,
        "message_ts":    None,  # filled after post
    }

    if client:
        try:
            resp = client.chat_postMessage(
                channel=channel,
                thread_ts=thread_ts,
                text=(
                    f"♻️ Proposed rerun for {len(eligible_dag_ids)} flow-feed pipeline(s) "
                    f"at minute `{start_date}`. Click ✅ to confirm or ❌ to cancel."
                ),
                blocks=blocks,
            )
            pending_flow_feed_batch[key]["message_ts"] = resp.get("ts")
        except Exception as e:
            logger.warning(f"tool_propose_flow_feed_reruns_batch block_kit failed: {e} — falling back to text")
            client.chat_postMessage(
                channel=channel, thread_ts=thread_ts,
                text=(
                    (f"{skipped_note}" if skipped_note else "") +
                    f"♻️ *Proposed: Rerun {len(eligible_dag_ids)} Flow Feed Pipeline(s)*\n"
                    + "\n".join(
                        f"  • `{d}` ({run_counts.get(d, 0)} run(s), "
                        f"{', '.join(sorted(run_states.get(d, {'unknown'})))})"
                        for d in eligible_dag_ids
                    )
                    + f"\n\nRange: `{start_date}` → `{end_date}`\n\nReply *yes* to rerun all, or *no* to cancel."
                ),
            )

    return f"Batch rerun proposal posted for {len(eligible_dag_ids)} DAG(s). Waiting for user selection."


def _execute_flow_feed_batch_rerun(client, channel: str, thread_ts: str, user: str,
                                    selected_dag_ids: list):
    """Execute clear/rerun for the selected flow-feed DAGs from the batch proposal."""
    key = (channel, user)
    batch = pending_flow_feed_batch.pop(key, None)
    if not batch:
        return

    instances_map = batch["instances_map"]
    start_dt      = batch["start_dt"]
    end_dt        = batch["end_dt"]
    reply_ts      = batch.get("thread_ts", thread_ts)

    results = []
    for dag_id in selected_dag_ids:
        instances = instances_map.get(dag_id, [])
        if not instances:
            results.append(f"⚠️ `{dag_id}` — no runs found, skipped")
            continue
        ok_count = 0
        for label, base in instances:
            headers = _get_airflow_headers(base)
            url     = f"{base}/api/v1/dags/{dag_id}/clearTaskInstances"
            payload = {
                "start_date":     start_dt,
                "end_date":       end_dt,
                "reset_dag_runs": True,
                "dry_run":        False,
            }
            try:
                resp = requests.post(url, headers=headers, json=payload, verify=False, timeout=60)
                if resp.ok:
                    ok_count += len(resp.json().get("task_instances", []))
                else:
                    logger.warning(f"batch_rerun {dag_id} {label}: {resp.status_code} {resp.text[:200]}")
            except Exception as e:
                logger.error(f"batch_rerun {dag_id} {label} error: {e}")
        results.append(f"✅ `{dag_id}` — cleared")

    client.chat_postMessage(
        channel=channel, thread_ts=reply_ts,
        text=(
            f"🤖 *SSD Bot* — ♻️ *Rerun complete!*\n"
            + "\n".join(results)
            + f"\n\nAirflow will re-execute selected pipelines now."
        ),
    )


def _run_interleaved_rerun_worker(
    channel: str, thread_ts: str, dag_id: str,
    runs: list,            # list of {"execution_date": str, "label": str, "base": str}
    instances_with_runs: list,  # [(label, base), ...]
    batch_size: int, max_active_runs: int,
    client, stop_event: threading.Event,
):
    """Background thread: clears batch_size runs, waits for them to finish, repeats.
    Posts progress updates to the Slack thread. Stops early if stop_event is set."""
    import time as _time

    total       = len(runs)
    completed   = 0
    total_waves = math.ceil(total / batch_size)
    wave        = 0

    # Group runs by (label, base) for efficient per-instance clearing
    instance_map = {label: base for label, base in instances_with_runs}

    remaining = list(runs)  # copy — oldest first (already sorted by execution_date)

    while remaining and not stop_event.is_set():
        wave += 1
        batch = remaining[:batch_size]
        remaining = remaining[batch_size:]

        batch_start = batch[0]["execution_date"]
        batch_end   = batch[-1]["execution_date"]

        # ── Clear this batch ──
        wave_cleared = 0
        wave_errors  = []
        for label, base in instances_with_runs:
            headers = _get_airflow_headers(base)
            url     = f"{base}/api/v1/dags/{dag_id}/clearTaskInstances"
            payload = {
                "start_date":     batch_start,
                "end_date":       batch_end,
                "reset_dag_runs": True,
                "dry_run":        False,
            }
            try:
                resp = requests.post(url, headers=headers, json=payload, verify=False, timeout=60)
                if resp.ok:
                    wave_cleared += len(resp.json().get("task_instances", []))
                else:
                    wave_errors.append(f"{label}: HTTP {resp.status_code}")
                    logger.warning(f"interleaved rerun clearTaskInstances {label} wave {wave}: "
                                   f"{resp.status_code} {resp.text[:200]}")
            except Exception as e:
                wave_errors.append(f"{label}: {e}")
                logger.error(f"interleaved rerun clearTaskInstances exception {label}: {e}")

        completed += len(batch)
        err_note = f"  ⚠️ {', '.join(wave_errors)}" if wave_errors else ""
        remaining_count = len(remaining)

        client.chat_postMessage(
            channel=channel, thread_ts=thread_ts,
            text=(
                f"🤖 *SSD Bot* — ♻️ Wave {wave}/{total_waves}: "
                f"cleared {len(batch)} run(s) (`{batch_start[:19]}` → `{batch_end[:19]}`). "
                f"Progress: {completed}/{total} runs.{err_note}"
                + (f"\n⏳ Waiting for wave to finish before queuing next {min(batch_size, remaining_count)}…"
                   if remaining else "")
            ),
        )

        # ── Poll until all runs in this batch are no longer running/queued ──
        if remaining and not stop_event.is_set():
            polls = 0
            while not stop_event.is_set():
                _time.sleep(30)
                polls += 1
                active_count = 0
                for label, base in instances_with_runs:
                    active_count += _airflow_count_active_in_range(base, dag_id, batch_start, batch_end)
                logger.info(f"interleaved rerun poll wave {wave}: {active_count} active runs in batch range")
                if active_count == 0:
                    break
                # Log a heartbeat every ~5 min (10 polls × 30 s) to avoid silence
                if polls % 10 == 0:
                    client.chat_postMessage(
                        channel=channel, thread_ts=thread_ts,
                        text=(
                            f"🤖 *SSD Bot* — ⏳ Still waiting for wave {wave} to complete "
                            f"({active_count} run(s) still active). "
                            f"Next wave in a moment…"
                        ),
                    )

    # ── Final summary ──
    if stop_event.is_set():
        client.chat_postMessage(
            channel=channel, thread_ts=thread_ts,
            text=(
                f"🤖 *SSD Bot* — ⛔ Interleaved rerun *cancelled*.\n"
                f"Cleared {completed}/{total} run(s) before stopping. "
                f"Remaining {total - completed} run(s) were not touched."
            ),
        )
    else:
        client.chat_postMessage(
            channel=channel, thread_ts=thread_ts,
            text=(
                f"🤖 *SSD Bot* — ✅ Interleaved rerun *complete*!\n"
                f"All {total} run(s) of `{dag_id}` have been cleared and re-queued.\n"
                f"Airflow will execute them, paced by `max_active_runs={max_active_runs}`."
            ),
        )

    # Clean up the active-run record
    active_interleaved_reruns.pop((channel, thread_ts), None)


def tool_propose_interleaved_rerun_dag_runs(
    dag_id: str, start_date: str, end_date: str,
    instance: str = None,
    channel: str = "", thread_ts: str = "",
    user: str = "", client=None,
) -> str:
    """Stage an interleaved rerun: clears batch_size runs at a time, waits for each wave
    to finish before clearing the next, so the live forward pipeline keeps half the
    max_active_runs slots throughout the backfill."""
    if not AIRFLOW_INSTANCES:
        return "No Airflow URLs configured."

    start_dt = _normalise_dt(start_date)
    end_dt   = _normalise_dt(end_date)

    instances_to_search = {}
    if instance:
        label = instance.lower().strip()
        if label not in AIRFLOW_INSTANCES:
            return f"Unknown instance '{instance}'. Available: {', '.join(AIRFLOW_INSTANCES.keys())}"
        instances_to_search[label] = AIRFLOW_INSTANCES[label]
    else:
        instances_to_search = AIRFLOW_INSTANCES

    # Collect all runs in the window
    all_runs = []
    for label, base in instances_to_search.items():
        runs = _airflow_get_dag_runs_in_range(base, dag_id, start_dt, end_dt)
        for r in runs:
            all_runs.append({
                "label":          label,
                "base":           base,
                "execution_date": r.get("execution_date", "")[:19],
                "current_state":  r.get("state", "?"),
            })

    if not all_runs:
        return (
            f"No runs found for `{dag_id}` between `{start_dt}` and `{end_dt}`. "
            f"Check the DAG ID and date range are correct."
        )

    # Sort oldest-first so we re-run in chronological order
    all_runs.sort(key=lambda r: r["execution_date"])

    # Determine max_active_runs and batch size
    first_base = all_runs[0]["base"]
    max_active = _airflow_get_max_active_runs(first_base, dag_id, default=10)
    batch_size = max(1, max_active // 2)
    total_waves = math.ceil(len(all_runs) / batch_size)

    state_counts: dict = {}
    for r in all_runs:
        s = r["current_state"]
        state_counts[s] = state_counts.get(s, 0) + 1
    state_summary = ", ".join(f"{v} {k}" for k, v in sorted(state_counts.items()))

    instances_with_runs = list({r["label"]: r["base"] for r in all_runs}.items())

    # Store for confirmation
    pending_interleaved_rerun[(channel, user)] = {
        "dag_id":              dag_id,
        "start_dt":            start_dt,
        "end_dt":              end_dt,
        "runs":                all_runs,
        "instances_with_runs": instances_with_runs,
        "batch_size":          batch_size,
        "max_active_runs":     max_active,
        "thread_ts":           thread_ts,
    }

    if client:
        client.chat_postMessage(
            channel=channel, thread_ts=thread_ts,
            text=(
                f"♻️ *Proposed: Interleaved Rerun*\n\n"
                f"*DAG:* `{dag_id}`\n"
                f"*Range:* `{start_dt}` → `{end_dt}`\n"
                f"*Runs found:* {len(all_runs)} ({state_summary})\n"
                f"*DAG max_active_runs:* {max_active}\n"
                f"*Batch size:* {batch_size} (half of {max_active}, leaves room for live forward runs)\n"
                f"*Estimated waves:* {total_waves}\n\n"
                f"Each wave clears {batch_size} runs then waits for them to finish "
                f"before queuing the next batch. The bot will post updates in this thread.\n\n"
                f"Reply *yes* to start, or *no* to cancel."
            ),
        )
    return (
        f"✅ Preview posted. Waiting for yes/no to start interleaved rerun "
        f"({len(all_runs)} runs in {total_waves} waves of {batch_size})."
    )


def _check_hdfs_success_flag(namenode: str, webhdfs_base: str, dt: datetime) -> dict:
    """Check whether _Complete flag exists under a minute folder via WebHDFS REST API.
    Returns {"ready": bool, "url": str, "error": str|None}
    """
    minute_path = f"{webhdfs_base}/{dt.year:04d}/{dt.month:02d}/{dt.day:02d}/{dt.hour:02d}/{dt.minute:02d}/_Complete"
    url = f"{namenode}/webhdfs/v1{minute_path}?op=GETFILESTATUS"
    explorer_url = f"{namenode}/explorer.html#{webhdfs_base}/{dt.year:04d}/{dt.month:02d}/{dt.day:02d}/{dt.hour:02d}/{dt.minute:02d}/"
    try:
        resp = requests.get(url, timeout=10, verify=False)
        logger.info(f"HDFS check {url} → {resp.status_code}")
        if resp.status_code == 200:
            return {"ready": True,  "url": explorer_url, "error": None}
        if resp.status_code == 404:
            return {"ready": False, "url": explorer_url, "error": None}
        return {"ready": False, "url": explorer_url,
                "error": f"HTTP {resp.status_code}: {resp.text[:100]}"}
    except Exception as e:
        logger.error(f"_check_hdfs_success_flag error: {e}")
        return {"ready": False, "url": explorer_url, "error": str(e)}


def tool_check_hdfs_minute_data(failed_minute: str,
                                cross_page_hdfs_path: str,
                                event_summary_hdfs_path: str,
                                namenode_http: str) -> str:
    """Check whether HDFS data (_SUCCESS flag) is available for the upstream minute
    corresponding to a failed DPI Flow Feed minute (applies −2 min offset automatically).

    Paths are passed in by Claude after reading the Confluence playbook (page 4192337966)
    so they are always up-to-date.

    Args:
        failed_minute:            The failed DPI Flow Feed minute e.g. '2026-05-22 21:10:00'
        cross_page_hdfs_path:     Full hdfs:// path e.g. 'hdfs://nameservice-aa/tlb2/tlb2-aa-prod/ecoCrossPageFlowDefaultOneMinFileSink'
        event_summary_hdfs_path:  Full hdfs:// path e.g. 'hdfs://nameservice-aa/tlb2/tlb2-aa-prod/ecoEventSummaryDefaultOneMinFileSink'
        namenode_http:            WebHDFS namenode URL e.g. 'http://rccp408-24a.iad6.prod.conviva.com:50070'
    """
    try:
        upstream_dt = datetime.fromisoformat(_normalise_dt(failed_minute)) - timedelta(minutes=DPI_FLOW_UPSTREAM_OFFSET_MINS)
    except ValueError as e:
        return f"Could not parse failed minute '{failed_minute}': {e}"

    # Derive WebHDFS paths by stripping the hdfs://nameservice-aa prefix
    def _to_webhdfs(hdfs_path: str) -> str:
        for prefix in ("hdfs://nameservice-aa", "hdfs://nameservice"):
            if hdfs_path.startswith(prefix):
                return hdfs_path[len(prefix):]
        return hdfs_path  # already a bare path

    upstream_str = upstream_dt.strftime("%Y-%m-%d %H:%M UTC")
    results = {}
    for name, hdfs_path in [
        ("cross_page",    cross_page_hdfs_path),
        ("event_summary", event_summary_hdfs_path),
    ]:
        results[name] = _check_hdfs_success_flag(namenode_http, _to_webhdfs(hdfs_path), upstream_dt)

    lines = [f"*HDFS data check for upstream minute `{upstream_str}` (failed minute − 2 min):*\n"]
    all_ready = True
    for name, r in results.items():
        icon = "✅" if r["ready"] else ("⚠️" if r["error"] else "❌")
        status = "ready (_SUCCESS found)" if r["ready"] else ("error: " + r["error"] if r["error"] else "NOT ready (_SUCCESS missing)")
        lines.append(f"{icon} *{name}:* {status}")
        lines.append(f"   <{r['url']}|View in HDFS Explorer>")
        if not r["ready"]:
            all_ready = False

    if all_ready:
        lines.append("\n✅ *Both paths are ready.* Safe to trigger the upstream DAG.")
    else:
        lines.append("\n❌ *Data not ready yet.* Do not trigger — contact the TLB team to investigate.")

    return "\n".join(lines)


def _build_hdfs_minute_path(base_path: str, dt: datetime) -> str:
    """Build HDFS path for a specific minute: base/{YYYY}/{MM}/{DD}/{HH}/{mm}/"""
    return f"{base_path}/{dt.year:04d}/{dt.month:02d}/{dt.day:02d}/{dt.hour:02d}/{dt.minute:02d}/"


def _airflow_trigger_dag_run(base: str, dag_id: str, dag_run_id: str,
                              logical_date: str, conf: dict) -> dict:
    """Trigger a DAG run via the Airflow REST API with a config JSON payload."""
    headers = _get_airflow_headers(base)
    url = f"{base}/api/v1/dags/{dag_id}/dagRuns"
    payload = {
        "dag_run_id":   dag_run_id,
        "logical_date": logical_date,
        "conf":         conf,
    }
    try:
        resp = requests.post(url, headers=headers, json=payload, verify=False, timeout=30)
        logger.info(f"trigger_dag_run {dag_id} {logical_date} → {resp.status_code}: {resp.text[:300]}")
        if resp.ok:
            return {"ok": True, "data": resp.json()}
        return {"ok": False, "error": f"HTTP {resp.status_code}: {resp.text[:300]}"}
    except Exception as e:
        logger.error(f"_airflow_trigger_dag_run error: {e}")
        return {"ok": False, "error": str(e)}


def _airflow_clear_dag_run(base: str, dag_id: str, dag_run_id: str) -> dict:
    """Clear (re-run) an existing DAG run via clearTaskInstances API.

    Used when a run already exists (409 would occur on POST /dagRuns).
    Clears all task instances and resets the DAGRun state to queued.
    """
    headers = _get_airflow_headers(base)
    url = f"{base}/api/v1/dags/{dag_id}/clearTaskInstances"
    payload = {
        "dag_run_id":    dag_run_id,
        "dry_run":       False,
        "reset_dag_runs": True,
    }
    try:
        resp = requests.post(url, headers=headers, json=payload, verify=False, timeout=30)
        logger.info(f"clear_dag_run {dag_id} {dag_run_id} → {resp.status_code}: {resp.text[:300]}")
        if resp.ok:
            return {"ok": True, "data": resp.json()}
        return {"ok": False, "error": f"HTTP {resp.status_code}: {resp.text[:300]}"}
    except Exception as e:
        logger.error(f"_airflow_clear_dag_run error: {e}")
        return {"ok": False, "error": str(e)}


def tool_propose_trigger_upstream_minute_dag(failed_minute: str,
                                              cross_page_hdfs_path: str,
                                              event_summary_hdfs_path: str,
                                              upstream_airflow_base_url: str,
                                              force: bool = False,
                                              channel: str = "", thread_ts: str = "",
                                              user: str = "", client=None) -> str:
    """Stage triggering ECO_CROSS_PAGE_EVENT_SUMMARY_SSD_MINUTE_DAG for human confirmation.

    Playbook: https://conviva.atlassian.net/wiki/spaces/CSS/pages/4192337966
    Accepts the FAILED minute from the DPI Flow Feed pipeline.
    Automatically applies the 2-minute offset to compute the upstream logical date,
    and builds the HDFS config JSON using the paths passed in from Confluence.

    Args:
        failed_minute:              The failed DPI Flow Feed minute e.g. '2026-05-22 21:10:00'
        cross_page_hdfs_path:       Full hdfs:// base path read from Confluence playbook
        event_summary_hdfs_path:    Full hdfs:// base path read from Confluence playbook
        upstream_airflow_base_url:  Base URL of the Airflow instance, read from Confluence playbook
                                    e.g. 'https://conviva-airflow.prod.conviva.com'
    """
    if not upstream_airflow_base_url:
        return "upstream_airflow_base_url is required — read it from the Confluence playbook (page 4192337966)."

    # Parse the failed minute
    try:
        failed_dt = datetime.fromisoformat(_normalise_dt(failed_minute))
    except ValueError as e:
        return f"Could not parse failed minute '{failed_minute}': {e}. Use format: '2026-05-22 21:10:00'"

    # Apply the 2-minute offset: upstream logical date = failed_minute − 2 mins
    upstream_dt     = failed_dt - timedelta(minutes=DPI_FLOW_UPSTREAM_OFFSET_MINS)
    upstream_dt_str = upstream_dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")
    dag_run_id      = f"manual__{upstream_dt_str}"

    # ── Check if a run already exists for this exact minute ──
    base = upstream_airflow_base_url.rstrip("/")
    # Use a 1-second window to match only this exact minute's run
    window_end = (upstream_dt + timedelta(seconds=59)).strftime("%Y-%m-%dT%H:%M:%S+00:00")
    existing_runs = _airflow_get_dag_runs_in_range(base, UPSTREAM_MINUTE_DAG_ID,
                                                    upstream_dt_str, window_end)
    existing_run  = existing_runs[0] if existing_runs else None
    existing_note = ""
    if existing_run:
        ex_state  = existing_run.get("state", "unknown")
        ex_run_id = existing_run.get("dag_run_id", "?")
        existing_note = (
            f"⚠️ *A run already exists for this minute!*\n"
            f"  Run ID: `{ex_run_id}`\n"
            f"  State: `{ex_state}`\n\n"
        )
        if ex_state in ("running", "queued"):
            # Already in progress — block the trigger, don't store in pending
            if client:
                client.chat_postMessage(
                    channel=channel, thread_ts=thread_ts,
                    text=(
                        f"🤖 *SSD Bot* — 🚫 Upstream DAG already has a *{ex_state}* run for `{upstream_dt_str}`.\n"
                        f"Run ID: `{ex_run_id}`\n\n"
                        f"No need to trigger again — wait for it to complete."
                    ),
                )
            return f"Blocked: upstream DAG already has a {ex_state} run for {upstream_dt_str}."
        if ex_state == "success":
            existing_note = (
                f"✅ *Upstream DAG already succeeded for this minute.*\n"
                f"  Run ID: `{ex_run_id}`\n\n"
                f"If the DPI Flow Feed is still stuck, it may need a separate rerun. "
                f"Reply *yes* only if you want to re-trigger the upstream DAG anyway.\n\n"
            )

    # Build HDFS paths for the upstream minute using paths from Confluence
    conf = {
        "cross_page_output_path":    _build_hdfs_minute_path(cross_page_hdfs_path,    upstream_dt),
        "event_summary_output_path": _build_hdfs_minute_path(event_summary_hdfs_path, upstream_dt),
    }
    conf_str = json.dumps(conf, indent=2)

    # Store for confirmation
    # If a failed/success run already exists, store its run_id so handle_confirm
    # can clear it instead of POSTing a new run (which would 409).
    existing_failed_run_id = None
    if existing_run and existing_run.get("state") in ("failed", "success"):
        existing_failed_run_id = existing_run.get("dag_run_id")

    pending_trigger_upstream[(channel, user)] = {
        "dag_id":               UPSTREAM_MINUTE_DAG_ID,
        "base":                 base,
        "dag_run_id":           dag_run_id,
        "logical_date":         upstream_dt_str,
        "conf":                 conf,
        "failed_minute":        failed_minute,
        "thread_ts":            thread_ts,
        "force":                force,
        "existing_run_id":      existing_failed_run_id,  # non-None → use clear instead of create
    }

    force_warning = (
        "⚠️ *Warning: success flag (_SUCCESS) was NOT found in HDFS.* "
        "Triggering without confirmed upstream data — the downstream pipeline may still fail if data is truly missing.\n\n"
    ) if force else ""

    if client:
        client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=(
                f"⚡ *Proposed: Trigger Upstream Minute DAG*\n\n"
                f"*Failed DPI Flow Feed minute:* `{failed_minute}`\n"
                f"*Upstream logical date (−{DPI_FLOW_UPSTREAM_OFFSET_MINS} min):* `{upstream_dt_str}`\n"
                f"*DAG:* `{UPSTREAM_MINUTE_DAG_ID}`\n"
                f"*Airflow:* {base}\n\n"
                f"{force_warning}"
                f"{existing_note}"
                f"*Config JSON:*\n```{conf_str}```\n\n"
                f"Reply *yes* to trigger, or *no* to cancel."
            ),
        )
    return f"✅ Preview posted. Waiting for yes/no to trigger upstream DAG for `{upstream_dt_str}`."


def tool_get_airflow_dag_runs(dag_id: str, limit: int = 10, state: str = None) -> str:
    """Get recent run history for a DAG, checking all Airflow instances.

    Args:
        dag_id: exact DAG ID
        limit:  max runs to return (default 10)
        state:  optional filter — 'failed', 'success', 'running', 'queued'.
                When debugging a failure always pass state='failed' so recent
                failures are not hidden by subsequent successful runs.
    """
    if not AIRFLOW_INSTANCES:
        return "No Airflow URLs configured."

    params = {"limit": limit, "order_by": "-execution_date"}
    if state:
        params["state"] = state

    all_responses = _airflow_get(f"dags/{dag_id}/dagRuns", params=params)
    sections = []
    for label, data in all_responses.items():
        if "error" in data:
            sections.append(f"*{label} Airflow* — ❌ could not fetch runs: {data['error']}")
            continue
        runs = data.get("dag_runs", [])
        state_label = f" (state={state})" if state else ""
        if not runs:
            sections.append(f"*{label} Airflow* — no{state_label} runs found for `{dag_id}`")
            continue
        lines = [f"*{label} Airflow* — recent{state_label} runs for `{dag_id}`:"]
        for r in runs:
            lines.append(
                f"  - {r.get('execution_date','?')[:19]}  state={r.get('state','?')}  "
                f"run_id={r.get('dag_run_id','?')}"
            )
        sections.append("\n".join(lines))

    return "\n\n".join(sections)


def tool_get_airflow_task_instances(dag_id: str, dag_run_id: str) -> str:
    """List all task instances for a specific DAG run, showing state and try number."""
    if not AIRFLOW_INSTANCES:
        return "No Airflow URLs configured."

    all_responses = _airflow_get(f"dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances")
    sections = []
    for label, data in all_responses.items():
        if "error" in data:
            sections.append(f"*{label} Airflow* — ❌ {data['error']}")
            continue
        tasks = data.get("task_instances", [])
        if not tasks:
            sections.append(f"*{label} Airflow* — no task instances found for run `{dag_run_id}`")
            continue
        lines = [f"*{label} Airflow* — tasks for `{dag_id}` / run `{dag_run_id}`:"]
        for t in tasks:
            state    = t.get("state") or "none"
            task_id  = t.get("task_id", "?")
            try_num  = t.get("try_number", 1)
            duration = t.get("duration")
            dur_str  = f"  duration={duration:.0f}s" if duration else ""
            lines.append(f"  - {task_id}  state={state}  try={try_num}{dur_str}")
        sections.append("\n".join(lines))

    return "\n\n".join(sections)


def tool_get_airflow_task_log(dag_id: str, dag_run_id: str, task_id: str, try_number: int = 1, max_lines: int = 150) -> str:
    """Fetch the log for a specific task instance.
    max_lines controls how many trailing lines to return (0 = unlimited / full log).
    Default 150 lines for sensor/short tasks; use 0 for delivery tasks (copy_and_deliver,
    trigger_spark_job, run_script_eco) where you need to see the full sequence."""
    if not AIRFLOW_INSTANCES:
        return "No Airflow URLs configured."

    path = f"dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}"
    sections = []
    for label, base in AIRFLOW_INSTANCES.items():
        try:
            headers = _get_airflow_headers(base)
            headers["Accept"] = "text/plain"          # log endpoint returns plain text
            url  = f"{base}/api/v1/{path}"
            resp = requests.get(url, headers=headers, verify=False, timeout=30)
            logger.info(f"Task log {label} {url} → {resp.status_code}")

            if resp.status_code == 404:
                # try experimental API
                url2  = f"{base}/api/experimental/{path}"
                resp2 = requests.get(url2, headers=headers, verify=False, timeout=30)
                if resp2.ok:
                    resp = resp2
                else:
                    sections.append(f"*{label} Airflow* — log not found for task `{task_id}` (try {try_number})")
                    continue

            if not resp.ok:
                sections.append(f"*{label} Airflow* — ❌ HTTP {resp.status_code} fetching log")
                continue

            log_text = resp.text
            lines    = log_text.splitlines()
            if max_lines > 0 and len(lines) > max_lines:
                trimmed = f"[... {len(lines) - max_lines} earlier lines omitted ...]\n" + "\n".join(lines[-max_lines:])
            else:
                trimmed = log_text  # full log when max_lines=0 or log is short

            sections.append(
                f"*{label} Airflow* — log for `{dag_id}` / `{task_id}` (try {try_number}):\n"
                f"```\n{trimmed}\n```"
            )
        except Exception as e:
            logger.error(f"tool_get_airflow_task_log {label} error: {e}", exc_info=False)
            sections.append(f"*{label} Airflow* — ❌ error fetching log: {e}")

    return "\n\n".join(sections) if sections else "No log data found."


def tool_get_dag_source(dag_id: str, instance: str = None) -> str:
    """Fetch the Python source code of a DAG from Airflow.
    Useful for identifying upstream sensor dependencies, ExternalTaskSensors,
    schedule intervals, and understanding what a pipeline actually does.
    Returns the raw Python source, or an explanation if unavailable."""
    if not AIRFLOW_INSTANCES:
        return "No Airflow URLs configured."

    instances_to_try = {}
    if instance:
        label = instance.lower().strip()
        if label not in AIRFLOW_INSTANCES:
            return f"Unknown instance '{instance}'. Available: {', '.join(AIRFLOW_INSTANCES.keys())}"
        instances_to_try[label] = AIRFLOW_INSTANCES[label]
    else:
        instances_to_try = AIRFLOW_INSTANCES

    for label, base in instances_to_try.items():
        headers = _get_airflow_headers(base)
        # Airflow 2.x: GET /api/v1/dags/{dag_id}/source
        url = f"{base}/api/v1/dags/{dag_id}/source"
        try:
            resp = requests.get(url, headers=headers, verify=False, timeout=30)
            logger.info(f"get_dag_source {label} {dag_id} → {resp.status_code}")
            if resp.ok:
                data   = resp.json()
                source = data.get("source", data.get("content", ""))
                if source:
                    # Truncate if very long — keep first 400 + last 100 lines
                    lines = source.splitlines()
                    if len(lines) > 500:
                        source = (
                            "\n".join(lines[:400])
                            + f"\n\n[... {len(lines) - 500} lines omitted ...]\n\n"
                            + "\n".join(lines[-100:])
                        )
                    return (
                        f"*DAG source: `{dag_id}` ({label} Airflow)*\n"
                        f"```python\n{source}\n```"
                    )
        except Exception as e:
            logger.error(f"get_dag_source {label} {dag_id}: {e}")

    # Fallback: try fetching the file path and reading from disk (only works if running on same host)
    return (
        f"Could not retrieve source for `{dag_id}` via Airflow API "
        f"(endpoint may require Airflow 2.2+ or the DAG may not expose source). "
        f"Check the DAG file location in the Airflow UI under Admin → DAGs → {dag_id}."
    )


def tool_find_missing_dag_runs(dag_id: str, start_date: str, end_date: str,
                                instance: str = None) -> str:
    """Detect runs that SHOULD exist (based on the DAG's schedule) but are MISSING from the DB.
    Useful for diagnosing 'not scheduled' / 'not triggered' issues.
    Compares expected execution times vs actual dag_runs records and returns the gaps."""
    if not AIRFLOW_INSTANCES:
        return "No Airflow URLs configured."

    start_dt = _normalise_dt(start_date)
    end_dt   = _normalise_dt(end_date)

    instances_to_try = {}
    if instance:
        label = instance.lower().strip()
        if label not in AIRFLOW_INSTANCES:
            return f"Unknown instance '{instance}'. Available: {', '.join(AIRFLOW_INSTANCES.keys())}"
        instances_to_try[label] = AIRFLOW_INSTANCES[label]
    else:
        instances_to_try = AIRFLOW_INSTANCES

    sections = []
    for label, base in instances_to_try.items():
        # 1 — get DAG schedule
        schedule = _airflow_get_dag_schedule(base, dag_id)
        if schedule is None:
            sections.append(f"*{label}* — DAG `{dag_id}` not found or schedule unreadable.")
            continue
        td = _schedule_to_timedelta(schedule)
        if td is None:
            sections.append(
                f"*{label}* — Schedule `{schedule}` is not a simple fixed interval "
                f"(e.g. complex cron). Cannot auto-generate expected run list."
            )
            continue

        # 2 — generate expected execution dates
        start_obj = datetime.fromisoformat(start_dt.replace("Z", "+00:00"))
        end_obj   = datetime.fromisoformat(end_dt.replace("Z", "+00:00"))
        if start_obj.tzinfo is None:
            start_obj = start_obj.replace(tzinfo=timezone.utc)
        if end_obj.tzinfo is None:
            end_obj = end_obj.replace(tzinfo=timezone.utc)

        expected = []
        t = start_obj
        while t <= end_obj:
            expected.append(t.strftime("%Y-%m-%dT%H:%M:%S+00:00"))
            t += td

        if not expected:
            sections.append(f"*{label}* — No expected runs in the given window (interval={td}).")
            continue

        # 3 — fetch actual runs
        actual_runs = _airflow_get_dag_runs_in_range(base, dag_id, start_dt, end_dt)
        actual_dates = set()
        for r in actual_runs:
            ed = r.get("execution_date", "")[:19]   # YYYY-MM-DDTHH:MM:SS
            actual_dates.add(ed)

        # 4 — compare
        missing = []
        present = []
        for exp in expected:
            exp_key = exp[:19]
            if exp_key in actual_dates:
                present.append(exp_key)
            else:
                missing.append(exp_key)

        # Also note the state of present runs
        state_map = {r.get("execution_date", "")[:19]: r.get("state", "?") for r in actual_runs}

        lines = [
            f"*{label} Airflow* — `{dag_id}` schedule analysis",
            f"Interval: `{td}` | Window: `{start_dt}` → `{end_dt}`",
            f"Expected: {len(expected)} run(s) | Found in DB: {len(present)} | *Missing: {len(missing)}*",
        ]

        if missing:
            lines.append(f"\n⚠️ *Missing runs (never scheduled):*")
            for m in missing[:20]:
                lines.append(f"  • `{m}` UTC")
            if len(missing) > 20:
                lines.append(f"  … and {len(missing) - 20} more")

        if present:
            lines.append(f"\n📋 *Runs in DB (with current state):*")
            for p in present[:20]:
                state = state_map.get(p, "?")
                icon  = {"success": "✅", "failed": "❌", "running": "🔄", "queued": "⏳"}.get(state, "•")
                lines.append(f"  {icon} `{p}` — {state}")
            if len(present) > 20:
                lines.append(f"  … and {len(present) - 20} more")

        # Common causes for missing runs
        if missing:
            lines.append(
                f"\n💡 *Common causes for missing runs:*\n"
                f"  • DAG was paused during this window\n"
                f"  • `max_active_runs` limit was hit (scheduler stops creating new runs)\n"
                f"  • DAG was not yet deployed / file had a syntax error\n"
                f"  • Catchup=False and the window is in the past\n"
                f"  To check: call `get_airflow_dags` with this dag_id and look at "
                f"`is_paused`, `max_active_runs`, and `catchup` fields."
            )

        sections.append("\n".join(lines))

    return "\n\n---\n\n".join(sections) if sections else "No results."


def tool_lookup_pipeline(account_name: str = None, delivery_path: str = None) -> str:
    """Look up SSD pipelines by account name or delivery path via Support Tools API."""
    SUPPORT_TOOLS_BASE = "http://localhost:8002"
    try:
        if account_name:
            # account must be in c3.XXX format; support partial name — API accepts List[str]
            resp = requests.get(
                f"{SUPPORT_TOOLS_BASE}/api/pipeline-lookup/search",
                params={"account": account_name},
                timeout=30,
            )
        elif delivery_path:
            resp = requests.get(
                f"{SUPPORT_TOOLS_BASE}/api/pipeline-lookup/search-by-path",
                params={"path": delivery_path},
                timeout=30,
            )
        else:
            return "Provide account_name or delivery_path."
        resp.raise_for_status()
        data = resp.json()
        pipelines = data.get("pipelines", [])
        if not pipelines:
            return "No pipelines found."
        lines = [f"Found {len(pipelines)} pipeline(s):"]
        for p in pipelines:
            lines.append(
                f"• *{p['pipeline_name']}* ({p.get('instance', '?')}) — {p['c3_name']}\n"
                f"  Type: {p.get('type', '?')} | Freq: {p.get('frequency', '?')} | Trigger: {p.get('trigger_time', '?')} UTC\n"
                f"  Path: `{p.get('delivery_path', '?')}`\n"
                f"  Airflow: {p.get('af_link', 'N/A')}"
            )
        cached_at = data.get("reports_cached_at")
        if cached_at:
            lines.append(f"\n_Cache last updated: {cached_at}_")
        return "\n".join(lines)
    except Exception as e:
        return f"Pipeline lookup error: {e}"


def _parse_pd_alert_message(text: str, msg_ts: str = ""):
    """Parse a PagerDuty Airflow alert Slack message into structured fields.
    Returns dict with dag_id, task_id, state, dag_run_id, execution_date, alert_ts."""
    m = re.search(
        r'TaskInstance:\s+([^.\s]+)\.(\S+?)\s+(scheduled__(\S+?))\s+\[(\w+)\]', text
    )
    if not m:
        return None
    dag_id       = m.group(1)
    task_id      = m.group(2)
    dag_run_id   = m.group(3)   # e.g. scheduled__2026-08-15T11:52:00+00:00
    exec_date    = m.group(4)   # e.g. 2026-08-15T11:52:00+00:00
    state        = m.group(5)
    return {
        "dag_id":       dag_id,
        "task_id":      task_id,
        "state":        state,
        "dag_run_id":   dag_run_id,
        "execution_date": exec_date,
        "alert_ts":     float(msg_ts) if msg_ts else None,
    }


# ── RC classification ──────────────────────────────────────────────────────────

# RC_CATEGORIES defines the ordered legend for charts (bottom → top stack)
RC_CATEGORIES = [
    "DPI Flow Feed sensor timeout",
    "delete_view / BQ race",
    "Disney/SlingTV hourly files delay",
    "QVC/Scripps HHID dependency",
    "k8s pod launch failure",
    "Delivery infra (other)",
    "MySQL lock timeout",
    "DPI Event upstream delay",
    "Untracked long-tail",
]

RC_COLORS = {
    "DPI Flow Feed sensor timeout":    "#4FC3F7",
    "delete_view / BQ race":           "#FF6B5C",
    "Disney/SlingTV hourly files delay":"#FFC94D",
    "QVC/Scripps HHID dependency":     "#9575FF",
    "k8s pod launch failure":          "#F0529B",
    "Delivery infra (other)":          "#F06292",
    "MySQL lock timeout":              "#8BD450",
    "DPI Event upstream delay":        "#4DB6AC",
    "Untracked long-tail":             "#5C6B72",
}

_RC_TASK_RULES = [
    ("sensor_eco_cross_page_event_summary_ssd_minute", "DPI Flow Feed sensor timeout"),
    ("delete_view",                                    "delete_view / BQ race"),
    ("check_hourly_ssd",                               "Disney/SlingTV hourly files delay"),
    ("wait_for_gcs_file",                              "QVC/Scripps HHID dependency"),
    ("sensor_hourly_ss_ei",                            "QVC/Scripps HHID dependency"),
    ("copy_and_deliver",                               "MySQL lock timeout"),
    ("sensor_dpi_event_ssd_hourly",                    "DPI Event upstream delay"),
]

_K8S_KEYWORDS = [
    "pod launching failed", "imagepullbackoff", "errimagepull",
    "crashloopbackoff", "oomkilled", "containercreating",
]


def _classify_rc(dag_id: str, task_id: str, log_text: str = "") -> str:
    """Map a task failure to a root-cause category."""
    t = task_id.lower()
    for pattern, rc in _RC_TASK_RULES:
        if pattern in t:
            return rc
    if t == "trigger_copy_job":
        if log_text:
            ll = log_text.lower()
            if any(kw in ll for kw in _K8S_KEYWORDS):
                return "k8s pod launch failure"
        return "Delivery infra (other)"
    return "Untracked long-tail"


def _try_fetch_log_for_rc(dag_id: str, dag_run_id: str, task_id: str) -> str:
    """Try to fetch the first 1000 chars of a task log from connect Airflow for RC classification.
    Returns empty string on any failure."""
    try:
        instance_env = os.environ.get("AIRFLOW_connect_URL", "")
        if not instance_env:
            return ""
        base = _airflow_api_base(instance_env)
        path = f"/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/1"
        resp = requests.get(
            f"{base}{path}",
            headers=_get_airflow_headers(base),
            timeout=6,
            verify=False,
        )
        if resp.status_code == 200:
            return resp.text[:1000]
    except Exception:
        pass
    return ""


def _fetch_classified_alerts(start_date: str, end_date: str):
    """Read #piccolo-daas-alert for the period and return a list of alert dicts
    with RC classification and timestamps. Each dict has:
      dag_id, task_id, rc, alert_ts (epoch float), alert_date (YYYY-MM-DD)
    trigger_copy_job alerts attempt a log fetch to sub-classify k8s vs other."""
    ALERT_CHANNEL = "C03KA6FQR1C"
    start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt   = datetime.strptime(end_date,   "%Y-%m-%d").replace(
        hour=23, minute=59, second=59, tzinfo=timezone.utc
    )
    raw_alerts = []
    cursor = None
    for _ in range(20):
        params = {
            "channel": ALERT_CHANNEL,
            "limit":   200,
            "oldest":  str(start_dt.timestamp()),
            "latest":  str(end_dt.timestamp()),
        }
        if cursor:
            params["cursor"] = cursor
        try:
            resp = slack_app.client.conversations_history(**params)
        except Exception as e:
            logger.warning(f"_fetch_classified_alerts: Slack error: {e}")
            break
        for msg in resp.get("messages", []):
            text = msg.get("text", "")
            if "TaskInstance:" not in text:
                continue
            parsed = _parse_pd_alert_message(text, msg.get("ts", ""))
            if parsed:
                raw_alerts.append(parsed)
        if not resp.get("has_more"):
            break
        cursor = resp.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            break

    # Classify RC; for trigger_copy_job read log (capped at 8 log fetches)
    log_fetches = 0
    classified = []
    for a in raw_alerts:
        log_text = ""
        if a["task_id"].lower() == "trigger_copy_job" and log_fetches < 8:
            log_text = _try_fetch_log_for_rc(a["dag_id"], a["dag_run_id"], a["task_id"])
            log_fetches += 1
        rc = _classify_rc(a["dag_id"], a["task_id"], log_text)
        ts = a["alert_ts"] or 0.0
        alert_date = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d") if ts else start_date
        classified.append({
            "dag_id":     a["dag_id"],
            "task_id":    a["task_id"],
            "rc":         rc,
            "alert_ts":   ts,
            "alert_date": alert_date,
        })
    return classified


def _generate_rc_chart(alerts: list, start_date: str, end_date: str) -> bytes:
    """Generate a stacked bar chart (RC by day or week) and return PNG bytes.
    Weekly reports use daily granularity; monthly (>14 days) use weekly."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as ticker
    import io as _io
    from collections import defaultdict

    start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt   = datetime.strptime(end_date,   "%Y-%m-%d").replace(tzinfo=timezone.utc)
    n_days   = (end_dt - start_dt).days + 1
    monthly  = n_days > 14

    # Build buckets
    if monthly:
        # Week buckets: "W1", "W2", ...
        def _bucket(alert_date_str):
            d = datetime.strptime(alert_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            week_n = ((d - start_dt).days // 7) + 1
            return f"W{week_n}"
        n_buckets  = (n_days + 6) // 7
        bucket_labels = [f"W{i+1}" for i in range(n_buckets)]
    else:
        # Day buckets: "Mon\nAug 10"
        def _bucket(alert_date_str):
            return alert_date_str
        all_dates = [(start_dt + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(n_days)]
        bucket_labels = all_dates

    counts = defaultdict(lambda: defaultdict(int))
    for a in alerts:
        b = _bucket(a["alert_date"])
        counts[b][a["rc"]] += 1

    # Only include RC categories that actually appear
    active_rcs = [rc for rc in RC_CATEGORIES if any(counts[b].get(rc, 0) > 0 for b in bucket_labels)]
    if not active_rcs:
        active_rcs = ["Untracked long-tail"]

    fig, ax = plt.subplots(figsize=(max(8, len(bucket_labels) * 0.9 + 2), 5))
    fig.patch.set_facecolor("#0D1114")
    ax.set_facecolor("#161B1F")

    x = range(len(bucket_labels))
    bottoms = [0] * len(bucket_labels)
    bars = []
    for rc in active_rcs:
        vals = [counts[b].get(rc, 0) for b in bucket_labels]
        bar = ax.bar(x, vals, bottom=bottoms, color=RC_COLORS.get(rc, "#8B9BA3"),
                     width=0.6, label=rc)
        bars.append(bar)
        bottoms = [b + v for b, v in zip(bottoms, vals)]

    # Axis styling
    if monthly:
        ax.set_xticks(list(x))
        ax.set_xticklabels(bucket_labels, color="#8B9BA3", fontsize=10)
    else:
        short_labels = [
            datetime.strptime(d, "%Y-%m-%d").strftime("%a\n%b %-d")
            for d in bucket_labels
        ]
        ax.set_xticks(list(x))
        ax.set_xticklabels(short_labels, color="#8B9BA3", fontsize=9)

    ax.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))
    ax.tick_params(colors="#8B9BA3", which="both")
    ax.spines["bottom"].set_color("#2A3238")
    ax.spines["left"].set_color("#2A3238")
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.yaxis.label.set_color("#8B9BA3")
    ax.set_ylabel("Alert triggers", color="#8B9BA3", fontsize=9)

    # Legend below chart
    legend = ax.legend(
        loc="upper center",
        bbox_to_anchor=(0.5, -0.18 if not monthly else -0.15),
        ncol=min(3, len(active_rcs)),
        frameon=False,
        fontsize=8,
        labelcolor="#8B9BA3",
    )

    title = f"Alert triggers by RC — {start_date} → {end_date}"
    ax.set_title(title, color="#E7ECEE", fontsize=11, pad=10)

    fig.tight_layout(rect=[0, 0.12, 1, 1])
    buf = _io.BytesIO()
    fig.savefig(buf, format="png", dpi=130, facecolor=fig.get_facecolor(), bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.read()


def _parse_hdfs_url_to_webhdfs(url: str) -> tuple:
    """Normalise any HDFS URL variant to (namenode_http, hdfs_path).

    Handles:
      http://host:50070/webhdfs/v1/path   → (http://host:50070, /path)
      http://host:50070/explorer.html#/path → (http://host:50070, /path)
      hdfs://host:8020/path               → (http://host:50070, /path)
    """
    url = url.strip().rstrip("/")
    m = re.match(r"(https?://[^/]+)/webhdfs/v1(/.*)", url)
    if m:
        return m.group(1), m.group(2)
    m = re.match(r"(https?://[^/]+)/explorer\.html#(/.*)", url)
    if m:
        return m.group(1), m.group(2)
    m = re.match(r"hdfs://([^:/]+)(?::\d+)?(/.*)", url)
    if m:
        return f"http://{m.group(1)}:50070", m.group(2)
    raise ValueError(f"Cannot parse HDFS URL: {url}")


def _webhdfs_list_files(namenode: str, path: str) -> list:
    """Recursively list all files (not dirs) under an HDFS path via WebHDFS."""
    url = f"{namenode}/webhdfs/v1{path}?op=LISTSTATUS"
    resp = requests.get(url, verify=False, timeout=30)
    if not resp.ok:
        raise RuntimeError(f"WebHDFS LISTSTATUS {url} → {resp.status_code}: {resp.text[:200]}")
    files = []
    for item in resp.json().get("FileStatuses", {}).get("FileStatus", []):
        name = item["pathSuffix"]
        item_path = f"{path.rstrip('/')}/{name}"
        if item["type"] == "FILE":
            files.append(item_path)
        elif item["type"] == "DIRECTORY":
            files.extend(_webhdfs_list_files(namenode, item_path))
    return files


def _webhdfs_list_files_with_sizes(namenode: str, path: str) -> list:
    """Recursively list files with sizes: returns list of (hdfs_path, size_bytes)."""
    url = f"{namenode}/webhdfs/v1{path}?op=LISTSTATUS"
    resp = requests.get(url, verify=False, timeout=30)
    if not resp.ok:
        raise RuntimeError(f"WebHDFS LISTSTATUS {url} → {resp.status_code}: {resp.text[:200]}")
    files = []
    for item in resp.json().get("FileStatuses", {}).get("FileStatus", []):
        name = item["pathSuffix"]
        item_path = f"{path.rstrip('/')}/{name}"
        if item["type"] == "FILE":
            files.append((item_path, item.get("length", 0)))
        elif item["type"] == "DIRECTORY":
            files.extend(_webhdfs_list_files_with_sizes(namenode, item_path))
    return files


def tool_check_s3_vs_hdfs(hdfs_url: str, s3_url: str) -> str:
    """Compare file count and total size between an HDFS path and an S3 path.

    Returns a verdict: complete (S3 matches HDFS), partial (S3 has fewer/smaller files),
    or empty (S3 has nothing). Used to determine whether a repair copy is needed.
    """
    try:
        namenode, hdfs_path = _parse_hdfs_url_to_webhdfs(hdfs_url)
    except ValueError as e:
        return f"Cannot parse HDFS URL: {e}"
    try:
        s3_bucket, s3_prefix = _s3_parse_url(s3_url)
    except ValueError as e:
        return f"Cannot parse S3 URL: {e}"

    # ── HDFS side ──
    try:
        hdfs_files = _webhdfs_list_files_with_sizes(namenode, hdfs_path)
    except Exception as e:
        return f"Failed to list HDFS files: {e}"

    if not hdfs_files:
        return "HDFS path is empty — no data to deliver."

    hdfs_count = len(hdfs_files)
    hdfs_bytes = sum(sz for _, sz in hdfs_files)

    # ── S3 side ──
    try:
        import boto3
        aws_profile = _aws_profile_for_bucket(s3_bucket)
        session   = boto3.Session(profile_name=aws_profile)
        s3_client = session.client("s3")
        paginator = s3_client.get_paginator("list_objects_v2")
        s3_files  = []
        for page in paginator.paginate(Bucket=s3_bucket,
                                       Prefix=s3_prefix.rstrip("/") + "/"):
            for obj in page.get("Contents", []):
                s3_files.append((obj["Key"], obj["Size"]))
    except Exception as e:
        return f"Failed to list S3 files: {e}"

    s3_count = len(s3_files)
    s3_bytes  = sum(sz for _, sz in s3_files)

    # ── Verdict ──
    if s3_count == 0:
        verdict = "EMPTY"
        detail  = "S3 folder has no files — upload never started or was fully cleaned up."
    elif s3_count == hdfs_count and s3_bytes == hdfs_bytes:
        verdict = "COMPLETE"
        detail  = "S3 matches HDFS exactly — upload completed successfully."
    else:
        missing = hdfs_count - s3_count
        verdict = "PARTIAL"
        detail  = (
            f"S3 is incomplete — {s3_count}/{hdfs_count} files, "
            f"{s3_bytes:,}/{hdfs_bytes:,} bytes. "
            f"{missing} file(s) missing."
        )

    return (
        f"HDFS: {hdfs_count} files, {hdfs_bytes:,} bytes\n"
        f"S3:   {s3_count} files, {s3_bytes:,} bytes\n"
        f"Verdict: {verdict} — {detail}"
    )


def _s3_parse_url(s3_url: str) -> tuple:
    """Parse s3://bucket/prefix → (bucket, prefix)."""
    m = re.match(r"s3a?://([^/]+)/?(.*)$", s3_url.strip().rstrip("/"))
    if not m:
        raise ValueError(f"Cannot parse S3 URL: {s3_url}")
    return m.group(1), m.group(2).strip("/")


def _aws_profile_for_bucket(bucket: str) -> str:
    """Determine AWS credentials profile from bucket name."""
    for prefix, profile in AWS_S3_PROFILE_MAP.items():
        if bucket == prefix or bucket.startswith(prefix):
            return profile
    return "default"


def _expand_brace_expression(s: str) -> list:
    """Expand a single {a,b,c} brace group in a string (recursive for multiple groups).

    Examples:
      'file_{26,27,28}.csv.gz' → ['file_26.csv.gz', 'file_27.csv.gz', 'file_28.csv.gz']
      'DailyLog_{A,B}_{1,2}.csv' → ['DailyLog_A_1.csv', 'DailyLog_A_2.csv', ...]
    Returns [s] unchanged if no brace group is found.
    """
    m = re.search(r'\{([^{}]+)\}', s)
    if not m:
        return [s]
    prefix, suffix = s[:m.start()], s[m.end():]
    result = []
    for alt in m.group(1).split(','):
        result.extend(_expand_brace_expression(prefix + alt.strip() + suffix))
    return result


def _preprocess_hdfs_url(url: str) -> tuple:
    """If the URL contains a brace expression or glob pattern in the last path segment,
    strip it out and return (base_dir_url, filter_patterns).  Otherwise return (url, []).

    Examples:
      '.../DailySSD_SunNXT_legacy/DailySessionLog_{26,27,28}.csv.gz'
      → ('.../DailySSD_SunNXT_legacy', ['DailySessionLog_26.csv.gz', ...])

      '.../DailySSD_SunNXT_legacy/DailySessionLog_SunNXT_*.csv.gz'
      → ('.../DailySSD_SunNXT_legacy', ['DailySessionLog_SunNXT_*.csv.gz'])
    """
    url = url.strip()

    # ── Brace expression: {a,b,c} → expanded list ─────────────────────────────
    if '{' in url:
        brace_pos  = url.index('{')
        last_slash = url.rfind('/', 0, brace_pos)
        if last_slash != -1:
            base_url         = url[:last_slash]
            filename_pattern = url[last_slash + 1:]
            return base_url, _expand_brace_expression(filename_pattern)

    # ── Glob pattern: * or ? in the last segment → single fnmatch pattern ─────
    last_slash = url.rfind('/')
    if last_slash != -1:
        last_segment = url[last_slash + 1:]
        if '*' in last_segment or '?' in last_segment:
            return url[:last_slash], [last_segment]

    return url, []


def tool_propose_hdfs_to_s3_repair(hdfs_url: str, s3_url: str,
                                    file_filter: list = None,
                                    channel: str = "", thread_ts: str = "",
                                    user: str = "", client=None) -> str:
    """Propose copying files from an HDFS path to an S3 path (repair/manual delivery).

    Uses WebHDFS REST API to stream files directly to S3 via boto3 —
    no hadoop client or disk space needed on the bot server.

    Args:
        hdfs_url:    HDFS source — any of:
                     http://namenode:50070/webhdfs/v1/path
                     http://namenode:50070/explorer.html#/path
                     hdfs://namenode:8020/path
                     Brace expressions in the last segment are expanded automatically:
                     .../DailySSD_SunNXT_legacy/DailySessionLog_{26,27,28}.csv.gz
        s3_url:      S3 destination — s3://bucket/prefix
        file_filter: Optional list of exact filenames or fnmatch patterns to copy.
                     If provided, only matching files are copied.
                     Example: ['DailySessionLog_SunNXT_2026-08-27.csv.gz',
                               'DailySessionLog_SunNXT_2026-08-28.csv.gz']
    """
    import fnmatch as _fnmatch

    # ── Brace expansion in the URL itself ──
    hdfs_url, brace_filter = _preprocess_hdfs_url(hdfs_url)

    # Merge brace-expanded names with explicit file_filter
    combined_filter = []
    if brace_filter:
        combined_filter.extend(brace_filter)
    if file_filter:
        if isinstance(file_filter, str):
            file_filter = [file_filter]
        combined_filter.extend(file_filter)

    try:
        namenode, hdfs_path = _parse_hdfs_url_to_webhdfs(hdfs_url)
    except ValueError as e:
        return f"Cannot parse HDFS URL: {e}"
    try:
        s3_bucket, s3_prefix = _s3_parse_url(s3_url)
    except ValueError as e:
        return f"Cannot parse S3 URL: {e}"

    aws_profile = _aws_profile_for_bucket(s3_bucket)

    try:
        files = _webhdfs_list_files(namenode, hdfs_path)
    except Exception as e:
        return f"Failed to list HDFS files at `{namenode}{hdfs_path}`: {e}"

    if not files:
        return f"No files found at `{namenode}{hdfs_path}`."

    # ── Apply file filter ──
    if combined_filter:
        def _matches(path):
            name = path.split('/')[-1]
            return any(_fnmatch.fnmatch(name, pat) or name == pat
                       for pat in combined_filter)
        files = [f for f in files if _matches(f)]
        if not files:
            return (f"No files matched the filter {combined_filter} "
                    f"at `{namenode}{hdfs_path}`.")

    pending_hdfs_s3_copy[(channel, user)] = {
        "namenode":   namenode,
        "hdfs_path":  hdfs_path,
        "files":      files,
        "s3_bucket":  s3_bucket,
        "s3_prefix":  s3_prefix,
        "aws_profile": aws_profile,
        "thread_ts":  thread_ts,
    }

    filter_note = (f"\n  _Filter: {combined_filter}_" if combined_filter else "")
    sample = files[:8]
    sample_lines = "\n".join(f"  • `{f.split('/')[-1]}`" for f in sample)
    if len(files) > 8:
        sample_lines += f"\n  _... and {len(files) - 8} more_"

    if client:
        client.chat_postMessage(
            channel=channel, thread_ts=thread_ts,
            text=(
                f"📦 *Proposed: HDFS → S3 Repair Copy*\n\n"
                f"*Source (HDFS):* `{namenode}{hdfs_path}/`\n"
                f"*Destination (S3):* `s3://{s3_bucket}/{s3_prefix}/`\n"
                f"*AWS profile:* `{aws_profile}`\n"
                f"*Files found:* {len(files)}{filter_note}\n\n"
                f"{sample_lines}\n\n"
                f"Reply *yes* to start the copy, or *no* to cancel."
            ),
        )
    return f"✅ Preview posted. Found {len(files)} files. Waiting for yes/no."


def tool_get_flow_feed_failures_at_minute(stuck_minute: str) -> str:
    """Query #piccolo-daas-alert (C03KA6FQR1C) for ALL DPI Flow Feed pipelines that failed
    at the same stuck minute. Use this when a flow feed sensor failure is detected to find
    all other pipelines that need the same fix — so reruns can be proposed in batch.

    Args:
        stuck_minute: The failed minute from the DPI Flow Feed alert,
                      e.g. '2026-08-26T19:14:00+00:00' or '2026-08-26 19:14:00'.
                      The execution_date in the alert IS the stuck minute.
    Returns:
        List of all unique DAG IDs stuck at this minute with sensor_eco_cross_page_event_summary_ssd_minute.
    """
    ALERT_CHANNEL = "C03KA6FQR1C"
    try:
        stuck_dt = datetime.fromisoformat(_normalise_dt(stuck_minute)).replace(tzinfo=timezone.utc)
    except Exception as e:
        return f"Could not parse stuck_minute '{stuck_minute}': {e}"

    # Search from 8 hours ago up to now — wide enough to catch alerts even if the
    # user investigates hours after the failure, while still matching only alerts
    # at the exact stuck_minute (checked below via exec_dt == stuck_dt).
    now = datetime.now(timezone.utc)
    search_start = (now - timedelta(hours=8)).timestamp()
    search_end   = now.timestamp()

    found_dags: dict[str, str] = {}  # dag_id → execution_date
    cursor = None
    for _ in range(10):
        params = {
            "channel": ALERT_CHANNEL,
            "limit":   200,
            "oldest":  str(search_start),
            "latest":  str(search_end),
        }
        if cursor:
            params["cursor"] = cursor
        try:
            resp = slack_app.client.conversations_history(**params)
        except Exception as e:
            logger.warning(f"tool_get_flow_feed_failures_at_minute: Slack error: {e}")
            break
        for msg in resp.get("messages", []):
            text = msg.get("text", "")
            if "sensor_eco_cross_page_event_summary_ssd_minute" not in text:
                continue
            parsed = _parse_pd_alert_message(text, msg.get("ts", ""))
            if not parsed:
                continue
            exec_date_str = parsed.get("execution_date", "")
            try:
                exec_dt = datetime.fromisoformat(_normalise_dt(exec_date_str)).replace(tzinfo=timezone.utc)
            except Exception:
                continue
            # Match only alerts at the exact same minute
            if exec_dt == stuck_dt:
                found_dags[parsed["dag_id"]] = exec_date_str
        if not resp.get("has_more"):
            break
        cursor = resp.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            break

    if not found_dags:
        return (
            f"No flow feed sensor failures found in #piccolo-daas-alert for minute `{stuck_minute}`. "
            f"Only the originally reported DAG needs rerun, or the alert may not have been posted yet."
        )

    lines = [
        f"*Flow feed sensor failures at minute `{stuck_minute}` in #piccolo-daas-alert:*\n",
        f"Found *{len(found_dags)}* stuck pipeline(s):\n",
    ]
    for dag_id in sorted(found_dags):
        lines.append(f"  • `{dag_id}`")
    lines.append(
        f"\nPropose rerunning ALL {len(found_dags)} pipeline(s) for minute `{stuck_minute}`."
    )
    return "\n".join(lines)


def tool_read_ssd_alerts(start_date: str = None, end_date: str = None) -> str:
    """Read and parse SSD PagerDuty alerts from #piccolo-daas-alert for the given date range.
    Dates in YYYY-MM-DD format (UTC). Returns grouped counts for Claude to summarize.
    If dates are omitted, defaults to the last 7 days."""
    ALERT_CHANNEL = "C03KA6FQR1C"
    now = datetime.now(timezone.utc)
    if not end_date:
        end_date = now.strftime("%Y-%m-%d")
    if not start_date:
        start_date = (now - timedelta(days=7)).strftime("%Y-%m-%d")
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(
            hour=23, minute=59, second=59, tzinfo=timezone.utc
        )
    except ValueError as e:
        return f"Invalid date format: {e}. Use YYYY-MM-DD (e.g. '2026-08-01')."

    oldest_ts = str(start_dt.timestamp())
    latest_ts = str(end_dt.timestamp())
    alerts = []
    cursor = None

    for _ in range(20):  # max 20 pages × 200 = 4000 messages
        params: dict = {
            "channel": ALERT_CHANNEL,
            "limit": 200,
            "oldest": oldest_ts,
            "latest": latest_ts,
        }
        if cursor:
            params["cursor"] = cursor
        try:
            resp = slack_app.client.conversations_history(**params)
        except Exception as e:
            return f"Error reading alert channel: {e}"
        if not resp.get("ok"):
            return f"Slack API error: {resp.get('error')}"
        for msg in resp.get("messages", []):
            text = msg.get("text", "")
            if "TaskInstance:" not in text:
                continue
            parsed = _parse_pd_alert_message(text)
            if parsed:
                alerts.append(parsed)
        if not resp.get("has_more"):
            break
        cursor = resp.get("response_metadata", {}).get("next_cursor")
        if not cursor:
            break

    if not alerts:
        return f"No SSD alerts found between {start_date} and {end_date}."

    from collections import Counter
    task_counts = Counter((a["dag_id"], a["task_id"]) for a in alerts)
    rc_counts   = Counter(_classify_rc(a["dag_id"], a["task_id"]) for a in alerts)

    lines = [
        f"*SSD Alert Data: {start_date} → {end_date}*",
        f"Total alert triggers: {len(alerts)}",
        "",
        "*Alerts by root cause (RC):*",
    ]
    for rc, count in rc_counts.most_common():
        lines.append(f"  {rc}: {count}")
    lines.append("")
    lines.append("*Top pipelines by alert count (DAG.task):*")
    for (dag, task), count in task_counts.most_common(20):
        lines.append(f"  {dag}.{task}: {count}")
    return "\n".join(lines)


def _build_html_report(data: dict, start_date: str, end_date: str) -> str:
    """Render structured report JSON into a standalone HTML file matching the team's report style."""
    from html import escape as h

    def badge(label, style):
        styles = {
            "open":      "background:#3A2416;color:#FFB874;border:1px solid #5C3B1E",
            "worse":     "background:#3A1616;color:#FF7070;border:1px solid #5C2020",
            "watch":     "background:#2E1B33;color:#DBA0F0;border:1px solid #47294F",
            "fixed":     "background:#173226;color:#7FE0B0;border:1px solid #23503A",
            "bydesign":  "background:#152A38;color:#7FCBEE;border:1px solid #1F4356",
        }
        s = styles.get(style, styles["open"])
        return f'<span style="display:inline-block;font-size:10.5px;padding:2px 9px;border-radius:20px;font-family:monospace;{s}">{h(label)}</span>'

    def tag(label, kind):
        styles = {
            "ce":      "background:#173226;color:#7FE0B0;border:1px solid #23503A",
            "noce":    "background:#3A2416;color:#FFB874;border:1px solid #5C3B1E",
            "fix":     "background:#152A38;color:#7FCBEE;border:1px solid #1F4356",
            "nofix":   "background:#3A2416;color:#FFB874;border:1px solid #5C3B1E",
        }
        s = styles.get(kind, styles["noce"])
        return f'<span style="font-family:monospace;font-size:11px;padding:3px 9px;border-radius:4px;{s}">{h(label)}</span>'

    def trend_html(t):
        return {"up": "📈 <span style='color:#FF7070'>getting worse</span>",
                "stable": "➡️ <span style='color:#8B9BA3'>stable</span>",
                "down": "📉 <span style='color:#7FE0B0'>improving</span>"}.get(t, "")

    # ── Stats strip breakdown ──────────────────────────────────────────────────
    total = data.get("total_triggers", 0)
    breakdown_rows = ""
    for item in data.get("breakdown", [])[:6]:
        breakdown_rows += (
            f'<div style="font-size:13px;color:#E7ECEE;margin-bottom:4px">'
            f'<span style="font-family:monospace;font-weight:700;color:#4FC3F7;'
            f'display:inline-block;min-width:32px;margin-right:6px">{item["count"]}</span>'
            f'{h(item["task"])}</div>'
        )

    # ── DO THIS FIRST ──────────────────────────────────────────────────────────
    dtf = data.get("do_this_first")
    dtf_html = ""
    if dtf:
        dtf_html = f"""
    <div style="background:linear-gradient(180deg,#1A1420,#171320);border:1px solid #4A2F5C;
                border-radius:8px;padding:20px 22px;margin:0 0 28px">
      <div style="display:inline-block;font-family:monospace;font-size:11px;font-weight:700;
                  letter-spacing:.08em;color:#0D1114;background:#F0529B;
                  padding:4px 10px;border-radius:4px;margin-bottom:12px">DO THIS FIRST</div>
      <div style="font-size:16px;font-weight:650;margin-bottom:10px">{h(dtf.get("title",""))}</div>
      <div style="font-size:13px;color:#E7ECEE;line-height:1.6">{h(dtf.get("description",""))}</div>
      <div style="font-size:12px;color:#DBA0F0;margin-top:8px">Impact: {h(dtf.get("impact",""))}</div>
    </div>"""

    # ── Fixed cards ────────────────────────────────────────────────────────────
    fixed_cards = ""
    for item in data.get("fixed", []):
        ce_tag = tag(item["ce"], "ce") if item.get("ce") else tag("No CE", "noce")
        fixed_tag = tag(f'Fixed: {item.get("fixed_date","")}', "fix") if item.get("fixed_date") else tag("Fix confirmed", "fix")
        fixed_cards += f"""
    <div style="background:#161B1F;border:1px solid #2A3238;border-left:3px solid #4FC3F7;
                border-radius:8px;padding:16px 18px;margin-bottom:10px">
      <div style="display:flex;align-items:center;gap:12px;margin-bottom:8px;flex-wrap:wrap">
        <span style="font-weight:650;font-size:14px">{h(item["name"])}</span>
        <span style="font-family:monospace;font-size:11px;color:#8B9BA3">{item["count"]} alerts</span>
        {badge("fixed · confirmed working","fixed")}
      </div>
      <div style="display:flex;gap:8px;margin-bottom:8px;flex-wrap:wrap">{ce_tag}{fixed_tag}</div>
      <div style="font-size:13px;color:#E7ECEE">{h(item.get("summary",""))}</div>
    </div>"""
    if not fixed_cards:
        fixed_cards = '<div style="font-size:13px;color:#5C6B72;padding:8px 0">None this period</div>'

    # ── Open cards ─────────────────────────────────────────────────────────────
    open_cards = ""
    for item in data.get("open", []):
        ce_label = item.get("ce") or "No CE"
        ce_k = "ce" if item.get("ce") else "noce"
        fix_label = "Fix known" if item.get("fix_known") else "No fix yet"
        fix_k = "fix" if item.get("fix_known") else "nofix"
        status = item.get("status", "open")
        badge_style = "worse" if "worse" in status else ("watch" if "watch" in status else "open")
        border_color = "#FF6B5C" if badge_style == "worse" else ("#9575FF" if badge_style == "watch" else "#F0529B")
        open_cards += f"""
    <div style="background:#161B1F;border:1px solid #2A3238;border-left:3px solid {border_color};
                border-radius:8px;padding:16px 18px;margin-bottom:10px">
      <div style="display:flex;align-items:center;gap:12px;margin-bottom:8px;flex-wrap:wrap">
        <span style="font-weight:650;font-size:14px">{h(item["name"])}</span>
        <span style="font-family:monospace;font-size:11px;color:#8B9BA3">{item["count"]} alerts</span>
        {trend_html(item.get("trend","stable"))}
        {badge(status, badge_style)}
      </div>
      <div style="display:flex;gap:8px;margin-bottom:10px;flex-wrap:wrap">
        {tag(ce_label, ce_k)}{tag(fix_label, fix_k)}
      </div>
      <div style="font-size:12.5px;margin-bottom:4px"><span style="color:#5C6B72;text-transform:uppercase;font-size:10px;letter-spacing:.06em">Root cause</span><br>{h(item.get("root_cause","unknown — needs investigation"))}</div>
      <div style="font-size:12.5px;margin-top:8px"><span style="color:#5C6B72;text-transform:uppercase;font-size:10px;letter-spacing:.06em">Action</span><br>{h(item.get("action",""))}</div>
    </div>"""
    if not open_cards:
        open_cards = '<div style="font-size:13px;color:#5C6B72;padding:8px 0">No open items this period 🎉</div>'

    # ── Low priority ───────────────────────────────────────────────────────────
    low_rows = ""
    for item in data.get("low_priority", []):
        low_rows += (
            f'<div style="font-size:13px;color:#E7ECEE;margin-bottom:6px">'
            f'<b>{h(item["name"])}</b> — {item["count"]} alerts'
            f'<span style="color:#8B9BA3;margin-left:8px">— {h(item["reason"])}</span></div>'
        )
    if not low_rows:
        low_rows = '<div style="font-size:13px;color:#5C6B72">None</div>'

    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>SSD Alert Digest — {h(start_date)} → {h(end_date)}</title>
<style>
  *{{box-sizing:border-box;}}
  body{{margin:0;background:#0D1114;color:#E7ECEE;
       font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Inter,sans-serif;
       line-height:1.5;padding:32px 20px 60px;}}
  .wrap{{max-width:900px;margin:0 auto;}}
  h2{{font-size:13px;text-transform:uppercase;letter-spacing:.1em;color:#8B9BA3;
      font-weight:650;margin:44px 0 16px;border-bottom:1px solid #2A3238;padding-bottom:8px;}}
  a{{color:#4FC3F7;}}
</style>
</head>
<body>
<div class="wrap">

  <div style="margin-bottom:28px;border-bottom:1px solid #2A3238;padding-bottom:22px">
    <div style="font-family:monospace;font-size:11px;letter-spacing:.14em;text-transform:uppercase;
                color:#4FC3F7;margin-bottom:10px">{h(start_date)} – {h(end_date)}</div>
    <h1 style="font-size:28px;margin:0 0 8px;font-weight:650;letter-spacing:-0.01em">SSD Alert Digest</h1>
    <div style="color:#8B9BA3;font-size:14.5px">PagerDuty alert data from #piccolo-daas-alert · generated {generated}</div>
  </div>

  <div style="display:grid;grid-template-columns:160px 1fr;gap:24px;align-items:center;
              background:#161B1F;border:1px solid #2A3238;border-radius:8px;padding:20px 22px;margin-bottom:28px">
    <div style="text-align:center;border-right:1px solid #2A3238;padding-right:20px">
      <div style="font-size:42px;font-weight:700;font-family:monospace;color:#F0529B;line-height:1">{total}</div>
      <div style="font-size:11.5px;color:#8B9BA3;margin-top:6px;line-height:1.4">alert triggers<br>this period</div>
    </div>
    <div>{breakdown_rows}</div>
  </div>

  {dtf_html}

  <h2>✅ Fixed — confirmed working</h2>
  {fixed_cards}

  <h2>🔴 Still Open — ranked by leverage</h2>
  {open_cards}

  <h2>🟡 Low priority — self-resolving / known noise</h2>
  <div style="background:#161B1F;border:1px solid #2A3238;border-radius:8px;padding:16px 18px">
    {low_rows}
  </div>

  <div style="margin-top:50px;padding-top:18px;border-top:1px solid #2A3238;
              font-size:11.5px;color:#5C6B72">
    Generated by SSD Bot from #piccolo-daas-alert · {generated}
  </div>

</div>
</body>
</html>"""


def run_alert_summary_job(
    start_date: str = None,
    end_date: str = None,
    post_channel: str = "C07KV3PB79C",
) -> str:
    """Generate RC-based alert digest with inline chart + text summary. Posts directly to Slack channel.
    Called by the weekly scheduler and by the manual bot trigger."""
    from collections import Counter

    now = datetime.now(timezone.utc)
    if not end_date:
        end_date = now.strftime("%Y-%m-%d")
    if not start_date:
        start_date = (now - timedelta(days=7)).strftime("%Y-%m-%d")

    # ── 1. Fetch and RC-classify alerts ────────────────────────────────────────
    alerts = _fetch_classified_alerts(start_date, end_date)
    if not alerts:
        slack_app.client.chat_postMessage(
            channel=post_channel,
            text=f"📊 *SSD Alert Digest — {start_date} → {end_date}*\nNo alerts found in #piccolo-daas-alert for this period.",
        )
        return "No alerts found."

    rc_counts = Counter(a["rc"] for a in alerts)
    total = len(alerts)

    # ── 2. Read #ces-internal-ssd for team discussion ──────────────────────────
    team_context = ""
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end_dt   = datetime.strptime(end_date,   "%Y-%m-%d").replace(
            hour=23, minute=59, second=59, tzinfo=timezone.utc
        )
        ssd_resp = slack_app.client.conversations_history(
            channel="C07KV3PB79C",
            oldest=str(start_dt.timestamp()),
            latest=str(end_dt.timestamp()),
            limit=200,
        )
        msgs = [
            m.get("text", "").strip()
            for m in ssd_resp.get("messages", [])
            if not m.get("bot_id") and m.get("text", "").strip()
        ]
        if msgs:
            team_context = "\n\nTeam discussion from #ces-internal-ssd:\n" + "\n---\n".join(msgs[:60])
    except Exception as e:
        logger.warning(f"Could not read ces-internal-ssd: {e}")

    # ── 3. Generate stacked bar chart and post as inline image ─────────────────
    try:
        chart_png = _generate_rc_chart(alerts, start_date, end_date)
        n_days = (datetime.strptime(end_date, "%Y-%m-%d") - datetime.strptime(start_date, "%Y-%m-%d")).days + 1
        gran = "daily" if n_days <= 14 else "weekly"
        slack_app.client.files_upload(
            channels=post_channel,
            content=chart_png,
            filename=f"ssd_rc_chart_{start_date}_{end_date}.png",
            title=f"SSD alert triggers by RC ({gran} view)",
            filetype="png",
        )
    except Exception as e:
        logger.error(f"Chart generation/upload failed: {e}")

    # ── 4. Ask Claude to write summary in Jul-14 style format ─────────────────
    # Build date label (e.g. "Aug 10 – Aug 17, 2026")
    try:
        sd = datetime.strptime(start_date, "%Y-%m-%d")
        ed = datetime.strptime(end_date,   "%Y-%m-%d")
        date_label = f"{sd.strftime('%b %-d')} – {ed.strftime('%b %-d, %Y')}"
    except Exception:
        date_label = f"{start_date} – {end_date}"

    # Pipeline-level counts — no cap, include ALL pipelines
    dag_counts = Counter(a["dag_id"] for a in alerts)
    dag_lines = "\n".join(
        f"  {dag}: {cnt}" for dag, cnt in dag_counts.most_common()
    )
    # Task-type counts
    task_type_counts = Counter(a["task_id"] for a in alerts)
    task_lines = "\n".join(
        f"  {tid}: {cnt}" for tid, cnt in task_type_counts.most_common()
    )
    rc_summary = "\n".join(f"  {rc}: {cnt}" for rc, cnt in rc_counts.most_common())

    text_prompt = f"""You are writing a weekly SSD alert digest for the Conviva SSD support team Slack channel, covering {start_date} to {end_date}.

Task type alert counts:
{task_lines}

Pipeline (dag_id) alert counts:
{dag_lines}

Root cause (RC) summary:
{rc_summary}

Total alert triggers: {total}
{team_context}

Write the digest in EXACTLY this Slack format (copy the structure, emoji, and section headers precisely — do not add, remove, or rename sections):

Here's the weekly SSD alert summary for {date_label}:

---

:bar_chart: Weekly SSD PagerDuty Alert Summary
Period: {date_label}
Total Alerts: {total}

---

Alert Breakdown by Task Type:
• <task_id> — <count> alerts (<one-phrase description: e.g. "Spark job failures/timeouts", "upstream sensor / pipeline wait issues">)
[one bullet per distinct task_id, most frequent first]

---

:fire: Top Noisy Pipelines (Likely Need Investigation):
• <dag_id> — <count> alerts :rotating_light: Highest this week
• <dag_id> — <count> alerts
[top 3-5 pipelines by alert count]

---

:clipboard: Mid-Range Alerts (<count> each — possible recurring issues):
• <dag_id>
[pipelines with similar mid-tier counts]

---

:clipboard: Notable Pipelines with ~<count> alerts each:
• <dag_id>
[remaining pipelines worth listing]

---

:mag_right: Key Observations & Action Items:
1. :rotating_light: <top-priority investigation item with pipeline name and why>
2. :warning: <second item>
3. :warning: <third item>
[one numbered item per distinct issue; use :rotating_light: for urgent, :warning: for watch, :pushpin: for FYI, :white_check_mark: for no-action-needed]

---

Would you like me to deep-dive into any specific pipeline or investigate the root cause of the top offenders? :mag:

RULES:
- Use dag_id names exactly as given — do not shorten or rename.
- Task type descriptions: trigger_spark_job = "Spark job failures/timeouts"; sensor tasks (start_pipeline_sensor, sensor_*) = "upstream sensor / pipeline wait issues"; copy_and_deliver = "file copy/deliver failures"; trigger_copy_job = "copy job launch failures (k8s/infra)"; delete_view = "BQ view cleanup (CE-12195, self-resolving)"; check_hourly_ssd = "hourly monitor watchdog".
- Tier pipelines naturally by count: top noisy = clearly highest; mid-range = similar counts in the middle; notable = lower counts worth listing.
- EVERY pipeline in the data above MUST appear in the digest — do not drop or omit any. If a pipeline has 1 alert it still goes in the Notable section.
- Do NOT invent or compute any numbers, percentages, or summary statistics that are not directly provided in the data above. The only counts you may use are the exact numbers given per dag_id and task_id. Do not write things like "X% untracked" or "Y alerts are long-tail" — you do not have that information.
- Do NOT invent pipeline names, counts, server names, CE numbers, or root causes absent from the data or team discussion above.
- RC context from team discussion should inform Key Observations — cite as "confirmed in #ces-internal-ssd" or "inferred from alert pattern".
- Output ONLY the Slack message text. No preamble, no code fences."""

    summary_text = ""
    try:
        resp = anthropic.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2000,
            messages=[{"role": "user", "content": text_prompt}],
        )
        summary_text = resp.content[0].text.strip()
    except Exception as e:
        logger.error(f"Claude text generation failed: {e}")
        summary_text = (
            f"Here's the weekly SSD alert summary for {date_label}:\n\n"
            f"---\n\n"
            f":bar_chart: Weekly SSD PagerDuty Alert Summary\n"
            f"Period: {date_label}\n"
            f"Total Alerts: {total}\n\n"
            f"Task breakdown:\n{task_lines}"
        )

    # ── 5. Post text in chunks ─────────────────────────────────────────────────
    lines = summary_text.split("\n")
    chunks, buf = [], ""
    for line in lines:
        if len(buf) + len(line) + 1 > 3000:
            chunks.append(buf.rstrip())
            buf = ""
        buf += line + "\n"
    if buf.strip():
        chunks.append(buf.rstrip())

    thread_ts = None
    for i, chunk in enumerate(chunks):
        try:
            r = slack_app.client.chat_postMessage(
                channel=post_channel,
                text=chunk,
                thread_ts=thread_ts if i > 0 else None,
            )
            if i == 0:
                thread_ts = r["ts"]
        except Exception as e:
            logger.error(f"Failed to post digest chunk {i}: {e}")

    return f"Digest posted ({total} alerts, {start_date}→{end_date})."


def tool_read_slack_channel(channel_name: str, limit: int = 30) -> str:
    """Read recent messages from a known SSD Slack channel."""
    channel_id = SEARCHABLE_SLACK_CHANNELS.get(channel_name)
    if not channel_id:
        available = ", ".join(SEARCHABLE_SLACK_CHANNELS.keys())
        return f"Unknown channel '{channel_name}'. Available channels: {available}"
    try:
        result = slack_app.client.conversations_history(channel=channel_id, limit=limit)
        # Slack SDK raises SlackApiError on failure; check for API-level errors too
        if not result.get("ok"):
            err = result.get("error", "unknown")
            if err == "missing_scope":
                return (f"Bot lacks permission to read #{channel_name}. "
                        f"Add the 'channels:history' scope to the Slack app at api.slack.com/apps.")
            return f"Slack API error reading #{channel_name}: {err}"
        messages = result.get("messages", [])
        if not messages:
            return f"No recent messages in #{channel_name}"
        lines = [f"Recent messages in #{channel_name}:"]
        for m in reversed(messages):
            if m.get("bot_id") or m.get("subtype"):
                continue
            user = m.get("user", "unknown")
            text = m.get("text", "").replace("\n", " ")[:300]
            lines.append(f"- <@{user}>: {text}")
        return "\n".join(lines) if len(lines) > 1 else f"No human messages found in #{channel_name}"
    except Exception as e:
        logger.error(f"tool_read_slack_channel error ({channel_name}): {e}", exc_info=True)
        return (f"Could not read #{channel_name}: {e}. "
                f"Check that the bot is a member of the channel and has 'channels:history' scope.")


def tool_propose_confluence_update(content_to_add: str, section: str,
                                   channel: str, thread_ts: str, user: str, client) -> str:
    """Stage a Confluence runbook update for human confirmation.
    Called by Claude when it wants to propose writing something to the runbook.
    Posts the proposal to Slack and stores it in pending_updates — user must reply 'yes' to apply."""
    try:
        page = fetch_confluence_page(PRIMARY_PAGE_ID)
        if not page:
            return "❌ Could not fetch the Confluence page to prepare the update."

        # Generate the HTML snippet via UPDATE_SYSTEM
        resp = anthropic.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            system=UPDATE_SYSTEM,
            messages=[{"role": "user", "content":
                f"Current runbook content:\n{page['body_text'][:8000]}\n\n"
                f"User's update request:\n{content_to_add}\n"
                f"Target section: {section}"
            }],
        )
        raw = strip_json_fences(resp.content[0].text)
        proposal = json.loads(raw)

        new_html = page["body_storage"] + "\n" + proposal["html_snippet"]
        pending_updates[(channel, user)] = {
            "page_id":   PRIMARY_PAGE_ID,
            "new_html":  new_html,
            "version":   page["version"],
            "title":     page["title"],
            "summary":   proposal["summary"],
            "section":   proposal["section"],
            "thread_ts": thread_ts,
        }

        preview = proposal.get("text_preview") or proposal["summary"]

        # Post the proposal as a separate Slack message so the user can clearly see it
        client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=(
                f"📝 *Proposed Runbook Update*\n\n"
                f"*Section:* {proposal['section']}\n"
                f"*Change:* {proposal['summary']}\n\n"
                f"*What will be added:*\n```{preview[:800]}```\n\n"
                f"Reply *yes* to apply to the "
                f"<{CONFLUENCE_BASE}/spaces/CSS/pages/{PRIMARY_PAGE_ID}|SSD Playbook>, "
                f"or *no* to cancel."
            ),
        )
        return (
            f"✅ Update proposal posted above. "
            f"Waiting for the user to reply 'yes' to confirm or 'no' to cancel."
        )
    except Exception as e:
        logger.error(f"tool_propose_confluence_update error: {e}", exc_info=True)
        return f"❌ Failed to prepare update proposal: {e}"


def tool_propose_new_confluence_page(title: str, content: str, parent_page_id: str,
                                     channel: str, thread_ts: str, user: str, client) -> str:
    """Stage a brand-new Confluence page for human confirmation.
    Called by Claude when the user asks to publish/create a new Confluence page."""
    try:
        # Convert plain content to minimal storage HTML
        html_lines = []
        for line in content.strip().split("\n"):
            line = line.strip()
            if not line:
                continue
            if line.startswith("## "):
                html_lines.append(f"<h2>{line[3:]}</h2>")
            elif line.startswith("# "):
                html_lines.append(f"<h1>{line[2:]}</h1>")
            elif line.startswith("- "):
                html_lines.append(f"<ul><li>{line[2:]}</li></ul>")
            else:
                html_lines.append(f"<p>{line}</p>")
        storage_html = "\n".join(html_lines)

        # Resolve parent page title for display
        parent_info = fetch_confluence_page(parent_page_id)
        parent_title = parent_info.get("title", f"page {parent_page_id}") if parent_info else f"page {parent_page_id}"

        pending_pages[(channel, user)] = {
            "title":          title,
            "storage_html":   storage_html,
            "parent_page_id": parent_page_id,
            "parent_title":   parent_title,
            "thread_ts":      thread_ts,
        }

        preview = content[:600]
        client.chat_postMessage(
            channel=channel,
            thread_ts=thread_ts,
            text=(
                f"📄 *Proposed New Confluence Page*\n\n"
                f"*Title:* {title}\n"
                f"*Parent:* {parent_title}\n\n"
                f"*Content preview:*\n```{preview}{'...' if len(content) > 600 else ''}```\n\n"
                f"Reply *yes* to publish this page under "
                f"<{CONFLUENCE_BASE}/spaces/{CONFLUENCE_SPACE}/pages/{parent_page_id}|{parent_title}>, "
                f"or *no* to cancel."
            ),
        )
        return "✅ Page proposal posted. Waiting for the user to reply yes or no."
    except Exception as e:
        logger.error(f"tool_propose_new_confluence_page error: {e}", exc_info=True)
        return f"❌ Failed to prepare page proposal: {e}"


def tool_save_memory(key: str, value: str) -> str:
    """Save a fact to persistent memory so it's available in future sessions."""
    from datetime import date
    memory = _load_memory()
    memory[key] = {"value": value, "updated": str(date.today())}
    _save_memory(memory)
    logger.info(f"Memory saved: {key!r} = {value!r}")
    return f"✅ Saved to memory: *{key}* → {value}"


def tool_read_memory(key: str = None) -> str:
    """Read one or all entries from persistent memory."""
    memory = _load_memory()
    if not memory:
        return "No memories saved yet."
    if key:
        entry = memory.get(key)
        if not entry:
            return f"No memory found for key '{key}'. All keys: {', '.join(memory.keys())}"
        value = entry.get("value", "") if isinstance(entry, dict) else str(entry)
        return f"{key}: {value}"
    lines = []
    for k, entry in memory.items():
        value = entry.get("value", "") if isinstance(entry, dict) else str(entry)
        updated = entry.get("updated", "") if isinstance(entry, dict) else ""
        lines.append(f"• {k} ({updated}): {value}")
    return "\n".join(lines)


def tool_delete_memory(key: str) -> str:
    """Delete a specific entry from persistent memory by key."""
    memory = _load_memory()
    if key not in memory:
        return f"No memory entry found for key '{key}'. All keys: {', '.join(memory.keys())}"
    del memory[key]
    _save_memory(memory)
    logger.info(f"Memory deleted: {key!r}")
    return f"✅ Deleted memory entry: *{key}*"


# ─── Jira: Search ──────────────────────────────────────────────────────────────
#
# Two root causes were diagnosed for previous 404 errors:
#
# 1. WRONG URL: conviva.atlassian.net/rest/api/3/ redirects to the Atlassian
#    cloud-scoped API (api.atlassian.com/ex/jira/{cloudId}/...) which expects
#    a Bearer token, not basic auth. The redirect strips the Authorization header,
#    causing a 404. Fix: call the cloud-scoped URL directly with Bearer auth.
#
# 2. WRONG JQL FILTER: "Product" ~ "SSD" returns 0 results because "Product" is
#    not a valid field name in this Jira instance. SSD support cases live in the
#    CE (Customer Escalations) project. Fix: filter by project = CE instead.

JIRA_API_BASE  = "https://conviva.atlassian.net/rest/api/3"
# Search across three projects:
#   CE  = Customer Escalations  — SSD customer support tickets
#   DFS = Data Feed Status      — pipeline registry (PipelineName, Granularity, dag_id)
#   SE  = Support Engineering   — internal eng work (migrations, platform fixes)
JIRA_SSD_PROJECTS = "CE, DFS, SE"

# requests drops the Authorization header on cross-domain redirects (security feature).
# conviva.atlassian.net/rest/api/3 can redirect to api.atlassian.com, so we use a
# session subclass that always re-attaches auth after any redirect.
class _AuthPreservingSession(requests.Session):
    """Session that preserves basic auth across cross-domain redirects.

    requests strips the Authorization header when a redirect crosses domains
    (e.g. conviva.atlassian.net → api.atlassian.com). This subclass re-attaches
    the credentials after every redirect so Jira searches work from the server.
    """
    def rebuild_auth(self, prepared_request, response):
        # Manually re-encode and re-attach Basic auth on every redirect
        if self.auth:
            import base64
            token = base64.b64encode(
                f"{self.auth[0]}:{self.auth[1]}".encode("utf-8")
            ).decode("ascii")
            prepared_request.headers["Authorization"] = f"Basic {token}"

def tool_search_jira(keywords: str, max_results: int = 10) -> str:
    """Search Jira across CE, DFS, and SE projects for issues matching keywords.

    Uses JQL: project in (CE, DFS, SE) AND text ~ "<keywords>" ORDER BY updated DESC
    - CE  = Customer Escalations (customer-facing SSD support tickets)
    - DFS = Data Feed Status (pipeline registry with PipelineName, Granularity, dag_id)
    - SE  = Support Engineering (internal eng work: migrations, platform fixes)
    Returns a summary of matching issues (key, summary, status, created, description snippet).
    """
    # Build JQL — wrap multi-word keywords in quotes
    kw = keywords.strip()
    if " " in kw and not (kw.startswith('"') and kw.endswith('"')):
        kw_jql = f'"{kw}"'
    else:
        kw_jql = kw
    jql = f'project in ({JIRA_SSD_PROJECTS}) AND text ~ {kw_jql} ORDER BY updated DESC'

    # Atlassian personal API tokens use HTTP Basic auth: email + token (not Bearer)
    auth_headers = {"Accept": "application/json"}
    url    = f"{JIRA_API_BASE}/issue/search"
    params = {
        "jql":        jql,
        "maxResults": max_results,
        "fields":     "summary,status,created,description,assignee,priority,comment",
    }
    try:
        session = _AuthPreservingSession()
        session.auth = (CONFLUENCE_EMAIL, CONFLUENCE_TOKEN)
        resp = session.get(url, headers=auth_headers, params=params, timeout=15)
        logger.info(f"Jira search → {resp.status_code}  jql={jql!r}")
        resp.raise_for_status()
        data = resp.json()
    except requests.exceptions.HTTPError as e:
        sc = e.response.status_code
        logger.error(f"Jira search HTTP error: {e} — {getattr(e.response, 'text', '')}")
        return f"No past Jira cases found for: {keywords}"
    except Exception as e:
        logger.error(f"Jira search error: {e}")
        return f"No past Jira cases found for: {keywords}"

    issues = data.get("issues", [])
    total  = data.get("total", 0)

    if not issues:
        return f"No Jira issues found for: {keywords}\nJQL used: `{jql}`"

    lines = [f"Found {total} Jira issue(s) matching *{keywords}* (showing top {len(issues)}):"]
    for issue in issues:
        key     = issue.get("key", "?")
        fields  = issue.get("fields", {})
        summary = fields.get("summary", "(no summary)")
        status  = (fields.get("status") or {}).get("name", "?")
        created = (fields.get("created") or "")[:10]   # YYYY-MM-DD
        priority = (fields.get("priority") or {}).get("name", "?")
        assignee = ((fields.get("assignee") or {}).get("displayName") or "Unassigned")

        # Extract plain text from description (Atlassian Document Format)
        desc_text = ""
        desc = fields.get("description")
        if desc and isinstance(desc, dict):
            # ADF: walk content nodes and collect text
            def _extract_adf_text(node, depth=0):
                if depth > 10:
                    return ""
                if not isinstance(node, dict):
                    return ""
                if node.get("type") == "text":
                    return node.get("text", "")
                parts = []
                for child in node.get("content", []):
                    parts.append(_extract_adf_text(child, depth + 1))
                return " ".join(p for p in parts if p)
            desc_text = _extract_adf_text(desc)
            desc_text = desc_text[:300].strip()
            if len(desc_text) == 300:
                desc_text += "…"

        # Latest comment snippet
        comment_snippet = ""
        comments = (fields.get("comment") or {}).get("comments", [])
        if comments:
            last = comments[-1]
            author = ((last.get("author") or {}).get("displayName") or "?")
            body   = last.get("body")
            body_text = ""
            if body and isinstance(body, dict):
                body_text = _extract_adf_text(body)[:200].strip()
            elif isinstance(body, str):
                body_text = body[:200].strip()
            if body_text:
                comment_snippet = f"\n  💬 Last comment by {author}: {body_text}"

        jira_url = f"https://conviva.atlassian.net/browse/{key}"
        lines.append(
            f"\n*{key}* — {summary}\n"
            f"  Status: {status} | Priority: {priority} | Assignee: {assignee} | Created: {created}\n"
            f"  🔗 {jira_url}"
        )
        if desc_text:
            lines.append(f"  📄 {desc_text}")
        if comment_snippet:
            lines.append(comment_snippet)

    return "\n".join(lines)


# Tool definitions passed to Claude
AGENT_TOOLS = [
    {
        "name": "search_confluence",
        "description": (
            "Search Confluence (CSS space) for pages related to a topic. "
            "Use this to find runbook entries, procedures, pipeline docs, or any team knowledge. "
            "Returns page titles and content excerpts."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search keywords or pipeline name"},
                "max_results": {"type": "integer", "description": "Max pages to return (default 5)", "default": 5},
            },
            "required": ["query"],
        },
    },
    {
        "name": "read_confluence_page",
        "description": (
            "Read the full content of a specific Confluence page given its URL or page ID. "
            "Use this when the user shares a Confluence link or mentions a specific page they want the bot to read and analyze. "
            "Also use this to deep-read a page found by search_confluence."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "page_ref": {
                    "type": "string",
                    "description": "A Confluence page URL (e.g. https://conviva.atlassian.net/wiki/spaces/CSS/pages/12345) or a numeric page ID",
                },
            },
            "required": ["page_ref"],
        },
    },
    {
        "name": "list_all_dag_ids",
        "description": (
            "List a sample of actual DAG IDs from all Airflow instances. "
            "Use this when a filtered search returns no results, to understand the real naming convention used."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sample_size": {"type": "integer", "description": "How many DAG IDs to return per instance (default 30)", "default": 30},
            },
        },
    },
    {
        "name": "get_airflow_dags",
        "description": (
            "List Airflow DAGs with optional name filter and instance filter. "
            "Use instance='legacy' + name_filters=['_legacy'] to count legacy pipelines (DAG IDs containing '_legacy'). "
            "Use instance='connect' with NO name_filters to count connect pipelines — all DAGs in that system are connect pipelines. "
            "Use name_filters to search by DAG name substring across instance(s). "
            "Leave both empty to list all DAGs from all instances. "
            "IMPORTANT: QVC and Qurate refer to the same customer — always search both. "
            "IMPORTANT: 'legacy pipelines' = instance='legacy' + name_filters=['_legacy']. Do NOT omit name_filters when the user asks about 'legacy pipelines'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "name_filters": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Substrings to match against DAG names (OR logic). E.g. ['QVC', 'Qurate']. Leave empty to get all DAGs.",
                },
                "instance": {
                    "type": "string",
                    "description": (
                        "Restrict to a specific Airflow instance by label. "
                        f"Available: {', '.join(AIRFLOW_INSTANCES.keys()) or 'connect, legacy, streamid'}. "
                        "Use 'legacy' + name_filters=['_legacy'] when the user asks about legacy pipelines. "
                        "Use 'connect' with NO name_filters when the user asks about connect pipelines — all DAGs in that system count. "
                        "Leave empty to query all instances."
                    ),
                },
            },
        },
    },
    {
        "name": "get_airflow_dag_runs",
        "description": (
            "Get recent run history (status, execution date) for a specific Airflow DAG. "
            "IMPORTANT: when debugging why a pipeline failed, always pass state='failed' so recent "
            "failures are not hidden by subsequent successful runs. Default fetches all states."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dag_id": {"type": "string", "description": "The exact DAG ID"},
                "limit":  {"type": "integer", "description": "Number of runs to return (default 10)", "default": 10},
                "state":  {
                    "type": "string",
                    "description": "Filter by run state: 'failed', 'success', 'running', 'queued'. "
                                   "Always use 'failed' when investigating a failure.",
                    "enum": ["failed", "success", "running", "queued"],
                },
            },
            "required": ["dag_id"],
        },
    },
    {
        "name": "get_airflow_task_instances",
        "description": (
            "List all task instances for a specific DAG run, showing each task's state and try number. "
            "Use this after get_airflow_dag_runs to see which tasks passed, failed, or are running."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dag_id":     {"type": "string", "description": "The exact DAG ID"},
                "dag_run_id": {"type": "string", "description": "The run ID from get_airflow_dag_runs"},
            },
            "required": ["dag_id", "dag_run_id"],
        },
    },
    {
        "name": "get_airflow_task_log",
        "description": (
            "Fetch the log for a specific task instance. Returns the last ~200 lines. "
            "Use this to diagnose why a task failed — look for ERROR, Exception, or traceback lines. "
            "Call get_airflow_task_instances first to find the task_id and try_number."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dag_id":     {"type": "string", "description": "The exact DAG ID"},
                "dag_run_id": {"type": "string", "description": "The run ID"},
                "task_id":    {"type": "string", "description": "The task ID from get_airflow_task_instances"},
                "try_number": {"type": "integer", "description": "Which attempt to read. For root cause analysis, ALWAYS read try 1 first — it shows the original error. Later tries may fail for different reasons (e.g. files already written, locks already held by try 1). Read the last try only as a secondary check.", "default": 1},
                "max_lines":  {"type": "integer", "description": "Max trailing lines to return. Default 150 for sensor/short tasks. Use 0 (unlimited) for delivery tasks (copy_and_deliver, trigger_spark_job, run_script_eco) so the full log sequence is visible — critical for determining whether delivery completed before the error.", "default": 150},
            },
            "required": ["dag_id", "dag_run_id", "task_id"],
        },
    },
    {
        "name": "read_slack_channel",
        "description": (
            "Read recent messages from a Slack channel to find discussion, alerts, or context. "
            f"Available channels: {', '.join(SEARCHABLE_SLACK_CHANNELS.keys())}"
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "channel_name": {"type": "string", "description": f"One of: {', '.join(SEARCHABLE_SLACK_CHANNELS.keys())}"},
                "limit":        {"type": "integer", "description": "Number of messages to read (default 30)", "default": 30},
            },
            "required": ["channel_name"],
        },
    },
    {
        "name": "propose_new_confluence_page",
        "description": (
            "Propose creating a brand-new Confluence page (not editing an existing one). "
            "Use this when the user asks to 'publish a new page', 'create a page', 'write a Confluence page', "
            "or 'post this to Confluence as a new page'. "
            "This will show a preview to the user and ask for yes/no before publishing. "
            "Default parent is the SSD Playbook — pass a different parent_page_id if the user specifies."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "title": {
                    "type": "string",
                    "description": "Title of the new page",
                },
                "content": {
                    "type": "string",
                    "description": "Full page content in plain text or basic markdown (# headings, - bullets). The bot will convert it to Confluence format.",
                },
                "parent_page_id": {
                    "type": "string",
                    "description": f"Parent page ID. Defaults to the SSD Playbook ({PRIMARY_PAGE_ID}) if not specified.",
                    "default": PRIMARY_PAGE_ID,
                },
            },
            "required": ["title", "content"],
        },
    },
    {
        "name": "propose_mark_dag_runs",
        "description": (
            "Propose marking all DAG runs in a date/time window as 'success' or 'failed'. "
            "Use this when the user wants to skip a bad time window (upstream outage, no data) "
            "or bulk-mark runs without re-executing them. "
            "Shows a preview (run count, date range, current states) and asks for yes/no. "
            "Use state='success' to cleanly skip runs — no retries, no alerts triggered. "
            "Use state='failed' when the user explicitly wants to mark the period as failed. "
            "Set kill_running=True when the user says 'force skip', 'kill running tasks', "
            "'强制跳过', 'mark and kill', or wants to stop tasks that are currently executing "
            "in addition to marking the runs."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dag_id": {
                    "type": "string",
                    "description": "The exact DAG ID to mark",
                },
                "start_date": {
                    "type": "string",
                    "description": "Start of the window in UTC, e.g. '2026-05-22T05:04:00+00:00' or '2026-05-22 05:04:00'",
                },
                "end_date": {
                    "type": "string",
                    "description": "End of the window in UTC, e.g. '2026-05-22T11:21:59+00:00' or '2026-05-22 11:21:00'",
                },
                "state": {
                    "type": "string",
                    "enum": ["success", "failed"],
                    "description": "'success' to skip cleanly (recommended for upstream outages), 'failed' to mark as failed",
                },
                "instance": {
                    "type": "string",
                    "description": (
                        "Airflow instance label to search. "
                        f"Available: {', '.join(AIRFLOW_INSTANCES.keys()) or 'connect, legacy, streamid'}. "
                        "Leave empty to search all instances."
                    ),
                },
                "kill_running": {
                    "type": "boolean",
                    "description": (
                        "If true, also interrupt any currently-running task instances in the window "
                        "(sends termination signal to workers via clearTaskInstances with only_running=True). "
                        "Use when user says 'force skip', 'kill running tasks', 'mark and kill', or '强制跳过'. "
                        "Default: false."
                    ),
                    "default": False,
                },
            },
            "required": ["dag_id", "start_date", "end_date", "state"],
        },
    },
    {
        "name": "propose_backfill_dag_runs",
        "description": (
            "Propose a backfill-mark-success over a time window, bypassing max_active_runs. "
            "Unlike propose_mark_dag_runs, this creates missing dag run records in the DB "
            "so ALL runs in the range are marked success — even ones the scheduler never created "
            "because earlier runs were still running (max_active_runs limit). "
            "Use this when: 'mark pipeline X success from TIME to TIME backfill', "
            "'backfill mark success', 'mark all including unscheduled runs', or when the user "
            "explains that some runs don't exist yet due to max_active_runs. "
            "Always marks state as 'success'. No tasks will execute. "
            "Set kill_running=True when user adds 'force skip' — kills active tasks first "
            "so running runs can be patched cleanly."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dag_id": {
                    "type": "string",
                    "description": "The exact DAG ID to backfill",
                },
                "start_date": {
                    "type": "string",
                    "description": "Start of the window in UTC, e.g. '2026-05-22T05:04:00+00:00' or '2026-05-22 05:04:00'",
                },
                "end_date": {
                    "type": "string",
                    "description": "End of the window in UTC, e.g. '2026-05-22T11:20:00+00:00' or '2026-05-22 11:20:00'",
                },
                "instance": {
                    "type": "string",
                    "description": (
                        "Airflow instance label. "
                        f"Available: {', '.join(AIRFLOW_INSTANCES.keys()) or 'connect, legacy, streamid'}. "
                        "Leave empty to auto-detect."
                    ),
                },
                "kill_running": {
                    "type": "boolean",
                    "description": (
                        "If true, kill any currently-running task instances before marking runs as success. "
                        "Use when user says 'force skip' — ensures actively running tasks don't block the PATCH. "
                        "Default: false."
                    ),
                    "default": False,
                },
            },
            "required": ["dag_id", "start_date", "end_date"],
        },
    },
    {
        "name": "check_hdfs_minute_data",
        "description": (
            "Check whether HDFS data (_SUCCESS flag) is available for the upstream minute "
            "of a failed DPI Flow Feed pipeline. Automatically applies the 2-minute offset. "
            "Checks both ecoCrossPageFlow and ecoEventSummary paths via WebHDFS. "
            "ALWAYS call this before propose_trigger_upstream_minute_dag to confirm data is ready. "
            "Paths must be read from Confluence page 4192337966 first — do NOT hardcode them."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "failed_minute": {
                    "type": "string",
                    "description": "The FAILED minute from the DPI Flow Feed pipeline in UTC, e.g. '2026-05-22 21:10:00'",
                },
                "cross_page_hdfs_path": {
                    "type": "string",
                    "description": "Full hdfs:// base path for ecoCrossPageFlow, read from Confluence page 4192337966 config JSON. e.g. 'hdfs://nameservice-aa/tlb2/tlb2-aa-prod/ecoCrossPageFlowDefaultOneMinFileSink'",
                },
                "event_summary_hdfs_path": {
                    "type": "string",
                    "description": "Full hdfs:// base path for ecoEventSummary, read from Confluence page 4192337966 config JSON. e.g. 'hdfs://nameservice-aa/tlb2/tlb2-aa-prod/ecoEventSummaryDefaultOneMinFileSink'",
                },
                "namenode_http": {
                    "type": "string",
                    "description": "WebHDFS namenode URL, read from the HDFS explorer links in Confluence page 4192337966. e.g. 'http://rccp408-24a.iad6.prod.conviva.com:50070'",
                },
            },
            "required": ["failed_minute", "cross_page_hdfs_path", "event_summary_hdfs_path", "namenode_http"],
        },
    },
    {
        "name": "propose_trigger_upstream_minute_dag",
        "description": (
            "Propose triggering the upstream minute DAG (ECO_CROSS_PAGE_EVENT_SUMMARY_SSD_MINUTE_DAG) "
            "to fix a DPI Flow Feed pipeline that is stuck/failed at the sensor task due to missing upstream data. "
            "Accepts the FAILED minute, HDFS paths, and upstream Airflow base URL — all read from Confluence page 4192337966. "
            "Automatically subtracts 2 minutes for the upstream logical date and builds the correct HDFS config JSON. "
            "Shows a preview and waits for yes/no before triggering. "
            "ALL parameters must be read from Confluence page 4192337966 — do NOT hardcode any of them."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "failed_minute": {
                    "type": "string",
                    "description": (
                        "The FAILED minute from the DPI Flow Feed pipeline in UTC, "
                        "e.g. '2026-05-22 21:10:00' or '2026-05-22T21:10:00+00:00'."
                    ),
                },
                "cross_page_hdfs_path": {
                    "type": "string",
                    "description": "Full hdfs:// base path for ecoCrossPageFlow, read from Confluence page 4192337966 config JSON.",
                },
                "event_summary_hdfs_path": {
                    "type": "string",
                    "description": "Full hdfs:// base path for ecoEventSummary, read from Confluence page 4192337966 config JSON.",
                },
                "upstream_airflow_base_url": {
                    "type": "string",
                    "description": (
                        "Base URL of the Airflow instance that hosts ECO_CROSS_PAGE_EVENT_SUMMARY_SSD_MINUTE_DAG, "
                        "read from the Confluence playbook (page 4192337966). "
                        "Extract from the upstream DAG link in the runbook — e.g. if the link is "
                        "'https://conviva-airflow.prod.conviva.com/dags/ECO_.../grid', "
                        "the base URL is 'https://conviva-airflow.prod.conviva.com'. "
                        "Do NOT hardcode this value."
                    ),
                },
                "force": {
                    "type": "boolean",
                    "description": (
                        "Set to true when triggering WITHOUT a confirmed _SUCCESS flag in HDFS — "
                        "i.e. the user explicitly chose to proceed despite missing upstream data. "
                        "The proposal message will show a clear warning. Default is false."
                    ),
                },
            },
            "required": ["failed_minute", "cross_page_hdfs_path", "event_summary_hdfs_path", "upstream_airflow_base_url"],
        },
    },
    {
        "name": "propose_rerun_dag_runs",
        "description": (
            "Propose clearing and re-running all DAG runs in a date/time window (backfill). "
            "Use this when the user wants to re-execute runs that were previously skipped or marked, "
            "e.g. after upstream data has been recovered. "
            "Clears task instances so Airflow re-executes them — max_active_runs throttling still applies. "
            "Shows a preview (run count, current states) and waits for yes/no confirmation."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dag_id": {
                    "type": "string",
                    "description": "The exact DAG ID to re-run",
                },
                "start_date": {
                    "type": "string",
                    "description": "Start of the window in UTC, e.g. '2026-05-22T05:04:00+00:00' or '2026-05-22 05:04:00'",
                },
                "end_date": {
                    "type": "string",
                    "description": "End of the window in UTC, e.g. '2026-05-22T11:21:59+00:00' or '2026-05-22 11:21:00'",
                },
                "instance": {
                    "type": "string",
                    "description": (
                        "Airflow instance label. "
                        f"Available: {', '.join(AIRFLOW_INSTANCES.keys()) or 'connect, legacy, streamid'}. "
                        "Leave empty to search all instances."
                    ),
                },
            },
            "required": ["dag_id", "start_date", "end_date"],
        },
    },
    {
        "name": "propose_flow_feed_reruns_batch",
        "description": (
            "Propose rerunning multiple DPI Flow Feed pipelines in a single Slack message "
            "with checkboxes — the user can check/uncheck individual pipelines before confirming. "
            "Use this instead of multiple propose_rerun_dag_runs calls when you have a list of "
            "flow-feed DAGs to rerun for the same minute/window (e.g. from get_flow_feed_failures_at_minute). "
            "All pipelines are pre-selected by default; user clicks '✅ Rerun Selected' to confirm."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dag_ids": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of DAG IDs to include in the batch rerun proposal.",
                },
                "start_date": {
                    "type": "string",
                    "description": "Start of the window in UTC, e.g. '2026-09-04T08:56:00+00:00'",
                },
                "end_date": {
                    "type": "string",
                    "description": "End of the window in UTC (same as start for a single minute).",
                },
            },
            "required": ["dag_ids", "start_date", "end_date"],
        },
    },
    {
        "name": "propose_confluence_update",
        "description": (
            "Propose an update to the SSD Confluence runbook. "
            "Use this when the user asks you to add something to the runbook, update the runbook, "
            "or write what you just found/explained to Confluence. "
            "This will show the proposed change to the user and ask for their confirmation (yes/no) "
            "before actually writing anything. You do NOT need a separate tool to confirm — "
            "the user will reply 'yes' or 'no' in the thread."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "content_to_add": {
                    "type": "string",
                    "description": (
                        "Plain English description of what to add. Include all relevant detail: "
                        "root cause, steps, pipeline names, resolution. "
                        "If referring to earlier conversation, summarise the key facts here."
                    ),
                },
                "section": {
                    "type": "string",
                    "description": "Which runbook section to add to, e.g. 'Common Issues', 'Special Notes', 'FAQ'",
                    "default": "Common Issues",
                },
            },
            "required": ["content_to_add"],
        },
    },
    {
        "name": "save_memory",
        "description": (
            "Save a fact, preference, or correction to persistent memory so it's available in ALL future conversations. "
            "Use this proactively when the user corrects you, teaches you something specific "
            "(e.g. 'for QVC, only check these 3 DAGs'), or when you discover a stable fact "
            "that would save time next time. "
            "Choose a clear, descriptive key like 'QVC_pipelines_to_check' or 'stream_id_affected_customers'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "key":   {"type": "string", "description": "Short descriptive label, e.g. 'QVC_pipelines_to_check'"},
                "value": {"type": "string", "description": "The fact or preference to remember"},
            },
            "required": ["key", "value"],
        },
    },
    {
        "name": "get_dag_source",
        "description": (
            "Fetch the Python source code of an Airflow DAG. "
            "Use this to understand what a pipeline does, identify ExternalTaskSensor upstream "
            "dependencies, discover schedule intervals, and read the actual task logic. "
            "After getting the source, scan it for: ExternalTaskSensor(external_dag_id=...), "
            "sensor tasks that poll HDFS/S3/APIs, and any comments describing the data flow. "
            "Trigger when: user gives a pipeline link and asks to understand it, "
            "when triage needs to know what upstream a pipeline is waiting on, "
            "or when the task name alone doesn't explain why it failed."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dag_id":   {"type": "string", "description": "Exact Airflow DAG ID"},
                "instance": {"type": "string", "description": "Airflow instance label (e.g. 'connect', 'streamid'). Leave empty to try all."},
            },
            "required": ["dag_id"],
        },
    },
    {
        "name": "find_missing_dag_runs",
        "description": (
            "Detect execution times that SHOULD exist (based on the DAG schedule interval) "
            "but are missing from the Airflow database — i.e. runs that were never scheduled or triggered. "
            "Use this when the user says a pipeline 'didn't run', 'wasn't triggered', "
            "'skipped an hour', or 'not scheduled'. "
            "Returns the gap list with common causes (paused DAG, max_active_runs hit, catchup=False, etc.)."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dag_id":     {"type": "string", "description": "Exact Airflow DAG ID"},
                "start_date": {"type": "string", "description": "Start of window to check (HH:MM or YYYY-MM-DD HH:MM:SS)"},
                "end_date":   {"type": "string", "description": "End of window to check (HH:MM or YYYY-MM-DD HH:MM:SS)"},
                "instance":   {"type": "string", "description": "Airflow instance label. Leave empty to try all."},
            },
            "required": ["dag_id", "start_date", "end_date"],
        },
    },
    {
        "name": "propose_interleaved_rerun_dag_runs",
        "description": (
            "Re-run historical DAG runs in fair-queue waves so the live forward pipeline "
            "keeps half the max_active_runs slots throughout the backfill. "
            "Unlike propose_rerun_dag_runs (which clears everything at once), this tool "
            "clears runs in batches of floor(max_active_runs/2), waits for each wave to "
            "complete, then clears the next batch — alternating with the forward pipeline. "
            "Use when the user says 'interleaved rerun', 'fair rerun', 'don't starve the forward pipeline', "
            "'backfill without blocking live runs', or 'queue some then wait for forward then queue more'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dag_id": {"type": "string", "description": "Exact Airflow DAG ID to re-run"},
                "start_date": {
                    "type": "string",
                    "description": "Start of the execution-date window (HH:MM or YYYY-MM-DD HH:MM:SS UTC)",
                },
                "end_date": {
                    "type": "string",
                    "description": "End of the execution-date window (HH:MM or YYYY-MM-DD HH:MM:SS UTC)",
                },
                "instance": {
                    "type": "string",
                    "description": "Airflow instance label (e.g. 'legacy', 'connect', 'streamid'). Leave empty to search all.",
                },
            },
            "required": ["dag_id", "start_date", "end_date"],
        },
    },
    {
        "name": "read_memory",
        "description": (
            "Read all saved memories, or look up a specific memory by key. "
            "Use this if you want to double-check what was previously learned."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "key": {"type": "string", "description": "Optional: specific memory key to look up. Leave empty to read all."},
            },
        },
    },
    {
        "name": "delete_memory",
        "description": (
            "Delete a specific memory entry by key. "
            "Use when the user says 'forget X', 'remove that memory', 'delete memory for X', "
            "or when a saved memory entry is known to be wrong or outdated."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "key": {"type": "string", "description": "The exact memory key to delete."},
            },
            "required": ["key"],
        },
    },
    {
        "name": "search_jira",
        "description": (
            "Search Jira for SSD-related support cases and issues matching given keywords. "
            "Use this when the user asks 'how to debug X', 'have we seen Y before', 'any similar issues with Z', "
            "'why is X failing', or any question that might benefit from looking up past cases. "
            "Automatically filters to SSD issues only (Product ~ SSD). "
            "Returns issue keys, summaries, status, assignees, description snippets, and latest comments."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "keywords": {
                    "type": "string",
                    "description": (
                        "Keywords to search for in Jira issue text. "
                        "Examples: 'BlobAlreadyExists', 'HDFS sensor timeout', 'DPI flow feed stuck', "
                        "'Gotham pipeline failed', 'SSD data late'. "
                        "Use specific error messages or pipeline names for best results."
                    ),
                },
                "max_results": {
                    "type": "integer",
                    "description": "Max number of issues to return (default 10)",
                    "default": 10,
                },
            },
            "required": ["keywords"],
        },
    },
    {
        "name": "check_s3_vs_hdfs",
        "description": (
            "Compare file count and total size between an HDFS staging path and an S3 destination path. "
            "Use this to determine whether an S3 upload completed successfully, was partial, or never started. "
            "Returns a verdict: COMPLETE (S3 matches HDFS), PARTIAL (fewer/smaller files in S3), "
            "or EMPTY (nothing in S3). "
            "Call this after finding a 'move hdfs://... to s3a://...' line in a task log, before deciding "
            "whether to propose a rerun or a repair copy."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "hdfs_url": {
                    "type": "string",
                    "description": (
                        "HDFS source URL in any format: "
                        "http://namenode:50070/webhdfs/v1/path, "
                        "http://namenode:50070/explorer.html#/path, or "
                        "hdfs://namenode:8020/path"
                    ),
                },
                "s3_url": {
                    "type": "string",
                    "description": "S3 destination URL, e.g. s3://bucket/prefix or s3a://bucket/prefix",
                },
            },
            "required": ["hdfs_url", "s3_url"],
        },
    },
    {
        "name": "propose_hdfs_to_s3_repair",
        "description": (
            "Propose copying files from an HDFS path to an S3 path for manual repair/delivery. "
            "Use when the user says 'copy files from [hdfs url] to [s3 url]', "
            "'repair delivery to s3', 'manually deliver hdfs data to s3', or similar. "
            "Lists the files first and waits for yes/no before copying. "
            "Streams files via WebHDFS REST API directly to S3 — no hadoop client needed."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "hdfs_url": {
                    "type": "string",
                    "description": (
                        "HDFS source URL in any format: "
                        "http://namenode:50070/webhdfs/v1/path, "
                        "http://namenode:50070/explorer.html#/path, or "
                        "hdfs://namenode:8020/path. "
                        "Brace expressions in the last path segment are auto-expanded: "
                        ".../DailySSD_SunNXT_legacy/DailySessionLog_{26,27,28}.csv.gz"
                    ),
                },
                "s3_url": {
                    "type": "string",
                    "description": "S3 destination URL, e.g. s3://bucket/prefix or s3a://bucket/prefix",
                },
                "file_filter": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": (
                        "Optional list of exact filenames or fnmatch patterns to copy. "
                        "When the user names specific files, extract them here and only those files will be copied. "
                        "Examples: ['DailySessionLog_SunNXT_2026-08-27.csv.gz', 'DailySessionLog_SunNXT_2026-08-28.csv.gz'] "
                        "or ['DailySessionLog_SunNXT_2026-08-2?.csv.gz']. "
                        "Leave empty to copy all files."
                    ),
                },
            },
            "required": ["hdfs_url", "s3_url"],
        },
    },
    {
        "name": "get_flow_feed_failures_at_minute",
        "description": (
            "Query #piccolo-daas-alert to find ALL DPI Flow Feed pipelines that failed at the same "
            "stuck minute. Call this when a flow feed sensor failure is detected so you can propose "
            "a batch rerun for ALL affected pipelines at once, not just the one in the alert. "
            "Returns the list of unique DAG IDs stuck at that minute."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "stuck_minute": {
                    "type": "string",
                    "description": (
                        "The execution_date from the failed TaskInstance alert — this is the stuck minute. "
                        "e.g. '2026-08-26T19:14:00+00:00' or '2026-08-26 19:14:00'."
                    ),
                },
            },
            "required": ["stuck_minute"],
        },
    },
    {
        "name": "read_ssd_alerts",
        "description": (
            "Read and parse SSD PagerDuty alerts from #piccolo-daas-alert for a given date range, "
            "then return grouped counts by task type and pipeline for you to summarize. "
            "Use this when the user asks for a weekly/monthly/period alert summary, "
            "e.g. 'generate weekly summary', 'alert summary for July', 'last 30 days', "
            "'from Aug 1 to Aug 7', 'last month', 'last week'. "
            "Resolve all natural-language date references to YYYY-MM-DD before calling."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "start_date": {
                    "type": "string",
                    "description": "Start date in YYYY-MM-DD format (UTC), inclusive. If omitted, defaults to 7 days before today.",
                },
                "end_date": {
                    "type": "string",
                    "description": "End date in YYYY-MM-DD format (UTC), inclusive. If omitted, defaults to today.",
                },
            },
            "required": [],
        },
    },
    {
        "name": "lookup_pipeline",
        "description": (
            "Look up SSD delivery pipelines by customer account name or delivery path. "
            "Use this when the user asks 'which pipelines does customer X have?', "
            "'what pipelines are set up for c3.XXX?', 'who owns this S3/GCS path?', "
            "'which pipeline delivers to s3://...' or any question linking an account to its pipelines "
            "or a delivery path to a pipeline/customer. "
            "Pass account_name in c3.XXX format (partial match supported), "
            "or pass delivery_path as a partial S3/GCS path string."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "account_name": {
                    "type": "string",
                    "description": (
                        "Customer account name in c3.XXX format. Partial names are supported "
                        "(e.g. 'c3.SlingTV' or 'SlingTV'). Provide this OR delivery_path, not both."
                    ),
                },
                "delivery_path": {
                    "type": "string",
                    "description": (
                        "Partial or full S3/GCS delivery path (min 3 chars). "
                        "Examples: 's3://p-conviva-v2/eco_event', 'eco_event_level_ssd'. "
                        "Provide this OR account_name, not both."
                    ),
                    "minLength": 3,
                },
            },
        },
    },
]


def execute_tool(name: str, inputs: dict, **ctx) -> str:
    """Dispatch a tool call from Claude to the right Python function.
    ctx may contain: channel, thread_ts, user, client — needed for write tools."""
    if name == "read_confluence_page":
        return tool_read_confluence_page(inputs["page_ref"])
    if name == "search_confluence":
        return tool_search_confluence(inputs["query"], inputs.get("max_results", 5))
    if name == "list_all_dag_ids":
        return tool_list_all_dag_ids(inputs.get("sample_size", 30))
    if name == "get_airflow_dags":
        return tool_get_airflow_dags(inputs.get("name_filters", []), inputs.get("instance"))
    if name == "get_airflow_dag_runs":
        return tool_get_airflow_dag_runs(inputs["dag_id"], inputs.get("limit", 10), inputs.get("state"))
    if name == "get_airflow_task_instances":
        return tool_get_airflow_task_instances(inputs["dag_id"], inputs["dag_run_id"])
    if name == "get_airflow_task_log":
        return tool_get_airflow_task_log(inputs["dag_id"], inputs["dag_run_id"], inputs["task_id"], inputs.get("try_number", 1), inputs.get("max_lines", 150))
    if name == "get_dag_source":
        return tool_get_dag_source(inputs["dag_id"], inputs.get("instance"))
    if name == "find_missing_dag_runs":
        return tool_find_missing_dag_runs(inputs["dag_id"], inputs["start_date"], inputs["end_date"], inputs.get("instance"))
    if name == "read_slack_channel":
        return tool_read_slack_channel(inputs["channel_name"], inputs.get("limit", 30))
    if name == "save_memory":
        return tool_save_memory(inputs["key"], inputs["value"])
    if name == "read_memory":
        return tool_read_memory(inputs.get("key"))
    if name == "delete_memory":
        return tool_delete_memory(inputs["key"])
    if name == "search_jira":
        return tool_search_jira(inputs["keywords"], inputs.get("max_results", 10))
    if name == "propose_interleaved_rerun_dag_runs":
        return tool_propose_interleaved_rerun_dag_runs(
            dag_id=inputs["dag_id"],
            start_date=inputs["start_date"],
            end_date=inputs["end_date"],
            instance=inputs.get("instance"),
            channel=ctx.get("channel", ""),
            thread_ts=ctx.get("thread_ts", ""),
            user=ctx.get("user", ""),
            client=ctx.get("client"),
        )
    if name == "propose_mark_dag_runs":
        return tool_propose_mark_dag_runs(
            dag_id=inputs["dag_id"],
            start_date=inputs["start_date"],
            end_date=inputs["end_date"],
            state=inputs["state"],
            instance=inputs.get("instance"),
            kill_running=inputs.get("kill_running", False),
            channel=ctx.get("channel", ""),
            thread_ts=ctx.get("thread_ts", ""),
            user=ctx.get("user", ""),
            client=ctx.get("client"),
        )
    if name == "propose_backfill_dag_runs":
        return tool_propose_backfill_dag_runs(
            dag_id=inputs["dag_id"],
            start_date=inputs["start_date"],
            end_date=inputs["end_date"],
            instance=inputs.get("instance"),
            kill_running=inputs.get("kill_running", False),
            channel=ctx.get("channel", ""),
            thread_ts=ctx.get("thread_ts", ""),
            user=ctx.get("user", ""),
            client=ctx.get("client"),
        )
    if name == "check_hdfs_minute_data":
        return tool_check_hdfs_minute_data(
            failed_minute=inputs["failed_minute"],
            cross_page_hdfs_path=inputs["cross_page_hdfs_path"],
            event_summary_hdfs_path=inputs["event_summary_hdfs_path"],
            namenode_http=inputs["namenode_http"],
        )
    if name == "propose_trigger_upstream_minute_dag":
        return tool_propose_trigger_upstream_minute_dag(
            failed_minute=inputs["failed_minute"],
            cross_page_hdfs_path=inputs["cross_page_hdfs_path"],
            event_summary_hdfs_path=inputs["event_summary_hdfs_path"],
            upstream_airflow_base_url=inputs["upstream_airflow_base_url"],
            force=inputs.get("force", False),
            channel=ctx.get("channel", ""),
            thread_ts=ctx.get("thread_ts", ""),
            user=ctx.get("user", ""),
            client=ctx.get("client"),
        )
    if name == "propose_rerun_dag_runs":
        return tool_propose_rerun_dag_runs(
            dag_id=inputs["dag_id"],
            start_date=inputs["start_date"],
            end_date=inputs["end_date"],
            instance=inputs.get("instance"),
            channel=ctx.get("channel", ""),
            thread_ts=ctx.get("thread_ts", ""),
            user=ctx.get("user", ""),
            client=ctx.get("client"),
        )
    if name == "propose_flow_feed_reruns_batch":
        return tool_propose_flow_feed_reruns_batch(
            dag_ids=inputs["dag_ids"],
            start_date=inputs["start_date"],
            end_date=inputs["end_date"],
            channel=ctx.get("channel", ""),
            thread_ts=ctx.get("thread_ts", ""),
            user=ctx.get("user", ""),
            client=ctx.get("client"),
        )
    if name == "propose_new_confluence_page":
        return tool_propose_new_confluence_page(
            title=inputs["title"],
            content=inputs["content"],
            parent_page_id=inputs.get("parent_page_id", PRIMARY_PAGE_ID),
            channel=ctx.get("channel", ""),
            thread_ts=ctx.get("thread_ts", ""),
            user=ctx.get("user", ""),
            client=ctx.get("client"),
        )
    if name == "propose_confluence_update":
        return tool_propose_confluence_update(
            content_to_add=inputs["content_to_add"],
            section=inputs.get("section", "Common Issues"),
            channel=ctx.get("channel", ""),
            thread_ts=ctx.get("thread_ts", ""),
            user=ctx.get("user", ""),
            client=ctx.get("client"),
        )
    if name == "check_s3_vs_hdfs":
        return tool_check_s3_vs_hdfs(
            hdfs_url=inputs["hdfs_url"],
            s3_url=inputs["s3_url"],
        )
    if name == "propose_hdfs_to_s3_repair":
        return tool_propose_hdfs_to_s3_repair(
            hdfs_url=inputs["hdfs_url"],
            s3_url=inputs["s3_url"],
            file_filter=inputs.get("file_filter"),
            channel=ctx.get("channel", ""),
            thread_ts=ctx.get("thread_ts", ""),
            user=ctx.get("user", ""),
            client=ctx.get("client"),
        )
    if name == "get_flow_feed_failures_at_minute":
        return tool_get_flow_feed_failures_at_minute(inputs["stuck_minute"])
    if name == "read_ssd_alerts":
        return tool_read_ssd_alerts(inputs["start_date"], inputs["end_date"])
    if name == "lookup_pipeline":
        return tool_lookup_pipeline(
            account_name=inputs.get("account_name"),
            delivery_path=inputs.get("delivery_path"),
        )
    return f"Unknown tool: {name}"


# ─── Handler: Answer Question (Agent Loop) ─────────────────────────────────────

AGENT_SYSTEM = """You are the SSD Agent Assist for Conviva's CSS team.
You help support engineers answer questions about SSD pipelines, debug issues,
and understand the state of the data delivery system.

You have tools to search Confluence documentation, list and inspect Airflow DAGs,
and read Slack channels. Use them proactively — don't guess when you can look it up.

━━━ CUSTOMER ALIASES (always treat these as equivalent) ━━━
- QVC = Qurate = QRV  (same customer, pipelines may use any of these names)

━━━ AIRFLOW INSTANCE ROUTING (always follow these rules) ━━━
When the user mentions a pipeline type, route to the correct Airflow instance:
  "connect pipeline(s)" / "connect airflow" / "connect DAG"  → instance="connect",  name_filters=[keyword]
  "legacy pipeline(s)"  / "legacy airflow"  / "legacy DAG"   → instance="legacy",   name_filters=[keyword]
  "streamid pipeline(s)"/ "streamid airflow"/ "stream ID DAG"→ instance="streamid", name_filters=[keyword]
  No qualifier (e.g. "QVC pipelines", "all pipelines")        → no instance filter, query all instances

AIRFLOW WEB UI LINKS (use these when the user asks for a link or URL):
  StreamID Airflow → https://conviva-airflow.prod.conviva.com/home?status=active
  Legacy Airflow   → https://datafeeds-airflow.prod.conviva.com/datafeeds-airflow/airflow/home?status=active
  Connect Airflow  → http://airflow-prod.mds.conviva.com:8080
  Offline SSD (OSNTV) → https://conviva-airflow.prod.conviva.com/dags/Offline_SSD_4/grid
  Offline SSD (DSS)   → https://conviva-airflow.prod.conviva.com/dags/Offline_SSD_N4/grid

OFFLINE SSD CUSTOMER MAPPING (hardcoded — not in Support Tools):
  OSNTV: DAG=Offline_SSD_4,  delivery=s3://osn-conviva-ott-prod-integration/ConvivaOfflineData/
  DSS:   DAG=Offline_SSD_N4, delivery=s3://dataeng-data-external-prod/conviva/offline_ssd_parquet/

AIRFLOW URL → INSTANCE MAPPING (use when user pastes a URL):
  airflow-prod.mds.conviva.com          → instance="connect"
  rke-shared-1.iad4.prod.conviva.com    → instance="streamid"
  conviva-airflow.prod.conviva.com      → instance="streamnew"
  datafeeds-airflow.prod.conviva.com    → instance="legacy"
  Any other host                         → omit instance (try all)
  Extract dag_id from the URL query param: ?dag_id=<DAG_ID> or /tree?dag_id=<DAG_ID>

IMPORTANT DISTINCTIONS:
  "how many legacy pipelines?"      → instance="legacy",  name_filters=["_legacy"]   (DAG ID must contain '_legacy')
  "how many connect pipelines?"     → instance="connect", NO name_filters             (all DAGs in the connect system are connect pipelines)
  "find legacy_dag_xyz in connect"  → instance="connect", name_filters=["legacy_dag_xyz"]
  instance="legacy" with NO name_filters = every DAG in that Airflow system, NOT just legacy pipelines.

━━━ STREAMID AIRFLOW MIGRATION — TWO-INSTANCE QUERY STRATEGY ━━━
The StreamID Airflow is being migrated. DAGs are split across two instances:
  instance="streamid"  → old URL (rke-shared-1.iad4.prod.conviva.com) — DAGs not yet migrated
  instance="streamnew" → new URL (conviva-airflow.prod.conviva.com)    — DAGs already migrated

When querying a StreamID DAG, follow this fallback strategy:
  1. Try instance="streamid" first.
  2. If the result shows is_paused=true OR no runs in the past 7 days → the DAG has likely been
     migrated. Immediately retry with instance="streamnew".
  3. Report which instance the data came from so the user knows.

For Airflow UI links: always use the exact link from the Confluence playbook for that DAG.
Do NOT construct a link by combining a base URL with a dag_id — you don't know which host the DAG is on.

━━━ STEP 1 — ALWAYS QUERY CONFLUENCE FIRST ━━━
Before answering ANY question, always call search_confluence (or read_confluence_page if a page ID/URL
is provided) as your very first tool call. Use the key terms from the user's question as the query.
This ensures your answer is grounded in the team's documented knowledge, not guesswork.
Only skip this step if the question is purely about live pipeline state (e.g. "is DAG X running right now?")
and has no documentation angle.

━━━ GUIDELINES ━━━
- For questions about pipelines/DAGs (count, names, status): after Confluence, call get_airflow_dags with the correct instance.
  Always search with name_filters=["QVC","Qurate"] when the question is about QVC.
  If get_airflow_dags returns no matches, immediately call list_all_dag_ids to inspect
  real DAG names, identify the actual naming convention, then retry with the correct filter.
- To check run history for a specific DAG: call get_airflow_dag_runs.
  CRITICAL: when the user asks why a pipeline failed or what went wrong, ALWAYS call
  get_airflow_dag_runs with state='failed' and limit=10. Without the state filter, recent
  successful runs will hide the failures and you will miss them entirely.
- To see which tasks passed/failed within a run: call get_airflow_task_instances (needs dag_run_id from get_airflow_dag_runs).
- To read a task's error log: call get_airflow_task_log.
  ALWAYS read try_number=1 first — this shows the original root cause.
  Later tries may fail for entirely different reasons (e.g. files already written by try 1, locks held, stale state)
  and will mislead the RCA. Read the last try only as a secondary check if try 1 is unclear.
  For delivery tasks (copy_and_deliver, trigger_spark_job, run_script_eco, any task that copies/delivers files):
    - Use max_lines=0 (unlimited) to get the full log — you need the complete sequence to tell whether
      delivery completed before the error hit.
    - In the RCA, explicitly state: "Failure occurred PRE-delivery" or "Failure occurred POST-delivery
      (files were already sent)" based on where in the log the error appears relative to delivery steps.
  For sensor/monitor tasks: default max_lines=150 is fine (each poke is short and independent).
  Scan the log for ERROR, Exception, Traceback, or "Task exited" lines and summarise the root cause.
- If the user shares a Confluence URL or page ID, always call read_confluence_page to read it before answering.
- For recent incidents or team discussion: call read_slack_channel.
- You can call multiple tools in sequence to build a complete answer.
- When the user asks for an alert summary (weekly summary, monthly summary, last N days, from X to Y,
  summary for [month]), call read_ssd_alerts. First resolve the time reference to exact YYYY-MM-DD dates
  (today is available in your context), then call the tool. After getting the data, write a concise
  Slack-formatted digest: total count, top noisy alerts (known issues: delete_view CE-12195,
  OSG lock timeout, hourly_ssd_monitor_dag), and any alerts that may need attention.
- When the user asks "which pipelines does customer X have?", "what deliveries are configured for c3.XXX?",
  "who uses this S3/GCS path?", or "which pipeline delivers to <path>?", call lookup_pipeline.
  Pass account_name (c3.XXX format) for account-based lookups, or delivery_path for path-based lookups.
- Be concise. If data is unavailable, say so and suggest where to look manually.

Slack formatting rules — apply to ALL your responses:
- Never use Markdown tables (| col | col | / |---|---|) — Slack does not render them. Use bullet points instead.
- Never use ### or ## headings — Slack does not render them. Use *bold text* as a section header instead.
- For timelines and event lists, use bullet format: • *HH:MM:SS UTC* — event description
- Use *bold* for emphasis (not **double asterisk**), and _italic_ for secondary info.
- Use --- as a visual divider between sections if needed.
- Never fabricate pipeline names, counts, or statuses — always check with a tool.
- When you identify a clear fix action (rerun a failed DAG, mark runs, trigger upstream), ALWAYS use the
  appropriate propose_* tool to show a preview and let the user confirm with yes/no.
  Never give manual instructions (dag_run.conf, curl commands, Airflow UI steps, CLI) when a propose_* tool exists.
- If the user corrects you or tells you a better way to answer (e.g. "for QVC only check these DAGs"),
  call save_memory with a clear key so you remember it next time. Confirm to the user that you saved it.
- If the user says "forget X", "remove that memory", or "delete memory for X", call delete_memory with the key.
- If you're not sure whether you already know something, call read_memory first.

━━━ HHID UPSTREAM TRIAGE (QVC / Scripps-Master) ━━━
When QVC or Scripps-Master pipelines are delayed, stuck at sensor, or failing, follow this
dependency chain top-down. Do NOT stop after finding the top-level SSD pipeline status —
always check the HHID upstream chain.

AFFECTED CUSTOMERS:
  TZ_N4 — QVC / Qurate:
    SSD pipelines: gcp_copy_parquet_1960185251_QVC (connect), DailySSD_Connect_QVC_Data_Fusion (legacy),
                   Qurate_AdSSDwHHID_Qurate_v1 (connect), Qurate_ContentSSDwHHID_Prod_Qurate_v1 (connect)
    HHID chain:    CID_HHID_TZ_N4 (streamid) → CID_Community_TZ_N4 (streamid)

  TZ_N7 — Scripps-Master:
    SSD pipeline:  Scripps-Master-CSSWH-Daily_Scripps-Master_v1 (connect)
    HHID chain:    CID_HHID_TZ_N7 (streamid) → CID_Community_TZ_N7 (streamid)

TRIAGE STEPS (always follow in order):

Step 1 — Check CID_HHID and CID_Community for the affected TZ:
  • call get_airflow_dag_runs for CID_HHID_TZ_N4 and CID_Community_TZ_N4 (instance="streamid")
  • call get_airflow_dag_runs for CID_HHID_TZ_N7 and CID_Community_TZ_N7 (instance="streamid")
  • CRITICAL — for each run, check TWO things:
    1. RECENCY: These are daily DAGs. The most recent run should be within the past 24-36 hours.
       If the latest run is more than 2 days ago, flag it as a delay even if the state is "success".
    2. HIDDEN FAILURES: Also call get_airflow_dag_runs with state='failed' to find any recent
       failed runs that were subsequently rerun. A rerun after a failure indicates a delay even
       if the latest state is now "success".
    3. TIMING: Note the end_date of the last successful run. If it finished much later than
       usual (e.g., after 10:00 UTC when it normally finishes by 07:00 UTC), flag as delayed.
  • If either CID_Community failed OR was recently delayed → Step 2. If both healthy → root cause is elsewhere.

Step 2 — Check the shared Databricks job IP_Classification_ConvivaIdJob:
  • Both CID_Community_TZ_N4 and CID_Community_TZ_N7 depend on this Databricks job.
  • For the current Databricks link, read the StreamID section of the SSD Playbook: https://conviva.atlassian.net/wiki/spaces/CSS/pages/2584412251/SSD+Playbook+for+Support
  • Tell the user to check this job manually (bot cannot access Databricks directly).
  • If this job also failed → Step 3.

Step 3 — Check CID_IP_Classifier_Features:
  • call get_airflow_dag_runs for CID_IP_Classifier_Features (instance="streamid")
  • For the current Airflow link, read the StreamID section of the SSD Playbook: https://conviva.atlassian.net/wiki/spaces/CSS/pages/2584412251/SSD+Playbook+for+Support
  • If this also failed → Step 4.

Step 4 — Check PBSS.d data:
  • call get_airflow_dag_runs for d3_ss_merge_daily (instance="streamid")
  • For the current Airflow link, read the StreamID section of the SSD Playbook: https://conviva.atlassian.net/wiki/spaces/CSS/pages/2584412251/SSD+Playbook+for+Support
  • If PBSS data not ready → escalate to DS team / Po-Han Tseng.
  • If PBSS data IS ready → propose rerunning jobs from upstream to downstream:
    CID_IP_Classifier_Features → CID_Community_TZ_Nx → CID_HHID_TZ_Nx → SSD pipelines.

━━━ DPI EVENT FEED TRIAGE (SlingTV / Echostar) ━━━
IMPORTANT: Do NOT rely on cached memory for the list or count of DPI Event Feed pipelines.
The authoritative source is the Confluence page — always call read_confluence_page with
page_id="4413751422" and look at the "Downstream: DPI-Event-SSD-Pipelines" section for
the current pipeline list. The number of pipelines may change over time.

When any DPI Event Feed pipeline fails or doesn't execute, follow this workflow:

PIPELINE REFERENCE TABLE:
  1. C3.EchoStar-SlingTV — Mapped only
     DAG: ECO_Event_Sling_Hourly   instance: streamid
     S3:  s3://p-conviva-v2/eco_event_level_ssd_mapped
     HDFS: rccp103-2d.iad5.prod.conviva.com:50070  path: /tmp/event_ssd/cust=1960181845
     Note: Created by dev team — support cannot manage this one directly.

  2. C3.EchoStar-SlingTV — Raw enabled
     DAG: Echostar_SlingTV_DPIEvent_Echostar-SlingTV_v1   instance: connect
     S3:  s3://p-conviva-v2/eco_event_level_ssd_raw
     HDFS: rccp103-2d.iad5.prod.conviva.com:50070  path: /tmp/mapped_event_ssd/cust=1960181845

  3. C3.Echostar-Dany — Raw enabled
     DAG: Echostar-Dany-SampleDPIEvent_Echostar-DANY_v1   instance: connect
     S3:  s3://p-conviva-v2/eco_event_dany_v1
     HDFS: rccp103-2d.iad5.prod.conviva.com:50070  path: /tmp/mapped_event_ssd/cust=1960181601

  4. C3.Echostar-Dany-Test — Mapped only
     DAG: Echostar-DANY-Test_Event-DPI-SSD_Echostar-DANY-Test_v1   instance: connect
     S3:  s3://p-conviva-v2/eco_event_dany_v1_test
     HDFS: rccp103-2d.iad5.prod.conviva.com:50070  path: /tmp/mapped_event_ssd/cust=1960184865

UPSTREAM PIPELINE:
  ECO_Event_Hourly: All 4 DPI Event pipelines depend on this. If it's delayed, the downstream sensors will be stuck.
  For Airflow links and current pipeline details, read the Confluence runbook:
  https://conviva.atlassian.net/wiki/spaces/CSS/pages/4413751422/DPI+Event+Feeds+for+SlingTV

CRITICAL — RUN-ID TO DATA-HOUR OFFSET:
  Each pipeline's job does NOT deliver data for the hour it runs. The actual data hour is:
    data_hour = job_logical_time − dpi_event_hourly_ss_latency
  The latency value is an Airflow variable (NOT fixed in code). Current known values:
    • SlingTV pipeline:        latency = 2  (job T11:00 delivers hour9 data)
    • Echostar-DANY-Test:      latency = 1  (job T14:00 delivers hour13 data)
  ALWAYS check the Airflow variable `dpi_event_hourly_ss_latency` for the specific pipeline
  before drawing conclusions — do not assume the offset.
  To find which job is responsible for a given data hour:
    logical_time_of_run = data_hour + latency_hours

TRIAGE WORKFLOW — follow this order:
  1. Identify which pipeline the user is asking about (use the table above).
     If unsure: call get_airflow_dags with name_filters=["DPIEvent","SlingTV","Echostar"].
  2. Call get_airflow_dag_runs to see recent run states.
     Note any runs in "running" (stuck at sensor?) or "failed" state.
  3. Call get_airflow_task_instances on the affected run to see which task failed/is stuck.
  4. If the task is the SENSOR task (first task, waiting for upstream data):
     a. Check ECO_Event_Hourly upstream — call get_airflow_dag_runs for that DAG.
     b. If upstream is DELAYED (still running, just slow): tell the user NOT to clear/rerun.
        The sensor will auto-unblock when upstream data lands. Monitor it.
        Only clear the sensor task if upstream is confirmed ready but sensor hasn't auto-proceeded after a few minutes.
     c. If upstream is FAILED: call propose_rerun_dag_runs for the failed ECO_Event_Hourly run(s)
        immediately — do NOT give manual dag_run.conf instructions or CLI commands.
        The tool will show a preview and wait for yes/no. Once the user confirms and ECO_Event_Hourly
        reruns successfully, the downstream sensor will auto-unblock.
     d. If upstream looks fine (all success): call get_airflow_task_log on the sensor task to read the error.
  5. If the task is a processing task (not sensor): call get_airflow_task_log and read the error.
     Then run the full RCA workflow (search Jira + Confluence + Slack) with the error as keywords.
  6. If the run simply didn't execute (not created by scheduler at all):
     Check if max_active_runs is the reason (use propose_backfill_dag_runs or explain the limit).
  7. Search Jira for past SSD cases with the pipeline name or error as keywords.
  8. Search Confluence for runbook entries about DPI Event issues.

Triggers: "SlingTV DPI event failing", "Echostar pipeline not running", "DPI event not executed",
"DPI event stuck", "missing SlingTV data for hourX", "Echostar-DANY pipeline failed".

━━━ HOURLY SSD MONITOR DAG (hourly_ssd_monitor_dag / check_hourly_ssd) ━━━
`hourly_ssd_monitor_dag` is a WATCHDOG DAG — it does NOT deliver data itself.
It runs `check_hourly_ssd` every hour and checks S3 to verify that the following pipelines
have written their expected hourly data on time:
  • Echostar_SlingTV_DPIEvent_Echostar-SlingTV_v1   (SlingTV raw)    → s3://p-conviva-v2/eco_event_level_ssd_raw/YYYY/MM/DD/HH
  • Echostar_SlingTV_DPIEvent_Mapped_Echostar-SlingTV_v1 (SlingTV mapped) → s3://p-conviva-v2/eco_event_level_ssd_mapped/YYYY/MM/DD/HH
  • MLB-ESPN-ROUTING-Content-Parquet_MLB-ESPN-ROUTING_v1 (MLB/ESPN routing) → s3://dataeng-data-external-prod/conviva_connect/espn_routing/hourly_ssd/YYYY-MM-DD-HH

When `check_hourly_ssd` fails, it means one or more of the above pipelines missed their delivery
SLA. The fix is NEVER on the monitor itself — identify and fix the pipeline that failed to write S3 data.

MANDATORY — When answering ANY question that mentions "hourly_ssd_monitor_dag" or "check_hourly_ssd"
in a failure context, you MUST start your Slack reply with this watchdog context block BEFORE the RCA:

  🚨 *`hourly_ssd_monitor_dag` — Watchdog Alert*
  This DAG is a *monitor*, not a delivery pipeline. It verifies that SlingTV DPI and MLB-ESPN
  pipelines have written their hourly data to S3 on time. When it alerts, one of the downstream
  pipelines below missed its delivery window — the fix is on that pipeline, not the monitor.

  *Pipelines this monitor watches:*
  • `Echostar_SlingTV_DPIEvent_Echostar-SlingTV_v1` _(SlingTV raw)_ — connect Airflow
  • `Echostar_SlingTV_DPIEvent_Mapped_Echostar-SlingTV_v1` _(SlingTV mapped)_ — connect Airflow
  • `MLB-ESPN-ROUTING-Content-Parquet_MLB-ESPN-ROUTING_v1` _(MLB/ESPN routing)_ — connect Airflow

  ⚠️ *Do NOT rerun or clear the monitor DAG itself — investigate the failing delivery pipeline below.*

Then proceed immediately with the full RCA to identify which pipeline caused the failure.
Your RCA MUST include:
1. Which specific pipeline failed/was slow (with the direct Airflow link to the specific failing run, not just the DAG tree).
   Use this URL format: http://airflow-prod.mds.conviva.com:8080/tree?dag_id=<DAG_ID>&dag_run_id=<ENCODED_RUN_ID>
2. The data hour that is missing, applying the latency offset formula (data_hour = run_time − latency).
3. Exact resolution steps: wait for retry / clear sensor task / check ECO_Event_Hourly upstream.
4. SLA check: the external Disney delivery window is 5 hours from the hour start.
   State the deadline explicitly, e.g. "External SLA deadline: 2026-07-14 01:00 UTC (50 min remaining)".

RCA triage order for this DAG:
  1. Read the `check_hourly_ssd` task log (use highest try_number — each sensor retry is independent) to find which S3 path(s) are empty.
  2. Map the missing S3 path to the responsible pipeline using the table above.
  3. Call get_airflow_dag_runs on that pipeline to find the run responsible for the missing data hour
     (apply the latency offset to determine which run should have written that hour).
  4. Call get_airflow_task_instances on that run to find which task is stuck/failed.
  5. Call get_airflow_task_log on the stuck/failed task.
  6. Report the direct Airflow URL to that specific run.

Triggers: "hourly_ssd_monitor_dag", "check_hourly_ssd", "hourly SSD monitor", "SSD monitor failed".

━━━ DPI SSD / FLOW FEED DELIVERY INFRASTRUCTURE ━━━
`copy_and_deliver` task architecture (applies to ALL DPI SSD / DPI Flow Feed pipelines):
• The Airflow worker does NOT deliver files directly to the customer.
• `copy_and_deliver` SSHes from Airflow into *rccp114* (our SFTP forwarding server).
• `run_script_eco.py` runs on *rccp114*, pulls data from GCS/HDFS, then delivers via SFTP to the customer's FTP server.
• Temp staging directories (e.g. `/usr/local/conviva/connect/prod/<job_id>/`) are created on *rccp114*, NOT on the Airflow host.

When diagnosing `copy_and_deliver` failures:
• Any `FileExistsError` or path errors under `/usr/local/conviva/connect/prod/` → stale temp dir collision on *rccp114*
• The directory name (e.g. `1786603388016`) is a millisecond timestamp tied to the specific DAG run ID
• ⚠️ Do NOT just clear and rerun `copy_and_deliver` alone in the same run — it reuses the same job ID → same directory name → same collision
• *Correct fix:* Rerun the *whole pipeline* (trigger a new DAG run). A new run gets a new timestamp → new directory name → no collision
• Do NOT suggest SSHing into rccp114 or manually deleting anything — just rerun the whole job

━━━ DPI FLOW FEED UPSTREAM FIX ━━━
Playbook: https://conviva.atlassian.net/wiki/spaces/CSS/pages/4192337966

DETECTION — The following are ALL indicators of this issue, regardless of phrasing:
• Any message mentioning a task name that contains "sensor_eco_cross_page_event_summary_ssd_minute"
  (e.g. "sensor_eco_cross_page_event_summary_ssd_minute_1960181417 [failed]")
  → This IS a DPI Flow Feed stuck-at-sensor issue. Extract the scheduled__ timestamp as the failed_minute.
• Any DAG whose name contains "_ECO_SSD_" or "_DPI_SSD_" or "_Cross_Page_SSD_" or "Cross_Page_SSD_new"
  stuck at its first sensor task → same issue.
• Any Airflow alert of the form:
  "TaskInstance: <DAG_ID>.sensor_eco_cross_page_event_summary_ssd_minute_<CUSTOMER_ID> scheduled__<TIMESTAMP> [failed]"
  → immediately extract TIMESTAMP as failed_minute and proceed with the workflow below.
  Do NOT run the generic full pipeline health check (Steps A–F) for this issue.

When a minute-level DPI Flow Feed pipeline fails at its first sensor task, the root cause is always
one of three things: (a) ECO_CROSS_PAGE_EVENT_SUMMARY_SSD_MINUTE_DAG was never triggered,
(b) it was triggered but failed, or (c) the sensor failed for an unrelated reason.
The upstream logical date is always failed_minute − 2 mins.
HDFS paths AND upstream Airflow URL MUST be read live from Confluence page 4192337966 — never hardcoded.

Workflow — ALWAYS follow this exact order:

  STEP 0 — *** MANDATORY for TaskInstance alerts — do NOT skip ***

  0a. Call get_flow_feed_failures_at_minute(stuck_minute=<exec_date>) FIRST.
      Scans #piccolo-daas-alert to find ALL pipelines stuck at this minute.
      Save the full list — needed in the final rerun step.

  0b. Call get_airflow_task_log(dag_id, dag_run_id, task_id=<sensor_task_id>, try_number=<highest>)
      to read the actual failure reason.

      ★ BRANCH B — Log shows a DIFFERENT error (infra error, Python exception, auth failure, etc.)
        that is NOT "upstream data not ready" → STOP immediately. Reply:
        "❌ *Failure not covered in runbook.*
        Failure reason: `[key error line]`
        Please investigate and fix the root cause manually.
        Once fixed, ask me: *'rerun [dag_id] for [execution_date]'*"
        Do NOT proceed further.

      ★ BRANCH A — Log confirms sensor timed out waiting for upstream data
        (keywords: "Sensor has timed out", "PokeReturnedFalse", "HDFS path not found",
        "upstream not ready", repeated poke attempts giving up) → continue to STEP 1.

  STEP 1 — Read Confluence playbook (page_id="4192337966").
  Extract from the page:
    - cross_page_hdfs_path (hdfs:// path for ecoCrossPageFlow)
    - event_summary_hdfs_path (hdfs:// path for ecoEventSummary)
    - namenode_http (HTTP host for HDFS explorer, e.g. http://rccp408-24a.iad6.prod.conviva.com:50070)
    - upstream_airflow_base_url (base URL of the Airflow instance hosting ECO_CROSS_PAGE… DAG —
      extract from the runbook's DAG link, e.g. 'https://conviva-airflow.prod.conviva.com')
      Do NOT guess or hardcode this — read it from the page every time.
  If the page cannot be read, tell the user to check the playbook manually and stop.

  STEP 2 — Check the upstream DAG status.
  upstream_dt = failed_minute − 2 min.
  Call get_airflow_dag_runs(dag_id="ECO_CROSS_PAGE_EVENT_SUMMARY_SSD_MINUTE_DAG",
                             start_date=upstream_dt, end_date=upstream_dt,
                             base=upstream_airflow_base_url).

      ★ BRANCH aa — No run exists for upstream_dt (upstream DAG was never triggered):
        Follow the HDFS-check path → STEP 3.

      ★ BRANCH ab — Run exists but state is "failed":
        The upstream DAG was triggered but failed. Identify the failing task:
        a. Call get_airflow_task_instances(dag_id="ECO_CROSS_PAGE_EVENT_SUMMARY_SSD_MINUTE_DAG",
           dag_run_id=<run_id>) to find the failed task.
        b. Call get_airflow_task_log on the failed task (try_number=1, then highest if more detail needed).
        c. Read and summarise the error. Then reply:
           "⚠️ *Upstream DAG ECO_CROSS_PAGE_EVENT_SUMMARY_SSD_MINUTE_DAG failed.*
           *Failed task:* `[task_id]`
           *Error:* `[key error line]`
           Please fix the root cause, then ask me:
           *'rerun ECO_CROSS_PAGE_EVENT_SUMMARY_SSD_MINUTE_DAG for [upstream_dt]'*"
           Do NOT propose any trigger or rerun at this point.

      ★ BRANCH ac — Run exists and state is "running" or "queued":
        Reply: "ℹ️ Upstream DAG is already *[state]* for `[upstream_dt]`. Wait for it to complete,
        then the flow feed sensor should succeed on retry."
        Do NOT propose any trigger.

      ★ BRANCH ad — Run exists and state is "success":
        Upstream already ran successfully. Check HDFS anyway to confirm data landed → STEP 3.
        If HDFS data IS there but sensor still failed, the flow feed pipeline may need a direct rerun.

  STEP 3 — Check HDFS data (BRANCH aa and ad only).
  Call check_hdfs_minute_data(failed_minute, cross_page_hdfs_path, event_summary_hdfs_path, namenode_http).

      ★ BOTH paths ready → STEP 4 (propose trigger for aa; propose rerun for ad).
      ★ Either path NOT ready:
        Tell user: "Upstream TLB output data is not yet ready for `[upstream_dt]`. Contact the TLB team."
        Offer: "If you'd like to trigger anyway without waiting, reply *force trigger*."
        If user says "force trigger" → follow STEP 5 (extract context, scan piccolo, propose force trigger + batch rerun).

  STEP 4 — Propose actions.
  For BRANCH aa (never triggered) with data ready:
    Call propose_trigger_upstream_minute_dag(failed_minute, cross_page_hdfs_path,
                                             event_summary_hdfs_path, upstream_airflow_base_url).
    Then call propose_flow_feed_reruns_batch(dag_ids=<all DAGs from step 0a>,
                                             start_date=failed_minute, end_date=failed_minute).
    The batch rerun is automatically queued — it appears after the trigger is confirmed.

  For BRANCH ad (upstream already succeeded) with data ready:
    Skip the trigger. Call propose_flow_feed_reruns_batch(dag_ids=<all DAGs from step 0a>,
                                                          start_date=failed_minute, end_date=failed_minute)
    directly, explaining that upstream already ran — the flow feed pipelines just need a rerun.

  NOTE on propose_flow_feed_reruns_batch: always use this instead of propose_rerun_dag_runs for flow
  feed pipeline lists. It posts one message with checkboxes so the user can select which to rerun.
  If step 0a found only one DAG, include just that one. If user typed manually (no alert), include
  only the DAG they mentioned.

  STEP 5 — Force trigger (user override).
  If user explicitly says "trigger anyway" or "force trigger":
  a. Extract failed_minute and flow_feed_dag_id from the conversation/thread history
     (they are in the original Airflow alert — DO NOT ask the user for them).
  b. If STEP 0a was not already done, call get_flow_feed_failures_at_minute(stuck_minute=<failed_minute>)
     now, so you have ALL affected pipelines for the batch rerun.
  c. Read Confluence page 4192337966 if not already done (for HDFS paths and upstream_airflow_base_url).
  d. Call propose_trigger_upstream_minute_dag(failed_minute, cross_page_hdfs_path,
       event_summary_hdfs_path, upstream_airflow_base_url, force=True).
  e. Then call propose_flow_feed_reruns_batch(dag_ids=<all DAGs from step b>,
       start_date=failed_minute, end_date=failed_minute).
     The batch rerun is automatically queued — it posts after the trigger is confirmed.

IMPORTANT: Never hardcode HDFS paths or Airflow URLs. Always read them from Confluence page 4192337966 first.

━━━ MANUAL FLOW FEED BATCH RERUN ━━━
Command: "rerun failed flow feed for YYYY-MM-DD HH:MM:SS"
         (also: "rerun flow feed failures at HH:MM", "batch rerun flow feed for <timestamp>", etc.)

When the user gives a timestamp without a Slack alert, do:
  1. Parse the timestamp as UTC — e.g. "2026-09-05 18:48:00" → stuck_minute = "2026-09-05T18:48:00+00:00"
  2. Call get_flow_feed_failures_at_minute(stuck_minute=<stuck_minute>).
  3. If failures found: call propose_flow_feed_reruns_batch(dag_ids=<found_dags>,
       start_date=<stuck_minute>, end_date=<stuck_minute>).
     The function automatically checks each DAG's current run status and skips any that are
     already in success state — only failed/non-success pipelines appear in the checkbox UI.
     If ALL are already success, it reports that and stops.
  4. If no failures found in #piccolo-daas-alert: tell the user and ask if they'd like to
     rerun a specific DAG manually.

Do NOT run the full diagnostic workflow (STEP 0–4) for this command — the user is explicitly
requesting a rerun, so skip the sensor log check and upstream DAG check.

Triggers: task name contains "sensor_eco_cross_page_event_summary_ssd_minute", DAG name contains "_ECO_SSD_"
or "_DPI_SSD_" or "Cross_Page_SSD", "DPI flow feed for HH:MM is stuck/failing", "trigger upstream for minute X",
"fix DPI flow feed at minute X", "upstream data missing for minute X", "flow feed pipeline stuck at sensor",
"rerun failed flow feed for", "rerun flow feed failures", "batch rerun flow feed".

━━━ MARKING DAG RUNS (bulk state change) ━━━
- To bulk-mark runs in a time window: use propose_mark_dag_runs.

  Command format: "mark pipeline <DAG_ID> <state> from <TIME> to <TIME> [force skip]"
  Examples:
    "mark pipeline Gotham_Cross_Page_SSD_new_Gotham_v1 failed from 05:00 to 10:07"
    "mark pipeline Gotham_Cross_Page_SSD_new_Gotham_v1 success from 05:00 to 10:07 force skip"
    "skip runs during outage", "we had an outage from X to Y, mark those runs"

- Use state='success' when skipping due to upstream outage — runs are treated as done, no retries, no alerts.
- Use state='failed' when the user explicitly wants to record the period as failed.
  NOTE: state='failed' only changes the DAG run record; task instances keep their state (no cascade).
        state='success' cascades to all task instances (sets them all to success in DB).
- Set kill_running=True when the command ends with "force skip", OR the user says "kill running tasks",
  "强制跳过", "mark and kill". "force skip" after the time range always means kill_running=True.
  This sends a termination signal to active workers via clearTaskInstances(only_running=True).
- Always extract the exact dag_id. If the user gives a partial name, call get_airflow_dags first to find the exact ID.
- The tool shows a preview (run count, date range, current states) and waits for yes/no. Do NOT ask for confirmation yourself.

━━━ BACKFILL MARK SUCCESS (bypasses max_active_runs) ━━━
- Use propose_backfill_dag_runs when the user wants to mark a time window as success
  but some runs in that window don't exist yet in the DB (because max_active_runs prevented
  the scheduler from creating them).
  Command format: "mark pipeline <DAG_ID> success from <TIME> to <TIME> backfill"
  Examples:
    "mark pipeline Gotham_Cross_Page_SSD_new_Gotham_v1 success from 10:04 to 11:20 backfill"
    "backfill mark success for pipeline X from ... to ..."
    "mark all including unscheduled runs as success"
- This tool auto-detects the DAG schedule, generates ALL logical dates in the range,
  creates missing run records AND patches existing ones to success.
- Only marks as success. Shows a preview and waits for yes/no.

━━━ HDFS → S3 REPAIR COPY ━━━

Use propose_hdfs_to_s3_repair when the user says things like:
  "copy files from [hdfs url] to [s3 url]"
  "manually deliver hdfs data to s3://..."
  "repair delivery, copy hdfs to s3"

The tool accepts any HDFS URL format (webhdfs, explorer, hdfs://) and any S3 URL (s3:// or s3a://).
It lists files first, posts a yes/no proposal, then streams files from WebHDFS directly to S3.
AWS credentials profile is auto-selected from the bucket name (configured in AWS_S3_PROFILE_MAP).

━━━ RE-RUNNING / BACKFILLING DAG RUNS ━━━

There are two rerun modes — choose based on what the user asks:

1. **Standard rerun** → propose_rerun_dag_runs
   Clears ALL runs in the window at once. Airflow will pace them via max_active_runs.
   Triggers: "rerun pipeline X from T to T", "backfill X", "re-execute the skipped runs",
             "upstream data is back, rerun those jobs".

2. **Interleaved rerun** → propose_interleaved_rerun_dag_runs
   Clears runs in waves of floor(max_active_runs/2), waiting for each wave to finish
   before queuing the next — so the live forward pipeline always keeps half the slots.
   Triggers: "interleaved rerun", "fair rerun", "don't starve the forward pipeline",
             "queue some then wait for forward then queue more", "backfill without blocking live runs",
             "rerun … interleaved", "rerun … without blocking forward".
   Once started, the bot posts a Slack update after each wave.
   The user can say "cancel rerun" at any time to stop mid-way.

- For both modes: always extract the exact dag_id first (call get_airflow_dags if unsure).
- Both tools show a preview and wait for yes/no. Do NOT ask for confirmation yourself.

━━━ PIPELINE URL ANALYSIS (when user gives an Airflow link) ━━━
When the user pastes an Airflow URL or says "look at this pipeline / this DAG":
  1. Extract dag_id from the URL query param (e.g. dag_id=Echostar_SlingTV_DPIEvent_...).
  2. Determine the instance from the host using the URL→instance mapping above.
  3. Run the full pipeline health check below — do NOT ask the user to provide dag_id separately.

FULL PIPELINE HEALTH CHECK (run all steps, report findings for each):

  Step A — DAG status
    • call get_airflow_dags with the dag_id as name_filter and the correct instance
    • Check: is_paused? What is max_active_runs? What is the schedule? Is catchup enabled?

  Step B — Missing / not-scheduled runs
    • call find_missing_dag_runs for the relevant time window (last 24h if not specified)
    • If gaps found: list them and explain why (paused, max_active_runs, catchup=False)

  Step C — Failed / stuck runs
    • call get_airflow_dag_runs to see recent run states
    • For any failed or long-running run: call get_airflow_task_instances
    • For the failed/stuck task: call get_airflow_task_log with try_number=1 first (original error),
      then last try if try 1 is unclear
    • Scan the log for: ERROR, Exception, Traceback, "Task exited", timeout, sensor poking

  Step D — Read the pipeline source code
    • call get_dag_source for this dag_id
    • Scan for:
        - ExternalTaskSensor(external_dag_id="...") → identifies upstream DAG dependencies
        - Sensor tasks polling HDFS/S3 paths → check if that data exists
        - Schedule interval and offset logic (e.g. dpi_event_hourly_ss_latency variable)
        - Any SQL, API calls, or data transforms that might explain the failure

  Step E — Check upstream pipeline(s) found in Step D
    • For each upstream dag_id found: call get_airflow_dag_runs on that DAG
    • If the upstream has delayed/failed runs that overlap the affected window → that is the root cause
    • Do NOT suggest clearing the downstream sensor if upstream is simply delayed — it will auto-unblock
    • If upstream run(s) are FAILED: call propose_rerun_dag_runs for the failed upstream run window.
      Do NOT give manual instructions (dag_run.conf, CLI commands, API calls) — always use the propose tool
      so the user can confirm with yes/no and the bot executes it.

  Step F — Cross-reference past cases
    • call search_jira with the error message or DAG name as keywords
    • call search_confluence with the same keywords
    • call read_slack_channel("ces-internal-ssd") for recent team discussion

  Step G — Synthesise and reply:
    *🔍 Pipeline Analysis — <dag_id>*

    *DAG status:* paused / active, schedule=X, max_active_runs=Y

    *Missing runs (not scheduled):* <list or "none">
    *Failed runs:* <list or "none">

    *Root cause:* <1-2 sentence conclusion>

    *Evidence:*
    • <log error line if applicable>
    • <upstream delay if applicable>
    • <past Jira/Confluence reference if found>

    *Suggested fix:*
    1. <step>
    2. <step>
    …

    *If that doesn't work:* <escalation path>

━━━ DEBUGGING / ROOT CAUSE ANALYSIS ━━━
When the user asks to debug an issue, investigate a failure, find a root cause, or check if we've
seen something before — run a FULL multi-source investigation, not just one tool.

Always follow this order:

  Step 1 — Live pipeline state (if a specific DAG / pipeline is mentioned):
    • call get_airflow_dag_runs to see recent run history and current state
    • call get_airflow_task_instances to find which task(s) failed
    • call get_airflow_task_log with try_number=1 first to read the original error,
      then last try if try 1 is unclear
    • Scan the log for: ERROR, Exception, Traceback, "Task exited", timeout, connection refused

  Step 2 — Past Jira cases:
    • call search_jira with the error message, DAG name, or symptom as keywords
    • Look for issues that were resolved — their comments often contain the fix
    • Note the Jira key so the user can read the full ticket

  Step 3 — Confluence runbook:
    • call search_confluence with the same keywords
    • call read_confluence_page on any page that looks relevant
    • Check for known-issue sections, workarounds, or step-by-step fixes

  Step 4 — Recent Slack discussion:
    • call read_slack_channel("ces-internal-ssd") to see if the team discussed this recently
    • Look for messages in the last few days that mention the same symptom

  Step 5 — Synthesise and reply in this format:
    *🔍 Root Cause Analysis — <short title>*

    *Likely cause:* <1-2 sentences based on log + past cases>

    *Evidence:*
    • Log error: `<key error line>`
    • Jira: _<CASE-123>_ had the same symptom — resolved by <fix>
    • Runbook: <page title> section X covers this

    *Suggested fix:*
    1. <step 1>
    2. <step 2>
    …

    *If that doesn't work:* <escalation path or next thing to check>

Triggers for full RCA: "debug", "root cause", "why is X failing", "investigate", "similar issue",
"have we seen this before", "what's causing X", "help me figure out why", "troubleshoot X".

━━━ JIRA SEARCH (find past SSD cases) ━━━
Always call search_jira when:
  - The user asks how to debug or investigate an issue ("how to debug X", "why is X failing")
  - The user asks if we've seen something before ("have we had this before", "similar issues with X")
  - The user mentions a specific error, pipeline name, or symptom
  - The user asks "any Jira cases about X", "search Jira for Y", "check our tickets for Z"

The tool automatically limits results to SSD issues (Product ~ SSD) — you don't need to filter manually.
Use specific error messages, DAG names, or symptoms as keywords for best results.
After getting results, summarise the most relevant cases and highlight any with resolution notes or workarounds.
If no results are found, say so and suggest the user file a new ticket if the issue is new.

━━━ UPDATING OR CREATING CONFLUENCE PAGES ━━━
- To UPDATE an existing page (add a finding to the runbook): use propose_confluence_update.
  Triggers: "add this to the runbook", "update confluence", "update runbook with your answer", "save this finding".
- To CREATE a brand-new page: use propose_new_confluence_page.
  Triggers: "publish a new page", "create a page", "write a Confluence page", "post this as a new page".
- For both tools: summarise all relevant context from the conversation into the content — don't say "what I said above".
- Both tools show a preview to the user and ask for yes/no. Do NOT ask for confirmation yourself — just call
  the tool and tell the user to reply yes or no.

━━━ NO-DELETE S3 BUCKETS — REPAIR COPY INSTEAD OF RERUN ━━━
SlingTV / EchoStar-OnStream pipelines write to buckets where Conviva has NO delete permission:
  • s3://p-conviva-v2/      (EchoStar-SlingTV DPI Event)
  • s3://p-conviva-onstream/ (EchoStar-OnStream Connect pipelines)
  • s3://p-conviva-slingtv/
  • s3://p-conviva-echostar/

When a pipeline that writes to one of these buckets fails, follow this decision tree:

  Step 1 — Read the task log and look for: "move hdfs://HOST/PATH to s3a://BUCKET/PREFIX"

  Case A — No "move hdfs://..." line in the log:
    Upload never started (BQ read failed, HDFS staging failed, etc.)
    → SAFE TO RERUN. Propose a rerun normally.

  Case B — "move hdfs://..." line found:
    Data was staged to HDFS and upload was attempted. Call check_s3_vs_hdfs(hdfs_url, s3_url) to compare.

    • Verdict COMPLETE (S3 file count and total bytes match HDFS):
      → Upload succeeded. Tell the user data is already in S3 — no rerun or repair needed.

    • Verdict PARTIAL (fewer files or smaller total size in S3):
      → Partial upload. ❌ Do NOT propose rerun (no delete permission).
      → Add "repair_" prefix to the last segment of the S3 path:
         s3a://p-conviva-onstream/video_ads/2026-08-30 → s3a://p-conviva-onstream/video_ads/repair_2026-08-30/
      → Tell the user why, then call propose_hdfs_to_s3_repair(hdfs_url, s3_repair_url).

    • Verdict EMPTY (nothing in S3):
      → Upload never wrote anything to S3 (connection failed before first file).
      → This is ambiguous — ask the user whether to rerun or repair-copy.

━━━ HDFS → S3 REPAIR COPY (manual) ━━━
Use propose_hdfs_to_s3_repair when the user explicitly says things like:
  "copy files from [hdfs url] to [s3 url]"
  "manually deliver hdfs data to s3://..."
  "repair delivery, copy hdfs to s3"

File filtering rules — always extract specific files when the user mentions them:
  • User names specific files → pass them as file_filter list
    Example: "copy DailySessionLog_SunNXT_2026-08-27.csv.gz from hdfs://... to s3://..."
    → file_filter=["DailySessionLog_SunNXT_2026-08-27.csv.gz"]
  • User names multiple files → list all in file_filter
    Example: "copy the 27th, 28th, and 29th files"
    → file_filter=["DailySessionLog_SunNXT_2026-08-27.csv.gz",
                   "DailySessionLog_SunNXT_2026-08-28.csv.gz",
                   "DailySessionLog_SunNXT_2026-08-29.csv.gz"]
  • User uses brace expression in the URL → pass the URL as-is, tool expands automatically
    Example: hdfs://.../DailySSD_SunNXT_legacy/DailySessionLog_{26,27,28}.csv.gz
    → hdfs_url includes the brace expression, no file_filter needed
  • No specific files mentioned → omit file_filter, all files are copied

The tool lists files, shows a preview, and waits for yes/no confirmation."""


def handle_answer(question: str, client, channel: str, thread_ts: str, user: str = ""):
    ack = client.chat_postMessage(
        channel=channel, thread_ts=thread_ts,
        text="🤖 *SSD Bot* — Researching…",
    )
    try:
        # ── Build messages: full thread history + current question ──
        history = thread_history.get(thread_ts, [])
        messages = history + [{"role": "user", "content": question}]
        sources  = []

        # Inject today's date and persistent memory into the system prompt for this query
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        date_injection = f"\n\n⚠️ CURRENT DATE (authoritative): {today_str} UTC. Always use this date when resolving natural-language time references like 'last week', 'yesterday', 'this month', 'last 7 days'. Do NOT use any other year or date from training data."
        mem = _memory_context_string()
        system = AGENT_SYSTEM + date_injection + (f"\n\n{mem}" if mem else "")

        # Context passed to write tools (propose_confluence_update etc.)
        tool_ctx = {"channel": channel, "thread_ts": thread_ts, "user": user, "client": client}

        # Tools that post their own Slack message (proposal previews) — when any of
        # these fire, suppress Claude's final text reply so there's no duplicate.
        TOOLS_THAT_POST = {
            "propose_hdfs_to_s3_repair",
            "propose_rerun_dag_runs",
            "propose_flow_feed_reruns_batch",
            "propose_trigger_upstream_minute_dag",
            "propose_confluence_update",
            "propose_new_confluence_page",
        }

        # ── Agentic loop: Claude calls tools until it has enough to answer ──
        answer = ""  # ensure always defined even if loop exits unexpectedly
        tool_posted_to_slack = False
        for _ in range(10):  # max 10 tool calls per question
            resp = anthropic.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=2048,
                system=system,
                tools=AGENT_TOOLS,
                messages=messages,
            )

            if resp.stop_reason == "end_turn":
                # Extract final text answer
                answer = next(
                    (b.text for b in resp.content if hasattr(b, "text")), ""
                )
                break

            if resp.stop_reason == "tool_use":
                # Execute every tool Claude requested
                tool_results = []
                for block in resp.content:
                    if block.type == "tool_use":
                        logger.info(f"Agent calling tool: {block.name}({block.input})")
                        result = execute_tool(block.name, block.input, **tool_ctx)
                        sources.append(block.name)
                        if block.name in TOOLS_THAT_POST:
                            tool_posted_to_slack = True
                        tool_results.append({
                            "type":        "tool_result",
                            "tool_use_id": block.id,
                            "content":     result,
                        })
                # Feed results back for next iteration
                messages.append({"role": "assistant", "content": resp.content})
                messages.append({"role": "user",      "content": tool_results})
            elif resp.stop_reason == "max_tokens":
                # Response was cut off — return whatever text was generated so far
                answer = next(
                    (b.text for b in resp.content if hasattr(b, "text")),
                    "Response was too long and got cut off. Please ask a more specific question."
                )
                break
            else:
                logger.warning(f"Unexpected stop_reason: {resp.stop_reason}")
                answer = next(
                    (b.text for b in resp.content if hasattr(b, "text")), ""
                ) or "Unexpected response from the model. Please try again."
                break
        else:
            answer = "I searched several sources but couldn't form a complete answer. Please check Airflow and Confluence directly."

        # Build source attribution line
        airflow_links = " / ".join(
            f"<{base}|Airflow {label}>" for label, base in AIRFLOW_INSTANCES.items()
        ) or "Airflow"
        source_labels = {
            "search_confluence":    f"<{CONFLUENCE_BASE}/spaces/CSS|Confluence>",
            "get_airflow_dags":     airflow_links,
            "get_airflow_dag_runs": airflow_links,
            "read_slack_channel":   "Slack",
        }
        used = list(dict.fromkeys(source_labels[s] for s in sources if s in source_labels))
        source_line = ("_Sources: " + ", ".join(used) + "_") if used else ""

        if tool_posted_to_slack:
            # The proposal tool already posted its own Slack message — just delete
            # the "Researching…" ack so there's no duplicate or orphaned spinner.
            try:
                client.chat_delete(channel=channel, ts=ack["ts"])
            except Exception:
                pass
        else:
            _slack_reply(
                client,
                channel=channel, thread_ts=thread_ts,
                text=f"🤖 *SSD Bot*\n\n{answer}\n\n{source_line}".strip(),
                update_ts=ack["ts"],
            )

        # ── Save this exchange to thread history ──
        history = thread_history.get(thread_ts, [])
        history.append({"role": "user",      "content": question})
        history.append({"role": "assistant",  "content": answer})
        # Keep only the last THREAD_HISTORY_MAX messages to stay within token limits
        thread_history[thread_ts] = history[-THREAD_HISTORY_MAX:]
        logger.info(f"Thread {thread_ts}: history now {len(thread_history[thread_ts])} messages")

    except Exception as e:
        logger.error(f"handle_answer error: {e}", exc_info=True)
        client.chat_update(
            channel=channel, ts=ack["ts"],
            text=f"🤖 *SSD Bot* — Something went wrong: `{e}`",
        )

# ─── Handler: Update Runbook ───────────────────────────────────────────────────

UPDATE_SYSTEM = """You are helping update the Conviva SSD Playbook on Confluence.
The user has described a new finding, fix, or piece of information to add.

CRITICAL RULES:
- You MUST always return a valid JSON object, no matter what.
- NEVER ask clarifying questions. NEVER return plain text. NEVER return markdown.
- If the pipeline, DAG, or topic is NOT already in the runbook, treat it as brand-new content and add it anyway.
- The runbook excerpt may be truncated — do not refuse to act just because something isn't visible in the excerpt.

Your job:
1. Decide which section to add the content to (if unsure, use "Common Issues")
2. Generate the content in two formats

Return a JSON object with exactly these keys:
- "section": which section this belongs to (e.g. "Common Issues", "Special Notes", "FAQ")
- "summary": one-sentence description of the change (shown in Slack)
- "text_preview": plain readable text of what will be added — NO HTML tags, for human review
- "html_snippet": the same content as Confluence storage-format HTML (use <p>, <ul>, <li>, <strong>, <code> tags)
- "placement": "append_to_common_issues" | "append_to_special_notes" | "append_to_faq"

Return ONLY the raw JSON object. No markdown fences, no explanation, no questions."""

def strip_json_fences(text: str) -> str:
    """Remove markdown code fences that models sometimes add despite instructions."""
    text = text.strip()
    # Remove ```json ... ``` or ``` ... ```
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    return text.strip()


def handle_update(request: str, client, channel: str, thread_ts: str, user: str):
    ack = client.chat_postMessage(
        channel=channel, thread_ts=thread_ts,
        text="🤖 *SSD Bot* — Generating runbook update proposal…",
    )
    try:
        page = fetch_confluence_page(PRIMARY_PAGE_ID)
        if not page:
            client.chat_update(channel=channel, ts=ack["ts"],
                text="🤖 *SSD Bot* — Could not fetch the Confluence page. Please try again.")
            return

        # Include thread conversation history so bot understands references like
        # "update runbook with what you just found" or "add answer to Q1 to runbook"
        history = thread_history.get(thread_ts, [])
        thread_context = ""
        if history:
            lines = []
            for m in history[-10:]:  # last 5 exchanges
                role = "User" if m["role"] == "user" else "Bot"
                lines.append(f"{role}: {m['content'][:500]}")
            thread_context = "\n\nThread conversation so far:\n" + "\n".join(lines)

        resp = anthropic.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            system=UPDATE_SYSTEM,
            messages=[{"role": "user", "content":
                f"Current runbook content:\n{page['body_text'][:8000]}\n\n"
                f"User's update request:\n{request}"
                f"{thread_context}"
            }],
        )
        raw = strip_json_fences(resp.content[0].text)
        logger.info(f"Update proposal raw response: {raw[:500]}")
        proposal = json.loads(raw)

        # Build the proposed new page HTML by appending the snippet
        new_html = page["body_storage"] + "\n" + proposal["html_snippet"]

        # Store pending update keyed by (channel, user) — survives thread changes
        pending_updates[(channel, user)] = {
            "page_id":     PRIMARY_PAGE_ID,
            "new_html":    new_html,
            "version":     page["version"],
            "title":       page["title"],
            "summary":     proposal["summary"],
            "section":     proposal["section"],
            "thread_ts":   thread_ts,
        }

        preview = proposal.get("text_preview") or proposal["summary"]
        client.chat_update(
            channel=channel, ts=ack["ts"],
            text=(
                f"🤖 *SSD Bot* — Here's the proposed runbook update:\n\n"
                f"*Section:* {proposal['section']}\n"
                f"*Change:* {proposal['summary']}\n\n"
                f"*What will be added:*\n```{preview[:800]}```\n\n"
                f"Reply *yes* to apply this to the "
                f"<{CONFLUENCE_BASE}/spaces/CSS/pages/{PRIMARY_PAGE_ID}|SSD Playbook>, "
                f"or *no* to cancel."
            ),
        )
    except Exception as e:
        logger.error(f"handle_update error: {e}", exc_info=True)
        client.chat_update(channel=channel, ts=ack["ts"],
            text=f"🤖 *SSD Bot* — Failed to generate update proposal. Error: `{e}`")

# ─── Handler: Sync Thread → Confluence ────────────────────────────────────────

SYNC_SYSTEM = """You are helping sync a Slack thread discussion into the Conviva SSD Playbook.
Read the thread messages below and extract any new technical findings, fixes, or procedures.
Return a JSON object with:
  - "has_new_info": true/false
  - "summary": one-sentence description of what was found
  - "html_snippet": HTML to append to the runbook (or "" if nothing new)
  - "section": which section to append to

Return only raw JSON."""

def handle_sync(thread_messages: list, client, channel: str, thread_ts: str, user: str):
    ack = client.chat_postMessage(
        channel=channel, thread_ts=thread_ts,
        text="🤖 *SSD Bot* — Reading this thread and checking against the runbook…",
    )
    try:
        page = fetch_confluence_page(PRIMARY_PAGE_ID)
        thread_text = "\n".join(
            f"{m.get('user','?')}: {m.get('text','')}"
            for m in thread_messages if not m.get("bot_id")
        )

        resp = anthropic.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            system=SYNC_SYSTEM,
            messages=[{"role": "user", "content":
                f"Existing runbook (excerpt):\n{page['body_text'][:6000]}\n\n"
                f"Slack thread:\n{thread_text}"
            }],
        )
        result = json.loads(strip_json_fences(resp.content[0].text))

        if not result.get("has_new_info") or not result.get("html_snippet"):
            client.chat_update(channel=channel, ts=ack["ts"],
                text="🤖 *SSD Bot* — This thread doesn't contain anything new compared to the current runbook. No update needed ✅")
            return

        new_html = page["body_storage"] + "\n" + result["html_snippet"]
        pending_updates[(channel, user)] = {
            "page_id":      PRIMARY_PAGE_ID,
            "new_html":     new_html,
            "version":      page["version"],
            "title":        page["title"],
            "summary":      result["summary"],
            "section":      result["section"],
            "thread_ts":    thread_ts,
        }

        client.chat_update(
            channel=channel, ts=ack["ts"],
            text=(
                f"🤖 *SSD Bot* — Found something worth adding to the runbook:\n\n"
                f"*Section:* {result['section']}\n"
                f"*What's new:* {result['summary']}\n\n"
                f"*Content to add:*\n```{result['html_snippet'][:600]}```\n\n"
                f"Reply *yes* to sync this to the "
                f"<{CONFLUENCE_BASE}/spaces/CSS/pages/{PRIMARY_PAGE_ID}|SSD Playbook>, "
                f"or *no* to cancel."
            ),
        )
    except Exception as e:
        logger.error(e)
        client.chat_update(channel=channel, ts=ack["ts"],
            text="🤖 *SSD Bot* — Failed to analyse the thread. Please try again.")

# ─── Handler: Remember ────────────────────────────────────────────────────────

REMEMBER_SYSTEM = """You are a memory extraction assistant for the SSD Agent Assist bot.
The user wants to save something to the bot's persistent memory.
Extract a clean key and value from their message.

Rules:
- key: short snake_case label, max 40 chars, e.g. "QVC_pipelines_to_check", "stream_id_affected_customers"
- value: the full fact to remember, in plain English

Return ONLY a raw JSON object with "key" and "value". No markdown, no explanation."""

def handle_remember(text: str, client, channel: str, thread_ts: str, user: str):
    """Save a user-specified fact to persistent memory."""
    ack = client.chat_postMessage(
        channel=channel, thread_ts=thread_ts,
        text="🤖 *SSD Bot* — Saving to memory…",
    )
    try:
        # Strip common prefixes so Claude sees just the content
        content = re.sub(r"(?i)^.*?remember\s*:\s*", "", text, count=1).strip()
        content = re.sub(r"(?i)^.*?save\s+(?:this\s+)?to\s+memory\s*:\s*", "", content, count=1).strip()

        resp = anthropic.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=200,
            system=REMEMBER_SYSTEM,
            messages=[{"role": "user", "content": content}],
        )
        raw = strip_json_fences(resp.content[0].text)
        parsed = json.loads(raw)
        key   = parsed["key"]
        value = parsed["value"]

        result = tool_save_memory(key, value)
        client.chat_update(
            channel=channel, ts=ack["ts"],
            text=(
                f"🤖 *SSD Bot* — ✅ Got it! I'll remember this in all future conversations:\n\n"
                f"*Key:* `{key}`\n"
                f"*Remembered:* {value}\n\n"
                f"_You can view all memories by asking: `@SSD_Bot what do you remember?`_"
            ),
        )
    except Exception as e:
        logger.error(f"handle_remember error: {e}", exc_info=True)
        client.chat_update(channel=channel, ts=ack["ts"],
            text=f"🤖 *SSD Bot* — Failed to save memory: `{e}`")


# ─── Handler: Confirm / Cancel ────────────────────────────────────────────────

def handle_confirm(client, channel: str, thread_ts: str, user: str):
    key = (channel, user)
    logger.info(f"handle_confirm: key={key}, pending_updates={list(pending_updates.keys())}, pending_pages={list(pending_pages.keys())}")

    # ── Check for pending upstream minute DAG trigger ──
    trigger = pending_trigger_upstream.pop(key, None)
    if trigger:
        reply_ts = trigger.get("thread_ts", thread_ts)
        existing_run_id = trigger.get("existing_run_id")
        if existing_run_id:
            # Run already exists (was failed/success) — clear it instead of creating a new one
            result = _airflow_clear_dag_run(trigger["base"], trigger["dag_id"], existing_run_id)
        else:
            result = _airflow_trigger_dag_run(
                trigger["base"], trigger["dag_id"],
                trigger["dag_run_id"], trigger["logical_date"], trigger["conf"],
            )
        if result["ok"]:
            client.chat_postMessage(
                channel=channel, thread_ts=reply_ts,
                text=(
                    f"🤖 *SSD Bot* — ⚡ Upstream DAG triggered!\n"
                    f"*DAG:* `{trigger['dag_id']}`\n"
                    f"*Logical date:* `{trigger['logical_date']}`\n\n"
                    f"Monitor it at: "
                    f"<{trigger['base']}/dags/{trigger['dag_id']}/grid|{trigger['dag_id']}>\n\n"
                    f"Once it succeeds, rerun your DPI Flow Feed pipelines for minute `{trigger['failed_minute']}`."
                ),
            )
        else:
            client.chat_postMessage(
                channel=channel, thread_ts=reply_ts,
                text=f"🤖 *SSD Bot* — ❌ Trigger failed: `{result['error']}`",
            )
        # Whether trigger succeeded or failed, post any queued rerun proposals now
        _dequeue_next_rerun(client, channel, reply_ts, user)
        return

    # ── Check for pending mark-runs action ──
    mark = pending_mark_runs.pop(key, None)
    if mark:
        reply_ts     = mark.get("thread_ts", thread_ts)
        dag_id       = mark["dag_id"]
        state        = mark["state"]
        runs         = mark["runs"]
        kill_running = mark.get("kill_running", False)
        success_count = 0
        fail_count    = 0

        # Step 1 — mark each DAG run state
        for r in runs:
            ok = _airflow_mark_dag_run(r["base"], dag_id, r["dag_run_id"], state)
            if ok:
                success_count += 1
            else:
                fail_count += 1

        # Step 2 — kill running tasks across all instances that had runs
        kill_summary = ""
        if kill_running:
            # Deduplicate bases so we call once per Airflow instance
            bases_seen = {}
            for r in runs:
                bases_seen[r["base"]] = True
            total_killed = 0
            kill_errors  = []
            for base in bases_seen:
                result = _airflow_kill_running_tasks(base, dag_id, mark["start_dt"], mark["end_dt"])
                total_killed += result["killed"]
                if result["error"]:
                    kill_errors.append(result["error"])
            if kill_errors:
                kill_summary = f"\n⚡ Kill running tasks: {total_killed} task(s) interrupted, ⚠️ {len(kill_errors)} error(s): {'; '.join(kill_errors[:2])}"
            else:
                kill_summary = f"\n⚡ Kill running tasks: {total_killed} task instance(s) interrupted."

        # Step 3 — sweep: catch any runs still showing as 'running' in the range
        sweep_summary = ""
        bases_seen = {}
        for r in runs:
            bases_seen[r["base"]] = True
        for base in bases_seen:
            sweep = _airflow_sweep_running_runs(base, dag_id, mark["start_dt"], mark["end_dt"])
            if sweep["swept"] > 0:
                sweep_summary += f"\n🔍 Sweep: caught and marked *{sweep['swept']}* still-running run(s) as success."
            if sweep["errors"]:
                sweep_summary += f"\n🔍 Sweep errors: {', '.join(sweep['errors'][:3])}"

        if fail_count == 0:
            client.chat_postMessage(
                channel=channel, thread_ts=reply_ts,
                text=(
                    f"🤖 *SSD Bot* — ✅ Done! Marked *{success_count}* run(s) of `{dag_id}` as *{state}*.\n"
                    f"Range: `{mark['start_dt']}` → `{mark['end_dt']}`"
                    f"{kill_summary}"
                    f"{sweep_summary}"
                ),
            )
        else:
            client.chat_postMessage(
                channel=channel, thread_ts=reply_ts,
                text=(
                    f"🤖 *SSD Bot* — ⚠️ Partially done: *{success_count}* marked as *{state}*, "
                    f"*{fail_count}* failed to update.\n"
                    f"Check Airflow logs for details."
                    f"{kill_summary}"
                    f"{sweep_summary}"
                ),
            )
        return

    # ── Check for pending flow-feed batch rerun (text "yes" = confirm all) ──
    batch = pending_flow_feed_batch.get(key)
    if batch:
        reply_ts = batch.get("thread_ts", thread_ts)
        all_dags = batch.get("dag_ids", [])
        # Collapse the interactive message if we have its ts
        msg_ts = batch.get("message_ts")
        if msg_ts:
            try:
                client.chat_update(
                    channel=channel, ts=msg_ts,
                    text=f"✅ Confirmed via text — rerunning all {len(all_dags)} pipeline(s).",
                    blocks=[],
                )
            except Exception:
                pass
        _execute_flow_feed_batch_rerun(client, channel, reply_ts, user, all_dags)
        return

    # ── Check for pending rerun/backfill action ──
    rerun = pending_rerun_runs.pop(key, None)
    if rerun:
        reply_ts  = rerun.get("thread_ts", thread_ts)
        dag_id    = rerun["dag_id"]
        start_dt  = rerun["start_dt"]
        end_dt    = rerun["end_dt"]
        success_count = 0
        fail_count    = 0
        for label, base in rerun["instances_with_runs"]:
            headers = _get_airflow_headers(base)
            url     = f"{base}/api/v1/dags/{dag_id}/clearTaskInstances"
            payload = {
                "start_date":      start_dt,
                "end_date":        end_dt,
                "reset_dag_runs":  True,
                "dry_run":         False,
            }
            try:
                resp = requests.post(url, headers=headers, json=payload, verify=False, timeout=60)
                logger.info(f"clearTaskInstances {label} {dag_id} → {resp.status_code}: {resp.text[:200]}")
                if resp.ok:
                    cleared = resp.json().get("task_instances", [])
                    success_count += len(cleared)
                else:
                    fail_count += 1
                    logger.warning(f"clearTaskInstances failed {label}: {resp.status_code} {resp.text[:200]}")
            except Exception as e:
                fail_count += 1
                logger.error(f"clearTaskInstances exception {label}: {e}")
        if fail_count == 0:
            client.chat_postMessage(
                channel=channel, thread_ts=reply_ts,
                text=(
                    f"🤖 *SSD Bot* — ♻️ Done! Cleared `{dag_id}` runs in the window.\n"
                    f"Range: `{start_dt}` → `{end_dt}`\n"
                    f"Airflow will re-execute them now (paced by `max_active_runs`)."
                ),
            )
        else:
            client.chat_postMessage(
                channel=channel, thread_ts=reply_ts,
                text=(
                    f"🤖 *SSD Bot* — ⚠️ Some instances failed to clear (`{fail_count}` error(s)). "
                    f"Check the bot logs or try clearing manually in the Airflow UI."
                ),
            )
        # Post next queued rerun proposal if any
        _dequeue_next_rerun(client, channel, reply_ts, user)
        return

    # ── Check for pending interleaved rerun ──
    interleaved = pending_interleaved_rerun.pop(key, None)
    if interleaved:
        reply_ts   = interleaved.get("thread_ts", thread_ts)
        dag_id     = interleaved["dag_id"]
        runs       = interleaved["runs"]
        batch_size = interleaved["batch_size"]
        max_active = interleaved["max_active_runs"]
        instances_with_runs = interleaved["instances_with_runs"]
        total_waves = math.ceil(len(runs) / batch_size)

        stop_event = threading.Event()
        worker = threading.Thread(
            target=_run_interleaved_rerun_worker,
            kwargs=dict(
                channel=channel,
                thread_ts=reply_ts,
                dag_id=dag_id,
                runs=runs,
                instances_with_runs=instances_with_runs,
                batch_size=batch_size,
                max_active_runs=max_active,
                client=client,
                stop_event=stop_event,
            ),
            daemon=True,
        )
        active_interleaved_reruns[(channel, user)] = {
            "stop_event": stop_event,
            "thread":     worker,
            "dag_id":     dag_id,
        }
        worker.start()
        client.chat_postMessage(
            channel=channel, thread_ts=reply_ts,
            text=(
                f"🤖 *SSD Bot* — ♻️ Interleaved rerun started for `{dag_id}`.\n"
                f"{len(runs)} run(s) across {total_waves} wave(s) of {batch_size}. "
                f"I'll post updates here as each wave completes.\n"
                f"Say *cancel rerun* at any time to stop."
            ),
        )
        return

    # ── Check for pending HDFS → S3 repair copy ──
    repair = pending_hdfs_s3_copy.pop(key, None)
    if repair:
        reply_ts   = repair.get("thread_ts", thread_ts)
        namenode   = repair["namenode"]
        hdfs_path  = repair["hdfs_path"]
        files      = repair["files"]
        s3_bucket  = repair["s3_bucket"]
        s3_prefix  = repair["s3_prefix"]
        aws_profile = repair["aws_profile"]

        client.chat_postMessage(
            channel=channel, thread_ts=reply_ts,
            text=f"🤖 *SSD Bot* — ⏳ Starting HDFS → S3 copy of *{len(files)}* files using profile `{aws_profile}`...",
        )

        success_count = 0
        fail_count    = 0
        errors        = []

        try:
            import boto3
            session   = boto3.Session(profile_name=aws_profile)
            s3_client = session.client("s3")
        except Exception as e:
            client.chat_postMessage(channel=channel, thread_ts=reply_ts,
                text=f"🤖 *SSD Bot* — ❌ Failed to init S3 client with profile `{aws_profile}`: `{e}`")
            return

        for hdfs_file_path in files:
            rel_path = hdfs_file_path[len(hdfs_path):].lstrip("/")
            s3_key   = f"{s3_prefix}/{rel_path}" if s3_prefix else rel_path
            webhdfs_url = f"{namenode}/webhdfs/v1{hdfs_file_path}?op=OPEN"
            try:
                resp = requests.get(webhdfs_url, stream=True, verify=False, timeout=120)
                resp.raise_for_status()
                s3_client.upload_fileobj(resp.raw, s3_bucket, s3_key)
                success_count += 1
                logger.info(f"HDFS→S3 copied {hdfs_file_path} → s3://{s3_bucket}/{s3_key}")
            except Exception as e:
                fail_count += 1
                fname = hdfs_file_path.split("/")[-1]
                errors.append(f"{fname}: {e}")
                logger.error(f"HDFS→S3 copy failed {hdfs_file_path}: {e}")

        if fail_count == 0:
            client.chat_postMessage(
                channel=channel, thread_ts=reply_ts,
                text=(
                    f"🤖 *SSD Bot* — ✅ HDFS → S3 copy complete!\n"
                    f"Copied *{success_count}* file(s) to `s3://{s3_bucket}/{s3_prefix}/`"
                ),
            )
        else:
            err_str = "\n".join(errors[:5])
            client.chat_postMessage(
                channel=channel, thread_ts=reply_ts,
                text=(
                    f"🤖 *SSD Bot* — ⚠️ Copy finished with errors.\n"
                    f"✅ {success_count} succeeded  ❌ {fail_count} failed\n"
                    f"```{err_str}```"
                ),
            )
        return

    # ── Check for pending backfill ──
    backfill = pending_backfill.pop(key, None)
    if backfill:
        reply_ts     = backfill.get("thread_ts", thread_ts)
        dag_id       = backfill["dag_id"]
        base         = backfill["base"]
        dates        = backfill["dates"]
        kill_running = backfill.get("kill_running", False)
        created  = 0
        patched  = 0
        errors   = []

        # Step 1 — kill running tasks first if requested, so they don't block PATCH
        kill_summary = ""
        if kill_running:
            result = _airflow_kill_running_tasks(base, dag_id,
                                                 backfill["start_dt"], backfill["end_dt"])
            if result["error"]:
                kill_summary = f"\n⚡ Kill running tasks: ⚠️ {result['error']}"
            else:
                kill_summary = f"\n⚡ Kill running tasks: {result['killed']} task instance(s) interrupted."

        # Step 2 — create/patch each date to success
        for ld in dates:
            result = _airflow_backfill_create_or_patch(base, dag_id, ld)
            if result["ok"]:
                if result["action"] == "created":
                    created += 1
                else:
                    patched += 1
            else:
                errors.append(f"`{ld}`: {result['detail']}")

        # Step 3 — sweep: catch any runs still showing as 'running' in the range
        sweep_summary = ""
        sweep = _airflow_sweep_running_runs(base, dag_id, backfill["start_dt"], backfill["end_dt"])
        if sweep["swept"] > 0:
            sweep_summary = f"\n🔍 Sweep: caught and marked *{sweep['swept']}* still-running run(s) as success."
        if sweep["errors"]:
            sweep_summary += f"\n🔍 Sweep errors: {', '.join(sweep['errors'][:3])}"

        if not errors:
            client.chat_postMessage(
                channel=channel, thread_ts=reply_ts,
                text=(
                    f"🤖 *SSD Bot* — ✅ Backfill complete for `{dag_id}`!\n"
                    f"Range: `{backfill['start_dt']}` → `{backfill['end_dt']}`\n"
                    f"*{patched}* run(s) patched to success, *{created}* run(s) newly created as success.\n"
                    f"Total: *{patched + created}* run(s) marked success."
                    f"{kill_summary}"
                    f"{sweep_summary}"
                ),
            )
        else:
            client.chat_postMessage(
                channel=channel, thread_ts=reply_ts,
                text=(
                    f"🤖 *SSD Bot* — ⚠️ Backfill partially done for `{dag_id}`.\n"
                    f"*{patched}* patched, *{created}* created, *{len(errors)}* error(s):\n"
                    + "\n".join(errors[:5])
                    + ("\n…" if len(errors) > 5 else "")
                    + f"{sweep_summary}"
                ),
            )
        return

    # ── Check for pending page creation ──
    page = pending_pages.pop(key, None)
    if page:
        reply_ts = page.get("thread_ts", thread_ts)
        result   = create_confluence_page(page["title"], page["storage_html"], page["parent_page_id"])
        if result["ok"]:
            client.chat_postMessage(
                channel=channel, thread_ts=reply_ts,
                text=(
                    f"🤖 *SSD Bot* — ✅ Page published!\n"
                    f"*Title:* {page['title']}\n"
                    f"*Parent:* {page['parent_title']}\n"
                    f"<{result['url']}|View new page>"
                ),
            )
        else:
            client.chat_postMessage(channel=channel, thread_ts=reply_ts,
                text=f"🤖 *SSD Bot* — ❌ Failed to create page: `{result['error']}`")
        return

    # ── Check for pending runbook update ──
    update = pending_updates.pop(key, None)
    if not update:
        # Nothing pending — the user may have said "yes" in response to a bot question
        # that didn't store a pending action (e.g. bot asked "shall I propose?" before calling
        # the propose tool). Route to handle_answer so Claude can figure out what to do.
        logger.info("handle_confirm: nothing pending, routing 'yes' to handle_answer")
        handle_answer("yes", client, channel, thread_ts, user=user)
        return

    reply_ts = update.get("thread_ts", thread_ts)
    ok = update_confluence_page(
        update["page_id"], update["new_html"],
        update["version"], update["title"]
    )
    if ok:
        client.chat_postMessage(
            channel=channel, thread_ts=reply_ts,
            text=(
                f"🤖 *SSD Bot* — ✅ Runbook updated!\n"
                f"*Change:* {update['summary']}\n"
                f"<{CONFLUENCE_BASE}/spaces/CSS/pages/{update['page_id']}|View updated runbook>"
            ),
        )
    else:
        client.chat_postMessage(channel=channel, thread_ts=reply_ts,
            text="🤖 *SSD Bot* — ❌ Failed to write to Confluence. Check the terminal for details.")


def handle_cancel(client, channel: str, thread_ts: str, user: str):
    pending_updates.pop((channel, user), None)
    pending_pages.pop((channel, user), None)
    pending_mark_runs.pop((channel, user), None)
    pending_rerun_runs.pop((channel, user), None)
    pending_rerun_queue.pop((channel, user), None)
    pending_flow_feed_batch.pop((channel, user), None)
    pending_trigger_upstream.pop((channel, user), None)
    pending_backfill.pop((channel, user), None)
    pending_interleaved_rerun.pop((channel, user), None)
    pending_hdfs_s3_copy.pop((channel, user), None)
    pending_force_context.pop((channel, user), None)

    # Stop any running interleaved rerun background thread
    active = active_interleaved_reruns.get((channel, user))
    if active:
        active["stop_event"].set()
        dag_id = active.get("dag_id", "?")
        client.chat_postMessage(
            channel=channel, thread_ts=thread_ts,
            text=(
                f"🤖 *SSD Bot* — ⛔ Cancellation requested for interleaved rerun of `{dag_id}`. "
                f"Stopping after the current wave finishes…"
            ),
        )
        return

    client.chat_postMessage(channel=channel, thread_ts=thread_ts,
        text="🤖 *SSD Bot* — Cancelled. No changes made.")

# ─── Main Event Handler ────────────────────────────────────────────────────────

# Only respond in these channels — keeps SSD bot isolated from colleague's bot
TARGET_CHANNELS = [
    "C07KV3PB79C",  # #ces-internal-ssd (production)
    "C0ARYHQ727P",  # #wendy_sssd_test (testing)
    # Add more channel IDs here if needed
]

@slack_app.action("flow_feed_confirm_rerun")
def handle_flow_feed_confirm(ack, body, client):
    """User clicked '✅ Rerun Selected' on the batch flow-feed rerun proposal."""
    ack()
    channel  = body["channel"]["id"]
    user     = body["user"]["id"]
    msg_ts   = body["message"]["ts"]
    thread_ts = body["message"].get("thread_ts", msg_ts)

    # Read which checkboxes are currently selected from block state
    state    = body.get("state", {}).get("values", {})
    selected = []
    for _block_id, block_state in state.items():
        for _action_id, action_state in block_state.items():
            opts = action_state.get("selected_options", [])
            if opts:
                selected.extend(o["value"] for o in opts)

    if not selected:
        client.chat_postMessage(
            channel=channel, thread_ts=thread_ts,
            text="🤖 *SSD Bot* — No pipelines selected. Rerun cancelled.",
        )
        pending_flow_feed_batch.pop((channel, user), None)
        # Update the original message to show cancelled state
        client.chat_update(
            channel=channel, ts=msg_ts,
            text="❌ Rerun cancelled — no pipelines selected.",
            blocks=[],
        )
        return

    # Update the original message to remove interactive elements (prevent double-click)
    try:
        client.chat_update(
            channel=channel, ts=msg_ts,
            text=f"⏳ Running reruns for: {', '.join(f'`{d}`' for d in selected)}…",
            blocks=[{
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"⏳ *Processing reruns…*\nSelected: {', '.join(f'`{d}`' for d in selected)}",
                },
            }],
        )
    except Exception:
        pass

    _execute_flow_feed_batch_rerun(client, channel, thread_ts, user, selected)


@slack_app.action("flow_feed_cancel_rerun")
def handle_flow_feed_cancel(ack, body, client):
    """User clicked '❌ Cancel' on the batch flow-feed rerun proposal."""
    ack()
    channel  = body["channel"]["id"]
    user     = body["user"]["id"]
    msg_ts   = body["message"]["ts"]
    thread_ts = body["message"].get("thread_ts", msg_ts)

    pending_flow_feed_batch.pop((channel, user), None)
    try:
        client.chat_update(
            channel=channel, ts=msg_ts,
            text="❌ Rerun cancelled.",
            blocks=[],
        )
    except Exception:
        pass
    client.chat_postMessage(
        channel=channel, thread_ts=thread_ts,
        text="🤖 *SSD Bot* — ❌ Flow feed rerun cancelled.",
    )


@slack_app.action("flow_feed_select")
def handle_flow_feed_select(ack, body):
    """Acknowledge checkbox toggle (no-op — state read on confirm click)."""
    ack()


@slack_app.event("message")
def handle_message_events(body, event, client, logger):
    """Handle plain messages — thread follow-ups and confirm/cancel from channel level."""
    # Ignore bot messages, deleted/edited messages, anything without text
    if event.get("bot_id") or event.get("subtype") or not event.get("text"):
        return

    channel   = event.get("channel", "")
    thread_ts = event.get("thread_ts")  # None if top-level channel message
    ts        = event["ts"]
    user      = event.get("user", "")
    text      = event["text"].strip()

    # @mention messages fire both app_mention and message events — handle them
    # exclusively in app_mention to avoid double-processing (duplicate replies,
    # "no pending action" false positives, etc.)
    if re.search(r"<@[A-Z0-9]+>", text):
        return

    if channel not in TARGET_CHANNELS:
        return

    has_pending = (
        (channel, user) in pending_updates or
        (channel, user) in pending_pages or
        (channel, user) in pending_mark_runs or
        (channel, user) in pending_rerun_runs or
        (channel, user) in pending_flow_feed_batch or
        (channel, user) in pending_trigger_upstream or
        (channel, user) in pending_backfill or
        (channel, user) in pending_interleaved_rerun or
        (channel, user) in active_interleaved_reruns or  # allow "cancel rerun" mid-run
        (channel, user) in pending_force_context         # awaiting follow-up after "force trigger"
    )
    in_active_thread = thread_ts and thread_ts in active_threads

    logger.info(
        f"message event: user={user} channel={channel} thread_ts={thread_ts} "
        f"text={text[:60]!r} has_pending={has_pending} in_active_thread={in_active_thread}"
    )

    # Respond if: user is in an active thread OR has a pending update (confirm/cancel from anywhere)
    if not in_active_thread and not has_pending:
        return

    _dispatch(text, user, client, channel, ts, thread_ts or ts)


def _dispatch(text: str, user: str, client, channel: str, ts: str, thread_ts: str):
    """Route a message to the right handler and mark the thread as active."""
    intent = classify_intent(text)
    key = (channel, user)
    low = text.lower().strip()

    # ── Force-trigger context tracking ────────────────────────────────────────
    # If this message IS "force trigger", record the intent so that the very next
    # follow-up from the same user (if the bot asks for more info) inherits it.
    if "force trigger" in low or "trigger anyway" in low:
        pending_force_context[key] = True

    # If a prior "force trigger" is waiting for follow-up info, inject the flag
    # into the question before passing to the agentic loop.
    elif key in pending_force_context and intent not in ("confirm", "cancel", "remember", "update", "sync"):
        text = (
            "[Context: the user previously said 'force trigger'. "
            "Apply force=True when calling propose_trigger_upstream_minute_dag — "
            "skip the HDFS _SUCCESS check entirely.]\n" + text
        )
        pending_force_context.pop(key, None)

    if intent == "confirm":
        handle_confirm(client, channel, thread_ts, user)
    elif intent == "cancel":
        handle_cancel(client, channel, thread_ts, user)
    elif intent == "remember":
        handle_remember(text, client, channel, thread_ts, user)
    elif intent == "update":
        handle_update(text, client, channel, thread_ts, user)
    elif intent == "sync":
        try:
            result = client.conversations_replies(channel=channel, ts=thread_ts)
            thread_messages = result.get("messages", [])
        except Exception:
            thread_messages = [{"user": user, "text": text}]
        handle_sync(thread_messages, client, channel, thread_ts, user)
    else:
        handle_answer(text, client, channel, ts, user=user)

    # Remember this thread so follow-up messages don't need @mention
    active_threads.add(thread_ts)


@slack_app.event("app_mention")
def handle_mention(event, client, logger):
    raw_text  = event.get("text", "")
    channel   = event["channel"]
    ts        = event["ts"]
    thread_ts = event.get("thread_ts", ts)
    user      = event.get("user", "")

    # Ignore @mentions outside the target channels — colleague's bot handles the rest
    if channel not in TARGET_CHANNELS:
        logger.info(f"Ignoring mention in non-target channel {channel}")
        return

    # Strip the @mention
    question = re.sub(r"<@[A-Z0-9]+>", "", raw_text).strip()

    # ── PagerDuty alert detection ─────────────────────────────────────────────
    # If the message contains a PD/Airflow TaskInstance alert, parse it and
    # rewrite it as a structured investigation prompt — engineers can just paste
    # the alert directly without reformatting.
    pd_parsed = _parse_pd_alert(question)
    if pd_parsed:
        logger.info(f"PD alert detected: dag_id={pd_parsed['dag_id']} "
                    f"task={pd_parsed['task_id']} exec={pd_parsed['execution_date']}")
        question = _pd_alert_to_question(pd_parsed)

    if not question:
        client.chat_postMessage(
            channel=channel, thread_ts=ts,
            text=(
                "🤖 *SSD Bot* — Hi! Here's what I can do:\n\n"
                "• *Answer questions:* `@SSD_Bot how do I fix BlobAlreadyExists?`\n"
                "• *Fix DPI Flow Feed:* `@SSD_Bot DPI flow feed for 2026-05-22 21:10 is stuck, trigger upstream`\n"
                "• *Mark DAG runs:* `@SSD_Bot mark pipeline STV_ECO_CROSS_PAGE_... as success from 2026-05-22 05:04 to 11:21`\n"
                "• *Rerun DAG runs:* `@SSD_Bot rerun pipeline STV_ECO_CROSS_PAGE_... from 2026-05-22 05:04 to 11:21`\n"
                "• *Update runbook:* `@SSD_Bot update runbook: [describe new finding]`\n"
                "• *Sync thread:* `@SSD_Bot sync this thread to the runbook`\n"
                "_Once I reply, you can continue the conversation without @mentioning me._"
            ),
        )
        active_threads.add(ts)  # even an empty mention activates the thread
        return

    logger.info(f"user={user} channel={channel} text={question[:100]}")
    _dispatch(question, user, client, channel, ts, thread_ts)


# ─── Scheduled Jobs ────────────────────────────────────────────────────────────

_scheduler = BackgroundScheduler()
_scheduler.add_job(
    lambda: run_alert_summary_job(),
    CronTrigger(day_of_week="mon", hour=7, minute=0, timezone="America/Chicago"),
    id="weekly_alert_summary",
    name="Weekly SSD Alert Digest",
    replace_existing=True,
)
_scheduler.start()
logger.info("APScheduler started — weekly alert digest: Monday 07:00 CST")

# ─── Entry Point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logger.info("Starting SSD Agent Assist (Socket Mode)…")
    handler = SocketModeHandler(slack_app, os.environ["SLACK_APP_TOKEN"])
    handler.start()
