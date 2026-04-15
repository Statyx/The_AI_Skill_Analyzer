"""
Fabric Data Agent External Client

A standalone Python client for calling Microsoft Fabric Data Agents from outside
of the Fabric environment using interactive browser authentication.

Requirements:
    pip install openai azure-identity requests python-dotenv
"""

import time
import uuid
import json
import os
import requests
import warnings
from typing import Optional
from concurrent.futures import ThreadPoolExecutor

from azure.identity import InteractiveBrowserCredential
from openai import OpenAI

warnings.filterwarnings("ignore", ".*Assistants API is deprecated.*")

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


class FabricDataAgentClient:
    """Client for querying Microsoft Fabric Data Agents via the OpenAI Assistants API."""

    DEFAULT_API_VERSION = "2024-02-15-preview"

    def __init__(self, tenant_id: str, data_agent_url: str, stage: str = "production", api_version: str = None):
        if not tenant_id or not data_agent_url:
            raise ValueError("tenant_id and data_agent_url are required")
        self.tenant_id = tenant_id
        self.data_agent_url = data_agent_url
        self.stage = stage
        self.api_version = api_version or self.DEFAULT_API_VERSION
        self.credential = None
        self.token = None
        self._authenticate()

    def _authenticate(self):
        """Authenticate using InteractiveBrowserCredential."""
        print("Authenticating...")
        try:
            self.credential = InteractiveBrowserCredential(tenant_id=self.tenant_id)
            self._refresh_token()
        except Exception as e:
            raise Exception(f"Authentication failed: {e}")

    def _refresh_token(self):
        """Refresh the access token."""
        print("Refreshing token...")
        if self.credential is None:
            raise ValueError("No credential available")
        try:
            self.token = self.credential.get_token("https://api.fabric.microsoft.com/.default")
            print(f"Token valid until {time.ctime(self.token.expires_on)}")
        except Exception as e:
            raise Exception(f"Token refresh failed: {e}")

    def _get_openai_client(self):
        """Create an OpenAI client configured for Fabric Data Agent."""
        if self.token is None or self.token.expires_on <= time.time():
            self._refresh_token()
        if self.token is None:
            raise ValueError("No valid token available")
        return OpenAI(
            base_url=self.data_agent_url,
            api_key=str(uuid.uuid4()),
            default_headers={"Authorization": f"Bearer {self.token.token}"},
            default_query={"stage": self.stage, "api-version": self.api_version},
            timeout=60.0,
        )

    def _get_existing_or_create_new_thread(self, thread_name: str = None):
        """Get an existing thread or create a new one."""
        if thread_name is None:
            thread_name = str(uuid.uuid4())

        base_url = self.data_agent_url.removesuffix("/")
        get_new_thread_url = f"{base_url}/threads"

        headers = {
            "Authorization": f"Bearer {str(self.token.token)}",
            "Content-Type": "application/json",
        }
        params = {"stage": self.stage, "api-version": self.api_version}

        response = requests.post(get_new_thread_url, headers=headers, json={}, params=params, timeout=30)
        response.raise_for_status()
        thread = response.json()
        return thread

    def ask(self, question: str, timeout: int = 120, thread_name: str = None) -> str:
        """Ask a question to the Data Agent and return the response."""
        if not question or not question.strip():
            raise ValueError("Question cannot be empty")

        print(f"Asking: {question}")
        client = self._get_openai_client()

        # Create assistant
        assistant = client.beta.assistants.create(model="irrelevant")

        # Get or create thread
        thread = self._get_existing_or_create_new_thread(thread_name or self.data_agent_url)

        # Send message
        client.beta.threads.messages.create(
            thread_id=thread["id"],
            role="user",
            content=question,
        )

        # Create run
        run = client.beta.threads.runs.create(
            thread_id=thread["id"],
            assistant_id=assistant.id,
        )

        # Poll for completion
        start_time = time.time()
        while True:
            if time.time() - start_time > timeout:
                raise TimeoutError(f"Run did not complete within {timeout}s")
            time.sleep(2)
            run = client.beta.threads.runs.retrieve(
                thread_id=thread["id"],
                run_id=run.id,
            )
            if run.status in ("completed", "failed", "cancelled", "expired"):
                break

        # Get messages
        messages = client.beta.threads.messages.list(thread_id=thread["id"])

        responses = []
        for msg in messages.data:
            if msg.role == "assistant":
                for content in msg.content:
                    if hasattr(content, "text"):
                        text_content = getattr(content, "text", None)
                        if text_content:
                            responses.append(text_content.value)

        # Cleanup
        try:
            client.beta.threads.delete(thread_id=thread["id"])
        except Exception:
            pass

        return "\n".join(responses) if responses else ""

    def get_run_details(self, question: str, timeout: int = 120, thread_name: str = None) -> dict:
        """Ask a question and return full run details including SQL queries and data."""
        if not question or not question.strip():
            raise ValueError("Question cannot be empty")

        print(f"Asking (with details): {question}")
        client = self._get_openai_client()

        assistant = client.beta.assistants.create(model="irrelevant")
        thread = self._get_existing_or_create_new_thread(thread_name or self.data_agent_url)

        client.beta.threads.messages.create(
            thread_id=thread["id"],
            role="user",
            content=question,
        )

        run = client.beta.threads.runs.create(
            thread_id=thread["id"],
            assistant_id=assistant.id,
        )

        start_time = time.time()
        while True:
            if time.time() - start_time > timeout:
                raise TimeoutError(f"Run did not complete within {timeout}s")
            time.sleep(2)
            run = client.beta.threads.runs.retrieve(
                thread_id=thread["id"],
                run_id=run.id,
            )
            if run.status in ("completed", "failed", "cancelled", "expired"):
                break

        messages = client.beta.threads.messages.list(thread_id=thread["id"])

        responses = []
        for msg in messages.data:
            if msg.role == "assistant":
                for content in msg.content:
                    if hasattr(content, "text"):
                        text_content = getattr(content, "text", None)
                        if text_content:
                            responses.append(text_content.value)

        try:
            client.beta.threads.delete(thread_id=thread["id"])
        except Exception:
            pass

        return {
            "question": question,
            "answer": "\n".join(responses) if responses else "",
            "run_status": run.status,
            "run_id": run.id,
            "thread_id": thread["id"],
        }

    def _cancel_active_runs(self, client, thread_id):
        """Cancel any active runs on a thread and wait until the thread is clear.
        Uses requests directly because the OpenAI SDK's runs.list can hang with Fabric API.
        """
        base_url = self.data_agent_url.removesuffix("/")
        headers = {
            "Authorization": f"Bearer {str(self.token.token)}",
            "Content-Type": "application/json",
        }
        params = {"stage": self.stage, "api-version": self.api_version}
        try:
            r = self._http.get(f"{base_url}/threads/{thread_id}/runs", headers=headers, params=params, timeout=15)
            if r.status_code != 200:
                return
            runs_data = r.json().get("data", [])
            for run in runs_data:
                if run.get("status") in ("queued", "in_progress", "requires_action"):
                    self._http.post(
                        f"{base_url}/threads/{thread_id}/runs/{run['id']}/cancel",
                        headers=headers, params=params, timeout=15,
                    )
            # Wait until no runs are active (up to 15s)
            for _ in range(15):
                r = self._http.get(f"{base_url}/threads/{thread_id}/runs", headers=headers, params=params, timeout=15)
                if r.status_code != 200:
                    break
                active = [rd for rd in r.json().get("data", [])
                          if rd.get("status") in ("queued", "in_progress", "requires_action", "cancelling")]
                if not active:
                    break
                time.sleep(1)
        except Exception:
            pass

    # ── Adaptive poll intervals: start fast (0.5s), ramp to 3s ──
    _POLL_INTERVALS = [0.5, 0.5, 1, 1, 2, 2] + [3] * 50
    _THREAD_RECYCLE_EVERY = 8  # DELETE and recreate every N questions

    def _get_thread(self, headers, params, force_reset=False):
        """Get a usable thread. Reuse when possible, reset every N uses."""
        base_url = self.data_agent_url.removesuffix("/")

        # Reuse cached thread if not stale
        if (self._thread_id_cache
                and not force_reset
                and self._thread_use_count < self._THREAD_RECYCLE_EVERY):
            self._thread_use_count += 1
            return self._thread_id_cache

        # Need a fresh thread — DELETE + recreate
        thread_id = self._get_fresh_thread(base_url, headers, params)
        self._thread_id_cache = thread_id
        self._thread_use_count = 1
        return thread_id

    def _request_with_retry(self, method, url, headers, params, json_body=None, max_retries=3):
        """HTTP request with retry on 400/404/429/500/502/503 (eventual consistency + transient errors)."""
        retryable = {400, 404, 429, 500, 502, 503}
        for attempt in range(max_retries + 1):
            if method == "POST":
                r = self._http.post(url, headers=headers, json=json_body,
                                    params=params, timeout=30)
            elif method == "DELETE":
                r = self._http.delete(url, headers=headers, params=params, timeout=15)
            else:
                r = self._http.get(url, headers=headers, params=params, timeout=30)
            if r.status_code in retryable and attempt < max_retries:
                time.sleep(2.0 * (attempt + 1))
                continue
            r.raise_for_status()
            return r
        r.raise_for_status()

    def _get_fresh_thread(self, base_url, headers, params):
        """Always DELETE + recreate thread for a clean state."""
        # Get current thread (Fabric always returns same thread per user+agent)
        r = self._http.post(f"{base_url}/threads", headers=headers,
                            json={}, params=params, timeout=30)
        r.raise_for_status()
        thread_id = r.json()["id"]

        # Delete to clear accumulated messages and stale runs
        delete_params = {"api-version": self.api_version}
        try:
            self._http.delete(f"{base_url}/threads/{thread_id}",
                              headers=headers, params=delete_params, timeout=15)
        except Exception:
            pass
        time.sleep(0.5)

        # Recreate
        r = self._http.post(f"{base_url}/threads", headers=headers,
                            json={}, params=params, timeout=30)
        r.raise_for_status()
        return r.json()["id"]

    def get_raw_run_response(self, question: str, timeout: int = 120, thread_name: str = None) -> dict:
        """Ask a question and return the raw run response object.

        Optimizations (SPEC_PARALLEL_EXECUTION):
        - Connection pooling via requests.Session
        - Adaptive polling (0.5s→3s ramp)
        - Fresh thread per question (DELETE+recreate for clean state)
        - Retry 404 on all API calls (eventual consistency)
        - Parallel message + steps retrieval
        """
        if not question or not question.strip():
            raise ValueError("Question cannot be empty")

        # Ensure we have an HTTP session for connection pooling
        if not hasattr(self, '_http'):
            self._http = requests.Session()
            self._http.headers.update({"Content-Type": "application/json"})
        if not hasattr(self, '_thread_id_cache'):
            self._thread_id_cache = None
            self._thread_use_count = 0

        base_url = self.data_agent_url.removesuffix("/")
        if self.token is None or self.token.expires_on <= time.time():
            self._refresh_token()
        headers = {
            "Authorization": f"Bearer {str(self.token.token)}",
            "Content-Type": "application/json",
        }
        params = {"stage": self.stage, "api-version": self.api_version}

        # Smart thread reuse — recycle every N questions to avoid 50+ message BadRequest
        thread_id = self._get_thread(headers, params)

        # Retry loop: if the run fails, get a fresh thread and retry
        max_attempts = 3
        run_id = None
        run_status = None

        for attempt in range(max_attempts):
            # Send user message (with 404 retry for eventual consistency)
            msg_r = self._request_with_retry(
                "POST", f"{base_url}/threads/{thread_id}/messages",
                headers, params, json_body={"role": "user", "content": question},
            )

            # Create assistant
            asst_r = self._http.post(
                f"{base_url}/assistants",
                headers=headers, json={"model": "irrelevant"}, params=params, timeout=30,
            )
            asst_r.raise_for_status()
            assistant_id = asst_r.json()["id"]

            # Create run (with 404 retry)
            run_r = self._request_with_retry(
                "POST", f"{base_url}/threads/{thread_id}/runs",
                headers, params, json_body={"assistant_id": assistant_id},
            )
            run_data = run_r.json()
            run_id = run_data["id"]
            run_status = run_data.get("status", "queued")

            # Adaptive polling: 0.5, 0.5, 1, 1, 2, 2, 3, 3, 3...
            start_time = time.time()
            poll_idx = 0
            while run_status not in ("completed", "failed", "cancelled", "expired"):
                if time.time() - start_time > timeout:
                    break
                delay = self._POLL_INTERVALS[min(poll_idx, len(self._POLL_INTERVALS) - 1)]
                time.sleep(delay)
                poll_idx += 1
                poll_r = self._http.get(
                    f"{base_url}/threads/{thread_id}/runs/{run_id}",
                    headers=headers, params=params, timeout=30,
                )
                if poll_r.status_code == 200:
                    run_status = poll_r.json().get("status", run_status)

            if run_status == "completed":
                break

            # Run failed — force fresh thread and retry
            if attempt < max_attempts - 1:
                self._cancel_active_runs(None, thread_id)
                thread_id = self._get_thread(headers, params, force_reset=True)
                time.sleep(1)

        # ── Parallel message + steps retrieval (with 404/400 retry) ──
        # Small delay to allow API to settle after run completion
        time.sleep(0.5)
        with ThreadPoolExecutor(max_workers=2) as pool:
            msgs_future = pool.submit(
                self._request_with_retry,
                "GET", f"{base_url}/threads/{thread_id}/messages",
                headers, {**params, "limit": 10, "order": "desc"})
            steps_future = pool.submit(
                self._request_with_retry,
                "GET", f"{base_url}/threads/{thread_id}/runs/{run_id}/steps",
                headers, {**params, "limit": 100})
            try:
                msgs_r = msgs_future.result()
                msgs_data_raw_all = msgs_r.json().get("data", [])
            except Exception:
                msgs_data_raw_all = []
            try:
                steps_r = steps_future.result()
                steps_data_raw = steps_r.json().get("data", [])
            except Exception:
                steps_data_raw = []

        # Extract messages filtered by run_id
        our_msgs = [m for m in msgs_data_raw_all
                    if m.get("role") == "assistant" and m.get("run_id") == run_id]
        msgs_data_raw = our_msgs[:1]

        # Build messages in OpenAI-compatible format for the analyzer
        msgs_data = []
        for msg in msgs_data_raw:
            content_items = []
            for c in msg.get("content", []):
                if isinstance(c, dict) and "text" in c:
                    text_obj = c["text"]
                    if isinstance(text_obj, dict) and "value" in text_obj:
                        content_items.append({"text": {"value": text_obj["value"]}})
                    else:
                        content_items.append({"text": {"value": str(text_obj)}})
                else:
                    content_items.append({"text": {"value": str(c)}})
            msgs_data.append({"role": msg.get("role", "unknown"), "content": content_items})

        # Build run_steps in OpenAI-compatible format for the analyzer
        steps_data = []
        for step in steps_data_raw:
            step_dict = {
                "type": step.get("type", "unknown"),
                "status": step.get("status", "unknown"),
                "created_at": step.get("created_at"),
                "completed_at": step.get("completed_at"),
            }
            sd = step.get("step_details", {})
            if isinstance(sd, dict) and "tool_calls" in sd:
                tool_calls = []
                for tc in sd["tool_calls"]:
                    fn = tc.get("function", {})
                    tool_calls.append({
                        "function": {
                            "name": fn.get("name", "?"),
                            "arguments": fn.get("arguments", ""),
                            "output": fn.get("output"),
                        }
                    })
                step_dict["step_details"] = {"tool_calls": tool_calls}
            else:
                step_dict["step_details"] = sd if isinstance(sd, dict) else {"type": step.get("type", "unknown")}
            steps_data.append(step_dict)

        raw = {
            "question": question,
            "run_status": run_status,
            "run_id": run_id,
            "thread_id": thread_id,
            "messages": {"data": msgs_data},
            "run_steps": {"data": steps_data},
        }

        return raw

    def _extract_sql_queries_with_data(self, raw_response: dict) -> list:
        """Extract SQL queries and their result data from raw response."""
        queries = []
        for step in raw_response.get("steps", []):
            details = step.get("step_details", "")
            if details:
                sql = self._extract_sql_from_function_args(details)
                if sql:
                    queries.append({"query": sql, "source": "function_args"})
                sql = self._extract_sql_from_output(details)
                if sql:
                    queries.append({"query": sql, "source": "output"})
        return queries

    def _extract_sql_from_function_args(self, text: str) -> Optional[str]:
        """Extract SQL from function call arguments."""
        return self._find_sql_in_text(text)

    def _extract_sql_from_output(self, text: str) -> Optional[str]:
        """Extract SQL from step output."""
        return self._find_sql_in_text(text)

    def _extract_structured_data_from_output(self, raw_response: dict) -> list:
        """Extract structured data tables from the response."""
        data = []
        for msg in raw_response.get("messages", []):
            if msg.get("role") == "assistant":
                for content in msg.get("content", []):
                    if isinstance(content, str):
                        table = self._extract_markdown_table(content)
                        if table:
                            data.append(table)
                        extracted = self._extract_data_from_text_response(content)
                        if extracted:
                            data.append(extracted)
        return data

    def _extract_markdown_table(self, text: str) -> Optional[list]:
        """Parse a markdown table into a list of dicts."""
        lines = text.strip().split("\n")
        table_lines = [l for l in lines if "|" in l]
        if len(table_lines) < 3:
            return None

        headers = [h.strip() for h in table_lines[0].split("|") if h.strip()]
        rows = []
        for line in table_lines[2:]:  # skip header separator
            cells = [c.strip() for c in line.split("|") if c.strip()]
            if len(cells) == len(headers):
                rows.append(dict(zip(headers, cells)))
        return rows if rows else None

    def _extract_data_from_text_response(self, text: str) -> Optional[dict]:
        """Extract key-value data from text responses."""
        pairs = {}
        for line in text.split("\n"):
            if ":" in line:
                parts = line.split(":", 1)
                key = parts[0].strip().strip("*-")
                val = parts[1].strip()
                if key and val:
                    pairs[key] = val
        return pairs if pairs else None

    def _format_list_data(self, data: list) -> str:
        """Format a list of data items as a readable string."""
        if not data:
            return ""
        return json.dumps(data, indent=2)

    def _extract_data_preview(self, raw_response: dict) -> str:
        """Extract a preview of data from the response."""
        for msg in raw_response.get("messages", []):
            if msg.get("role") == "assistant":
                for content in msg.get("content", []):
                    if isinstance(content, str) and len(content) > 10:
                        return content[:500]
        return ""

    def _extract_sql_queries(self, raw_response: dict) -> list:
        """Extract all SQL/DAX queries from the raw response."""
        queries = []
        for step in raw_response.get("steps", []):
            sql = self._find_sql_in_text(step.get("step_details", ""))
            if sql:
                queries.append(sql)
        return queries

    def _find_sql_in_text(self, text: str) -> Optional[str]:
        """Find SQL/DAX query strings in text."""
        if not text:
            return None
        keywords = ["SELECT", "EVALUATE", "SUMMARIZE", "CALCULATE", "TOPN", "FILTER"]
        for kw in keywords:
            idx = text.upper().find(kw)
            if idx >= 0:
                return text[idx:idx + 500].strip()
        return None


def main():
    """Example usage."""
    thread_name = "example_threadname"
    questions = [
        "what is the total revenue",
        "how many customers do we have",
    ]
    print("FabricDataAgentClient — Example")
    print("Set TENANT_ID and DATA_AGENT_URL environment variables and run.")


if __name__ == "__main__":
    main()
