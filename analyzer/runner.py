"""Question runner — parallel and serial execution with retry and graceful interruption."""

import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

MAX_RETRIES = 2
RETRY_DELAY_BASE = 3  # seconds


def _is_retryable(error_str):
    """Check if an error is transient and worth retrying."""
    retryable = ["429", "503", "timeout", "throttl", "temporarily unavailable",
                 "connection", "reset by peer"]
    lower = error_str.lower()
    return any(kw in lower for kw in retryable)


def _run_single_question(client, question, idx, total, max_retries=MAX_RETRIES):
    """Run one question against the agent with retry on transient errors."""
    for attempt in range(max_retries + 1):
        t0 = time.monotonic()
        try:
            run_details = client.get_raw_run_response(question)
            elapsed = time.monotonic() - t0

            # Extract answer
            answer = ""
            for msg in run_details.get("messages", {}).get("data", []):
                if msg.get("role") == "assistant":
                    content = msg.get("content", [])
                    if content and isinstance(content[0], dict):
                        text = content[0].get("text", {})
                        answer = text.get("value", str(text)) if isinstance(text, dict) else str(text)

            # Extract tool chain
            tools = []
            for step in run_details.get("run_steps", {}).get("data", []):
                for tc in (step.get("step_details") or {}).get("tool_calls", []):
                    tools.append(tc.get("function", {}).get("name", "?"))

            # Compute duration from steps
            steps = run_details.get("run_steps", {}).get("data", [])
            all_created = [s.get("created_at") or 0 for s in steps if s.get("created_at")]
            all_completed = [s.get("completed_at") or 0 for s in steps if s.get("completed_at")]
            step_duration = (max(all_completed) - min(all_created)) if all_created and all_completed else None

            status = run_details.get("run_status", "unknown")
            icon = "+" if status == "completed" else "X"
            print(f"  {icon} [{idx}/{total}] ({elapsed:.1f}s) {question[:60]}")

            return {
                "question": question, "index": idx, "status": status,
                "answer": answer[:300], "tools": tools,
                "duration_wall": round(elapsed, 2), "duration_steps": step_duration,
                "run_details": run_details, "error": None,
            }

        except Exception as e:
            elapsed = time.monotonic() - t0
            error_str = str(e)

            # Retry on transient errors
            if attempt < max_retries and _is_retryable(error_str):
                delay = RETRY_DELAY_BASE * (2 ** attempt)
                print(f"  ~ [{idx}/{total}] Retry {attempt+1}/{max_retries} in {delay}s: {error_str[:80]}")
                time.sleep(delay)
                continue

            print(f"  X [{idx}/{total}] ({elapsed:.1f}s) {question[:60]} -> ERROR: {e}")
            return {
                "question": question, "index": idx, "status": "error",
                "answer": "", "tools": [],
                "duration_wall": round(elapsed, 2), "duration_steps": None,
                "run_details": {}, "error": error_str,
            }

    # Should not reach here, but safety net
    return {
        "question": question, "index": idx, "status": "error",
        "answer": "", "tools": [],
        "duration_wall": 0, "duration_steps": None,
        "run_details": {}, "error": "Max retries exceeded",
    }


def run_questions_parallel(session, questions, cfg):
    """Run all questions in parallel with graceful Ctrl+C handling.

    On KeyboardInterrupt, collects completed results and returns them
    so the caller can still save partial results.
    """
    max_w = min(cfg.get("max_workers", 4), len(questions))
    print(f"  Running {len(questions)} questions with {max_w} workers...\n")
    t0 = time.monotonic()

    results = [None] * len(questions)
    interrupted = False

    try:
        with ThreadPoolExecutor(max_workers=max_w) as pool:
            futures = {
                pool.submit(_run_single_question, session.client, q, i + 1, len(questions)): i
                for i, q in enumerate(questions)
            }
            for future in as_completed(futures):
                idx = futures[future]
                try:
                    results[idx] = future.result()
                except Exception as e:
                    results[idx] = {
                        "question": questions[idx], "index": idx + 1, "status": "error",
                        "answer": "", "tools": [],
                        "duration_wall": 0, "duration_steps": None,
                        "run_details": {}, "error": str(e),
                    }
    except KeyboardInterrupt:
        interrupted = True
        print("\n  ** Interrupted! Saving completed results... **")

    # Fill any None slots (questions that didn't start before interrupt)
    for i, r in enumerate(results):
        if r is None:
            results[i] = {
                "question": questions[i], "index": i + 1, "status": "cancelled",
                "answer": "", "tools": [],
                "duration_wall": 0, "duration_steps": None,
                "run_details": {}, "error": "Cancelled by user",
            }

    total_wall = time.monotonic() - t0
    completed = sum(1 for r in results if r.get("status") not in ("cancelled", "error"))
    serial_sum = sum(r["duration_wall"] for r in results)
    print(f"\n  Wall time: {total_wall:.1f}s ({completed}/{len(questions)} completed"
          f", vs ~{serial_sum:.1f}s serial)")

    return results, round(total_wall, 2), interrupted


def run_questions_serial(session, questions, cfg):
    """Run questions sequentially with graceful Ctrl+C handling."""
    print(f"  Running {len(questions)} questions sequentially...\n")
    t0 = time.monotonic()
    results = []
    interrupted = False

    for i, q in enumerate(questions):
        try:
            r = _run_single_question(session.client, q, i + 1, len(questions))
            results.append(r)
        except KeyboardInterrupt:
            interrupted = True
            print("\n  ** Interrupted! Saving completed results... **")
            # Fill remaining
            for j in range(i, len(questions)):
                results.append({
                    "question": questions[j], "index": j + 1, "status": "cancelled",
                    "answer": "", "tools": [],
                    "duration_wall": 0, "duration_steps": None,
                    "run_details": {}, "error": "Cancelled by user",
                })
            break

    total_wall = time.monotonic() - t0
    print(f"\n  Total wall time: {total_wall:.1f}s")
    return results, round(total_wall, 2), interrupted
