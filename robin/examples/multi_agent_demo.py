"""Minimal multi-agent literature-search demo: Crow + Falcon, combined.

Crow and Falcon are not local agents — they run on the Edison/FutureHouse
platform. In the current edison_client SDK they are exposed as:

    JobNames.LITERATURE       (was Crow)       — short literature search
    JobNames.LITERATURE_HIGH  (was Falcon)     — deep literature search

This script:
  1. Reads EDISON_API_KEY from .env
  2. Submits one query to Crow (LITERATURE) and one to Falcon (LITERATURE_HIGH)
  3. Polls both concurrently
  4. Concatenates their reports into a single combined answer

Run from the repo root:
    uv run python examples/multi_agent_demo.py "your question here"
"""

from __future__ import annotations

import asyncio
import os
import sys

from dotenv import load_dotenv
from edison_client import EdisonClient, JobNames

CROW = JobNames.LITERATURE
FALCON = JobNames.LITERATURE_HIGH

POLL_INTERVAL_S = 5
OVERALL_TIMEOUT_S = 1800


async def run_agent(client: EdisonClient, job: JobNames, query: str) -> str:
    """Submit a query to one agent and poll until it finishes."""
    task_id = client.create_task({"name": job, "query": query})
    if not isinstance(task_id, str):
        raise RuntimeError(f"{job}: create_task returned non-string id: {task_id!r}")

    while True:
        resp = client.get_task(task_id)
        status = resp.status
        if status == "success":
            return getattr(resp, "answer", None) or str(resp)
        if status in {"queued", "in progress"}:
            await asyncio.sleep(POLL_INTERVAL_S)
            continue
        raise RuntimeError(f"{job} task {task_id} ended in status {status!r}")


async def main(query: str) -> None:
    load_dotenv()
    api_key = os.environ.get("EDISON_API_KEY")
    if not api_key or api_key.startswith("your_"):
        sys.exit("EDISON_API_KEY missing or unfilled in .env")

    client = EdisonClient(api_key=api_key)

    crow_task = asyncio.create_task(run_agent(client, CROW, query))
    falcon_task = asyncio.create_task(run_agent(client, FALCON, query))

    crow_out, falcon_out = await asyncio.wait_for(
        asyncio.gather(crow_task, falcon_task), timeout=OVERALL_TIMEOUT_S
    )

    print("=" * 80)
    print(f"QUERY: {query}")
    print("=" * 80)
    print("\n--- Crow (LITERATURE) ---\n")
    print(crow_out)
    print("\n--- Falcon (LITERATURE_HIGH) ---\n")
    print(falcon_out)
    print("\n--- Combined ---\n")
    print(f"Crow summary:\n{crow_out}\n\nFalcon deep review:\n{falcon_out}")


if __name__ == "__main__":
    q = " ".join(sys.argv[1:]) or "What are emerging therapeutic targets for heart failure?"
    asyncio.run(main(q))
