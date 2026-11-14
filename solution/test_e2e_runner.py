"""End-to-end integration runner for testing with mock telephony."""
from __future__ import annotations

import asyncio
import logging
import os
import shutil
import subprocess
import sys
import time

sys.path.insert(0, os.path.abspath("."))
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

import httpx

from solution.cli import run_orchestrator
from solution.orchestrator import export_final_state_csv
from solution.provider_client import ProviderClient


async def wait_for_provider(provider_url: str, timeout: float = 10.0):
    start = time.time()
    while time.time() - start < timeout:
        try:
            async with httpx.AsyncClient(timeout=1.0) as client:
                resp = await client.get(f"{provider_url}/v1/clock")
                if resp.status_code == 200:
                    return True
        except Exception:
            pass
        await asyncio.sleep(0.2)
    return False


async def run_e2e(
    speed: float = 1200.0,
    seed: int = 7,
    failure_scale: float = 1.0,
    kill_after_sec: float = 0.0,
):
    print("=" * 60)
    print(f"STARTING E2E RUN (speed={speed}x, seed={seed}, failure_scale={failure_scale}, kill_after={kill_after_sec}s)")
    print("=" * 60)

    # Clean up old database and csv if starting fresh
    if os.path.exists("dispatcher.db"):
        os.remove("dispatcher.db")
    if os.path.exists("dispatcher.db-wal"):
        os.remove("dispatcher.db-wal")
    if os.path.exists("dispatcher.db-shm"):
        os.remove("dispatcher.db-shm")
    if os.path.exists("final_state.csv"):
        os.remove("final_state.csv")

    python_exe = sys.executable

    # Start mock telephony in a subprocess
    mock_cmd = [
        python_exe,
        "-m",
        "mock_telephony",
        "--speed",
        str(speed),
        "--seed",
        str(seed),
        "--failure-scale",
        str(failure_scale),
        "--port",
        "8099",
    ]
    mock_cwd = os.path.abspath("assignment-b-workflow")
    mock_proc = subprocess.Popen(
        mock_cmd,
        cwd=mock_cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    try:
        ready = await wait_for_provider("http://127.0.0.1:8099", timeout=15.0)
        if not ready:
            print("ERROR: Mock telephony provider did not start in time.")
            return

        print("Mock telephony is ready!")

        if kill_after_sec > 0:
            print(f"Simulating kill -9 crash after {kill_after_sec}s...")
            # Run dispatcher in subprocess so we can kill -9 it
            disp_cmd = [
                python_exe,
                "-m",
                "solution.cli",
                "run",
                "--fixtures",
                "assignment-b-workflow/fixtures/campaigns.json",
                "--provider-url",
                "http://127.0.0.1:8099",
                "--webhook-port",
                "8200",
                "--db",
                "dispatcher.db",
            ]
            disp_proc = subprocess.Popen(disp_cmd)
            await asyncio.sleep(kill_after_sec)

            print("Simulating KILL -9...")
            disp_proc.kill()
            disp_proc.wait()
            print("Dispatcher killed abruptly! Waiting 1s before restarting...")
            await asyncio.sleep(1.0)

            print("Restarting dispatcher after crash...")
            disp_proc_resumed = subprocess.Popen(disp_cmd)
            disp_proc_resumed.wait()
            print("Resumed dispatcher finished.")

        else:
            # Run directly to completion
            res = await run_orchestrator(
                fixtures_path="assignment-b-workflow/fixtures/campaigns.json",
                provider_url="http://127.0.0.1:8099",
                webhook_host="127.0.0.1",
                webhook_port=8200,
                db_path="dispatcher.db",
                output_csv="final_state.csv",
            )

        # Inspect provider admin endpoints
        client = ProviderClient("http://127.0.0.1:8099")
        report = await client.get_admin_report()
        admin_leads = await client.get_admin_leads()

        # Check final_state.csv
        rows = export_final_state_csv("dispatcher.db", "final_state.csv")
        lead_dict = {(r["campaign_id"], r["lead_id"]): r for r in rows}

        mismatches = []
        for al in admin_leads:
            k = (al["campaign_id"], al["lead_id"])
            ol = lead_dict.get(k)
            if not ol:
                mismatches.append(f"Missing {k}")
            elif ol["final_state"] != al["expected_state"]:
                mismatches.append(
                    f"State mismatch {k}: got {ol['final_state']}, expected {al['expected_state']}"
                )
            elif ol["attempts"] != al["attempts"]:
                mismatches.append(
                    f"Attempts mismatch {k}: got {ol['attempts']}, expected {al['attempts']}"
                )

        print("\n" + "=" * 60)
        print("E2E RESULTS SUMMARY:")
        print(f"Total calls: {report.get('calls_total')}")
        print(f"Violation count: {report.get('violation_count')}")
        print(f"Violations by type: {report.get('violations_by_type')}")
        if report.get("violations"):
            print("Violations:", report["violations"])
        print(f"Mismatches with provider expected states: {len(mismatches)}")
        if mismatches:
            for m in mismatches[:10]:
                print(f"  {m}")
        print(f"Callback lateness (minutes): {report.get('callback_lateness_minutes')}")
        print(f"Webhook stats: {report.get('webhooks')}")
        print("=" * 60 + "\n")

    finally:
        mock_proc.terminate()
        try:
            mock_proc.wait(timeout=3)
        except Exception:
            mock_proc.kill()


if __name__ == "__main__":
    speed = float(sys.argv[1]) if len(sys.argv) > 1 else 1200.0
    seed = int(sys.argv[2]) if len(sys.argv) > 2 else 7
    scale = float(sys.argv[3]) if len(sys.argv) > 3 else 1.0
    kill_s = float(sys.argv[4]) if len(sys.argv) > 4 else 0.0
    asyncio.run(run_e2e(speed=speed, seed=seed, failure_scale=scale, kill_after_sec=kill_s))
