"""Main orchestrator: boots webhook server, runs dispatcher, exports final_state.csv, and checks reports."""
from __future__ import annotations

import asyncio
import csv
import logging
import os
import sqlite3
from typing import Optional

import uvicorn

from .clock import SimClock
from .config import AppConfig
from .db import get_db_connection, init_db
from .dispatcher import Dispatcher
from .models import IST
from .provider_client import ProviderClient
from .webhook_server import create_webhook_app

logger = logging.getLogger("dispatcher.orchestrator")


def export_final_state_csv(
    db_path: str = "dispatcher.db",
    output_path: str = "final_state.csv",
) -> list[dict]:
    """Export final_state.csv conforming to assignment specification."""
    conn = get_db_connection(db_path)
    try:
        cur = conn.execute(
            """
            SELECT
                campaign_id,
                lead_id,
                COALESCE(final_state, 'incomplete') as final_state,
                attempts,
                COALESCE(last_status, '') as last_status,
                COALESCE(last_disposition, '') as last_disposition
            FROM leads
            ORDER BY campaign_id ASC, lead_id ASC
            """
        )
        rows = [dict(r) for r in cur.fetchall()]

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "campaign_id",
                "lead_id",
                "final_state",
                "attempts",
                "last_status",
                "last_disposition",
            ])
            for r in rows:
                writer.writerow([
                    r["campaign_id"],
                    r["lead_id"],
                    r["final_state"],
                    r["attempts"],
                    r["last_status"],
                    r["last_disposition"],
                ])

        logger.info(f"Wrote {len(rows)} records to {output_path}")
        return rows
    finally:
        conn.close()


async def run_orchestrator(
    fixtures_path: str = "assignment-b-workflow/fixtures/campaigns.json",
    provider_url: str = "http://127.0.0.1:8099",
    webhook_host: str = "127.0.0.1",
    webhook_port: int = 8200,
    db_path: str = "dispatcher.db",
    output_csv: str = "final_state.csv",
    auto_exit: bool = True,
) -> dict:
    """Run full workflow lifecycle from startup to completion."""
    config = AppConfig.load(fixtures_path)

    # Initialize DB schema & load leads
    conn = get_db_connection(db_path)
    init_db(conn, config)
    conn.close()

    # Telephony Provider client
    provider_client = ProviderClient(base_url=provider_url)

    # Sync clock with provider
    logger.info(f"Connecting to provider at {provider_url}...")
    clock_info = await provider_client.get_clock()
    clock = SimClock(
        sim_start=config.sim_start,
        speed=clock_info.get("speed", 600.0),
    )
    clock.sync_from_provider(
        sim_epoch=clock_info["sim_epoch"],
        real_epoch=clock_info["real_epoch"],
        speed=clock_info["speed"],
    )
    logger.info(
        f"Clock synced: sim_now={clock.now().isoformat()} speed={clock.speed}x"
    )

    # Create Webhook receiver
    webhook_app = create_webhook_app(config=config, db_path=db_path)
    uvi_config = uvicorn.Config(
        app=webhook_app,
        host=webhook_host,
        port=webhook_port,
        log_level="warning",
    )
    server = uvicorn.Server(uvi_config)
    server_task = asyncio.create_task(server.serve())

    # Give webhook server a moment to bind
    await asyncio.sleep(0.5)

    callback_url = f"http://{webhook_host}:{webhook_port}/webhooks/telephony"

    # Instantiate Dispatcher
    dispatcher = Dispatcher(
        config=config,
        clock=clock,
        provider_client=provider_client,
        callback_url=callback_url,
        db_path=db_path,
    )

    try:
        # Crash recovery
        await dispatcher.startup_recovery()

        # Run dispatch loop
        await dispatcher.run()

        # Final pass reconciliation
        await dispatcher.reconcile_active_calls()

        # Export final state CSV
        rows = export_final_state_csv(db_path, output_csv)

        # Validate with provider admin endpoints
        admin_report = await provider_client.get_admin_report()
        admin_leads = await provider_client.get_admin_leads()

        # Compare outputs
        mismatches = []
        lead_dict = {(r["campaign_id"], r["lead_id"]): r for r in rows}
        for admin_lead in admin_leads:
            key = (admin_lead["campaign_id"], admin_lead["lead_id"])
            ours = lead_dict.get(key)
            if not ours:
                mismatches.append(f"Missing lead {key}")
                continue
            if ours["final_state"] != admin_lead["expected_state"]:
                mismatches.append(
                    f"State mismatch {key}: ours={ours['final_state']} expected={admin_lead['expected_state']}"
                )
            if ours["attempts"] != admin_lead["attempts"]:
                mismatches.append(
                    f"Attempts mismatch {key}: ours={ours['attempts']} expected={admin_lead['attempts']}"
                )

        logger.info("=" * 60)
        logger.info("RUN COMPLETED")
        logger.info(f"Violations recorded by provider: {admin_report.get('violation_count')}")
        logger.info(f"Violations by type: {admin_report.get('violations_by_type')}")
        logger.info(f"Mismatches with /admin/leads: {len(mismatches)}")
        if mismatches:
            for m in mismatches[:10]:
                logger.warning(f"  Mismatch: {m}")
        logger.info("=" * 60)

        return {
            "admin_report": admin_report,
            "mismatches": mismatches,
            "rows": rows,
        }

    finally:
        server.should_exit = True
        await server_task
        await provider_client.aclose()
