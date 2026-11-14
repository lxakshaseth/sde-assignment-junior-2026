"""Dispatcher loop: concurrency management, lead scheduling, and provider dispatch."""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any, Optional

from .clock import SimClock, in_window, next_eligible_time
from .config import AppConfig
from .db import DB_LOCK, get_db_connection
from .models import IST
from .provider_client import ProviderClient
from .state_machine import (
    apply_dnd_to_pending_leads,
    get_progress_summary,
    handle_call_accepted,
    handle_call_rejected_503,
    handle_call_rejected_504_unplaced,
    process_call_result,
    select_next_leads_to_dial,
)

logger = logging.getLogger("dispatcher.engine")


class Dispatcher:
    def __init__(
        self,
        config: AppConfig,
        clock: SimClock,
        provider_client: ProviderClient,
        callback_url: str = "http://127.0.0.1:8200/webhooks/telephony",
        db_path: str = "dispatcher.db",
    ):
        self.config = config
        self.clock = clock
        self.provider_client = provider_client
        self.callback_url = callback_url
        self.db_path = db_path
        self._running = False

    def get_conn(self):
        return get_db_connection(self.db_path)

    async def startup_recovery(self) -> None:
        """
        Recover from crash / restart.
        Reconciles ambiguous DIALING calls and active calls with the provider.
        """
        logger.info("Executing startup crash recovery...")
        async with DB_LOCK:
            conn = self.get_conn()
            try:
                # 1. Snapshot pre-crash DIALING and ACTIVE calls
                cur = conn.execute("SELECT * FROM calls WHERE state = 'DIALING'")
                dialing_calls = cur.fetchall()

                cur_active = conn.execute("SELECT * FROM calls WHERE state = 'ACTIVE'")
                pre_crash_active_calls = cur_active.fetchall()

                # Reconcile DIALING calls against provider
                for call in dialing_calls:
                    rid = call["request_id"]
                    logger.info(f"Reconciling in-flight dialing call {rid}")
                    provider_call = await self.provider_client.get_call_by_request_id(rid)
                    if provider_call and not provider_call.get("_not_found"):
                        call_sid = provider_call.get("call_sid")
                        if provider_call.get("state") == "ended" and "result" in provider_call:
                            process_call_result(conn, provider_call["result"], self.config)
                        else:
                            handle_call_accepted(conn, rid, call_sid)
                    elif provider_call and provider_call.get("_not_found"):
                        # Definitely 404: Never placed on provider
                        handle_call_rejected_504_unplaced(conn, rid)

                # 2. Reconcile pre-crash ACTIVE calls against provider
                for call in pre_crash_active_calls:
                    sid = call["call_sid"]
                    if sid:
                        provider_call = await self.provider_client.get_call_by_sid(sid)
                        if provider_call and provider_call.get("state") == "ended":
                            if "result" in provider_call:
                                process_call_result(conn, provider_call["result"], self.config)

                # 3. Propagate DND to non-terminal leads
                apply_dnd_to_pending_leads(conn)
                logger.info("Startup crash recovery completed successfully.")
            finally:
                conn.close()

    async def reconcile_active_calls(self) -> None:
        """
        Background reconciliation to catch dropped webhooks or finished calls.
        """
        async with DB_LOCK:
            conn = self.get_conn()
            try:
                # Check calls active for longer than expected
                cur = conn.execute("SELECT * FROM calls WHERE state = 'ACTIVE'")
                active_calls = cur.fetchall()
                for call in active_calls:
                    sid = call["call_sid"]
                    if not sid:
                        continue
                    provider_call = await self.provider_client.get_call_by_sid(sid)
                    if provider_call and provider_call.get("state") == "ended":
                        if "result" in provider_call:
                            logger.info(f"Reconciled ended call {sid} (webhook was likely lost)")
                            process_call_result(conn, provider_call["result"], self.config)

                # Check any stuck DIALING calls
                cur_dialing = conn.execute("SELECT * FROM calls WHERE state = 'DIALING'")
                for call in cur_dialing.fetchall():
                    rid = call["request_id"]
                    provider_call = await self.provider_client.get_call_by_request_id(rid)
                    if provider_call and not provider_call.get("_not_found"):
                        if provider_call.get("state") == "ended" and "result" in provider_call:
                            process_call_result(conn, provider_call["result"], self.config)
                        else:
                            handle_call_accepted(conn, rid, provider_call.get("call_sid"))
                    elif provider_call and provider_call.get("_not_found"):
                        handle_call_rejected_504_unplaced(conn, rid)

            except Exception as exc:
                logger.warning(f"Error during call reconciliation: {exc}")
            finally:
                conn.close()

    async def dispatch_step(self) -> int:
        """
        Execute one dispatch step: select eligible leads, place calls to provider.
        Returns number of calls dispatched.
        """
        sim_now = self.clock.now()
        async with DB_LOCK:
            conn = self.get_conn()
            try:
                prepared_calls = select_next_leads_to_dial(conn, self.config, sim_now)
            finally:
                conn.close()

        if not prepared_calls:
            return 0

        logger.info(f"Dispatching {len(prepared_calls)} calls at sim_time={sim_now.isoformat()}")

        # Place calls concurrently
        tasks = [self._place_single_call(c) for c in prepared_calls]
        await asyncio.gather(*tasks)
        return len(prepared_calls)

    async def _place_single_call(self, call_spec: dict[str, Any]) -> None:
        rid = call_spec["request_id"]
        cid = call_spec["campaign_id"]
        lid = call_spec["lead_id"]
        phone = call_spec["phone"]

        # R1 pre-dial safety check: verify window is strictly open with at least 10-minute buffer
        camp = self.config.campaigns.get(cid)
        if camp:
            sim_now_check = self.clock.now()
            rw = self.config.regulatory_window
            if not in_window(sim_now_check + timedelta(minutes=10), camp.window.start, camp.window.end, camp.window.days, grace_s=0) or \
               not in_window(sim_now_check + timedelta(minutes=10), rw.start, rw.end, None, grace_s=0):
                logger.warning(
                    f"Calling window near closure before placing {rid} ({sim_now_check.isoformat()}). Aborting safely."
                )
                async with DB_LOCK:
                    conn = self.get_conn()
                    try:
                        handle_call_rejected_503(conn, rid)
                    finally:
                        conn.close()
                return

        status, body = await self.provider_client.place_call(
            request_id=rid,
            campaign_id=cid,
            lead_id=lid,
            phone=phone,
            callback_url=self.callback_url,
        )

        async with DB_LOCK:
            conn = self.get_conn()
            try:
                if status in (200, 201):
                    call_sid = body.get("call_sid") if body else None
                    handle_call_accepted(conn, rid, call_sid)
                    # If provider already included ended result in body:
                    if body and body.get("state") == "ended" and "result" in body:
                        process_call_result(conn, body["result"], self.config)
                elif status == 503:
                    logger.info(f"Call {rid} failed with 503; returning lead to pending queue")
                    handle_call_rejected_503(conn, rid)
                elif status == 504:
                    # If definitely verified as not placed:
                    if body and body.get("detail") == "call_not_placed":
                        handle_call_rejected_504_unplaced(conn, rid)
                    # Else still unresolved due to transient network glitch; remains DIALING for reconciliation
                else:
                    logger.error(f"Call {rid} rejected by provider with status {status}: {body}")
                    handle_call_rejected_503(conn, rid)
            finally:
                conn.close()

    async def is_finished(self) -> bool:
        """True if every lead has reached a final state and no calls are active."""
        async with DB_LOCK:
            conn = self.get_conn()
            try:
                cur = conn.execute(
                    """
                    SELECT
                        sum(CASE WHEN final_state IS NULL THEN 1 ELSE 0 END) as remaining,
                        (SELECT count(*) FROM calls WHERE state IN ('DIALING', 'ACTIVE')) as active_calls
                    FROM leads
                    """
                )
                row = cur.fetchone()
                remaining = row["remaining"] or 0
                active_calls = row["active_calls"] or 0
                return (remaining == 0) and (active_calls == 0)
            finally:
                conn.close()

    async def calculate_sleep_seconds(self) -> float:
        """
        Compute optimal real sleep time until the earliest upcoming lead event,
        bounded by [0.05, 1.0] real seconds for responsiveness and reconciliation.
        """
        sim_now = self.clock.now()
        earliest_event: Optional[datetime] = None

        async with DB_LOCK:
            conn = self.get_conn()
            try:
                # Check earliest callback
                cur_cb = conn.execute(
                    "SELECT min(callback_at) as earliest FROM leads WHERE state = 'CALLBACK_WAIT' AND final_state IS NULL"
                )
                row_cb = cur_cb.fetchone()
                if row_cb and row_cb["earliest"]:
                    dt_cb = datetime.fromisoformat(row_cb["earliest"]).astimezone(IST)
                    earliest_event = dt_cb

                # Check earliest retry
                cur_ret = conn.execute(
                    "SELECT min(next_retry_at) as earliest FROM leads WHERE state = 'RETRY_WAIT' AND final_state IS NULL"
                )
                row_ret = cur_ret.fetchone()
                if row_ret and row_ret["earliest"]:
                    dt_ret = datetime.fromisoformat(row_ret["earliest"]).astimezone(IST)
                    if earliest_event is None or dt_ret < earliest_event:
                        earliest_event = dt_ret

                # Check window openings for campaigns that have pending/waiting leads
                cur_camps = conn.execute(
                    "SELECT DISTINCT campaign_id FROM leads WHERE final_state IS NULL"
                )
                for camp_row in cur_camps.fetchall():
                    cid = camp_row["campaign_id"]
                    camp = self.config.campaigns.get(cid)
                    if not camp:
                        continue
                    next_open_dt = next_eligible_time(
                        sim_now,
                        camp.window.start,
                        camp.window.end,
                        camp.window.days,
                        self.config.regulatory_window.start,
                        self.config.regulatory_window.end,
                    )
                    if earliest_event is None or next_open_dt < earliest_event:
                        earliest_event = next_open_dt

            finally:
                conn.close()

        if earliest_event is not None and earliest_event > sim_now:
            real_secs = (earliest_event - sim_now).total_seconds() / self.clock.speed
            return max(0.05, min(real_secs, 1.0))

        return 0.1

    async def run(self) -> None:
        """Main dispatcher loop."""
        self._running = True
        logger.info("Dispatcher engine started.")
        reconcile_counter = 0

        while self._running:
            if await self.is_finished():
                logger.info("All leads reached final state and all calls ended. Dispatcher finished.")
                break

            reconcile_counter += 1
            if reconcile_counter >= 5:  # Every ~1 second
                reconcile_counter = 0
                await self.reconcile_active_calls()

            dispatched = await self.dispatch_step()

            if dispatched == 0:
                sleep_sec = await self.calculate_sleep_seconds()
                await asyncio.sleep(sleep_sec)
            else:
                # If we placed calls, yield briefly to let event loop handle network/webhooks
                await asyncio.sleep(0.02)

    def stop(self) -> None:
        self._running = False
