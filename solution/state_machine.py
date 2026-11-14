"""Lead lifecycle, concurrency rules, state transitions, and scheduling priorities."""
from __future__ import annotations

import logging
import sqlite3
import uuid
from datetime import datetime, timedelta
from typing import Any, Optional

from .clock import in_window
from .config import AppConfig
from .models import IST, LeadState, CallState, FinalState, TERMINAL_DISPOSITIONS

logger = logging.getLogger("dispatcher.state_machine")


def get_tenant_active_count(conn: sqlite3.Connection, tenant_id: str) -> int:
    """Number of calls currently reserving concurrency for this tenant (DIALING or ACTIVE)."""
    cur = conn.execute(
        "SELECT count(*) as count FROM calls WHERE tenant_id = ? AND state IN ('DIALING', 'ACTIVE')",
        (tenant_id,),
    )
    return cur.fetchone()["count"]


def get_active_phones(conn: sqlite3.Connection) -> set[str]:
    """Set of phone numbers with a call currently DIALING or ACTIVE."""
    cur = conn.execute("SELECT DISTINCT phone FROM calls WHERE state IN ('DIALING', 'ACTIVE')")
    return {row["phone"] for row in cur.fetchall()}


def get_dnd_phones(conn: sqlite3.Connection) -> set[str]:
    """Set of phone numbers recorded on the DND registry."""
    cur = conn.execute("SELECT phone FROM dnd_phones")
    return {row["phone"] for row in cur.fetchall()}


def select_next_leads_to_dial(
    conn: sqlite3.Connection,
    config: AppConfig,
    sim_now: datetime,
) -> list[dict[str, Any]]:
    """
    Select eligible leads to dial at sim_now following priority order R6:
      1. Due Callbacks (earliest callback_at first)
      2. Due Retries (earliest next_retry_at first)
      3. Fresh Leads (row_order first)
    Enforces R1 (windows), R2 (tenant concurrency), R3 (single phone lock), R4 (max attempts), R8 (DND).
    Reserves selected leads immediately as DIALING in DB to guarantee concurrency bounds.
    """
    sim_now_iso = sim_now.isoformat()
    reg_start = config.regulatory_window.start
    reg_end = config.regulatory_window.end
    # Ensure current time and a safe 30-minute buffer into the future are inside regulatory window
    reg_open = in_window(sim_now, reg_start, reg_end, None, grace_s=0) and \
               in_window(sim_now + timedelta(minutes=30), reg_start, reg_end, None, grace_s=0)

    if not reg_open:
        # Regulatory window 09:00 - 21:00 is closed or closing within 30 sim minutes
        return []

    allocated_calls = []

    with conn:
        active_phones = get_active_phones(conn)
        dnd_phones = get_dnd_phones(conn)

        cur_tenants = conn.execute("SELECT tenant_id, max_concurrent_calls FROM tenants")
        tenants_db = {row["tenant_id"]: row["max_concurrent_calls"] for row in cur_tenants.fetchall()}

        for tenant_id, max_concurrent in tenants_db.items():
            active_count = get_tenant_active_count(conn, tenant_id)
            slots_available = max_concurrent - active_count
            if slots_available <= 0:
                continue

            # Find campaigns for this tenant whose window is open with a safe 30-minute boundary buffer
            eligible_cids = []
            for cid, camp in config.campaigns.items():
                if camp.tenant_id != tenant_id:
                    continue
                w = camp.window
                # Require current time and 30 sim minutes into future to be in-window
                if in_window(sim_now, w.start, w.end, w.days, grace_s=0) and in_window(
                    sim_now + timedelta(minutes=30), w.start, w.end, w.days, grace_s=0
                ):
                    eligible_cids.append(cid)

            if not eligible_cids:
                continue

            # Query candidate leads across open campaigns for this tenant
            placeholders = ",".join("?" for _ in eligible_cids)
            query = f"""
                SELECT l.*, c.max_attempts
                FROM leads l
                JOIN campaigns c ON l.campaign_id = c.campaign_id
                WHERE l.campaign_id IN ({placeholders})
                  AND l.final_state IS NULL
                  AND l.state IN ('CALLBACK_WAIT', 'RETRY_WAIT', 'PENDING')
                  AND l.attempts < c.max_attempts
                ORDER BY
                  CASE l.state
                    WHEN 'CALLBACK_WAIT' THEN 1
                    WHEN 'RETRY_WAIT' THEN 2
                    WHEN 'PENDING' THEN 3
                    ELSE 4
                  END,
                  l.callback_at ASC,
                  l.next_retry_at ASC,
                  l.row_order ASC
            """
            cur = conn.execute(query, eligible_cids)
            candidates = cur.fetchall()

            for row in candidates:
                if slots_available <= 0:
                    break

                phone = row["phone"]
                cid = row["campaign_id"]
                lid = row["lead_id"]
                state = row["state"]

                # R8: Check global DND
                if phone in dnd_phones:
                    # Update lead to DND_BLOCKED
                    conn.execute(
                        "UPDATE leads SET state = 'DND_BLOCKED', final_state = 'dnd_blocked' WHERE campaign_id = ? AND lead_id = ?",
                        (cid, lid),
                    )
                    continue

                # R3: Check one call per phone
                if phone in active_phones:
                    continue

                # Check schedule readiness
                if state == "CALLBACK_WAIT":
                    if not row["callback_at"] or row["callback_at"] > sim_now_iso:
                        continue
                elif state == "RETRY_WAIT":
                    if not row["next_retry_at"] or row["next_retry_at"] > sim_now_iso:
                        continue

                # Lead is eligible! Reserve slot and create call record
                next_attempt = row["attempts"] + 1
                request_id = f"req_{cid}_{lid}_{next_attempt}_{uuid.uuid4().hex[:8]}"

                conn.execute(
                    """
                    INSERT INTO calls (
                        request_id, tenant_id, campaign_id, lead_id, phone,
                        attempt_no, state, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, 'DIALING', ?)
                    """,
                    (
                        request_id,
                        tenant_id,
                        cid,
                        lid,
                        phone,
                        next_attempt,
                        sim_now_iso,
                    ),
                )

                conn.execute(
                    "UPDATE leads SET state = 'DIALING' WHERE campaign_id = ? AND lead_id = ?",
                    (cid, lid),
                )

                active_phones.add(phone)
                slots_available -= 1

                allocated_calls.append(
                    {
                        "request_id": request_id,
                        "campaign_id": cid,
                        "lead_id": lid,
                        "phone": phone,
                        "tenant_id": tenant_id,
                        "attempt_no": next_attempt,
                    }
                )

    return allocated_calls


def handle_call_accepted(
    conn: sqlite3.Connection,
    request_id: str,
    call_sid: str,
) -> None:
    """Provider accepted call (201 / 200). Transition to ACTIVE if not already ENDED."""
    with conn:
        cur = conn.execute("SELECT * FROM calls WHERE request_id = ?", (request_id,))
        call = cur.fetchone()
        if not call:
            logger.warning(f"Accepted call {request_id} not found in DB")
            return

        cid = call["campaign_id"]
        lid = call["lead_id"]
        attempt_no = call["attempt_no"]

        # If webhook already ended this call, do not overwrite state back to ACTIVE
        if call["state"] == "ENDED":
            conn.execute(
                "UPDATE calls SET call_sid = ? WHERE request_id = ?",
                (call_sid, request_id),
            )
            conn.execute(
                "UPDATE leads SET attempts = MAX(attempts, ?), last_call_sid = ? WHERE campaign_id = ? AND lead_id = ?",
                (attempt_no, call_sid, cid, lid),
            )
            return

        conn.execute(
            "UPDATE calls SET state = 'ACTIVE', call_sid = ? WHERE request_id = ?",
            (call_sid, request_id),
        )
        conn.execute(
            "UPDATE leads SET state = 'ACTIVE', attempts = ?, last_call_sid = ? WHERE campaign_id = ? AND lead_id = ?",
            (attempt_no, call_sid, cid, lid),
        )


def handle_call_rejected_503(
    conn: sqlite3.Connection,
    request_id: str,
) -> None:
    """Provider returned 503 (transient failure). Call definitely was not placed."""
    with conn:
        cur = conn.execute("SELECT * FROM calls WHERE request_id = ?", (request_id,))
        call = cur.fetchone()
        if not call:
            return

        cid = call["campaign_id"]
        lid = call["lead_id"]

        conn.execute("UPDATE calls SET state = 'ABORTED' WHERE request_id = ?", (request_id,))

        # Reset lead back to prior state (PENDING, CALLBACK_WAIT, or RETRY_WAIT)
        cur_lead = conn.execute(
            "SELECT callback_at, next_retry_at, attempts FROM leads WHERE campaign_id = ? AND lead_id = ?",
            (cid, lid),
        )
        lead_row = cur_lead.fetchone()
        if lead_row:
            if lead_row["callback_at"]:
                next_state = "CALLBACK_WAIT"
            elif lead_row["next_retry_at"]:
                next_state = "RETRY_WAIT"
            else:
                next_state = "PENDING"
            conn.execute(
                "UPDATE leads SET state = ? WHERE campaign_id = ? AND lead_id = ?",
                (next_state, cid, lid),
            )


def handle_call_rejected_504_unplaced(
    conn: sqlite3.Connection,
    request_id: str,
) -> None:
    """Ambiguous 504 verified as NOT placed on provider."""
    handle_call_rejected_503(conn, request_id)


def process_call_result(
    conn: sqlite3.Connection,
    result: dict[str, Any],
    config: AppConfig,
) -> bool:
    """
    Process ended call outcome idempotently (from webhook or polling reconciliation).
    Returns True if state was updated, False if already processed or ignored.
    """
    request_id = result.get("request_id")
    call_sid = result.get("call_sid")
    cid = result.get("campaign_id")
    lid = result.get("lead_id")
    phone = result.get("phone")
    status = result.get("status")
    disposition = result.get("disposition")
    callback_at_str = result.get("callback_at")
    started_at_str = result.get("started_at")
    ended_at_str = result.get("ended_at")
    duration_sec = result.get("duration_sec", 0)

    with conn:
        # Find call by request_id or call_sid
        cur = conn.execute(
            "SELECT * FROM calls WHERE request_id = ? OR call_sid = ?",
            (request_id, call_sid),
        )
        call = cur.fetchone()

        if call and call["state"] == "ENDED":
            # Already processed idempotently
            return False

        # If call was not found by request_id, insert it (e.g. edge case)
        if not call:
            camp = config.campaigns.get(cid)
            tenant_id = camp.tenant_id if camp else "unknown"
            conn.execute(
                """
                INSERT OR IGNORE INTO calls (
                    request_id, call_sid, tenant_id, campaign_id, lead_id, phone,
                    attempt_no, state, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, 1, 'ENDED', ?)
                """,
                (request_id or call_sid, call_sid, tenant_id, cid, lid, phone, ended_at_str or ""),
            )
        else:
            conn.execute(
                """
                UPDATE calls SET
                    state = 'ENDED',
                    call_sid = COALESCE(call_sid, ?),
                    status = ?,
                    disposition = ?,
                    started_at = ?,
                    ended_at = ?,
                    duration_sec = ?,
                    callback_at = ?
                WHERE request_id = ? OR call_sid = ?
                """,
                (
                    call_sid,
                    status,
                    disposition,
                    started_at_str,
                    ended_at_str,
                    duration_sec,
                    callback_at_str,
                    request_id,
                    call_sid,
                ),
            )

        # Update lead
        cur_lead = conn.execute(
            "SELECT * FROM leads WHERE campaign_id = ? AND lead_id = ?",
            (cid, lid),
        )
        lead = cur_lead.fetchone()
        if not lead:
            logger.error(f"Lead not found for ended call: {cid}/{lid}")
            return True

        camp_config = config.campaigns.get(cid)
        max_attempts = camp_config.max_attempts if camp_config else 3
        attempts = max(lead["attempts"], call["attempt_no"] if call else 1)

        # Check if phone is already on DND
        cur_dnd = conn.execute("SELECT 1 FROM dnd_phones WHERE phone = ?", (phone,))
        is_phone_dnd = cur_dnd.fetchone() is not None

        # Precedence: completed > exhausted > dnd_blocked (R7, R8, R9)
        final_state: Optional[str] = None
        next_lead_state: str = "PENDING"
        next_callback_at: Optional[str] = None
        next_retry_at: Optional[str] = None

        if disposition in TERMINAL_DISPOSITIONS:
            final_state = FinalState.COMPLETED.value
            next_lead_state = LeadState.COMPLETED.value

            if disposition == "dnd":
                # R8: DND is global
                ended_now = ended_at_str or datetime.now(IST).isoformat()
                conn.execute(
                    "INSERT OR IGNORE INTO dnd_phones (phone, recorded_at, source_call_sid) VALUES (?, ?, ?)",
                    (phone, ended_now, call_sid),
                )
                # Block other leads with this phone that are not completed/exhausted/active
                conn.execute(
                    """
                    UPDATE leads
                    SET state = 'DND_BLOCKED', final_state = 'dnd_blocked'
                    WHERE phone = ?
                      AND final_state IS NULL
                      AND state NOT IN ('ACTIVE', 'DIALING')
                      AND NOT (campaign_id = ? AND lead_id = ?)
                    """,
                    (phone, cid, lid),
                )

        elif attempts >= max_attempts:
            # R4 & R9: Exhausted attempts without terminal disposition
            final_state = FinalState.EXHAUSTED.value
            next_lead_state = LeadState.EXHAUSTED.value

        elif is_phone_dnd or disposition == "dnd":
            # Phone is DND and not terminal/exhausted
            final_state = FinalState.DND_BLOCKED.value
            next_lead_state = LeadState.DND_BLOCKED.value

        elif disposition == "callback_requested":
            # R6: Schedule callback
            next_lead_state = LeadState.CALLBACK_WAIT.value
            next_callback_at = callback_at_str

        else:
            # R5: Retry after no_answer (60m), busy (60m), or failed (15m)
            delay_mins = config.retry_policy.get_delay_minutes(status or "failed")
            ended_dt = datetime.fromisoformat(ended_at_str) if ended_at_str else datetime.now(IST)
            retry_dt = ended_dt + timedelta(minutes=delay_mins)
            next_retry_at = retry_dt.isoformat()
            next_lead_state = LeadState.RETRY_WAIT.value

        conn.execute(
            """
            UPDATE leads SET
                state = ?,
                attempts = ?,
                last_status = ?,
                last_disposition = ?,
                last_call_sid = ?,
                final_state = ?,
                callback_at = ?,
                next_retry_at = ?
            WHERE campaign_id = ? AND lead_id = ?
            """,
            (
                next_lead_state,
                attempts,
                status,
                disposition or "",
                call_sid,
                final_state,
                next_callback_at,
                next_retry_at,
                cid,
                lid,
            ),
        )

    return True


def apply_dnd_to_pending_leads(conn: sqlite3.Connection) -> int:
    """Ensure all non-terminal, non-active leads on DND phones are marked dnd_blocked."""
    with conn:
        cur = conn.execute(
            """
            UPDATE leads
            SET state = 'DND_BLOCKED', final_state = 'dnd_blocked'
            WHERE phone IN (SELECT phone FROM dnd_phones)
              AND final_state IS NULL
              AND state NOT IN ('ACTIVE', 'DIALING')
            """
        )
        return cur.rowcount


def get_progress_summary(conn: sqlite3.Connection) -> dict[str, Any]:
    """Summary of leads progress and active calls."""
    cur = conn.execute(
        """
        SELECT
            count(*) as total,
            sum(CASE WHEN final_state = 'completed' THEN 1 ELSE 0 END) as completed,
            sum(CASE WHEN final_state = 'exhausted' THEN 1 ELSE 0 END) as exhausted,
            sum(CASE WHEN final_state = 'dnd_blocked' THEN 1 ELSE 0 END) as dnd_blocked,
            sum(CASE WHEN final_state IS NULL THEN 1 ELSE 0 END) as remaining,
            sum(CASE WHEN state = 'ACTIVE' THEN 1 ELSE 0 END) as active_leads,
            sum(CASE WHEN state = 'DIALING' THEN 1 ELSE 0 END) as dialing_leads
        FROM leads
        """
    )
    row = cur.fetchone()
    cur_calls = conn.execute(
        "SELECT count(*) as active_calls FROM calls WHERE state IN ('DIALING', 'ACTIVE')"
    )
    active_calls = cur_calls.fetchone()["active_calls"]

    return {
        "total": row["total"] or 0,
        "completed": row["completed"] or 0,
        "exhausted": row["exhausted"] or 0,
        "dnd_blocked": row["dnd_blocked"] or 0,
        "remaining": row["remaining"] or 0,
        "active_calls": active_calls or 0,
    }
