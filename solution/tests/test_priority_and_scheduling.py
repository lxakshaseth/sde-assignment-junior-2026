"""Tests for Rule R5 (retry backoff) and Rule R6 (priority ordering: callbacks > retries > fresh)."""
import sqlite3
from datetime import datetime, timedelta

import pytest

from solution.clock import SimClock
from solution.config import AppConfig, CampaignConfig, CampaignWindow, RegulatoryWindow, RetryPolicy, TenantConfig
from solution.db import SCHEMA_SQL
from solution.models import IST
from solution.state_machine import process_call_result, select_next_leads_to_dial


def dt(h: int, m: int) -> datetime:
    return datetime(2026, 11, 12, h, m, tzinfo=IST)


@pytest.fixture
def mock_config():
    tenants = {"t1": TenantConfig(tenant_id="t1", max_concurrent_calls=2)}
    campaigns = {
        "c1": CampaignConfig(
            campaign_id="c1",
            tenant_id="t1",
            leads_file="",
            window=CampaignWindow(start="10:00", end="19:00", days=["thu"]),
            max_attempts=3,
        )
    }
    return AppConfig(
        fixtures_path="",
        fixtures_dir="",
        sim_start=dt(12, 0),
        timezone_name="Asia/Kolkata",
        regulatory_window=RegulatoryWindow(start="09:00", end="21:00"),
        window_grace_seconds=120,
        retry_policy=RetryPolicy(no_answer_minutes=60, busy_minutes=60, failed_minutes=15),
        webhook_secret="secret",
        tenants=tenants,
        campaigns=campaigns,
        raw_data={},
    )


@pytest.fixture
def memory_db(mock_config):
    conn = sqlite3.connect(":memory:", autocommit=False)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL)
    with conn:
        conn.execute("INSERT INTO tenants VALUES ('t1', 2)")
        conn.execute("INSERT INTO campaigns VALUES ('c1', 't1', '10:00', '19:00', 'thu', 3)")
    return conn


def test_priority_due_callback_over_retry_and_fresh(memory_db, mock_config):
    """Rule R6: Due callbacks go before retries, and retries before fresh leads."""
    sim_now = dt(14, 0)
    with memory_db:
        # Fresh lead
        memory_db.execute(
            "INSERT INTO leads (campaign_id, lead_id, phone, row_order, state, attempts) VALUES ('c1', 'fresh_1', '+919000000001', 1, 'PENDING', 0)"
        )
        # Due retry
        memory_db.execute(
            """
            INSERT INTO leads (campaign_id, lead_id, phone, row_order, state, attempts, next_retry_at)
            VALUES ('c1', 'retry_1', '+919000000002', 2, 'RETRY_WAIT', 1, ?)
            """,
            (dt(13, 30).isoformat(),),
        )
        # Due callback
        memory_db.execute(
            """
            INSERT INTO leads (campaign_id, lead_id, phone, row_order, state, attempts, callback_at)
            VALUES ('c1', 'cb_1', '+919000000003', 3, 'CALLBACK_WAIT', 1, ?)
            """,
            (dt(13, 45).isoformat(),),
        )

    # 1 slot available
    with memory_db:
        memory_db.execute("UPDATE tenants SET max_concurrent_calls = 1 WHERE tenant_id = 't1'")

    chosen = select_next_leads_to_dial(memory_db, mock_config, sim_now)
    assert len(chosen) == 1
    assert chosen[0]["lead_id"] == "cb_1"


def test_priority_due_retry_over_fresh_when_callback_not_due(memory_db, mock_config):
    """When callback is in the future, due retry beats fresh lead."""
    sim_now = dt(14, 0)
    with memory_db:
        memory_db.execute("UPDATE tenants SET max_concurrent_calls = 1 WHERE tenant_id = 't1'")
        # Fresh lead
        memory_db.execute(
            "INSERT INTO leads (campaign_id, lead_id, phone, row_order, state, attempts) VALUES ('c1', 'fresh_1', '+919000000001', 1, 'PENDING', 0)"
        )
        # Due retry
        memory_db.execute(
            """
            INSERT INTO leads (campaign_id, lead_id, phone, row_order, state, attempts, next_retry_at)
            VALUES ('c1', 'retry_1', '+919000000002', 2, 'RETRY_WAIT', 1, ?)
            """,
            (dt(13, 30).isoformat(),),
        )
        # Future callback (not due yet: 14:15 > 14:00)
        memory_db.execute(
            """
            INSERT INTO leads (campaign_id, lead_id, phone, row_order, state, attempts, callback_at)
            VALUES ('c1', 'cb_future', '+919000000003', 3, 'CALLBACK_WAIT', 1, ?)
            """,
            (dt(14, 15).isoformat(),),
        )

    chosen = select_next_leads_to_dial(memory_db, mock_config, sim_now)
    assert len(chosen) == 1
    assert chosen[0]["lead_id"] == "retry_1"


def test_callback_lateness_minimization_ordering(memory_db, mock_config):
    """Among multiple due callbacks, earliest callback_at is picked first."""
    sim_now = dt(15, 0)
    with memory_db:
        memory_db.execute("UPDATE tenants SET max_concurrent_calls = 1 WHERE tenant_id = 't1'")
        # cb_later was scheduled for 14:30
        memory_db.execute(
            """
            INSERT INTO leads (campaign_id, lead_id, phone, row_order, state, attempts, callback_at)
            VALUES ('c1', 'cb_later', '+919000000001', 1, 'CALLBACK_WAIT', 1, ?)
            """,
            (dt(14, 30).isoformat(),),
        )
        # cb_earlier was scheduled for 13:30 (more overdue)
        memory_db.execute(
            """
            INSERT INTO leads (campaign_id, lead_id, phone, row_order, state, attempts, callback_at)
            VALUES ('c1', 'cb_earlier', '+919000000002', 2, 'CALLBACK_WAIT', 1, ?)
            """,
            (dt(13, 30).isoformat(),),
        )

    chosen = select_next_leads_to_dial(memory_db, mock_config, sim_now)
    assert len(chosen) == 1
    assert chosen[0]["lead_id"] == "cb_earlier"


def test_retry_delay_calculations(memory_db, mock_config):
    """Rule R5: 60m for no_answer/busy, 15m for failed."""
    with memory_db:
        memory_db.execute(
            "INSERT INTO leads (campaign_id, lead_id, phone, row_order, state, attempts) VALUES ('c1', 'l1', '+919000000001', 1, 'ACTIVE', 1)"
        )
        memory_db.execute(
            "INSERT INTO calls (request_id, tenant_id, campaign_id, lead_id, phone, attempt_no, state, created_at) VALUES ('req1', 't1', 'c1', 'l1', '+919000000001', 1, 'ACTIVE', ?)",
            (dt(12, 0).isoformat(),),
        )

    # 1. no_answer at 12:05 -> next retry at 13:05 (+60m)
    res_no_answer = {
        "request_id": "req1",
        "call_sid": "CA001",
        "campaign_id": "c1",
        "lead_id": "l1",
        "phone": "+919000000001",
        "status": "no_answer",
        "disposition": None,
        "ended_at": dt(12, 5).isoformat(),
    }
    process_call_result(memory_db, res_no_answer, mock_config)
    cur = memory_db.execute("SELECT state, next_retry_at FROM leads WHERE lead_id = 'l1'")
    row = cur.fetchone()
    assert row["state"] == "RETRY_WAIT"
    assert row["next_retry_at"] == dt(13, 5).isoformat()

    # 2. failed at 13:10 -> next retry at 13:25 (+15m)
    with memory_db:
        memory_db.execute(
            "INSERT INTO calls (request_id, tenant_id, campaign_id, lead_id, phone, attempt_no, state, created_at) VALUES ('req2', 't1', 'c1', 'l1', '+919000000001', 2, 'ACTIVE', ?)",
            (dt(13, 5).isoformat(),),
        )
    res_failed = {
        "request_id": "req2",
        "call_sid": "CA002",
        "campaign_id": "c1",
        "lead_id": "l1",
        "phone": "+919000000001",
        "status": "failed",
        "disposition": None,
        "ended_at": dt(13, 10).isoformat(),
    }
    process_call_result(memory_db, res_failed, mock_config)
    cur = memory_db.execute("SELECT state, next_retry_at FROM leads WHERE lead_id = 'l1'")
    row = cur.fetchone()
    assert row["state"] == "RETRY_WAIT"
    assert row["next_retry_at"] == dt(13, 25).isoformat()
