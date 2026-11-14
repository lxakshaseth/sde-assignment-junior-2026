"""Tests for state machine business rules: concurrency (R2), phone lock (R3), attempts (R4), terminal (R7), DND (R8), precedence (R9)."""
import sqlite3
from datetime import datetime

import pytest

from solution.config import AppConfig, CampaignConfig, CampaignWindow, RegulatoryWindow, RetryPolicy, TenantConfig
from solution.db import SCHEMA_SQL
from solution.models import IST
from solution.state_machine import process_call_result, select_next_leads_to_dial


def dt(h: int, m: int) -> datetime:
    return datetime(2026, 11, 12, h, m, tzinfo=IST)


@pytest.fixture
def mock_app():
    tenants = {
        "acme": TenantConfig(tenant_id="acme", max_concurrent_calls=2),
        "zen": TenantConfig(tenant_id="zen", max_concurrent_calls=1),
    }
    campaigns = {
        "c_acme_1": CampaignConfig(
            campaign_id="c_acme_1",
            tenant_id="acme",
            leads_file="",
            window=CampaignWindow(start="10:00", end="19:00", days=["thu"]),
            max_attempts=3,
        ),
        "c_acme_2": CampaignConfig(
            campaign_id="c_acme_2",
            tenant_id="acme",
            leads_file="",
            window=CampaignWindow(start="10:00", end="19:00", days=["thu"]),
            max_attempts=3,
        ),
    }
    return AppConfig(
        fixtures_path="",
        fixtures_dir="",
        sim_start=dt(12, 0),
        timezone_name="Asia/Kolkata",
        regulatory_window=RegulatoryWindow(start="09:00", end="21:00"),
        window_grace_seconds=120,
        retry_policy=RetryPolicy(),
        webhook_secret="secret",
        tenants=tenants,
        campaigns=campaigns,
        raw_data={},
    )


@pytest.fixture
def memory_db():
    conn = sqlite3.connect(":memory:", autocommit=False)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL)
    with conn:
        conn.execute("INSERT INTO tenants VALUES ('acme', 2)")
        conn.execute("INSERT INTO tenants VALUES ('zen', 1)")
        conn.execute("INSERT INTO campaigns VALUES ('c_acme_1', 'acme', '10:00', '19:00', 'thu', 3)")
        conn.execute("INSERT INTO campaigns VALUES ('c_acme_2', 'acme', '10:00', '19:00', 'thu', 3)")
    return conn


def test_tenant_concurrency_shared_across_campaigns(memory_db, mock_app):
    """Rule R2: Tenant concurrency limit is shared across all campaigns of that tenant."""
    sim_now = dt(14, 0)
    with memory_db:
        # Campaign 1 has 1 active call
        memory_db.execute(
            "INSERT INTO calls (request_id, tenant_id, campaign_id, lead_id, phone, attempt_no, state, created_at) VALUES ('r1', 'acme', 'c_acme_1', 'l1', '+919111111111', 1, 'ACTIVE', ?)",
            (sim_now.isoformat(),),
        )
        # Campaign 2 has 1 active call
        memory_db.execute(
            "INSERT INTO calls (request_id, tenant_id, campaign_id, lead_id, phone, attempt_no, state, created_at) VALUES ('r2', 'acme', 'c_acme_2', 'l2', '+919222222222', 1, 'ACTIVE', ?)",
            (sim_now.isoformat(),),
        )
        # ACME max_concurrent is 2. So ACME has 0 slots left!
        memory_db.execute(
            "INSERT INTO leads (campaign_id, lead_id, phone, row_order, state, attempts) VALUES ('c_acme_1', 'l3', '+919333333333', 1, 'PENDING', 0)"
        )

    chosen = select_next_leads_to_dial(memory_db, mock_app, sim_now)
    assert len(chosen) == 0


def test_one_call_per_phone_across_campaigns(memory_db, mock_app):
    """Rule R3: Never have two active calls to the same phone number."""
    sim_now = dt(14, 0)
    shared_phone = "+919999999999"
    with memory_db:
        # Phone is active in campaign 1
        memory_db.execute(
            "INSERT INTO calls (request_id, tenant_id, campaign_id, lead_id, phone, attempt_no, state, created_at) VALUES ('r1', 'acme', 'c_acme_1', 'l1', ?, 1, 'ACTIVE', ?)",
            (shared_phone, sim_now.isoformat()),
        )
        # Same phone is pending in campaign 2
        memory_db.execute(
            "INSERT INTO leads (campaign_id, lead_id, phone, row_order, state, attempts) VALUES ('c_acme_2', 'l2', ?, 1, 'PENDING', 0)",
            (shared_phone,),
        )

    # 1 slot remaining for tenant
    chosen = select_next_leads_to_dial(memory_db, mock_app, sim_now)
    # The lead with shared_phone cannot be chosen because phone is active
    assert len(chosen) == 0


def test_terminal_disposition_ends_lead(memory_db, mock_app):
    """Rule R7: Terminal dispositions end the lead with completed."""
    with memory_db:
        memory_db.execute(
            "INSERT INTO leads (campaign_id, lead_id, phone, row_order, state, attempts) VALUES ('c_acme_1', 'l1', '+919111111111', 1, 'ACTIVE', 1)"
        )
        memory_db.execute(
            "INSERT INTO calls (request_id, tenant_id, campaign_id, lead_id, phone, attempt_no, state, created_at) VALUES ('r1', 'acme', 'c_acme_1', 'l1', '+919111111111', 1, 'ACTIVE', ?)",
            (dt(12, 0).isoformat(),),
        )

    res = {
        "request_id": "r1",
        "call_sid": "CA1",
        "campaign_id": "c_acme_1",
        "lead_id": "l1",
        "phone": "+919111111111",
        "status": "completed",
        "disposition": "interested",
        "ended_at": dt(12, 5).isoformat(),
    }
    process_call_result(memory_db, res, mock_app)
    cur = memory_db.execute("SELECT state, final_state, attempts FROM leads WHERE lead_id = 'l1'")
    row = cur.fetchone()
    assert row["state"] == "COMPLETED"
    assert row["final_state"] == "completed"
    assert row["attempts"] == 1


def test_max_attempts_exhausts_lead(memory_db, mock_app):
    """Rule R4 & R9: At most 3 attempts; leads reach exhausted without terminal disposition."""
    with memory_db:
        memory_db.execute(
            "INSERT INTO leads (campaign_id, lead_id, phone, row_order, state, attempts) VALUES ('c_acme_1', 'l1', '+919111111111', 1, 'ACTIVE', 3)"
        )
        memory_db.execute(
            "INSERT INTO calls (request_id, tenant_id, campaign_id, lead_id, phone, attempt_no, state, created_at) VALUES ('r3', 'acme', 'c_acme_1', 'l1', '+919111111111', 3, 'ACTIVE', ?)",
            (dt(14, 0).isoformat(),),
        )

    res = {
        "request_id": "r3",
        "call_sid": "CA3",
        "campaign_id": "c_acme_1",
        "lead_id": "l1",
        "phone": "+919111111111",
        "status": "busy",
        "disposition": None,
        "ended_at": dt(14, 2).isoformat(),
    }
    process_call_result(memory_db, res, mock_app)
    cur = memory_db.execute("SELECT state, final_state, attempts FROM leads WHERE lead_id = 'l1'")
    row = cur.fetchone()
    assert row["state"] == "EXHAUSTED"
    assert row["final_state"] == "exhausted"
    assert row["attempts"] == 3


def test_dnd_global_propagation(memory_db, mock_app):
    """Rule R8: DND on one lead immediately marks other leads with that phone as dnd_blocked."""
    shared_phone = "+919876543210"
    with memory_db:
        # Lead in c_acme_1
        memory_db.execute(
            "INSERT INTO leads (campaign_id, lead_id, phone, row_order, state, attempts) VALUES ('c_acme_1', 'l_dnd', ?, 1, 'ACTIVE', 1)",
            (shared_phone,),
        )
        memory_db.execute(
            "INSERT INTO calls (request_id, tenant_id, campaign_id, lead_id, phone, attempt_no, state, created_at) VALUES ('rdnd', 'acme', 'c_acme_1', 'l_dnd', ?, 1, 'ACTIVE', ?)",
            (shared_phone, dt(12, 0).isoformat()),
        )
        # Lead in c_acme_2 with same phone
        memory_db.execute(
            "INSERT INTO leads (campaign_id, lead_id, phone, row_order, state, attempts) VALUES ('c_acme_2', 'l_other', ?, 2, 'PENDING', 0)",
            (shared_phone,),
        )

    # Lead 1 receives DND
    res_dnd = {
        "request_id": "rdnd",
        "call_sid": "CADND",
        "campaign_id": "c_acme_1",
        "lead_id": "l_dnd",
        "phone": shared_phone,
        "status": "completed",
        "disposition": "dnd",
        "ended_at": dt(12, 5).isoformat(),
    }
    process_call_result(memory_db, res_dnd, mock_app)

    # Lead 1 got terminal disposition dnd -> completed
    cur1 = memory_db.execute("SELECT state, final_state FROM leads WHERE lead_id = 'l_dnd'")
    r1 = cur1.fetchone()
    assert r1["state"] == "COMPLETED"
    assert r1["final_state"] == "completed"

    # Lead 2 with same phone -> dnd_blocked!
    cur2 = memory_db.execute("SELECT state, final_state FROM leads WHERE lead_id = 'l_other'")
    r2 = cur2.fetchone()
    assert r2["state"] == "DND_BLOCKED"
    assert r2["final_state"] == "dnd_blocked"


def test_precedence_completed_over_exhausted_over_dnd(memory_db, mock_app):
    """Rule R9: Precedence completed > exhausted > dnd_blocked."""
    phone = "+919777777777"
    with memory_db:
        # Lead 1: already completed (interested)
        memory_db.execute(
            "INSERT INTO leads (campaign_id, lead_id, phone, row_order, state, attempts, final_state) VALUES ('c_acme_1', 'l_comp', ?, 1, 'COMPLETED', 1, 'completed')",
            (phone,),
        )
        # Lead 2: already exhausted (3 attempts)
        memory_db.execute(
            "INSERT INTO leads (campaign_id, lead_id, phone, row_order, state, attempts, final_state) VALUES ('c_acme_1', 'l_exh', ?, 2, 'EXHAUSTED', 3, 'exhausted')",
            (phone,),
        )
        # Active call on lead 3 that receives DND
        memory_db.execute(
            "INSERT INTO leads (campaign_id, lead_id, phone, row_order, state, attempts) VALUES ('c_acme_2', 'l_dnd', ?, 3, 'ACTIVE', 1)",
            (phone,),
        )
        memory_db.execute(
            "INSERT INTO calls (request_id, tenant_id, campaign_id, lead_id, phone, attempt_no, state, created_at) VALUES ('rdnd', 'acme', 'c_acme_2', 'l_dnd', ?, 1, 'ACTIVE', ?)",
            (phone, dt(12, 0).isoformat()),
        )

    res_dnd = {
        "request_id": "rdnd",
        "call_sid": "CADND",
        "campaign_id": "c_acme_2",
        "lead_id": "l_dnd",
        "phone": phone,
        "status": "completed",
        "disposition": "dnd",
        "ended_at": dt(12, 5).isoformat(),
    }
    process_call_result(memory_db, res_dnd, mock_app)

    # Completed lead must remain completed
    cur1 = memory_db.execute("SELECT final_state FROM leads WHERE lead_id = 'l_comp'")
    assert cur1.fetchone()["final_state"] == "completed"

    # Exhausted lead must remain exhausted (not dnd_blocked)
    cur2 = memory_db.execute("SELECT final_state FROM leads WHERE lead_id = 'l_exh'")
    assert cur2.fetchone()["final_state"] == "exhausted"
