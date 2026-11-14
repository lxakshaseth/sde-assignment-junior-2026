"""Tests for crash recovery (kill -9) and state reconciliation."""
import sqlite3
from datetime import datetime
from unittest.mock import AsyncMock, patch

import pytest

from solution.clock import SimClock
from solution.config import AppConfig, CampaignConfig, CampaignWindow, RegulatoryWindow, RetryPolicy, TenantConfig
from solution.db import SCHEMA_SQL
from solution.dispatcher import Dispatcher
from solution.models import IST
from solution.provider_client import ProviderClient


def dt(h: int, m: int) -> datetime:
    return datetime(2026, 11, 12, h, m, tzinfo=IST)


@pytest.fixture
def mock_app_config():
    tenants = {"acme": TenantConfig(tenant_id="acme", max_concurrent_calls=2)}
    campaigns = {
        "c1": CampaignConfig(
            campaign_id="c1",
            tenant_id="acme",
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
        retry_policy=RetryPolicy(),
        webhook_secret="test_secret",
        tenants=tenants,
        campaigns=campaigns,
        raw_data={},
    )


@pytest.mark.asyncio
async def test_recovery_from_crash_with_dialing_call_accepted(mock_app_config, tmp_path):
    """Crash happened while call was in DIALING, and provider DID accept the call."""
    db_path = str(tmp_path / "test_recovery1.db")
    conn = sqlite3.connect(db_path, autocommit=False)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL)
    with conn:
        conn.execute("INSERT INTO tenants VALUES ('acme', 2)")
        conn.execute("INSERT INTO campaigns VALUES ('c1', 'acme', '10:00', '19:00', 'thu', 3)")
        conn.execute(
            "INSERT INTO leads (campaign_id, lead_id, phone, row_order, state, attempts) VALUES ('c1', 'l1', '+919111111111', 1, 'DIALING', 0)"
        )
        conn.execute(
            "INSERT INTO calls (request_id, tenant_id, campaign_id, lead_id, phone, attempt_no, state, created_at) VALUES ('req_crash_1', 'acme', 'c1', 'l1', '+919111111111', 1, 'DIALING', ?)",
            (dt(12, 0).isoformat(),),
        )
    conn.close()

    mock_provider = ProviderClient()
    mock_provider.get_call_by_sid = AsyncMock(return_value={"call_sid": "CA_RECOVERED_1", "state": "active"})
    # Provider reports call was accepted and active
    mock_provider.get_call_by_request_id = AsyncMock(return_value={
        "call_sid": "CA_RECOVERED_1",
        "request_id": "req_crash_1",
        "state": "active",
    })

    clock = SimClock(sim_start=dt(12, 0), manual=True)
    dispatcher = Dispatcher(
        config=mock_app_config,
        clock=clock,
        provider_client=mock_provider,
        db_path=db_path,
    )

    await dispatcher.startup_recovery()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur_lead = conn.execute("SELECT state, attempts, last_call_sid FROM leads WHERE lead_id = 'l1'")
    lead = cur_lead.fetchone()
    assert lead["state"] == "ACTIVE"
    assert lead["attempts"] == 1
    assert lead["last_call_sid"] == "CA_RECOVERED_1"

    cur_call = conn.execute("SELECT state, call_sid FROM calls WHERE request_id = 'req_crash_1'")
    call = cur_call.fetchone()
    assert call["state"] == "ACTIVE"
    assert call["call_sid"] == "CA_RECOVERED_1"
    conn.close()


@pytest.mark.asyncio
async def test_recovery_from_crash_with_dialing_call_unplaced(mock_app_config, tmp_path):
    """Crash happened while call was in DIALING, and provider NEVER received the call."""
    db_path = str(tmp_path / "test_recovery2.db")
    conn = sqlite3.connect(db_path, autocommit=False)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL)
    with conn:
        conn.execute("INSERT INTO tenants VALUES ('acme', 2)")
        conn.execute("INSERT INTO campaigns VALUES ('c1', 'acme', '10:00', '19:00', 'thu', 3)")
        conn.execute(
            "INSERT INTO leads (campaign_id, lead_id, phone, row_order, state, attempts) VALUES ('c1', 'l2', '+919222222222', 1, 'DIALING', 0)"
        )
        conn.execute(
            "INSERT INTO calls (request_id, tenant_id, campaign_id, lead_id, phone, attempt_no, state, created_at) VALUES ('req_crash_2', 'acme', 'c1', 'l2', '+919222222222', 1, 'DIALING', ?)",
            (dt(12, 0).isoformat(),),
        )
    conn.close()

    mock_provider = ProviderClient()
    # Provider returns 404 not found
    mock_provider.get_call_by_request_id = AsyncMock(return_value={"_not_found": True})

    clock = SimClock(sim_start=dt(12, 0), manual=True)
    dispatcher = Dispatcher(
        config=mock_app_config,
        clock=clock,
        provider_client=mock_provider,
        db_path=db_path,
    )

    await dispatcher.startup_recovery()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur_lead = conn.execute("SELECT state, attempts FROM leads WHERE lead_id = 'l2'")
    lead = cur_lead.fetchone()
    assert lead["state"] == "PENDING"
    assert lead["attempts"] == 0

    cur_call = conn.execute("SELECT state FROM calls WHERE request_id = 'req_crash_2'")
    call = cur_call.fetchone()
    assert call["state"] == "ABORTED"
    conn.close()


@pytest.mark.asyncio
async def test_recovery_from_crash_reconciles_ended_call(mock_app_config, tmp_path):
    """Crash happened while call was ACTIVE, and call completed while dispatcher was offline."""
    db_path = str(tmp_path / "test_recovery3.db")
    conn = sqlite3.connect(db_path, autocommit=False)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL)
    with conn:
        conn.execute("INSERT INTO tenants VALUES ('acme', 2)")
        conn.execute("INSERT INTO campaigns VALUES ('c1', 'acme', '10:00', '19:00', 'thu', 3)")
        conn.execute(
            "INSERT INTO leads (campaign_id, lead_id, phone, row_order, state, attempts) VALUES ('c1', 'l3', '+919333333333', 1, 'ACTIVE', 1)"
        )
        conn.execute(
            "INSERT INTO calls (request_id, call_sid, tenant_id, campaign_id, lead_id, phone, attempt_no, state, created_at) VALUES ('req_crash_3', 'CA_ENDED_3', 'acme', 'c1', 'l3', '+919333333333', 1, 'ACTIVE', ?)",
            (dt(12, 0).isoformat(),),
        )
    conn.close()

    mock_provider = ProviderClient()
    mock_provider.get_call_by_sid = AsyncMock(return_value={
        "call_sid": "CA_ENDED_3",
        "request_id": "req_crash_3",
        "state": "ended",
        "result": {
            "event": "call.completed",
            "call_sid": "CA_ENDED_3",
            "request_id": "req_crash_3",
            "campaign_id": "c1",
            "lead_id": "l3",
            "phone": "+919333333333",
            "status": "completed",
            "disposition": "interested",
            "ended_at": dt(12, 5).isoformat(),
        }
    })

    clock = SimClock(sim_start=dt(12, 0), manual=True)
    dispatcher = Dispatcher(
        config=mock_app_config,
        clock=clock,
        provider_client=mock_provider,
        db_path=db_path,
    )

    await dispatcher.startup_recovery()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur_lead = conn.execute("SELECT state, final_state FROM leads WHERE lead_id = 'l3'")
    lead = cur_lead.fetchone()
    assert lead["state"] == "COMPLETED"
    assert lead["final_state"] == "completed"
    conn.close()
