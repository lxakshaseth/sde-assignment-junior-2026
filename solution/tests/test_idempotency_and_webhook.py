"""Tests for HMAC signature verification, duplicate webhook deduplication, 503 and 504 handling."""
import hashlib
import hmac
import json
import sqlite3
from datetime import datetime

import httpx
import pytest

from solution.config import AppConfig, CampaignConfig, CampaignWindow, RegulatoryWindow, RetryPolicy, TenantConfig
from solution.db import SCHEMA_SQL
from solution.models import IST
from solution.state_machine import handle_call_accepted, handle_call_rejected_503, handle_call_rejected_504_unplaced
from solution.webhook_server import create_webhook_app


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
        webhook_secret="test_secret_123",
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
        conn.execute("INSERT INTO campaigns VALUES ('c1', 'acme', '10:00', '19:00', 'thu', 3)")
        conn.execute(
            "INSERT INTO leads (campaign_id, lead_id, phone, row_order, state, attempts) VALUES ('c1', 'l1', '+919876543210', 1, 'ACTIVE', 1)"
        )
        conn.execute(
            "INSERT INTO calls (request_id, tenant_id, campaign_id, lead_id, phone, attempt_no, state, created_at) VALUES ('r1', 'acme', 'c1', 'l1', '+919876543210', 1, 'ACTIVE', ?)",
            (dt(12, 0).isoformat(),),
        )
    return conn


@pytest.mark.asyncio
async def test_webhook_hmac_signature_verification(memory_db, mock_app_config):
    app = create_webhook_app(mock_app_config, conn_override=memory_db)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        body = {
            "event": "call.completed",
            "call_sid": "CA12345",
            "request_id": "r1",
            "campaign_id": "c1",
            "lead_id": "l1",
            "phone": "+919876543210",
            "status": "completed",
            "disposition": "interested",
            "ended_at": dt(12, 5).isoformat(),
        }
        raw_bytes = json.dumps(body).encode("utf-8")
        valid_sig = hmac.new(b"test_secret_123", raw_bytes, hashlib.sha256).hexdigest()

        # 1. Valid signature -> 200 OK
        resp_valid = await client.post(
            "/webhooks/telephony",
            content=raw_bytes,
            headers={"Content-Type": "application/json", "X-AU-Signature": valid_sig, "X-AU-Delivery-Id": "del_1"},
        )
        assert resp_valid.status_code == 200

        # Verify DB updated
        cur = memory_db.execute("SELECT state, final_state FROM leads WHERE lead_id = 'l1'")
        row = cur.fetchone()
        assert row["state"] == "COMPLETED"
        assert row["final_state"] == "completed"

        # 2. Invalid signature -> 401
        resp_bad = await client.post(
            "/webhooks/telephony",
            content=raw_bytes,
            headers={"Content-Type": "application/json", "X-AU-Signature": "invalid_hex", "X-AU-Delivery-Id": "del_2"},
        )
        assert resp_bad.status_code == 401

        # 3. Missing signature -> 401
        resp_missing = await client.post(
            "/webhooks/telephony",
            content=raw_bytes,
            headers={"Content-Type": "application/json"},
        )
        assert resp_missing.status_code == 401


@pytest.mark.asyncio
async def test_webhook_duplicate_delivery(memory_db, mock_app_config):
    app = create_webhook_app(mock_app_config, conn_override=memory_db)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        body = {
            "event": "call.completed",
            "call_sid": "CA12345",
            "request_id": "r1",
            "campaign_id": "c1",
            "lead_id": "l1",
            "phone": "+919876543210",
            "status": "completed",
            "disposition": "interested",
            "ended_at": dt(12, 5).isoformat(),
        }
        raw_bytes = json.dumps(body).encode("utf-8")
        sig = hmac.new(b"test_secret_123", raw_bytes, hashlib.sha256).hexdigest()

        # First delivery
        resp1 = await client.post(
            "/webhooks/telephony",
            content=raw_bytes,
            headers={"Content-Type": "application/json", "X-AU-Signature": sig, "X-AU-Delivery-Id": "dup_delivery"},
        )
        assert resp1.status_code == 200

        # Second duplicate delivery
        resp2 = await client.post(
            "/webhooks/telephony",
            content=raw_bytes,
            headers={"Content-Type": "application/json", "X-AU-Signature": sig, "X-AU-Delivery-Id": "dup_delivery"},
        )
        assert resp2.status_code == 200
        assert resp2.json().get("duplicate") is True


def test_503_rejection_restores_lead(memory_db):
    """When provider returns 503, the attempt does not count and lead returns to ready."""
    with memory_db:
        # Lead in DIALING
        memory_db.execute(
            "INSERT INTO leads (campaign_id, lead_id, phone, row_order, state, attempts) VALUES ('c1', 'l503', '+919999999999', 1, 'DIALING', 0)"
        )
        memory_db.execute(
            "INSERT INTO calls (request_id, tenant_id, campaign_id, lead_id, phone, attempt_no, state, created_at) VALUES ('r503', 'acme', 'c1', 'l503', '+919999999999', 1, 'DIALING', ?)",
            (dt(12, 0).isoformat(),),
        )

    handle_call_rejected_503(memory_db, "r503")

    cur = memory_db.execute("SELECT state, attempts FROM leads WHERE lead_id = 'l503'")
    row = cur.fetchone()
    assert row["state"] == "PENDING"
    assert row["attempts"] == 0

    cur_call = memory_db.execute("SELECT state FROM calls WHERE request_id = 'r503'")
    assert cur_call.fetchone()["state"] == "ABORTED"
