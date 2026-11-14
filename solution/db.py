"""SQLite database schema, connection manager, and initialization."""
from __future__ import annotations

import asyncio
import csv
import os
import sqlite3
from typing import Optional

from .config import AppConfig

class LoopBoundLock:
    """An asyncio lock wrapper that binds to the current running event loop.
    Prevents cross-loop deadlock when running under pytest or multiple test loops."""

    def __init__(self):
        self._locks = {}

    def _get_lock(self) -> asyncio.Lock:
        loop = asyncio.get_running_loop()
        if loop not in self._locks:
            self._locks[loop] = asyncio.Lock()
        return self._locks[loop]

    async def __aenter__(self):
        return await self._get_lock().__aenter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self._get_lock().__aexit__(exc_type, exc_val, exc_tb)


DB_LOCK = LoopBoundLock()


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS tenants (
    tenant_id TEXT PRIMARY KEY,
    max_concurrent_calls INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS campaigns (
    campaign_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    window_start TEXT NOT NULL,
    window_end TEXT NOT NULL,
    window_days TEXT NOT NULL,
    max_attempts INTEGER NOT NULL,
    FOREIGN KEY (tenant_id) REFERENCES tenants(tenant_id)
);

CREATE TABLE IF NOT EXISTS leads (
    campaign_id TEXT NOT NULL,
    lead_id TEXT NOT NULL,
    phone TEXT NOT NULL,
    customer_name TEXT,
    row_order INTEGER NOT NULL,
    state TEXT NOT NULL DEFAULT 'PENDING',
    attempts INTEGER NOT NULL DEFAULT 0,
    callback_at TEXT,
    next_retry_at TEXT,
    last_status TEXT,
    last_disposition TEXT,
    last_call_sid TEXT,
    final_state TEXT,
    PRIMARY KEY (campaign_id, lead_id),
    FOREIGN KEY (campaign_id) REFERENCES campaigns(campaign_id)
);

CREATE TABLE IF NOT EXISTS calls (
    request_id TEXT PRIMARY KEY,
    call_sid TEXT UNIQUE,
    tenant_id TEXT NOT NULL,
    campaign_id TEXT NOT NULL,
    lead_id TEXT NOT NULL,
    phone TEXT NOT NULL,
    attempt_no INTEGER NOT NULL,
    state TEXT NOT NULL, -- DIALING, ACTIVE, ENDED, ABORTED
    created_at TEXT NOT NULL,
    started_at TEXT,
    ended_at TEXT,
    duration_sec INTEGER DEFAULT 0,
    status TEXT,
    disposition TEXT,
    callback_at TEXT,
    FOREIGN KEY (campaign_id, lead_id) REFERENCES leads(campaign_id, lead_id)
);

CREATE TABLE IF NOT EXISTS dnd_phones (
    phone TEXT PRIMARY KEY,
    recorded_at TEXT NOT NULL,
    source_call_sid TEXT
);

CREATE TABLE IF NOT EXISTS processed_webhooks (
    delivery_id TEXT PRIMARY KEY,
    call_sid TEXT NOT NULL,
    received_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_leads_state ON leads(state);
CREATE INDEX IF NOT EXISTS idx_leads_phone ON leads(phone);
CREATE INDEX IF NOT EXISTS idx_leads_final_state ON leads(final_state);
CREATE INDEX IF NOT EXISTS idx_calls_state ON calls(state);
CREATE INDEX IF NOT EXISTS idx_calls_tenant_state ON calls(tenant_id, state);
CREATE INDEX IF NOT EXISTS idx_calls_phone_state ON calls(phone, state);
CREATE INDEX IF NOT EXISTS idx_calls_call_sid ON calls(call_sid);
"""


def get_db_connection(db_path: str = "dispatcher.db") -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=60.0)
    conn.row_factory = sqlite3.Row
    if db_path != ":memory:":
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
        except Exception:
            pass
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA busy_timeout=60000;")
    return conn


def init_db(conn: sqlite3.Connection, config: AppConfig) -> None:
    """Initialize schema and populate initial tenant/campaign/lead data if not present."""
    with conn:
        conn.executescript(SCHEMA_SQL)

        # Upsert tenants
        for t in config.tenants.values():
            conn.execute(
                """
                INSERT INTO tenants (tenant_id, max_concurrent_calls)
                VALUES (?, ?)
                ON CONFLICT(tenant_id) DO UPDATE SET max_concurrent_calls=excluded.max_concurrent_calls
                """,
                (t.tenant_id, t.max_concurrent_calls),
            )

        # Upsert campaigns
        for c in config.campaigns.values():
            conn.execute(
                """
                INSERT INTO campaigns (campaign_id, tenant_id, window_start, window_end, window_days, max_attempts)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(campaign_id) DO UPDATE SET
                    tenant_id=excluded.tenant_id,
                    window_start=excluded.window_start,
                    window_end=excluded.window_end,
                    window_days=excluded.window_days,
                    max_attempts=excluded.max_attempts
                """,
                (
                    c.campaign_id,
                    c.tenant_id,
                    c.window.start,
                    c.window.end,
                    ",".join(c.window.days),
                    c.max_attempts,
                ),
            )

        # Check if leads exist
        cur = conn.execute("SELECT count(*) as count FROM leads")
        count = cur.fetchone()["count"]
        if count == 0:
            row_order = 0
            for c in config.campaigns.values():
                leads_file_path = os.path.join(config.fixtures_dir, c.leads_file)
                if not os.path.exists(leads_file_path):
                    continue
                with open(leads_file_path, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        conn.execute(
                            """
                            INSERT INTO leads (campaign_id, lead_id, phone, customer_name, row_order, state, attempts)
                            VALUES (?, ?, ?, ?, ?, 'PENDING', 0)
                            """,
                            (
                                c.campaign_id,
                                row["lead_id"],
                                row["phone"],
                                row.get("customer_name", ""),
                                row_order,
                            ),
                        )
                        row_order += 1
