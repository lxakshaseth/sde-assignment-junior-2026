"""FastAPI Webhook Server for Telephony Provider result callbacks."""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import sqlite3
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse

from .config import AppConfig
from .db import DB_LOCK, get_db_connection
from .models import IST
from .state_machine import get_progress_summary, process_call_result

logger = logging.getLogger("dispatcher.webhook_server")


def create_webhook_app(
    config: AppConfig,
    db_path: str = "dispatcher.db",
    conn_override: Optional[sqlite3.Connection] = None,
) -> FastAPI:
    app = FastAPI(title="AU Campaign Dispatcher - Webhook Server")

    def get_conn() -> sqlite3.Connection:
        if conn_override is not None:
            return conn_override
        return get_db_connection(db_path)

    @app.post("/webhooks/telephony")
    async def telephony_webhook(
        request: Request,
        x_au_signature: Optional[str] = Header(None, alias="X-AU-Signature"),
        x_au_delivery_id: Optional[str] = Header(None, alias="X-AU-Delivery-Id"),
    ):
        raw_body = await request.body()

        # Signature verification
        if config.webhook_secret:
            expected_sig = hmac.new(
                config.webhook_secret.encode("utf-8"),
                raw_body,
                hashlib.sha256,
            ).hexdigest()
            if not x_au_signature or not hmac.compare_digest(expected_sig, x_au_signature):
                logger.warning("Rejected webhook due to invalid signature")
                raise HTTPException(status_code=401, detail={"error": "invalid_signature"})

        try:
            payload = json.loads(raw_body)
        except Exception:
            raise HTTPException(status_code=400, detail={"error": "invalid_json"})

        delivery_id = x_au_delivery_id or payload.get("call_sid") or "unknown"
        call_sid = payload.get("call_sid", "")

        async with DB_LOCK:
            conn = get_conn()
            try:
                with conn:
                    # Deduplication check via delivery_id
                    cur = conn.execute(
                        "SELECT 1 FROM processed_webhooks WHERE delivery_id = ?",
                        (delivery_id,),
                    )
                    if cur.fetchone():
                        logger.debug(f"Duplicate webhook delivery {delivery_id} ignored")
                        return JSONResponse({"status": "ok", "duplicate": True}, status_code=200)

                    # Record delivery
                    conn.execute(
                        "INSERT INTO processed_webhooks (delivery_id, call_sid, received_at) VALUES (?, ?, ?)",
                        (delivery_id, call_sid, datetime.now(IST).isoformat()),
                    )

                # Process call result idempotently
                process_call_result(conn, payload, config)
                return JSONResponse({"status": "ok"}, status_code=200)
            finally:
                if conn_override is None:
                    conn.close()

    @app.get("/status")
    async def status():
        conn = get_conn()
        try:
            summary = get_progress_summary(conn)
            return summary
        finally:
            if conn_override is None:
                conn.close()

    @app.get("/health")
    async def health():
        return {"status": "healthy"}

    return app
