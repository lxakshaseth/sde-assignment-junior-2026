"""HTTP Client for interacting with the Telephony Provider API."""
from __future__ import annotations

import logging
from typing import Any, Optional

import httpx

logger = logging.getLogger("dispatcher.provider_client")


class ProviderClient:
    def __init__(self, base_url: str = "http://127.0.0.1:8099", timeout: float = 10.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=self.timeout,
                limits=httpx.Limits(max_keepalive_connections=20, max_connections=50),
            )
        return self._client

    async def aclose(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def get_clock(self) -> dict[str, Any]:
        """Fetch current clock state from provider."""
        client = self._get_client()
        resp = await client.get(f"{self.base_url}/v1/clock")
        resp.raise_for_status()
        return resp.json()

    async def place_call(
        self,
        request_id: str,
        campaign_id: str,
        lead_id: str,
        phone: str,
        callback_url: str,
    ) -> tuple[int, Optional[dict[str, Any]]]:
        """
        Place call to provider with idempotency request_id.
        Handles 503, 504, network timeouts with verification via request_id lookup.
        """
        payload = {
            "request_id": request_id,
            "campaign_id": campaign_id,
            "lead_id": lead_id,
            "phone": phone,
            "callback_url": callback_url,
        }
        client = self._get_client()
        try:
            resp = await client.post(f"{self.base_url}/v1/calls", json=payload)
            if resp.status_code in (200, 201):
                return resp.status_code, resp.json()

            if resp.status_code == 503:
                logger.warning(f"Provider returned 503 for {request_id}")
                return 503, resp.json() if resp.content else {}

            if resp.status_code == 504:
                logger.warning(f"Provider returned 504 for {request_id}, verifying placement...")
                return await self._verify_call_placement(request_id)

            # 4xx or unexpected
            logger.error(f"Provider returned {resp.status_code} for {request_id}: {resp.text}")
            return resp.status_code, resp.json() if resp.content else {}

        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            logger.warning(f"Network error/timeout placing call {request_id}: {exc}. Verifying...")
            return await self._verify_call_placement(request_id)

    async def _verify_call_placement(self, request_id: str) -> tuple[int, Optional[dict[str, Any]]]:
        """Query GET /v1/calls?request_id=... to disambiguate 504 / network timeout."""
        call_info = await self.get_call_by_request_id(request_id)
        if call_info is not None and not call_info.get("_not_found"):
            logger.info(f"Verified call {request_id} was successfully accepted by provider")
            return 200, call_info
        if call_info is not None and call_info.get("_not_found"):
            logger.info(f"Verified call {request_id} was definitely NOT placed on provider (404)")
            return 504, {"error": "upstream_timeout", "detail": "call_not_placed"}
        logger.warning(f"Verification of {request_id} unresolved due to network error; will retry in reconciliation")
        return 504, {"error": "upstream_timeout", "detail": "unresolved"}

    async def get_call_by_request_id(self, request_id: str) -> Optional[dict[str, Any]]:
        """Look up call by request_id. Returns {'_not_found': True} if 404, None on network error."""
        try:
            client = self._get_client()
            resp = await client.get(f"{self.base_url}/v1/calls", params={"request_id": request_id})
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 404:
                return {"_not_found": True}
            logger.warning(f"GET /v1/calls?request_id={request_id} returned {resp.status_code}")
            return None
        except Exception as exc:
            logger.warning(f"Error querying call by request_id {request_id}: {exc}")
            return None

    async def get_call_by_sid(self, call_sid: str) -> Optional[dict[str, Any]]:
        """Look up call by call_sid. Returns None if 404."""
        try:
            client = self._get_client()
            resp = await client.get(f"{self.base_url}/v1/calls/{call_sid}")
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 404:
                return None
            logger.warning(f"GET /v1/calls/{call_sid} returned {resp.status_code}")
            return None
        except Exception as exc:
            logger.warning(f"Error querying call by sid {call_sid}: {exc}")
            return None

    async def get_admin_report(self) -> dict[str, Any]:
        """Fetch grading report from provider."""
        client = self._get_client()
        resp = await client.get(f"{self.base_url}/admin/report")
        resp.raise_for_status()
        return resp.json()

    async def get_admin_leads(self) -> list[dict[str, Any]]:
        """Fetch expected leads state from provider."""
        client = self._get_client()
        resp = await client.get(f"{self.base_url}/admin/leads")
        resp.raise_for_status()
        return resp.json()
