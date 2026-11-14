"""Configuration loader for campaigns and provider settings."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .models import CampaignConfig, CampaignWindow, TenantConfig, IST


@dataclass
class RetryPolicy:
    no_answer_minutes: int = 60
    busy_minutes: int = 60
    failed_minutes: int = 15

    def get_delay_minutes(self, status: str) -> int:
        if status in ("no_answer", "busy"):
            return self.no_answer_minutes
        if status == "failed":
            return self.failed_minutes
        return self.failed_minutes


@dataclass
class RegulatoryWindow:
    start: str = "09:00"
    end: str = "21:00"


@dataclass
class AppConfig:
    fixtures_path: str
    fixtures_dir: str
    sim_start: datetime
    timezone_name: str
    regulatory_window: RegulatoryWindow
    window_grace_seconds: int
    retry_policy: RetryPolicy
    webhook_secret: str
    tenants: dict[str, TenantConfig]
    campaigns: dict[str, CampaignConfig]
    raw_data: dict[str, Any]

    @classmethod
    def load(cls, fixtures_path: str) -> "AppConfig":
        fixtures_path = os.path.abspath(fixtures_path)
        fixtures_dir = os.path.dirname(fixtures_path)
        with open(fixtures_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        sim_start = datetime.fromisoformat(data["sim_start"]).astimezone(IST)
        reg_win = RegulatoryWindow(
            start=data.get("regulatory_window", {}).get("start", "09:00"),
            end=data.get("regulatory_window", {}).get("end", "21:00"),
        )
        rp_data = data.get("retry_policy", {})
        retry_policy = RetryPolicy(
            no_answer_minutes=rp_data.get("no_answer_minutes", 60),
            busy_minutes=rp_data.get("busy_minutes", 60),
            failed_minutes=rp_data.get("failed_minutes", 15),
        )

        tenants = {}
        for t in data.get("tenants", []):
            tenants[t["tenant_id"]] = TenantConfig(
                tenant_id=t["tenant_id"],
                max_concurrent_calls=t["max_concurrent_calls"],
            )

        campaigns = {}
        for c in data.get("campaigns", []):
            win = CampaignWindow(
                start=c["window"]["start"],
                end=c["window"]["end"],
                days=c["window"]["days"],
            )
            campaigns[c["campaign_id"]] = CampaignConfig(
                campaign_id=c["campaign_id"],
                tenant_id=c["tenant_id"],
                leads_file=c["leads_file"],
                window=win,
                max_attempts=c.get("max_attempts", 3),
            )

        return cls(
            fixtures_path=fixtures_path,
            fixtures_dir=fixtures_dir,
            sim_start=sim_start,
            timezone_name=data.get("timezone", "Asia/Kolkata"),
            regulatory_window=reg_win,
            window_grace_seconds=data.get("window_grace_seconds", 120),
            retry_policy=retry_policy,
            webhook_secret=data.get("webhook_secret", ""),
            tenants=tenants,
            campaigns=campaigns,
            raw_data=data,
        )
