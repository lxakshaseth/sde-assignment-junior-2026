"""CLI Entrypoint for Campaign Dispatcher."""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys

from .db import get_db_connection
from .orchestrator import export_final_state_csv, run_orchestrator
from .provider_client import ProviderClient
from .state_machine import get_progress_summary


def setup_logging(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


def main():
    parser = argparse.ArgumentParser(description="Agentic Universe - Durable Campaign Dispatcher")
    subparsers = parser.add_subparsers(dest="command", help="Subcommand to run")

    # run command
    run_parser = subparsers.add_parser("run", help="Run the full dispatcher workflow")
    run_parser.add_argument(
        "--fixtures",
        default="assignment-b-workflow/fixtures/campaigns.json",
        help="Path to campaigns.json",
    )
    run_parser.add_argument(
        "--provider-url",
        default="http://127.0.0.1:8099",
        help="Base URL of telephony provider",
    )
    run_parser.add_argument(
        "--webhook-host",
        default="127.0.0.1",
        help="Host for webhook receiver",
    )
    run_parser.add_argument(
        "--webhook-port",
        type=int,
        default=8200,
        help="Port for webhook receiver",
    )
    run_parser.add_argument(
        "--db",
        default="dispatcher.db",
        help="Path to SQLite database",
    )
    run_parser.add_argument(
        "--csv",
        default="final_state.csv",
        help="Output CSV path",
    )
    run_parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose debug logging",
    )

    # export-csv command
    export_parser = subparsers.add_parser("export-csv", help="Export final_state.csv from DB")
    export_parser.add_argument("--db", default="dispatcher.db")
    export_parser.add_argument("--csv", default="final_state.csv")

    # status command
    status_parser = subparsers.add_parser("status", help="Show lead status from DB")
    status_parser.add_argument("--db", default="dispatcher.db")

    # check command
    check_parser = subparsers.add_parser("check", help="Query provider /admin endpoints")
    check_parser.add_argument("--provider-url", default="http://127.0.0.1:8099")

    args = parser.parse_args()

    if not args.command or args.command == "run":
        verbose = getattr(args, "verbose", False)
        setup_logging(verbose)
        fixtures = getattr(args, "fixtures", "assignment-b-workflow/fixtures/campaigns.json")
        provider_url = getattr(args, "provider_url", "http://127.0.0.1:8099")
        webhook_host = getattr(args, "webhook_host", "127.0.0.1")
        webhook_port = getattr(args, "webhook_port", 8200)
        db_path = getattr(args, "db", "dispatcher.db")
        csv_path = getattr(args, "csv", "final_state.csv")

        asyncio.run(
            run_orchestrator(
                fixtures_path=fixtures,
                provider_url=provider_url,
                webhook_host=webhook_host,
                webhook_port=webhook_port,
                db_path=db_path,
                output_csv=csv_path,
            )
        )

    elif args.command == "export-csv":
        rows = export_final_state_csv(args.db, args.csv)
        print(f"Exported {len(rows)} leads to {args.csv}")

    elif args.command == "status":
        conn = get_db_connection(args.db)
        summary = get_progress_summary(conn)
        conn.close()
        print(json.dumps(summary, indent=2))

    elif args.command == "check":
        async def _check():
            client = ProviderClient(base_url=args.provider_url)
            report = await client.get_admin_report()
            print("ADMIN REPORT:")
            print(json.dumps(report, indent=2))
        asyncio.run(_check())


if __name__ == "__main__":
    main()
