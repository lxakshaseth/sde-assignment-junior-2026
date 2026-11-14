# AI Usage Disclosure

## Tools used
- Google Antigravity / Gemini 2.5 Pro

## What I used them for
- Scaffolding the initial SQLite schema and data models (`solution/models.py`, `solution/db.py`).
- Generating boilerplate for FastAPI webhook route handlers and HMAC-SHA256 signature verification.
- Drafting initial unit test cases for timezone/window edge cases and priority sorting.
- Formatting the Mermaid state machine diagrams and markdown documentation.

## What I wrote or decided myself
- **Core Architecture & Concurrency Model:** Chose an embedded SQLite database in WAL mode over heavy distributed engines (e.g. Temporal) to keep the project lightweight and zero-dependency while ensuring full durability against process kills (`kill -9`).
- **Simulated Clock Skew & Safety Buffers:** Identified that at high simulation speeds (1200x–2400x), milliseconds of real network/HTTP latency translate to 10–40 simulated minutes. Designed the two-stage safety buffer (30-minute lookahead buffer in candidate selection and 10-minute pre-dial buffer) which eliminated all `outside_calling_window` violations.
- **Ambiguous Request-ID Disambiguation:** Designed the deterministic idempotency key structure (`req_{tenant}_{campaign}_{lead}_att{attempt}_{uuid}`) and the recovery protocol for 504 timeouts and ungraceful crashes using `/calls/by-request-id/{id}`.
- **Connection Keep-Alive Optimization:** Replaced per-request HTTP client instantiation with a shared `httpx.AsyncClient` keep-alive connection pool to handle rapid bursts of API calls under high simulation speed.
- **Active Call Polling Reconciler:** Formulated the background reconciliation loop to query `/calls/{call_sid}` for stuck calls, guaranteeing that dropped webhooks under network chaos do not leave leads permanently unresolved.
- **LoopBoundLock Pattern:** Replaced standard global `asyncio.Lock` with an event-loop-aware lock wrapper to resolve pytest-asyncio event-loop binding deadlocks.

## Where the AI was wrong or unhelpful
- **Simulation Time Drift:** The initial AI-generated dispatch logic checked `in_window(sim_now, ...)` at the moment of lead selection without any forward-looking buffer. Under 1800x speed, by the time the HTTP request reached the mock telephony server, the simulated clock had ticked past 21:00, resulting in `outside_calling_window` violations. I diagnosed the clock skew, calculated the real-to-sim time conversion, and added the 30-minute forward lookahead buffer.
- **Module-Level asyncio.Lock in Pytest:** The AI generated a module-level `DB_LOCK = asyncio.Lock()`. In production this worked fine, but in pytest where each test function creates a fresh event loop, the lock bound to the first test's loop and caused subsequent async tests to hang indefinitely. I replaced it with `LoopBoundLock` which dynamically binds locks per active event loop.
- **Client Socket Exhaustion:** The initial AI draft created a new `httpx.AsyncClient()` inside every dial attempt. Under high concurrency on Windows, this exhausted local ephemeral sockets and caused connection delays. I refactored `ProviderClient` to maintain a persistent connection pool with reusable keep-alive connections.

## How I verified AI-generated code
- **Automated Unit Tests:** Developed and executed a 24-test test suite (`solution/tests/`) covering window math, scheduling priorities, retry backoff calculations, DND propagation, webhook deduplication, and crash recovery.
- **End-to-End Chaos Verification:** Built an end-to-end integration runner (`solution/test_e2e_runner.py`) that spun up the mock telephony server with maximum chaos (`failure_scale=1.0`) at 1800x speed, executed all 200 leads across both campaigns, and automatically verified against `/admin/report` and `/admin/leads` to confirm 0 rule violations and 0 final state mismatches.
- **Line-by-line Code Review:** Manually reviewed and adjusted all state transitions, SQL queries, index definitions, and exception handling blocks.
