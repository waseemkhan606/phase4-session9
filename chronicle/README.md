# Chronicle — Session 13.3: SRE-Style Monitoring Daemon

Chronicle is a 5-agent LangGraph swarm (FastAPI + OpenTelemetry + Arize
Phoenix) that produces a personal "life analysis" from multiple data
sources. Session 13.3 adds `monitoring_daemon.py`: an SRE-style daemon
that consumes the OTel spans and LLM-judge scores Sessions 13.1/13.2
already produce, and pages the right on-call owner when an SLO breaches.

This README covers two independent things:

1. **Cloning/setting this project up from scratch with zero errors.**
2. **What Session 13.3 specifically added, verified, and fixed**, so the
   build is reproducible.

---

## 0. Prerequisites

| Requirement | Why | Check |
|---|---|---|
| Python 3.11+ | matches the Docker image (`python:3.11-slim`) and all dependencies | `python3 --version` |
| Docker Desktop (or compatible engine) running | runs Phoenix, the API, and the UI | `docker info` |
| A free Gemini API key | powers every agent + the LLM-as-Judge calls | https://aistudio.google.com → "Get API key" |
| Ports `3000`, `4317`, `6006`, `8000` free | UI, OTLP/gRPC, Phoenix, API | see Troubleshooting §D if not |

You do **not** need Docker to run `monitoring_daemon.py` on its own — it
has an offline fixture mode (see §2). You only need Docker for the full
stack (Phoenix + API + UI) used to *generate* live traces.

---

## 1. Clone and set up (do this exactly once)

```bash
cd chronicle

# 1. Create an isolated virtual environment — do NOT reuse a global
#    Python. This avoids version drift and keeps `pip install` idempotent.
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Copy the env template and fill in your real key
cp .env.example .env
# now edit .env and replace `your_actual_key_here` with your real
# GEMINI_API_KEY. Never commit .env — it's already in .gitignore.
```

**Do NOT run `pip install asyncio`.** `asyncio` is part of the Python
standard library since 3.4. The PyPI package of the same name is an
abandoned pre-3.4 backport that conflicts with the stdlib module and
will break imports if installed. If a guide tells you to install it,
skip that line.

---

## 2. Running things

### 2a. Standalone: the monitoring daemon (no Docker needed)

This runs the full Session 13.3 deliverable against synthetic ("seeded")
trace fixtures — no live Phoenix instance required.

```bash
source .venv/bin/activate
set -a && source .env && set +a     # loads GEMINI_API_KEY into the shell
python monitoring_daemon.py
```

Expected: `6/6 checks passed`, followed by a degradation demo that fires
`tool_hallucination`, `swarm_latency_exceeded`, `token_burn_rate_spike`,
and `trajectory_below_threshold_3x` alerts, ending with:

```
[CRITICAL ALERT] - Swarm Latency Exceeded. Paging On-Call Engineer.
```

### 2b. Full stack: Phoenix + API + UI (Docker)

```bash
docker-compose up -d --build
```

Wait ~15s for Phoenix's healthcheck, then verify:

```bash
curl http://localhost:6006/healthz        # → OK
curl http://localhost:8000/health/ready   # → {"status":"ready","graph":true,"mcp":true}
curl -o /dev/null -w "%{http_code}\n" http://localhost:3000/   # → 200
```

Open the UI at **http://localhost:3000** and Phoenix's trace explorer at
**http://localhost:6006**.

To stop everything:

```bash
docker-compose down
```

**Idiot-proofing note:** the `docker-compose.yml` in this directory pins
an explicit project name (`name: chronicle-session13-3`). Every session
folder in this course (`session1/chronicle`, `session2/chronicle`, …)
is literally named `chronicle`, and Docker Compose defaults its project
name to the current directory's *basename* — not its full path. Without
a pinned name, running `docker-compose up` here after having run it in
any other session's `chronicle/` folder collides on container and
network names (`chronicle-api-1`, `chronicle_default`, …). This is
already fixed in this file; you don't need to do anything, but if you
copy this `docker-compose.yml` into a sibling session folder, give it a
different `name:` too.

---

## 3. Verification checklist

Run these after `docker-compose up -d` to confirm the whole chain — UI →
API → 5-agent swarm → OTel spans → Phoenix — actually works, not just
that containers are "Up".

- [ ] `docker-compose ps` shows all three containers as `Up`, Phoenix as `(healthy)`
- [ ] `curl http://localhost:6006/healthz` → `OK`
- [ ] `curl http://localhost:8000/health` → `"status":"ok"`, lists 5 agents
- [ ] `curl http://localhost:8000/health/ready` → `{"status":"ready","graph":true,"mcp":true}`
- [ ] Open http://localhost:3000 — UI loads, no console errors, its `fetch()` calls target `http://localhost:8000`
- [ ] Send a real request:
      ```bash
      curl -X POST http://localhost:8000/analyze \
        -H "Content-Type: application/json" \
        -d '{"question": "test question", "depth": "quick", "debug": true}'
      ```
      → HTTP 200 with `correlations`, `honest_analysis`, `final_brief`, `confidence`
- [ ] Streaming works:
      ```bash
      curl -N -X POST http://localhost:8000/analyze/stream \
        -H "Content-Type: application/json" -d '{"question":"test"}'
      ```
      → live `event: stream_start`, `agent_handoff`, `tool_call`, `tool_result` lines
- [ ] Open http://localhost:6006 — the `default` project shows a trace with 5 nested spans (`chronicle.ingestion` → `chronicle.pattern` → `chronicle.timeline` → `chronicle.brutality` → `chronicle.synthesis`, plus `llm.call` / `mcp.*` children)
- [ ] The `chronicle.ingestion` span's attributes include `internal_notes` (planted in Session 13.2)
- [ ] `llm.call` spans include `token_count`, `temperature`, `agent` under their `llm` attribute
- [ ] `python judge_pipeline.py` (in the venv, with `.env` loaded) → `Verification: PASS` on `SEEDED_BUGGY_TRACE`
- [ ] `python monitoring_daemon.py` → `6/6 checks passed` and all 4 alert signatures fire

**Known gap, not a bug to "fix" silently:** live Phoenix spans use
`langgraph_node` and a nested `token_count: {input, output}` object.
`monitoring_daemon.py`'s `PhoenixTraceRecord` (and its docstrings)
assume flat `langgraph_thread_id` and a scalar `token_count` — that's
correct for the fixture path used in the demo, but whoever wires
`PhoenixClient.query_recent_traces()` to a real GraphQL query later will
need to map these fields, not assume a 1:1 match.

---

## 4. Troubleshooting

**A. `Error response from daemon: Conflict. The container name "/phoenix" is already in use`**
A stale container from a previous run (possibly in a different session
folder) still holds that name.
```bash
docker rm -f phoenix        # or whatever name docker ps -a shows
docker-compose up -d
```
This shouldn't happen anymore now that `docker-compose.yml` has a pinned
project name and no longer hardcodes `container_name: phoenix`.

**B. `ModuleNotFoundError` when running `monitoring_daemon.py` or `judge_pipeline.py`**
You're not in the venv, or dependencies weren't installed there.
```bash
source .venv/bin/activate
pip install -r requirements.txt
```

**C. Gemini calls fail / judge falls back to `7.0` every time**
`.env` still has the placeholder key. Confirm:
```bash
grep GEMINI_API_KEY .env
```
should NOT show `your_actual_key_here`.

**D. Port already in use (`3000`, `6006`, `4317`, or `8000`)**
Something else on the host is bound to it.
```bash
lsof -i :8000        # find what's holding it
docker-compose down  # if it's a stale Chronicle stack
```

**E. `the attribute 'version' is obsolete` warning**
Already removed from `docker-compose.yml` in this session. If you see
it, you're looking at an older copy of the file.

**F. `pip install` fails with `No space left on device`, or `docker` commands hang indefinitely**
Check host disk space first, before assuming anything is broken in this
repo:
```bash
df -h /
```
Docker Desktop's own VM disk (`~/Library/Containers/com.docker.docker/Data/vms/0/data/Docker.raw`)
can silently grow to tens of GB and, combined with its build cache, can
eat all remaining free space — at which point the Docker daemon itself
stops responding (CLI commands hang forever, containers stop answering
on their ports) rather than failing loudly. If `df -h /` shows single-digit
percent free:
```bash
docker system df                # see what's reclaimable
docker builder prune -af        # safe — only clears build cache, rebuilds on next `docker build`
```
If `docker` commands themselves hang (not just slow, actually stuck with
no output for 60s+), the daemon is wedged and a prune won't help — quit
and relaunch Docker Desktop, which clears it:
```bash
osascript -e 'quit app "Docker Desktop"'
# if that doesn't fully exit it within ~10s:
pkill -9 -f com.docker.backend
open -a "Docker Desktop"
```
Then wait for `docker info` to succeed before retrying `docker-compose up`.

---

## 5. What Session 13.3 added

New file: **`monitoring_daemon.py`** — nothing else in the repo changed
from Session 13.2. It is a pure *consumer* of Sessions 13.1/13.2's
output (OTel span attributes + the Judge pipeline); no new
instrumentation was introduced.

Nine logical sections, mirroring the session doc's Colab cells:

1. `SLO` model + `SWARM_SLOS` register (4 SLOs: latency, token budget, trajectory quality, tool-registry integrity)
2. `TokenBurnRateCalculator` — rolling-window burn-rate SLI, AND-predicate (rate *and* per-ticket mean must both breach)
3. `ToolHallucinationTripwire` — hard-fail, no window, no threshold
4. `PhoenixClient` / `PhoenixTraceRecord` — simulated Phoenix GraphQL client with a 5s query lag and `seed_fixture()` for offline demo
5. `call_judge()` + `ThreeStrikesJudge` — live Gemini judge calls, catastrophic short-circuit below 5.0, 3-strikes state machine otherwise
6. `Incident` model + `route_incident()` — routes by `point_of_failure`, raises `KeyError` on an unknown one (fail loud, no silent default)
7. `CooldownRegistry` — suppresses repeat pages for the same signature within a window
8. `MonitoringDaemon` — ties it together: `tick()` runs all 5 checks in order, with two-window hysteresis on latency and a `min_sample_size` guard
9. Trace fixtures (`build_healthy_traces` / `build_degraded_traces`) + `run_degradation_demo()` — end-to-end offline demo

### Fixes applied on top of the original spec

The session doc's own example code had three issues that were caught by
actually *running* it, not just reading it — fixed here rather than
weakened the underlying logic:

- **Demo timing bug**: the second "degraded" trace batch's timestamps
  landed inside `PhoenixClient`'s 5-second query lag and were excluded,
  so the two-window latency hysteresis could never reach its second
  breach. Fixed by giving that batch an earlier base timestamp.
- **Burn-rate dilution**: with a 60s window, healthy-baseline tokens
  from a few hundred milliseconds earlier (this demo compresses real
  time) stayed in the rolling deque and diluted the degraded batch's
  per-ticket mean below the 3,000-token AND-predicate threshold.
  Fixed by shortening the burn-rate window to 20s for the demo — real
  production traffic is separated by minutes, not milliseconds, so this
  doesn't change the production semantics.
- **Missing `trace_id` in alert output**: `_page()`'s print statement
  didn't actually include `trace_id`, contradicting its own docstring
  ("a naked metric with no trace_id is a rumour, not an alert") and the
  session's own checklist. Added it.

All fixes were verified by running the script 5 times back-to-back
(randomized fixture data each run) with no flakiness, plus a separate
extended test script exercising `route_incident`'s `KeyError`, real
`CooldownRegistry` suppression, the Phoenix client's 5s-lag boundary,
and the `min_sample_size` guard — 7/7 passed.

---

## 6. File manifest

```
otel_setup.py        OTLPSpanExporter → Phoenix. Session 13.1.
agent.py              5-agent LangGraph swarm + span instrumentation. Session 11–13.1.
judge_pipeline.py     3 Judges, grade_trajectory(), SEEDED_BUGGY_TRACE. Session 13.2.
api.py                FastAPI app: /analyze, /analyze/stream, /health*. Session 11–13.1.
monitoring_daemon.py  SRE monitoring daemon. Session 13.3. ← this session's deliverable
index.html             UI served by nginx.
docker-compose.yml     Phoenix + api + ui stack.
Dockerfile             Builds the api image.
requirements.txt       Python deps for venv and Docker image.
.env.example           Template — copy to .env and fill in GEMINI_API_KEY.
```
