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

### Repo layout

```
phase4-session9/
├── README.md          ← you are here
├── .gitignore          ← repo-root ignores (.claude/, .idea/, .venv/, ...)
└── chronicle/           ← the actual project — everything runs from inside here
    ├── monitoring_daemon.py   ← Session 13.3's deliverable
    ├── agent.py, api.py, otel_setup.py, judge_pipeline.py, ...
    ├── docker-compose.yml, Dockerfile
    ├── requirements.txt
    └── .env.example
```

Every command below is run from inside `chronicle/`, not the repo root.

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
git clone https://github.com/waseemkhan606/phase4-session9.git
cd phase4-session9/chronicle

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

**G. A second clone of this repo silently recreates your first clone's containers**
`docker-compose.yml` pins the project name to a fixed literal
(`name: chronicle-session13-3`) to solve a *different* problem — every
session folder in this course being named `chronicle`, which made
Compose's directory-basename default collide across sessions. That fix
has a side effect: it's still just one fixed name, so if you (or a
grader, or a CI runner) `git clone` this repo into a **second** location
on the same machine and run `docker-compose up` there while your first
clone's stack is still running, Compose sees the same project name and
**reuses/recreates your first clone's containers instead of starting an
independent stack** — no error, no warning, it just quietly swaps out
what's backing your running services. Confirmed by actually doing this:
`docker ps` showed only one set of `chronicle-session13-3-*` containers
after starting the "second" stack.

This only bites you if you deliberately need two independent copies
running at once. If so, override the project name per clone:
```bash
docker-compose -p chronicle-session13-3-clone2 up -d --build
```
Any unique `-p` value works — just don't reuse one that's already active
elsewhere on the machine. For the normal case (one clone, one running
stack) you don't need to do anything.

---

## 5. What Session 13.3 added

New file: **`monitoring_daemon.py`** — nothing else in the repo changed
from Session 13.2. It is a pure *consumer* of Sessions 13.1/13.2's
output (OTel span attributes + the Judge pipeline); no new
instrumentation was introduced.

### Step-by-step implementation

The file is built as 10 sections, in dependency order — each one only
depends on what came before it. This is the order to read (or rebuild)
it in. Line numbers are anchors into `chronicle/monitoring_daemon.py`.

**Step 1 — Declare SLOs as first-class objects** (`monitoring_daemon.py:58-119`)
Before writing a single tripwire, every alert this daemon can ever fire
is declared as an `SLO` up front. The rule: if you can't point at the
`SLO` behind an alert, don't ship the alert.
```python
class SLO(BaseModel):
    name:             str
    sli_description:  str
    target:           str
    window_seconds:   int
    span_attributes:  List[str]   # span attrs from Session 13.1 this SLO reads
    breach_action:    str

SWARM_SLOS = [
    SLO(name="latency_p95", target="p95 <= 12.0 seconds", window_seconds=60, ...),
    SLO(name="token_budget", target="mean <= 1500, p99 <= 3500", window_seconds=300, ...),
    SLO(name="trajectory_quality", target="mean >= 8.0; 3 consecutive < 8.0 => CRITICAL", ...),
    SLO(name="tool_registry_integrity", target="exactly zero, ever", window_seconds=0, ...),
]
```
Four SLOs, one per tripwire built in the steps below.

**Step 2 — `TokenBurnRateCalculator`: the burn-rate SLI** (`:124-181`)
A rolling `deque` of `(timestamp, token_count)`. `ingest()` appends and
evicts anything older than `window_seconds`. The key design decision is
`is_tripped()`'s **AND**, not OR:
```python
def is_tripped(self) -> bool:
    return (
        self.current_rate()     > self.threshold
        and self.mean_per_ticket() > self.per_ticket_threshold
    )
```
Rate alone spikes on one legitimately large ticket; mean-per-ticket
alone spikes on a short burst of small tickets. Only a real ReAct spiral
trips both at once.

**Step 3 — `ToolHallucinationTripwire`: hard-fail, no window** (`:186-217`)
Every other check in this daemon is a rolling statistic. This one isn't
— it's a single lookup against `TOOL_REGISTRY`. If an agent calls a tool
name that was never bound to it, that's a prompt/tool-binding regression,
not noise to smooth over:
```python
def check(self, agent_name: str, tool_name: str) -> Optional[dict]:
    known = self.registry.get(agent_name, set())
    if tool_name in known:
        return None
    return {"violation": "tool_hallucination", "severity": "CRITICAL", ...}
```

**Step 4 — `PhoenixClient` / `PhoenixTraceRecord`: the data source** (`:220-292`)
Defines the shape of one trace record and a client with a **5-second
query lag** — it queries `[now - window - 5s, now - 5s]`, never the
trailing 5 seconds, because Phoenix is still indexing the newest spans
when the daemon polls. A 0-lag query would read partial data and produce
false negatives. `seed_fixture()` swaps in synthetic traces so the rest
of the daemon can be built and demoed without a live Phoenix instance —
`query_recent_traces()` is the only method a real GraphQL implementation
would need to replace.

**Step 5 — `call_judge()` + `ThreeStrikesJudge`: trajectory quality** (`:305-372`)
`call_judge()` makes one `temperature=0` Gemini call per sampled trace
and parses a `{"score": ..., "reason": ...}` JSON response, falling back
to a neutral `7.0` (not a crash) on any schema violation.
`ThreeStrikesJudge` is the state machine on top: three consecutive
scores below 8.0 trips the alarm, any passing score resets the counter,
and — the one exception to "three strikes" — a single score below 5.0
trips it immediately:
```python
def observe(self, score: float) -> bool:
    if score < self.CATASTROPHIC_THRESHOLD:
        self.strike_count = self.strikes_needed   # instant trip
    elif score < self.threshold:
        self.strike_count += 1
    else:
        self.strike_count = 0                      # reset on pass
    return self.strike_count >= self.strikes_needed
```

**Step 6 — `Incident` + `route_incident()`: routing** (`:376-421`)
`Incident` requires `trace_id` and `langgraph_thread_id` — pydantic
rejects construction without them, because an alert with no trace to
pull up in Phoenix is a rumour, not an incident. `route_incident()` maps
`point_of_failure` (`infra` / `queue` → Infra On-Call; `prompt` / `graph`
→ AI Architect) to an owner, and **raises `KeyError`** on an unrecognized
`point_of_failure` rather than defaulting silently — a dead routing rule
should be loud, not quietly drop a page.

**Step 7 — `CooldownRegistry`: alert suppression** (`:426-451`)
One method, keyed by signature: `should_page()` returns `True` only if
more than `cooldown_seconds` have passed since that *same signature*
last paged. A different signature firing mid-cooldown still pages —
this is what stops one degradation from producing 40 pages an hour
without ever masking a second, unrelated failure.

**Step 8 — `MonitoringDaemon`: wiring it together** (`:455-626`)
`tick()` runs, in order: ingest tokens → tool-hallucination check (no
window) → latency p95 with **two-window hysteresis** (needs two
consecutive breaching windows before paging, so one cold-start outlier
can't wake anyone at 3am) → token burn rate → sampled judge scores. A
`min_sample_size` guard skips the latency check entirely on tiny
windows, where p95 is meaningless. `_page()` is the single choke point
every alert passes through: cooldown check → build `Incident` → route →
print, with `trace_id` in the printed line every time.

**Step 9 — Trace fixtures + `run_degradation_demo()`: proving it works offline** (`:630-780`)
`build_healthy_traces()` and `build_degraded_traces()` synthesize
before/after trace batches — the degraded batch simulates idempotency
keys being removed from the swarm config (latency 11-18s vs 1.5-2.8s,
tokens 3.8-5.2k vs 600-1k, judge scores 4.5-7.2 vs 8.2-9.8, plus one
hallucinated tool name). `run_degradation_demo()` seeds healthy traces,
ticks (no alerts), seeds degraded traces, ticks twice (hysteresis needs
two windows), and prints the alert ledger.

**Step 10 — `run_session_verification()`: the unit checks** (`:784-` end)
Six checks run before the demo ever executes — burn rate trips/doesn't
trip correctly, the tripwire passes/fails correctly, three-strikes and
the catastrophic short-circuit both fire correctly. If any of these
fail, the script exits before running the demo (`sys.exit(1)`) rather
than showing a degradation demo built on broken primitives.

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
