"""
╔══════════════════════════════════════════════════════════════════╗
║  CHRONICLE — api.py                                              ║
║  Session 12.3: Real MCP Servers + Async 202 Job Queue            ║
╚══════════════════════════════════════════════════════════════════╝

Changes in Session 12.3 (additions only — nothing removed):
  - POST /analyze/async: returns 202 + job_id in <100ms, schedules
    run_chronicle_analysis() via FastAPI BackgroundTasks. Solves the
    29s API Gateway ceiling now that real MCP servers + 5 sequential
    Gemini calls make a full analysis take 60-90s.
  - GET /analyze/jobs/{job_id}: polls job_store for live progress —
    active_node moves through all 5 agents, then final_result appears.
  - Idempotency: identical (question, data_sources) within a job's
    lifetime returns the SAME job_id instead of spawning a duplicate
    analysis — protects against retry storms from clients that see
    a slow 202 ack and resend.
  - GET /health: session → "12.3", async_job_queue capability enabled.

Changes in Session 12.2 (additions only — nothing removed):
  - POST /analyze/stream: replaces the 501 stub with a live SSE stream —
    StreamingResponse + text/event-stream, disconnect detection, keepalive
  - GET /health: updated to session "12.2", sse_streaming capability added
  - POST /analyze (S12.1 JSON endpoint) preserved unchanged

Changes in Session 12.1 (additions only — nothing removed):
  - asynccontextmanager lifespan: MCP pool built + LangGraph graph
    compiled once at startup, closed gracefully at shutdown
  - AnalysisResponse: response_model for /analyze
  - POST /analyze: now uses graph.ainvoke() instead of
    run_concurrent_analysis()
  - GET /health: updated to session "12.1", MCP connection status added
  - GET /health/live, GET /health/ready: k8s-style health probes
  - GET /calibration-stats: summary of the 30-sample calibration dataset
  - All S11.1/S11.2/S11.3 endpoints preserved unchanged
"""

# ── Imports (Session 12.1) ────────────────────────────────────────
import time
import uuid
import hashlib
import logging
import os
import asyncio   # S12.2: keepalive timeout on the SSE event stream
from contextlib import asynccontextmanager
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
import uvicorn

from agent import (
    # S11.1 — preserved
    calculate_chronicle_vram_budget,
    CHRONICLE_AGENTS,
    GPU_VRAM_GB,
    VRAM_BYTES_PER_PARAM,
    KV_CACHE_GB_PER_AGENT_4K,
    CUDA_OVERHEAD_GB,
    # S11.2/S11.3 — preserved
    calculate_tiered_vram_budget,
    calculate_monthly_gpu_cost,
    calculate_max_safe_concurrent,
    task_survivability_matrix,
    oom_prevention_check,
    vllm_config_per_agent,
    colocation_partitioner,
    kv_cache_growth_simulator,
    TASK_SURVIVABILITY_MATRIX,
    CHRONICLE_CALIBRATION_DATASET,
    # S12.1 — preserved
    AnalysisRequest,
    AnalysisResponse,
    build_chronicle_graph,
    build_mcp_client_pool,
    close_mcp_client_pool,
    build_initial_state,
    # S12.2 — new
    chronicle_stream_events,
    NODE_LABELS,
    # S12.3 — new
    run_chronicle_analysis,
)
from stream_schemas import (
    ErrorStreamEvent,
    to_sse_frame,
    keepalive_frame,
)
from job_store import (
    JobAcceptedResponse,
    JobRecord,
    JobStatus,
    read_job,
    write_job,
)

# ── Imports (Session 13.1 — OpenTelemetry) ────────────────────────
from opentelemetry import context as otel_context
from opentelemetry import trace as otel_trace
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from otel_setup import setup_tracing, shutdown_tracing, get_tracer

tracer = get_tracer("chronicle.api")  # S13.1

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("chronicle.api")


# ── Lifespan (Session 12.1) ───────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    What it does:   Startup: build MCP pool + compile LangGraph graph once.
                    Shutdown: close MCP sessions gracefully.
    Why async:      build_mcp_client_pool() awaits aiohttp session creation.
                    close_mcp_client_pool() awaits session close.
    Why here:       graph.compile() costs real time. Per-request compilation
                    adds that overhead to every Chronicle analysis. Startup-once
                    means zero compilation overhead at request time.
    Introduced:     Session 12.1. Permanent.
    """
    # ── S13.1: Init tracing FIRST — before anything else ─────────
    # TracerProvider must exist before FastAPIInstrumentor is called
    # and before any span is created in this lifespan block.
    setup_tracing(use_batch=False)  # SimpleSpanProcessor in dev

    log.info("Chronicle gateway starting — building MCP pool and LangGraph graph...")

    # ── MCP client pool ───────────────────────────────────────────
    # One aiohttp session per Chronicle data source.
    # Reused across all requests. Created once, closed at shutdown.
    app.state.mcp_pool = await build_mcp_client_pool()
    log.info(f"MCP pool ready: {list(app.state.mcp_pool._sessions.keys())}")

    # ── LangGraph graph ───────────────────────────────────────────
    # Compiled once. Reused for every ainvoke() call.
    # The graph itself is stateless — state is passed per-request via ainvoke().
    # No shared mutable state between concurrent Chronicle analyses.
    app.state.graph = build_chronicle_graph(app.state.mcp_pool)
    log.info("LangGraph Chronicle swarm compiled. Gateway ready.")

    app.state.start_time = time.monotonic()

    yield  # Gateway accepts requests from here until shutdown signal

    # ── Shutdown ──────────────────────────────────────────────────
    # In-flight requests complete (not cancelled) before this runs.
    log.info("Chronicle gateway shutting down — closing MCP pool...")
    await close_mcp_client_pool(app.state.mcp_pool)

    # ── S13.1: Flush spans BEFORE process exits ────────────────────
    # BatchSpanProcessor buffers in memory.
    # Without shutdown: last ~500ms of spans lost on every deploy.
    shutdown_tracing()
    log.info("Tracing flushed. Shutdown complete.")


# ── App Setup (Session 12.1) ──────────────────────────────────────

app = FastAPI(
    title="Chronicle API",
    description=(
        "Local-first personal AI analyst. "
        "Session 13.1 — OpenTelemetry Instrumentation."
    ),
    version="13.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# S13.1: Auto-instrument all HTTP requests.
# Must be called AFTER app creation, BEFORE first request.
FastAPIInstrumentor.instrument_app(app)


@app.get("/")
async def root():
    return FileResponse(os.path.join(os.path.dirname(__file__), "index.html"))


# ── Endpoints ─────────────────────────────────────────────────────

@app.get("/health")
async def health():
    """
    Session 12.3 update: session → "12.3", async_job_queue capability
    enabled now that job_store.py + run_chronicle_analysis() exist.
    """
    budget     = calculate_tiered_vram_budget()
    cost       = calculate_monthly_gpu_cost()
    oom        = oom_prevention_check()
    uptime_s   = round(time.monotonic() - getattr(app.state, "start_time", 0))
    mcp_status = (
        app.state.mcp_pool.connection_status()
        if hasattr(app.state, "mcp_pool")
        else {}
    )
    return {
        "status":    "ok",
        "session":   "13.1",
        "version":   "13.1.0",
        "uptime_s":  uptime_s,
        "oom_safe":  oom["all_safe"],
        "agents": {
            name: {
                "role":                   info["role"],
                "tier":                   info["tier"],
                "precision":              info["precision"],
                "gpu_tier":               info["gpu_tier"],
                "max_model_len":          info["max_model_len"],
                "gpu_memory_utilization": info["gpu_memory_utilization"],
            }
            for name, info in CHRONICLE_AGENTS.items()
        },
        "mcp_sources": {
            source: {"connected": connected}
            for source, connected in mcp_status.items()
        },
        "vram_summary": {
            "s11_3_calibrated_gb":    budget["s11_3_calibrated_gb"],
            "vram_saved_vs_s11_1_gb": budget["vram_saved_vs_s11_1_gb"],
            "recommended_gpu":        budget["recommended_gpu"],
        },
        "cost_summary": {
            "recommended_scenario":    cost["recommended_scenario"],
            "monthly_usd":             cost["scenarios"]["D_colocation_l4_a100"]["monthly_usd"],
            "annual_savings_vs_naive": cost["scenarios"]["D_colocation_l4_a100"]["annual_savings_vs_a"],
        },
        "capabilities": [
            "concurrent_5_agent_inference",      # S11.1
            "tiered_quantization_assignments",   # S11.2
            "task_survivability_matrix",         # S11.2
            "oom_prevention_check",              # S11.3
            "colocation_partitioning",           # S11.3
            "vllm_deployment_config",            # S11.3
            "pydantic_request_validation",       # S12.1
            "langgraph_swarm_via_ainvoke",       # S12.1
            "mcp_live_data_ingestion",           # S12.1
            "fastapi_lifespan_graph_compile",    # S12.1
            "sse_streaming",                     # S12.2
            "mid_stream_mcp_tool_calls",         # S12.2
            "async_job_queue",                   # S12.3
            "real_mcp_servers",                  # S12.3
            "opentelemetry_tracing",              # S13.1
            "langgraph_node_spans",               # S13.1
            "ai_attribute_schema",                # S13.1
            "backgroundtask_context_prop",        # S13.1
        ],
    }


@app.post("/analyze", response_model=AnalysisResponse)
async def analyze(request: AnalysisRequest):
    """
    Session 12.1: Runs Chronicle's full 5-agent LangGraph swarm via ainvoke().

    WHY async def:      This function awaits graph.ainvoke() which makes LLM API calls.
                         Synchronous def would block the event loop for the full analysis
                         duration, preventing all other requests from making progress.

    WHY graph.ainvoke(): Routes through all 5 LangGraph nodes in sequence.
                         Replaces S11's direct Gemini REST calls per-agent.
                         State flows through ingestion → pattern → timeline →
                         brutality → synthesis.

    WHY response_model: Validates LangGraph's returned state against
                        AnalysisResponse before serialising. Catches agent
                        output bugs at the gateway.

    Session 12.2 adds POST /analyze/stream, a live SSE stream. This
    synchronous endpoint is preserved unchanged for clients that just
    want the final JSON.
    """
    if not hasattr(app.state, "graph"):
        raise HTTPException(
            status_code=503,
            detail="Chronicle graph not initialised. Check /health/ready.",
        )

    analysis_id = str(uuid.uuid4())
    wall_start  = time.monotonic()

    try:
        # ── Build initial state ────────────────────────────────────
        # Pydantic validation already passed — request is clean.
        # build_initial_state() maps validated fields to ChronicleState.
        initial_state = build_initial_state(request, analysis_id)

        # ── THE AWAIT POINT ────────────────────────────────────────
        # This coroutine suspends here.
        # Event loop runs other coroutines while Chronicle analyses.
        # When graph completes, coroutine resumes with final_state.
        final_state = await app.state.graph.ainvoke(initial_state)

        processing_ms = round((time.monotonic() - wall_start) * 1000)

        return AnalysisResponse(
            analysis_id=     analysis_id,
            question=        request.question,
            correlations=    final_state.get("correlations", []),
            honest_analysis= final_state.get("honest_analysis", ""),
            final_brief=     final_state.get("final_brief", ""),
            confidence=      float(final_state.get("confidence", 0.75)),
            sources_used=    list(final_state.get("raw_data", {}).keys()),
            sources_live=    final_state.get("sources_live", {}),
            processing_ms=   processing_ms,
            session=         "12.1",
            agent_trace=     final_state.get("agent_trace") if request.debug else None,
        )

    except Exception as e:
        log.exception(f"Analysis {analysis_id} failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/analyze/stream")
async def analyze_stream(request: Request, body: AnalysisRequest):
    """
    Session 12.2: Live SSE stream of every Chronicle agent event.

    Replaces the 501 stub from Session 12.1.

    WHY POST not GET:
        Chronicle requires a question body.
        Browser EventSource only supports GET.
        We use fetch() with ReadableStream on the client instead.
        curl -N works fine for testing.

    WHY StreamingResponse:
        Keeps the HTTP connection open and writes frames incrementally.
        The response body is never closed until the generator exits.

    HEADERS:
        X-Accel-Buffering: no  → disables nginx proxy buffering
        Cache-Control: no-cache → prevents intermediate caching
        These two headers are MANDATORY. Without them: all events
        arrive simultaneously after the analysis completes (buffered).

    DISCONNECT DETECTION:
        await request.is_disconnected() checked before every yield.
        If True: generator returns → GeneratorExit propagates to
        graph.astream() → Chronicle analysis stops immediately.
        Without this: GPU runs to completion for a client who left.

    KEEPALIVE:
        If no event arrives within 15 seconds (a slow frontier-tier LLM
        call), a comment-line keepalive frame is sent so intermediate
        proxies (nginx, load balancers) don't kill the idle connection.
    """
    if not hasattr(app.state, "graph"):
        raise HTTPException(status_code=503, detail="Graph not ready.")

    analysis_id = str(uuid.uuid4())
    wall_start  = time.time()
    log.info(f"SSE stream started: {analysis_id}")

    async def sse_generator():
        """
        Async generator that drives the SSE response.
        Yields SSE-formatted strings — FastAPI writes each to the client.
        """
        try:
            initial_state = build_initial_state(body, analysis_id)
            event_gen     = chronicle_stream_events(
                app.state.graph, initial_state, analysis_id, wall_start
            ).__aiter__()

            while True:
                # ── Disconnect check BEFORE every yield ───────────
                # If client closed tab: stop immediately.
                # Without this check: GPU runs 60+ more seconds for nobody.
                if await request.is_disconnected():
                    log.info(f"Client disconnected mid-stream: {analysis_id}")
                    return

                # ── Keepalive: don't block forever waiting on a slow node ──
                try:
                    event = await asyncio.wait_for(event_gen.__anext__(), timeout=15.0)
                except asyncio.TimeoutError:
                    yield keepalive_frame()
                    continue
                except StopAsyncIteration:
                    log.info(f"SSE stream complete: {analysis_id}")
                    return

                yield to_sse_frame(event)

                # Stop after final event — do not wait for client to close
                if getattr(event, "final", False):
                    log.info(f"SSE stream complete: {analysis_id}")
                    return

        except (BrokenPipeError, ConnectionResetError):
            # Client transport closed during a write. Normal — not an error.
            log.info(f"Client transport closed: {analysis_id}")
            return

        except Exception as exc:
            log.exception(f"Stream error for {analysis_id}: {exc}")
            err = ErrorStreamEvent(
                seq=999,
                error_code="STREAM_ERROR",
                message="Internal streaming error. Please retry.",
            )
            yield to_sse_frame(err, "error")

    return StreamingResponse(
        sse_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control":     "no-cache",
            "Connection":        "keep-alive",
            "X-Accel-Buffering": "no",       # ← disables nginx buffer
            "Transfer-Encoding": "chunked",
        },
    )


# ── Async Job Queue (Session 12.3) ────────────────────────────────

def make_idempotency_key(request: AnalysisRequest) -> str:
    """
    Same question + same sources -> same key -> same job_id returned.
    Protects against ghost analyses when a client retries a POST after
    a slow ack (or a proxy timeout) without knowing the first one landed.
    Introduced: Session 12.3. Permanent.
    """
    raw = f"{request.question}|{'|'.join(sorted(request.data_sources))}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


# In-memory idempotency map: key -> job_id (production: Redis key, 24h TTL)
_idempotency_map: dict[str, str] = {}


@app.post("/analyze/async", status_code=202, response_model=JobAcceptedResponse)
async def analyze_async(
    request:          AnalysisRequest,
    background_tasks: BackgroundTasks,
) -> JobAcceptedResponse:
    """
    Session 12.3: 202 Accepted async analysis submission.

    Returns in <100ms regardless of how long the underlying Chronicle
    analysis takes — real MCP servers + 5 sequential Gemini calls
    (2 of which make a mid-reasoning MCP fetch on top) routinely run
    60-90 seconds, well past most gateway timeouts (AWS API Gateway's
    29s ceiling among them). The client polls GET /analyze/jobs/{job_id}
    instead of holding the connection open.

    WHY write_job() BEFORE background_tasks.add_task():
        If the background task starts before the record exists, a
        client polling immediately after the 202 can race the worker
        and see a 404 for a job that is, in fact, running.

    WHY the idempotency check:
        A client that saw a slow ack (or a proxy that timed out on ITS
        OWN 30s budget) may resend the identical question. Without this
        check that spawns a second full LangGraph run — two Ingestion
        Agents hitting the same 5 MCP servers, two sets of Gemini calls,
        for one logical request.
    """
    if not hasattr(app.state, "graph"):
        raise HTTPException(status_code=503, detail="Chronicle graph not initialised. Check /health/ready.")

    idem_key = make_idempotency_key(request)
    if idem_key in _idempotency_map:
        existing_job_id = _idempotency_map[idem_key]
        existing = await read_job(existing_job_id)
        if existing and existing.status not in (JobStatus.FAILED, JobStatus.CANCELLED):
            log.info(f"Duplicate submission detected, returning existing job: {existing_job_id}")
            return JobAcceptedResponse(
                job_id=existing_job_id,
                ticket_id=existing_job_id,
                analysis_id=existing.analysis_id,
                poll_url=f"/analyze/jobs/{existing_job_id}",
            )

    job_id      = str(uuid.uuid4())
    analysis_id = str(uuid.uuid4())

    # ── S13.1: Set job_id on the current HTTP span ────────────────
    # This joins the public job handle to the internal trace.
    # A client who polls /analyze/jobs/{job_id} can look up
    # the trace by querying the job_id attribute.
    current_span = otel_trace.get_current_span()
    current_span.set_attribute("job_id",      job_id)
    current_span.set_attribute("analysis_id", analysis_id)

    # ── S13.1: Capture context BEFORE returning 202 ───────────────
    # The new asyncio task starts with EMPTY context.
    # Capture here, attach inside the background function.
    ctx = otel_context.get_current()

    record = JobRecord(
        job_id=job_id, ticket_id=job_id, analysis_id=analysis_id, question=request.question,
    )

    # CRITICAL: write the record before scheduling the task — see docstring.
    await write_job(record)
    _idempotency_map[idem_key] = job_id

    background_tasks.add_task(
        run_chronicle_analysis,
        job_id,
        job_id,
        app.state.graph,
        request,
        analysis_id,
        ctx,           # S13.1: pass captured context
    )

    log.info(f"Analysis queued: job_id={job_id} analysis_id={analysis_id}")

    return JobAcceptedResponse(
        job_id=job_id, ticket_id=job_id, analysis_id=analysis_id, poll_url=f"/analyze/jobs/{job_id}",
    )


@app.get("/analyze/jobs/{job_id}")
async def get_job_status(job_id: str) -> dict:
    """
    Session 12.3: job status polling endpoint.

    404 = job_id has never been seen. 200 = job exists, inspect `status`.
    Stale detection: a job stuck in "processing" with no heartbeat for
    over 90 seconds means the worker crashed mid-analysis (process
    restart, uncaught exception outside the try/except in
    run_chronicle_analysis, etc.) — reported as failed rather than left
    to poll forever.
    """
    record = await read_job(job_id)
    if record is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found.")

    if (
        record.status == JobStatus.PROCESSING
        and record.last_heartbeat_ms is not None
        and (time.time() * 1000 - record.last_heartbeat_ms) > 90_000
    ):
        record.status = JobStatus.FAILED
        record.error_message = "Worker heartbeat timeout — analysis stalled."
        await write_job(record)

    return record.model_dump()


# ── S11.1/S11.2/S11.3 endpoints preserved exactly ─────────────────

@app.get("/vram-budget")
async def vram_budget(precision: str = "fp16"):
    """Session 11.1: uniform VRAM budget at a given precision. Unchanged."""
    valid = {"fp32", "fp16", "int8", "int4"}
    if precision not in valid:
        raise HTTPException(status_code=422, detail=f"Invalid precision. Must be one of: {sorted(valid)}")
    return calculate_chronicle_vram_budget(precision)


@app.get("/vram-budget/tiered")
async def vram_budget_tiered():
    """Session 11.2/11.3: per-agent tiered budget. S11.3 uses max_model_len-aware KV calc."""
    return calculate_tiered_vram_budget()


@app.get("/cost-model")
async def cost_model():
    """Session 11.2/11.3: monthly GPU cost. S11.3 adds Scenario D co-location."""
    return calculate_monthly_gpu_cost()


@app.get("/survivability")
async def survivability(task_type: str = None):
    """Session 11.2: task survivability matrix. Unchanged."""
    return task_survivability_matrix(task_type)


@app.get("/calibration-stats")
async def calibration_stats():
    """Session 11.2: calibration dataset summary. Restored with S12.1's full dataset."""
    from collections import Counter
    sources = Counter(s["source"] for s in CHRONICLE_CALIBRATION_DATASET)
    tasks   = Counter(s["expected_task"] for s in CHRONICLE_CALIBRATION_DATASET)
    return {
        "total_samples":  len(CHRONICLE_CALIBRATION_DATASET),
        "sources":        dict(sources),
        "task_types":     dict(tasks),
        "unique_sources": len(sources),
    }


@app.get("/deployment-config")
async def deployment_config():
    """
    Session 11.3: vLLM launch configuration per agent.
    Returns the exact --flags to pass to `vllm serve` for each Chronicle agent.
    Includes model ID, port, tensor_parallel_size, max_model_len,
    gpu_memory_utilization, max_num_seqs, and the full launch command.
    """
    return {
        "agents":     vllm_config_per_agent(),
        "colocation": colocation_partitioner(),
        "session":    "11.3",
        "note": (
            "Model IDs are placeholders. Replace with your AWQ/FP16 "
            "HuggingFace model ID before production deploy. "
            "Gemini API is used for inference until local vLLM is configured."
        ),
    }


@app.get("/oom-check")
async def oom_check():
    """
    Session 11.3: OOM prevention check for all 5 Chronicle agents.
    Returns per-agent max_safe_concurrent calculation and overall pass/fail.
    Use this endpoint in your monitoring stack to verify deployment safety.
    Alert if any agent returns max_safe_concurrent == 0.
    """
    oom   = oom_prevention_check()
    coloc = colocation_partitioner()
    return {
        "oom_prevention": oom,
        "colocation":     coloc,
        "all_safe":       oom["all_safe"] and coloc["safe"],
        "session":        "11.3",
    }


@app.get("/concurrency-table")
async def concurrency_table():
    """
    Session 11.3: max_model_len vs max_concurrent_requests table.
    Shows the concurrency cost of each context window size for each agent's GPU.
    Use this to validate that Chronicle's locked max_model_len values
    provide enough concurrent capacity for expected traffic.
    """
    results     = {}
    mml_options = [1_024, 2_048, 4_096, 8_192, 16_384, 32_768, 65_536, 131_072]

    for name, info in CHRONICLE_AGENTS.items():
        gpu_vram  = GPU_VRAM_GB.get(info["gpu_tier"], 24)
        bytes_pp  = VRAM_BYTES_PER_PARAM[info["precision"]]
        weight_gb = (info["model_size_b"] * 1e9 * bytes_pp) / (1024 ** 3)

        if info["tier"] == "utility":
            effective_vram = gpu_vram * info["gpu_memory_utilization"]
        else:
            effective_vram = gpu_vram

        table = []
        for mml in mml_options:
            kv_per_req = KV_CACHE_GB_PER_AGENT_4K * (mml / 4_096) * (info["model_size_b"] / 7.0)
            overhead   = CUDA_OVERHEAD_GB
            buffer     = effective_vram * 0.10
            available  = effective_vram - weight_gb - overhead - buffer
            max_conc   = max(0, int(available / kv_per_req)) if kv_per_req > 0 else 0
            table.append({
                "max_model_len":  mml,
                "kv_per_req_gb":  round(kv_per_req, 3),
                "max_concurrent": max_conc,
                "locked":         mml == info["max_model_len"],
            })

        results[name] = {
            "gpu_tier":             info["gpu_tier"],
            "effective_vram_gb":    round(effective_vram, 1),
            "locked_max_model_len": info["max_model_len"],
            "table":                table,
        }

    return {"per_agent": results, "session": "11.3"}


# ── Health probes (Session 12.1) ──────────────────────────────────

@app.get("/health/live")
async def health_live():
    """Kubernetes liveness probe: process is running."""
    return {"status": "alive"}


@app.get("/health/ready")
async def health_ready():
    """Kubernetes readiness probe: graph is compiled and MCP pool is ready."""
    graph_ready = hasattr(app.state, "graph") and app.state.graph is not None
    mcp_ready   = hasattr(app.state, "mcp_pool")
    ready       = graph_ready and mcp_ready
    return JSONResponse(
        content={"status": "ready" if ready else "not_ready",
                 "graph":  graph_ready,
                 "mcp":    mcp_ready},
        status_code=200 if ready else 503,
    )


# ── Server Entry Point ────────────────────────────────────────────

if __name__ == "__main__":
    print("\n  Chronicle API — Session 13.1")
    print("  Starting on http://localhost:8000")
    print("  Swagger UI: http://localhost:8000/docs\n")
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=False)
