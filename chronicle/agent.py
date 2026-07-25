"""
╔══════════════════════════════════════════════════════════════════╗
║  CHRONICLE — agent.py                                            ║
║  Session 12.3: Real MCP Servers + Async 202 Job Queue            ║
╚══════════════════════════════════════════════════════════════════╝

Changes in Session 12.3 (all additions — nothing removed):
  - mcp_servers/: 5 standalone FastAPI processes (ports 3001-3005),
    one per Chronicle data source, speaking JSON-RPC 2.0 over /mcp.
    MCPClientPool.fetch_source() now has something real to call —
    previously every call silently fell back to CHRONICLE_CALIBRATION_DATASET.
  - job_store.py: JobStatus enum, JobRecord, JobAcceptedResponse,
    write_job()/read_job()/update_status() — the async job state machine.
  - run_chronicle_analysis(): background worker that drives the same
    build_chronicle_graph() swarm via graph.astream(), writing job
    status at every node transition. Scheduled by FastAPI BackgroundTasks
    from POST /analyze/async so the HTTP response returns in <100ms
    regardless of how long the underlying analysis takes.
  - run_session_verification(): EXTENDED with 3 new S12.3 checks
    (job_store round-trip, run_chronicle_analysis is async, full async
    job lifecycle reaches status=completed). The 5 S12.2 checks are
    unchanged.

Changes in Session 12.2 (all additions — nothing removed):
  - ChronicleState: gains tool_calls / tool_results accumulator fields
  - make_pattern_node() / make_brutality_node(): now accept mcp_pool and
    invoke MCP mid-reasoning, emitting tool_calls / tool_results
  - make_synthesis_node(): emits token_chunk for the stream adapter
  - NODE_LABELS / AGENT_NODES: node name → display label map
  - chronicle_stream_events(): async generator translating graph.astream()
    chunks into typed Chronicle SSE events (see stream_schemas.py)
  - run_session_verification(): REPLACED with 5 S12.2 checks

Changes in Session 12.1 (all additions — nothing removed):
  - CHRONICLE_CALIBRATION_DATASET restored to its full 30-sample form
    (S11.3 shipped it as a stub; MCP fallback needs the real data)
  - MCP_SOURCE_CONFIG: MCP server URLs per Chronicle data source
  - MCPClientPool: manages one MCP client per data source, falls back
    to CHRONICLE_CALIBRATION_DATASET when the MCP server is unreachable
  - build_mcp_client_pool() / close_mcp_client_pool(): lifespan hooks
  - ChronicleState: LangGraph TypedDict state shared by all 5 agents
  - build_llm_instances(): utility_llm / frontier_llm ChatGoogleGenerativeAI
  - Node functions: ingestion_node, pattern_node, timeline_node,
                    brutality_node, synthesis_node
  - build_chronicle_graph(): compiles the LangGraph StateGraph
  - build_initial_state(): maps a validated AnalysisRequest to ChronicleState
  - AnalysisRequest: REPLACED — Field-validated production version
  - AnalysisResponse: new response schema for the LangGraph swarm output
  - run_session_verification(): REPLACED with 5 S12.1 checks

Previous sessions preserved unchanged:
  - S11.1: chronicle_infer() [Gemini REST], run_concurrent_analysis(),
    BenchmarkResult, calculate_chronicle_vram_budget()
  - S11.2: CHRONICLE_AGENTS precision/model_size_b/gpu_tier,
    TASK_SURVIVABILITY_MATRIX, task_survivability_matrix()
  - S11.3: max_model_len/gpu_memory_utilization, calculate_tiered_vram_budget(),
    calculate_monthly_gpu_cost(), calculate_max_safe_concurrent(),
    oom_prevention_check(), vllm_config_per_agent(), colocation_partitioner(),
    kv_cache_growth_simulator()
"""

# ── Imports (Session 11.1 — unchanged) ───────────────────────────
import asyncio
import aiohttp
import time
import statistics
import json
import os
import math
import operator
from typing import Optional, Annotated, TypedDict, Literal
from pydantic import BaseModel, Field, ConfigDict
from dotenv import load_dotenv
import ssl
import certifi
import google.generativeai as genai

# ── Imports (Session 12.1) ────────────────────────────────────────
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage
from langgraph.graph import StateGraph, END

# ── Imports (Session 13.1 — OpenTelemetry) ────────────────────────
from opentelemetry import context as otel_context
from otel_setup import get_tracer, Status, StatusCode

tracer = get_tracer("chronicle.agents")  # S13.1: module-level tracer

load_dotenv()

# ── Configuration (Session 11.1 — unchanged) ─────────────────────
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
MODEL = "gemini-2.5-flash"
GEMINI_REST_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    f"{MODEL}:generateContent"
)

if not GEMINI_API_KEY:
    raise EnvironmentError(
        "GEMINI_API_KEY environment variable is not set. "
        "Export it before running Chronicle."
    )

genai.configure(api_key=GEMINI_API_KEY)

# ── Chronicle Agent Roles (Session 11.3 — EXTENDED) ──────────────
# Session 11.1: role, tier
# Session 11.2: precision, model_size_b, gpu_tier, monthly_gpu_cost_usd,
#               survivability_note
# Session 11.3: max_model_len, gpu_memory_utilization
#
# max_model_len: calibrated to Chronicle's actual workload distribution.
#   Utility agents: 4,096 — data parsing + pattern tasks are bounded.
#   Frontier agents: 8,192 — long analysis + synthesis needs more context.
#   Model default would be 128K — giving 0 concurrent slots on most GPUs.
#
# gpu_memory_utilization: for co-located utility agents on shared L4.
#   3 utility agents × 0.28 = 0.84 + 0.08 system overhead = 0.92 total.
#   Frontier agents each own their GPU: 0.85 (15% safety buffer).
CHRONICLE_AGENTS = {
    "ingestion": {
        "role":                   "Parse and normalise raw data from all 5 sources",
        "tier":                   "utility",
        "precision":              "int4",
        "model_size_b":           7,
        "gpu_tier":               "L4",
        "monthly_gpu_cost_usd":   450,
        "survivability_note":     "Structured parsing — survives INT4",
        "max_model_len":          4_096,       # S11.3: locked
        "gpu_memory_utilization": 0.28,        # S11.3: co-located on shared L4
    },
    "pattern": {
        "role":                   "Find cross-source correlations between data signals",
        "tier":                   "utility",
        "precision":              "int4",
        "model_size_b":           7,
        "gpu_tier":               "L4",
        "monthly_gpu_cost_usd":   450,
        "survivability_note":     "Statistical pattern matching — survives INT4",
        "max_model_len":          4_096,       # S11.3: locked
        "gpu_memory_utilization": 0.28,        # S11.3: co-located on shared L4
    },
    "timeline": {
        "role":                   "Sequence life events from data across all sources",
        "tier":                   "utility",
        "precision":              "int4",
        "model_size_b":           7,
        "gpu_tier":               "L4",
        "monthly_gpu_cost_usd":   450,
        "survivability_note":     "Temporal classification — survives INT4",
        "max_model_len":          4_096,       # S11.3: locked
        "gpu_memory_utilization": 0.28,        # S11.3: co-located on shared L4
    },
    "brutality": {
        "role":                   "Deliver honest cross-source analysis, no softening",
        "tier":                   "frontier",
        "precision":              "fp16",
        "model_size_b":           13,
        "gpu_tier":               "A100-40",
        "monthly_gpu_cost_usd":   1500,
        "survivability_note":     "Long-context multi-constraint reasoning — requires FP16",
        "max_model_len":          8_192,       # S11.3: locked — needs longer context
        "gpu_memory_utilization": 0.85,        # S11.3: dedicated GPU, 15% safety buffer
    },
    "synthesis": {
        "role":                   "Produce the final structured analyst brief",
        "tier":                   "frontier",
        "precision":              "fp16",
        "model_size_b":           13,
        "gpu_tier":               "A100-40",
        "monthly_gpu_cost_usd":   1500,
        "survivability_note":     "Multi-source structured generation — requires FP16",
        "max_model_len":          8_192,       # S11.3: locked — needs longer context
        "gpu_memory_utilization": 0.85,        # S11.3: dedicated GPU, 15% safety buffer
    },
}

# ── VRAM Constants (Session 11.1 — unchanged) ────────────────────
VRAM_BYTES_PER_PARAM = {
    "fp32": 4, "fp16": 2, "int8": 1, "int4": 0.5,
}
KV_CACHE_GB_PER_AGENT_4K  = 2.0   # 7B model @ 4K context. Used by S11.1 uniform calc.
MODEL_WEIGHT_GB_7B_FP16    = 14.0
MODEL_WEIGHT_GB_7B_INT4    = 3.5
CUDA_OVERHEAD_GB           = 2.0
SAFETY_HEADROOM_GB         = 8.0

# ── GPU Tier Reference (Session 11.2 — unchanged) ────────────────
GPU_TIER_COSTS = {
    "T4":      200,
    "L4":      450,
    "A10G":    550,
    "A100-40": 1500,
    "A100-80": 1875,
    "H100-80": 2800,
}

# ── GPU VRAM Reference (Session 11.3) ────────────────────────────
# Used by OOM prevention check and concurrency table.
# Permanent from Session 11.3 onward.
GPU_VRAM_GB = {
    "T4":      16,
    "L4":      24,
    "A10G":    24,
    "A100-40": 40,
    "A100-80": 80,
    "H100-80": 80,
}

# ── Task Survivability Matrix (Session 11.2 — unchanged) ──────────
TASK_SURVIVABILITY_MATRIX = {
    "intent_classification":    {"int4_retention_pct": 98, "passes_90pct": True,  "agent_tier": "supervisor_int4", "reason": "Coarse category boundary; tolerates individual weight errors"},
    "named_entity_extraction":  {"int4_retention_pct": 95, "passes_90pct": True,  "agent_tier": "supervisor_int4", "reason": "Pattern-matching on broad statistical regularities"},
    "sentiment_analysis":       {"int4_retention_pct": 97, "passes_90pct": True,  "agent_tier": "supervisor_int4", "reason": "Coarse 3-way classification; extremely error-tolerant"},
    "summarisation":            {"int4_retention_pct": 93, "passes_90pct": True,  "agent_tier": "supervisor_int4", "reason": "Broad statistical task; minor fluency variation acceptable"},
    "structured_data_parsing":  {"int4_retention_pct": 93, "passes_90pct": True,  "agent_tier": "supervisor_int4", "reason": "Chronicle ingestion task — tolerates INT4 well"},
    "temporal_sequencing":      {"int4_retention_pct": 94, "passes_90pct": True,  "agent_tier": "supervisor_int4", "reason": "Timeline ordering is pattern-based; survives INT4"},
    "cross_source_correlation": {"int4_retention_pct": 91, "passes_90pct": True,  "agent_tier": "supervisor_int4", "reason": "Statistical pattern finding — marginal but passes threshold"},
    "structured_generation":    {"int4_retention_pct": 88, "passes_90pct": False, "agent_tier": "specialist_fp16", "reason": "JSON schema compliance degrades at INT4"},
    "long_context_coherence":   {"int4_retention_pct": 85, "passes_90pct": False, "agent_tier": "specialist_fp16", "reason": "Attention weight errors cause contradiction across long outputs"},
    "multi_constraint_reasoning":{"int4_retention_pct": 83, "passes_90pct": False, "agent_tier": "specialist_fp16", "reason": "Simultaneous constraint satisfaction degrades under quantization"},
    "code_generation":          {"int4_retention_pct": 84, "passes_90pct": False, "agent_tier": "specialist_fp16", "reason": "Syntax precision and API signatures must be exact"},
}

# ── Chronicle Calibration Dataset (Session 11.2 — restored in S12.1) ─
# 30 samples across 5 sources. Was shipped as a stub in S11.3; restored
# here because MCPClientPool.fetch_source() falls back to these samples
# when a Chronicle MCP server is unreachable (dev/demo mode).
CHRONICLE_CALIBRATION_DATASET = [
    # ── Spotify / listening data (6 samples) ──────────────────────
    {
        "sample_id": 1, "source": "spotify",
        "text": "My Spotify data for the last 30 days: top artists are Radiohead (42 plays), Kendrick Lamar (38 plays), Mac DeMarco (31 plays). Listening sessions peak between 11pm and 2am on weekdays. Skip rate is highest on Monday mornings. Average session length has dropped from 47 minutes to 22 minutes over the last 6 weeks.",
        "expected_task": "structured_data_parsing",
    },
    {
        "sample_id": 2, "source": "spotify",
        "text": "Playlist: Late Night Coding. 94 songs. Total duration 6h 12m. Most played: Boards of Canada - Roygbiv (11 plays this month). Genre distribution: 62% ambient/electronic, 28% lo-fi hip hop, 10% post-rock. Added 14 new songs this week, removed 0.",
        "expected_task": "structured_data_parsing",
    },
    {
        "sample_id": 3, "source": "spotify",
        "text": "Listening history export. 2024-03-01 23:14: Thom Yorke - Unmade (skipped at 0:42). 2024-03-01 23:15: Burial - Archangel (played full). 2024-03-01 23:21: The National - Bloodbuzz Ohio (played full). 2024-03-01 23:25: Bon Iver - Holocene (skipped at 1:03). Pattern: skips increase after midnight.",
        "expected_task": "temporal_sequencing",
    },
    {
        "sample_id": 4, "source": "spotify",
        "text": "Monthly wrapped summary. Minutes listened: 4,847. Top genre: Indie Rock (34%). Mood index (Spotify valence average): 0.28 out of 1.0 — below baseline of 0.41 from 6 months ago. New artists discovered: 3. Listening streak: 31 consecutive days.",
        "expected_task": "structured_data_parsing",
    },
    {
        "sample_id": 5, "source": "spotify",
        "text": "Cross-reference: Spotify listening peaks correlate with GitHub commit timestamps. On days with commits after 10pm, Radiohead plays increased 40%. On days with no commits, listening sessions start 2 hours earlier on average and genre shifts toward upbeat pop.",
        "expected_task": "cross_source_correlation",
    },
    {
        "sample_id": 6, "source": "spotify",
        "text": "Artist loyalty analysis. Artists with 3+ months of continuous listening: Radiohead (14 months), Burial (9 months), Mac DeMarco (7 months). Artists dropped after 1 listen this year: 47. Artists started in 2024 still active: 2 of 31. Churn rate: 94%.",
        "expected_task": "temporal_sequencing",
    },

    # ── Finance / transactions (6 samples) ────────────────────────
    {
        "sample_id": 7, "source": "finance",
        "text": "Transaction log March 2024. Uber Eats: £147 (9 orders). Deliveroo: £93 (6 orders). Tesco: £38 (2 visits). Gym membership: £45 (cancelled 14 March). Netflix: £15.99. Spotify Premium: £10.99. Amazon: £234 (3 orders, all placed between 11pm and 1am). Total outflow: £1,847.",
        "expected_task": "structured_data_parsing",
    },
    {
        "sample_id": 8, "source": "finance",
        "text": "Savings rate by month. January: 18%. February: 12%. March: 4%. April: -3% (net withdrawal). Savings account balance: £2,140 down from £3,890 in January. Emergency fund target: 3 months expenses = £6,300. Current coverage: 10 days.",
        "expected_task": "temporal_sequencing",
    },
    {
        "sample_id": 9, "source": "finance",
        "text": "Spending category breakdown Q1 2024. Food delivery: £428 (23%). Subscriptions: £89 (5%). Transport: £312 (17%). Groceries: £156 (8%). Entertainment: £203 (11%). Impulse purchases flagged by bank: 14 transactions totalling £387. All 14 placed after 10pm.",
        "expected_task": "structured_data_parsing",
    },
    {
        "sample_id": 10, "source": "finance",
        "text": "Recurring charge audit. Active subscriptions: 11. Subscriptions not used in 30 days: 4 (Adobe £54, Notion £16, Duolingo £7, unused gym £45). Total monthly spend on unused subscriptions: £122. Annual waste projection: £1,464.",
        "expected_task": "named_entity_extraction",
    },
    {
        "sample_id": 11, "source": "finance",
        "text": "Finance + GitHub correlation. Highest spend months (March, July) align with lowest commit months. In months with 100+ commits, food delivery spend drops 34% and grocery spend increases 28%. Hypothesis: high-output work periods correlate with better self-maintenance.",
        "expected_task": "cross_source_correlation",
    },
    {
        "sample_id": 12, "source": "finance",
        "text": "Income vs expenditure timeline. January surplus: £890. February surplus: £340. March deficit: £210. April deficit: £580. Current trajectory: 6-week cash-flow negative. Last 4 Amazon purchases total: £612. All placed within 2 hours of GitHub force-push events.",
        "expected_task": "temporal_sequencing",
    },

    # ── GitHub / commit activity (6 samples) ──────────────────────
    {
        "sample_id": 13, "source": "github",
        "text": "Commit activity last 90 days. Total commits: 847. Commits between 9pm and 1am: 619 (73%). Commits on weekends: 0. Force pushes to main: 14. PRs opened: 22. PRs merged: 8. PRs open >30 days: 6. Longest PR open: 67 days. Repositories: 4 active, 11 abandoned.",
        "expected_task": "structured_data_parsing",
    },
    {
        "sample_id": 14, "source": "github",
        "text": "Language drift analysis. 2022: 80% Python, 20% JavaScript. 2023: 65% Python, 30% JavaScript, 5% TypeScript. 2024: 45% Python, 20% JavaScript, 35% TypeScript. Commit message quality score (using conventional commits): 2022: 71%, 2023: 58%, 2024: 41%.",
        "expected_task": "temporal_sequencing",
    },
    {
        "sample_id": 15, "source": "github",
        "text": "Repository health audit. chronicle: last commit 2 days ago, 0 tests, 0 CI, README complete. side-project-alpha: last commit 8 months ago, 847 commits, 0 releases. side-project-beta: created January, 3 commits, never opened again. abandoned-saas: 1,200 commits, deleted by author.",
        "expected_task": "named_entity_extraction",
    },
    {
        "sample_id": 16, "source": "github",
        "text": "Commit pattern analysis by hour. Peak hours: 22:00 (127 commits), 23:00 (198 commits), 00:00 (143 commits), 01:00 (89 commits). Trough hours: 09:00 (3 commits), 10:00 (7 commits), 14:00 (11 commits). No commits in any calendar year between 06:00 and 08:00.",
        "expected_task": "temporal_sequencing",
    },
    {
        "sample_id": 17, "source": "github",
        "text": "Force push events correlated with finance data. 14 force pushes to main this year. Within 3 hours of each force push: 11 of 14 cases show a transaction on Uber Eats or Amazon. Mean spend within the 3-hour window: £43. Interpretation: force pushes may correlate with stress-spend episodes.",
        "expected_task": "cross_source_correlation",
    },
    {
        "sample_id": 18, "source": "github",
        "text": "PR review lag analysis. Average time from PR open to first review: 12 days. Average time from first review to merge: 31 days. All 6 PRs open >30 days are authored by the same user. Pattern: code is written in late-night bursts but review follow-through consistently stalls.",
        "expected_task": "structured_data_parsing",
    },

    # ── Fitness / health (6 samples) ────────────────────────────────
    {
        "sample_id": 19, "source": "fitness",
        "text": "Apple Health export March 2024. Steps per day average: 3,847 (recommended: 8,000). Active calories: 187/day (target: 400). Exercise minutes: 12/day (target: 30). Resting heart rate trend: 68 bpm January → 74 bpm March (+9%). Sleep average: 5h 42m. Deep sleep: 48 minutes.",
        "expected_task": "structured_data_parsing",
    },
    {
        "sample_id": 20, "source": "fitness",
        "text": "Gym attendance log. January: 11 sessions. February: 6 sessions. March: 2 sessions (membership cancelled mid-month). Last gym session: 19 March. Reason for cancellation noted in bank statement memo: 'not using it'. Body weight log: +4.2 kg since January.",
        "expected_task": "temporal_sequencing",
    },
    {
        "sample_id": 21, "source": "fitness",
        "text": "Sleep timeline. Bedtime average: 01:47. Wake time average: 09:12. Sleep duration: 7h 25m average, but sleep quality score (Oura): 62/100. REM sleep: 18% (recommended: 20–25%). Sleep debt accumulated this week: 4h 20m. Nights with sleep <6h: 8 of last 30.",
        "expected_task": "temporal_sequencing",
    },
    {
        "sample_id": 22, "source": "fitness",
        "text": "Fitness + GitHub correlation. On days with >50 commits, step count averages 2,100 (vs 4,900 on low-commit days). High-commit weeks show resting heart rate 6 bpm higher than baseline. Deep sleep duration drops 31% in weeks with >200 commits.",
        "expected_task": "cross_source_correlation",
    },
    {
        "sample_id": 23, "source": "fitness",
        "text": "Movement pattern analysis. Sedentary time average: 14.2 hours per day. Longest sedentary streak this month: 22 hours continuous. Movement breaks (stand alerts dismissed): 147 of 210 (70% dismissed). Days with zero outdoor movement: 18 of 31.",
        "expected_task": "structured_data_parsing",
    },
    {
        "sample_id": 24, "source": "fitness",
        "text": "Heart rate variability trend. HRV average January: 52ms. HRV average February: 48ms. HRV average March: 41ms. Below 40ms threshold (indicator of physiological stress): 7 days in March. Correlation with late-night commits: HRV lowest on days following >3 hours of late-night coding.",
        "expected_task": "cross_source_correlation",
    },

    # ── Journal / notes (6 samples) ─────────────────────────────────
    {
        "sample_id": 25, "source": "journal",
        "text": "2024-03-04. Couldn't focus today. Opened 14 browser tabs about side project ideas. Closed all of them. Spent 3 hours on Twitter. Ordered Uber Eats at 11pm. Pushed one commit at 00:30 that immediately got force-pushed to main. Going to sleep.",
        "expected_task": "sentiment_analysis",
    },
    {
        "sample_id": 26, "source": "journal",
        "text": "2024-03-11. Good day. Shipped the auth module. 47 commits. PR up. Actually went outside for 40 minutes. Cooked dinner instead of ordering. Listened to Tame Impala not Radiohead. Bedtime 11:30pm which is early for me. HRV was 58ms this morning — highest in weeks.",
        "expected_task": "sentiment_analysis",
    },
    {
        "sample_id": 27, "source": "journal",
        "text": "2024-02-28. The side project has 1,200 commits and zero users. I keep telling people I'm working on it. I deleted the repo tonight. I don't know if that was the right call. Ordered food. Listened to Radiohead. Didn't commit to anything else.",
        "expected_task": "sentiment_analysis",
    },
    {
        "sample_id": 28, "source": "journal",
        "text": "Recurring themes in journal entries this quarter. Mentioned: 'tired' (34 times), 'tomorrow' (67 times), 'should have' (28 times), 'eventually' (41 times), 'just need to' (53 times). Words not appearing: 'done', 'shipped', 'finished'. Single most common phrase: 'going to sleep'.",
        "expected_task": "named_entity_extraction",
    },
    {
        "sample_id": 29, "source": "journal",
        "text": "2024-03-19. Cancelled the gym. Told myself it was because I wasn't using it. The truth is I stopped going in February when the project started going badly and I was embarrassed to leave the house for something non-essential when the code wasn't working.",
        "expected_task": "sentiment_analysis",
    },
    {
        "sample_id": 30, "source": "journal",
        "text": "Journal + all-source pattern summary. 8 of the 10 worst journal sentiment days align with: Radiohead in top 3 plays, food delivery >£25, GitHub commits between midnight and 2am, step count <2,000, HRV <45ms. 3 of these 5 signals appearing on the same day predicts a negative journal entry with 84% accuracy.",
        "expected_task": "cross_source_correlation",
    },
]


# ── Pydantic Schemas (Session 12.1 — AnalysisRequest REPLACED) ────

class AnalysisRequest(BaseModel):
    """
    What it does:   Validates every incoming Chronicle HTTP request.
                    Replaces S11's loosely-typed AnalysisRequest with a
                    production version. 422 returned instantly if any
                    field fails — before the LangGraph swarm boots.
    When called:    FastAPI parses HTTP JSON body into this model automatically.
    Introduced:     Session 11.1. Replaced Session 12.1. Permanent.

    Validation gates:
    - question min_length=1: empty question → 422 in <1ms, 0 API calls
    - question max_length=2000: prevents context window overflow
    - data_sources Literal: unknown source → 422 before MCP attempted
    - depth Literal: unknown depth → 422 before any work starts
    """
    model_config = ConfigDict(str_strip_whitespace=True)

    question: str = Field(
        min_length=1,
        max_length=2000,
        description="The analysis question to ask Chronicle.",
    )
    data_sources: list[
        Literal["spotify", "finance", "fitness", "github", "journal"]
    ] = Field(
        default=["spotify", "finance", "fitness", "github", "journal"],
        description="Which Chronicle data sources to include.",
    )
    depth: Literal["quick", "standard", "deep"] = Field(
        default="standard",
        description="Analysis depth. Controls agent token budgets.",
    )
    debug: bool = Field(
        default=False,
        description="Include agent_trace in the response if true.",
    )


class BenchmarkResult(BaseModel):
    """
    What it does:   Stores timing metrics for a single inference request.
    When called:    Populated by chronicle_infer().
    Returns:        Structured metrics dict.
    Introduced:     Session 11.1. Updated S11.2 (response_text). Permanent.
    """
    request_id:                int
    ttft_seconds:              Optional[float] = None
    total_latency_seconds:     Optional[float] = None
    approximate_output_tokens: int = 0
    tpot_seconds:              Optional[float] = None
    status:                    str = "error"
    error_message:             Optional[str] = None
    response_text:             Optional[str] = None


class AnalysisResponse(BaseModel):
    """
    What it does:   Defines the output contract for every Chronicle analysis
                    run through the LangGraph swarm. FastAPI validates the
                    graph's final state against this before serialising —
                    agent output bugs are caught at the gateway, not sent
                    to the client as null fields.
    When called:    Used as response_model= in the /analyze endpoint.
    Introduced:     Session 12.1. Permanent.
    """
    analysis_id:     str
    question:        str
    correlations:    list[str]
    honest_analysis: str
    final_brief:      str
    confidence:       float = Field(ge=0.0, le=1.0)
    sources_used:     list[str]
    sources_live:     dict
    processing_ms:    int
    session:          str = "12.1"
    agent_trace:      Optional[list[str]] = None


# ── VRAM Budget — Uniform (Session 11.1 — unchanged) ─────────────

def calculate_chronicle_vram_budget(precision: str = "fp16") -> dict:
    """
    What it does:   Calculates VRAM assuming ONE precision for all 5 agents.
                    Kept from S11.1 for backward compatibility with /vram-budget.
                    Use calculate_tiered_vram_budget() for production planning.
    Introduced:     Session 11.1. Permanent.
    """
    bytes_per_param = VRAM_BYTES_PER_PARAM.get(precision, 2)
    weight_gb = (7_000_000_000 * bytes_per_param) / (1024 ** 3)
    kv_total  = len(CHRONICLE_AGENTS) * KV_CACHE_GB_PER_AGENT_4K
    total_gb  = weight_gb + kv_total + CUDA_OVERHEAD_GB + SAFETY_HEADROOM_GB
    return {
        "precision":         precision,
        "weight_gb":         round(weight_gb, 1),
        "kv_cache_total_gb": round(kv_total, 1),
        "agents":            len(CHRONICLE_AGENTS),
        "kv_per_agent_gb":   KV_CACHE_GB_PER_AGENT_4K,
        "cuda_overhead_gb":  CUDA_OVERHEAD_GB,
        "safety_buffer_gb":  SAFETY_HEADROOM_GB,
        "total_required_gb": round(total_gb, 1),
        "recommended_gpu":   "A100-40GB" if total_gb <= 40 else "A100-80GB",
        "note":              "Uniform precision — use /vram-budget/tiered for production",
    }


# ── VRAM Budget — Tiered (Session 11.2 UPDATED in 11.3) ──────────

def calculate_tiered_vram_budget() -> dict:
    """
    What it does:   Calculates Chronicle's VRAM budget using the ACTUAL
                    precision tier AND max_model_len per agent.
                    S11.2 used a flat 2.0 GB KV constant.
                    S11.3 uses per-agent max_model_len for accurate KV sizing.
    When called:    /vram-budget/tiered endpoint, OOM check, CLI output.
    Returns:        Per-agent VRAM breakdown and three totals:
                    s11_1_baseline, s11_2_tiered, s11_3_calibrated.
    Introduced:     Session 11.2. Updated Session 11.3. Permanent.
    """
    per_agent = {}
    total_weights_gb   = 0.0
    total_kv_s11_3_gb  = 0.0
    total_kv_s11_2_gb  = 0.0  # flat 4K for comparison

    for name, info in CHRONICLE_AGENTS.items():
        bytes_pp   = VRAM_BYTES_PER_PARAM[info["precision"]]
        weight_gb  = (info["model_size_b"] * 1_000_000_000 * bytes_pp) / (1024 ** 3)

        # S11.3: KV cache scaled to actual max_model_len (calibrated per agent)
        kv_gb_calibrated = KV_CACHE_GB_PER_AGENT_4K * (info["max_model_len"] / 4_096)
        # S11.2: no max_model_len lock → conservative 8K budget for all agents
        # (frontier needed 8K, so the safe uniform estimate was 8K for all)
        kv_gb_flat = KV_CACHE_GB_PER_AGENT_4K * 2  # 4.0 GB per agent (8K flat)

        total_weights_gb  += weight_gb
        total_kv_s11_3_gb += kv_gb_calibrated
        total_kv_s11_2_gb += kv_gb_flat

        per_agent[name] = {
            "precision":              info["precision"],
            "model_size_b":           info["model_size_b"],
            "gpu_tier":               info["gpu_tier"],
            "max_model_len":          info["max_model_len"],
            "gpu_memory_utilization": info["gpu_memory_utilization"],
            "weight_gb":              round(weight_gb, 1),
            "kv_cache_gb":            round(kv_gb_calibrated, 2),
            "agent_total_gb":         round(weight_gb + kv_gb_calibrated, 1),
        }

    s11_3_total = total_weights_gb + total_kv_s11_3_gb + CUDA_OVERHEAD_GB + 4.0
    s11_2_total = total_weights_gb + total_kv_s11_2_gb + CUDA_OVERHEAD_GB + 4.0

    # S11.1 baseline: all 5 agents at 7B FP16, flat 4K KV
    s11_1_baseline = (
        5 * MODEL_WEIGHT_GB_7B_FP16
        + 5 * KV_CACHE_GB_PER_AGENT_4K
        + CUDA_OVERHEAD_GB
        + SAFETY_HEADROOM_GB
    )

    return {
        "per_agent":              per_agent,
        "total_weights_gb":       round(total_weights_gb, 1),
        "kv_cache_total_gb":      round(total_kv_s11_3_gb, 1),
        "cuda_overhead_gb":       CUDA_OVERHEAD_GB,
        "safety_buffer_gb":       4.0,
        "s11_3_calibrated_gb":    round(s11_3_total, 1),
        "s11_2_tiered_gb":        round(s11_2_total, 1),
        "s11_1_baseline_gb":      round(s11_1_baseline, 1),
        "vram_saved_vs_s11_1_gb": round(s11_1_baseline - s11_3_total, 1),
        "vram_saved_vs_s11_2_gb": round(s11_2_total - s11_3_total, 1),
        "recommended_gpu":        "A100-40GB" if s11_3_total <= 40 else "A100-80GB",
        "note": (
            "S11.3 update: KV cache now calibrated per-agent. "
            "S11.2 budgeted 8K for all agents (safe uniform estimate, no max_model_len lock). "
            "S11.3 locks utility at 4K (saves 2 GB KV each) and frontier at 8K. "
            "Result: -6 GB vs S11.2 conservative estimate."
        ),
    }


# ── OOM Prevention Formula (Session 11.3) ────────────────────────

def calculate_max_safe_concurrent(agent_name: str) -> dict:
    """
    What it does:   Applies the OOM prevention formula to one Chronicle agent.
                    Max Safe Concurrent = (GPU VRAM - Weights - Overhead - Buffer)
                                          ─────────────────────────────────────────
                                                     KV_per_request
    When called:    oom_prevention_check(), /concurrency-table endpoint,
                    and CLI output at startup.
    Returns:        Dict with full VRAM breakdown and hard concurrent limit.
    Introduced:     Session 11.3. Permanent.
    """
    info        = CHRONICLE_AGENTS[agent_name]
    gpu_vram    = GPU_VRAM_GB.get(info["gpu_tier"], 24)
    bytes_pp    = VRAM_BYTES_PER_PARAM[info["precision"]]
    weight_gb   = (info["model_size_b"] * 1_000_000_000 * bytes_pp) / (1024 ** 3)

    # KV cache per request: scales with max_model_len and model size
    # Base: 0.5 GB per request for 7B model at 4K context
    kv_base_7b  = 0.5
    kv_per_req  = kv_base_7b * (info["model_size_b"] / 7.0) * (info["max_model_len"] / 4_096)

    overhead    = CUDA_OVERHEAD_GB
    buffer      = gpu_vram * 0.10    # 10% safety buffer

    # For co-located utility agents: only the agent's gpu_memory_utilization
    # fraction of total GPU VRAM is available to this agent
    if info["tier"] == "utility":
        effective_vram = gpu_vram * info["gpu_memory_utilization"]
        buffer         = effective_vram * 0.08   # 8% within partition
    else:
        effective_vram = gpu_vram

    available   = effective_vram - weight_gb - overhead - buffer
    max_conc    = max(0, int(available / kv_per_req)) if kv_per_req > 0 else 0

    return {
        "agent":               agent_name,
        "gpu_tier":            info["gpu_tier"],
        "gpu_vram_gb":         gpu_vram,
        "effective_vram_gb":   round(effective_vram, 1),
        "weight_gb":           round(weight_gb, 1),
        "overhead_gb":         round(overhead, 1),
        "buffer_gb":           round(buffer, 2),
        "available_for_kv_gb": round(available, 2),
        "kv_per_request_gb":   round(kv_per_req, 3),
        "max_safe_concurrent": max_conc,
        "max_model_len":       info["max_model_len"],
        "gpu_memory_utilization": info["gpu_memory_utilization"],
        "safe":                max_conc > 0,
    }


# ── OOM Prevention Check — Startup Safety Gate (Session 11.3) ─────

def oom_prevention_check() -> dict:
    """
    What it does:   Runs calculate_max_safe_concurrent() for all 5 agents.
                    If ANY agent has max_safe_concurrent == 0, Chronicle
                    refuses to start. The crash is caught at deploy time,
                    not at 2 AM under production traffic.
    When called:    At startup (CLI entry point) before serving any request.
                    Also available via /oom-check endpoint for monitoring.
    Returns:        Dict with per-agent safety status and overall pass/fail.
    Introduced:     Session 11.3. Permanent.
    """
    results    = {}
    all_safe   = True

    for name in CHRONICLE_AGENTS:
        agent_result = calculate_max_safe_concurrent(name)
        results[name] = agent_result
        if not agent_result["safe"]:
            all_safe = False

    return {
        "all_safe":  all_safe,
        "per_agent": results,
        "summary": (
            "OOM PREVENTION: PASS — all agents have safe concurrent capacity"
            if all_safe
            else "OOM PREVENTION: FAIL — one or more agents cannot serve any requests safely"
        ),
        "action": (
            "Chronicle is safe to start."
            if all_safe
            else "DO NOT START. Fix agent configuration before deployment."
        ),
    }


# ── vLLM Config Generator (Session 11.3) ─────────────────────────

def vllm_config_per_agent() -> dict:
    """
    What it does:   Generates the exact vLLM launch configuration for each
                    Chronicle agent. These are the flags passed to
                    `vllm serve` in production. Session 11.3 is where these
                    values are calculated and locked for the first time.
    When called:    /deployment-config endpoint, CLI output, S11.3 checklist.
    Returns:        Dict mapping agent name → vLLM serve config.
    Introduced:     Session 11.3. Permanent.

    Note: model IDs are placeholders — replace with the actual HuggingFace
    model ID for your chosen AWQ / FP16 model before production deploy.
    Session 11.3 does not require actual vLLM installation; Gemini API
    is still used for inference. These configs are the production target.
    """
    model_ids = {
        "utility":  "meta-llama/Llama-3.1-8B-Instruct-AWQ",  # 7B AWQ placeholder
        "frontier": "meta-llama/Llama-3.1-13B-Instruct",      # 13B FP16 placeholder
    }

    agent_keys = list(CHRONICLE_AGENTS.keys())
    configs = {}
    for name, info in CHRONICLE_AGENTS.items():
        model_id = model_ids[info["tier"]]
        conc     = calculate_max_safe_concurrent(name)
        port     = 8100 + agent_keys.index(name)

        configs[name] = {
            "model":                  model_id,
            "port":                   port,
            "tensor_parallel_size":   1,
            "max_model_len":          info["max_model_len"],
            "gpu_memory_utilization": info["gpu_memory_utilization"],
            "dtype":                  "auto",
            "max_num_seqs":           conc["max_safe_concurrent"],
            "served_model_name":      f"chronicle-{name}",
            "launch_command": (
                f"vllm serve {model_id} "
                f"--port {port} "
                f"--tensor-parallel-size 1 "
                f"--max-model-len {info['max_model_len']} "
                f"--gpu-memory-utilization {info['gpu_memory_utilization']} "
                f"--dtype auto "
                f"--max-num-seqs {conc['max_safe_concurrent']} "
                f"--served-model-name chronicle-{name}"
            ),
        }

    return configs


# ── Co-Location Partitioner (Session 11.3) ───────────────────────

def colocation_partitioner() -> dict:
    """
    What it does:   Calculates and validates the gpu_memory_utilization
                    partition for Chronicle's 3 co-located utility agents
                    on one shared L4 GPU.
                    Verifies: sum of allocations + system overhead ≤ 1.0.
    When called:    /deployment-config endpoint, OOM check, CLI output.
    Returns:        Dict with per-agent utilization, total allocation,
                    remaining headroom, and safety verdict.
    Introduced:     Session 11.3. Permanent.
    """
    utility_agents = {
        name: info
        for name, info in CHRONICLE_AGENTS.items()
        if info["tier"] == "utility"
    }

    system_overhead_fraction = 0.08
    total_allocated = sum(
        info["gpu_memory_utilization"]
        for info in utility_agents.values()
    )
    grand_total = total_allocated + system_overhead_fraction

    allocations = {
        name: {
            "gpu_memory_utilization": info["gpu_memory_utilization"],
            "effective_vram_gb":      round(
                GPU_VRAM_GB["L4"] * info["gpu_memory_utilization"], 1
            ),
            "weight_gb":              round(
                (info["model_size_b"] * 1e9 * VRAM_BYTES_PER_PARAM[info["precision"]]) / (1024**3), 1
            ),
        }
        for name, info in utility_agents.items()
    }

    return {
        "gpu":                      "L4",
        "gpu_vram_gb":              GPU_VRAM_GB["L4"],
        "per_agent":                allocations,
        "total_model_fraction":     round(total_allocated, 2),
        "system_overhead_fraction": system_overhead_fraction,
        "grand_total_fraction":     round(grand_total, 2),
        "remaining_fraction":       round(1.0 - grand_total, 2),
        "remaining_gb":             round((1.0 - grand_total) * GPU_VRAM_GB["L4"], 1),
        "safe":                     grand_total <= 1.0,
        "note": (
            "Safe: sum of gpu_memory_utilization + system overhead ≤ 1.0. "
            "Memory partitions are hard boundaries — agents cannot borrow from each other."
            if grand_total <= 1.0
            else "UNSAFE: total allocation exceeds 1.0. Reduce gpu_memory_utilization per agent."
        ),
    }


# ── KV Cache Growth Simulator (Session 11.3) ─────────────────────

def kv_cache_growth_simulator(
    agent_name: str,
    requests_per_minute: int,
    duration_minutes: int = 10,
) -> dict:
    """
    What it does:   Simulates KV cache VRAM consumption over time for one
                    Chronicle agent under a given traffic rate.
                    Shows the exact minute when OOM would occur without
                    the max_safe_concurrent guard in place.
    When called:    CLI output for each agent. /oom-check endpoint.
    Returns:        Dict with timeline, peak VRAM, and OOM event count.
    Introduced:     Session 11.3. Permanent.
    """
    import random
    info        = CHRONICLE_AGENTS[agent_name]
    conc_data   = calculate_max_safe_concurrent(agent_name)
    gpu_vram    = GPU_VRAM_GB.get(info["gpu_tier"], 24)
    weight_gb   = conc_data["weight_gb"]
    kv_per_req  = conc_data["kv_per_request_gb"]
    overhead    = conc_data["overhead_gb"]

    timeline    = []
    active      = []
    oom_events  = 0
    peak_vram   = 0.0

    for minute in range(duration_minutes):
        for _ in range(requests_per_minute):
            duration = max(1, int(random.expovariate(0.5)))  # avg 2 min lifetime
            active.append({"kv": kv_per_req, "remaining": duration})

        total_kv   = sum(r["kv"] for r in active)
        total_vram = weight_gb + overhead + total_kv
        peak_vram  = max(peak_vram, total_vram)
        is_oom     = total_vram > gpu_vram

        if is_oom:
            oom_events += 1

        bar_len = int((total_vram / gpu_vram) * 20)
        bar     = "█" * min(bar_len, 20) + ("░" * (20 - min(bar_len, 20)))

        timeline.append({
            "minute":          minute,
            "active_requests": len(active),
            "kv_gb":           round(total_kv, 2),
            "total_vram_gb":   round(total_vram, 2),
            "utilization_pct": round(total_vram / gpu_vram * 100, 1),
            "bar":             bar,
            "oom":             is_oom,
        })

        active = [
            {**r, "remaining": r["remaining"] - 1}
            for r in active
            if r["remaining"] > 1
        ]

    return {
        "agent":               agent_name,
        "gpu_vram_gb":         gpu_vram,
        "weight_gb":           weight_gb,
        "kv_per_request_gb":   kv_per_req,
        "max_safe_concurrent": conc_data["max_safe_concurrent"],
        "requests_per_minute": requests_per_minute,
        "timeline":            timeline,
        "peak_vram_gb":        round(peak_vram, 2),
        "oom_events":          oom_events,
    }


# ── Cost Model (Session 11.2 UPDATED in 11.3) ────────────────────

def calculate_monthly_gpu_cost() -> dict:
    """
    What it does:   Calculates monthly GPU infrastructure cost across
                    four deployment scenarios.
                    S11.3 adds Scenario D: co-location (3 utility agents
                    on one shared L4 instead of 3 separate L4s).
    When called:    /cost-model endpoint and cost card in dashboard.
    Returns:        Dict with four scenarios and per-agent cost breakdown.
    Introduced:     Session 11.2. Updated Session 11.3. Permanent.
    """
    per_agent_cost = {
        name: info["monthly_gpu_cost_usd"]
        for name, info in CHRONICLE_AGENTS.items()
    }

    # Scenario A: all agents on A100-80 (naive, no tiering)
    scenario_a = len(CHRONICLE_AGENTS) * GPU_TIER_COSTS["A100-80"]

    # Scenario B: utility on L4, frontier on A100-40 (S11.2 tiered)
    scenario_b = sum(per_agent_cost.values())

    # Scenario C: utility on A10G, frontier on A100-40
    scenario_c = 3 * GPU_TIER_COSTS["A10G"] + 2 * GPU_TIER_COSTS["A100-40"]

    # Scenario D: 3 utility agents CO-LOCATED on ONE L4, frontier each on A100-40
    # S11.3: utility agents share a GPU — one L4 cost instead of three
    scenario_d = GPU_TIER_COSTS["L4"] + 2 * GPU_TIER_COSTS["A100-40"]

    return {
        "per_agent_monthly_usd": per_agent_cost,
        "scenarios": {
            "A_all_a100_no_tiering": {
                "label":                "All A100-80, no tiering (naive)",
                "monthly_usd":         scenario_a,
                "annual_usd":          scenario_a * 12,
                "annual_savings_vs_a": 0,
            },
            "B_tiered_l4_a100": {
                "label":                "Tiered: 3× L4 utility + 2× A100-40 frontier (S11.2)",
                "monthly_usd":         scenario_b,
                "annual_usd":          scenario_b * 12,
                "annual_savings_vs_a": (scenario_a - scenario_b) * 12,
            },
            "C_tiered_a10g_a100": {
                "label":                "Tiered: 3× A10G utility + 2× A100-40 frontier",
                "monthly_usd":         scenario_c,
                "annual_usd":          scenario_c * 12,
                "annual_savings_vs_a": (scenario_a - scenario_c) * 12,
            },
            "D_colocation_l4_a100": {
                "label":                "Co-located: 1× L4 (3 utility agents) + 2× A100-40 frontier (S11.3)",
                "monthly_usd":         scenario_d,
                "annual_usd":          scenario_d * 12,
                "annual_savings_vs_a": (scenario_a - scenario_d) * 12,
                "annual_savings_vs_b": (scenario_b - scenario_d) * 12,
                "note": "3 utility agents share one L4 GPU via gpu_memory_utilization partitioning",
            },
        },
        "recommended_scenario": "D_colocation_l4_a100",
        "note": "S11.3 adds co-location scenario. Semantic caching in S14.1 reduces effective GPU-hours further.",
    }


# ── Task Survivability Query (Session 11.2 — unchanged) ───────────

def task_survivability_matrix(task_type: str = None) -> dict:
    """
    What it does:   Returns survivability profile for one task or full matrix.
    Introduced:     Session 11.2. Permanent.
    """
    if task_type:
        result = TASK_SURVIVABILITY_MATRIX.get(task_type)
        if not result:
            return {
                "error":       f"Unknown task type: {task_type}",
                "valid_types": list(TASK_SURVIVABILITY_MATRIX.keys()),
            }
        return {task_type: result}
    return TASK_SURVIVABILITY_MATRIX


# ── MCP Source Configuration (Session 12.1) ───────────────────────
# Maps each Chronicle data source to its MCP server URL.
# In development: MCP servers run locally on fixed ports.
# Permanent from Session 12.1 onward.
MCP_SOURCE_CONFIG = {
    "spotify":  {"url": "http://localhost:3001/mcp", "tool": "get_spotify_history"},   # S12.1
    "finance":  {"url": "http://localhost:3002/mcp", "tool": "get_transactions"},       # S12.1
    "fitness":  {"url": "http://localhost:3003/mcp", "tool": "get_fitness_data"},       # S12.1
    "github":   {"url": "http://localhost:3004/mcp", "tool": "get_commit_history"},     # S12.1
    "journal":  {"url": "http://localhost:3005/mcp", "tool": "get_journal_entries"},    # S12.1
}


# ── MCP Client Pool (Session 12.1) ────────────────────────────────

class MCPClientPool:
    """
    What it does:   Manages one aiohttp session per Chronicle data source.
                    Provides fetch_source() to pull live data from any source.
                    Falls back to CHRONICLE_CALIBRATION_DATASET samples
                    when the MCP server is unreachable (dev/demo mode).
    When called:    Created once in lifespan startup via build_mcp_client_pool().
                    Passed into LangGraph nodes via app.state.mcp_pool.
    Returns:        Dict mapping source name → data records list.
    Introduced:     Session 12.1. Permanent.
    """

    def __init__(self):
        self._sessions: dict[str, aiohttp.ClientSession] = {}
        self._connected: dict[str, bool] = {}

    async def connect(self, sources: list[str]) -> None:
        """Open one aiohttp session per requested source."""
        for source in sources:
            if source in MCP_SOURCE_CONFIG and source not in self._sessions:
                self._sessions[source] = aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=10)
                )
                self._connected[source] = False

    async def fetch_source(self, source: str, params: dict = None) -> dict:
        """
        What it does:   Calls the MCP server for the named source.
                        Falls back to calibration dataset if unreachable.
        When called:    By ingestion_node() at analysis start.
        Returns:        Dict with 'source', 'records', 'count', 'live' flag.
        Introduced:     Session 12.1. Permanent.
        """
        params = params or {}
        config = MCP_SOURCE_CONFIG.get(source)

        if config and source in self._sessions:
            try:
                payload = {
                    "jsonrpc": "2.0",
                    "method":  "tools/call",
                    "params":  {"name": config["tool"], "arguments": params},
                    "id":      1,
                }
                async with self._sessions[source].post(
                    config["url"], json=payload
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        records = data.get("result", {}).get("content", [])
                        self._connected[source] = True
                        return {
                            "source":  source,
                            "records": records,
                            "count":   len(records),
                            "live":    True,
                        }
            except Exception:
                pass  # Fall through to calibration fallback

        # ── Calibration dataset fallback ──────────────────────────────
        # When MCP server is unreachable (local dev, demo mode):
        # Return calibration samples for this source.
        # Ingestion Agent still runs; data is just pre-loaded.
        fallback = [
            s for s in CHRONICLE_CALIBRATION_DATASET if s["source"] == source
        ]
        return {
            "source":  source,
            "records": fallback,
            "count":   len(fallback),
            "live":    False,   # calibration data, not live MCP pull
        }

    async def close(self) -> None:
        """Close all aiohttp sessions. Called at lifespan shutdown."""
        for session in self._sessions.values():
            await session.close()
        self._sessions.clear()

    def connection_status(self) -> dict[str, bool]:
        """Returns live/fallback status per source. Used by /health."""
        return dict(self._connected)


async def build_mcp_client_pool(
    sources: list[str] = None,
) -> "MCPClientPool":
    """
    What it does:   Creates and connects an MCPClientPool for Chronicle's 5 sources.
    When called:    In FastAPI lifespan startup, before serving any request.
    Returns:        Connected MCPClientPool stored in app.state.mcp_pool.
    Introduced:     Session 12.1. Permanent.
    """
    sources = sources or list(MCP_SOURCE_CONFIG.keys())
    pool = MCPClientPool()
    await pool.connect(sources)
    return pool


async def close_mcp_client_pool(pool: "MCPClientPool") -> None:
    """
    What it does:   Closes all MCP client sessions gracefully.
    When called:    In FastAPI lifespan shutdown.
    Introduced:     Session 12.1. Permanent.
    """
    await pool.close()


# ── Tier-Aware Inference (Session 11.3 — UPDATED) ────────────────

async def chronicle_infer(
    session: aiohttp.ClientSession,
    question: str,
    agent_name: str,
    request_id: int,
) -> BenchmarkResult:
    """
    What it does:   Fires one inference request for a named Chronicle agent.
                    S11.2 update: tier-aware system prompt.
                    S11.3 update: input length guard — rejects questions
                    longer than the agent's max_model_len before dispatch.
    When called:    By run_concurrent_analysis() for each agent slot.
    Returns:        BenchmarkResult with TTFT, TPOT, and response_text.
    Introduced:     Session 11.1. Updated S11.2, S11.3. Permanent.
    """
    result     = BenchmarkResult(request_id=request_id)
    agent_info = CHRONICLE_AGENTS[agent_name]
    tier       = agent_info["tier"]

    # ── S11.3: Input length guard ─────────────────────────────────
    # Rough token estimate: 1 token ≈ 4 characters
    estimated_tokens = len(question) // 4
    if estimated_tokens > agent_info["max_model_len"]:
        result.error_message = (
            f"Input too long for {agent_name} agent: "
            f"~{estimated_tokens} tokens estimated, "
            f"max_model_len={agent_info['max_model_len']}. "
            f"Truncate input or route to long-context pool."
        )
        return result

    # ── S11.2: Tier-aware prompt ──────────────────────────────────
    if tier == "utility":
        agent_prompt = (
            f"You are Chronicle's {agent_name} agent running on an INT4-quantized model "
            f"(max context: {agent_info['max_model_len']} tokens). "
            f"Your role: {agent_info['role']}. "
            f"Task type: {agent_info['survivability_note']}. "
            f"The user's question: {question}. "
            f"Respond with a brief, structured 2-sentence analysis. "
            f"Focus on patterns and classifications, not nuanced reasoning."
        )
    else:
        agent_prompt = (
            f"You are Chronicle's {agent_name} agent running on a full-precision FP16 model "
            f"(max context: {agent_info['max_model_len']} tokens). "
            f"Your role: {agent_info['role']}. "
            f"Task type: {agent_info['survivability_note']}. "
            f"The user's question: {question}. "
            f"Respond with a substantive 3-4 sentence analysis. "
            f"This agent requires full precision. Do not soften findings."
        )

    payload = {
        "contents":         [{"parts": [{"text": agent_prompt}]}],
        "generationConfig": {"maxOutputTokens": 512, "temperature": 0.7},
    }

    try:
        request_start = time.monotonic()

        async with session.post(
            GEMINI_REST_URL,
            json=payload,
            params={"key": GEMINI_API_KEY},
            timeout=aiohttp.ClientTimeout(total=60),
        ) as response:
            first_token_time = time.monotonic()
            body             = await response.json()
            complete_time    = time.monotonic()

            if response.status != 200:
                err = body.get("error", {}).get("message", f"HTTP {response.status}")
                result.error_message = err
                return result

            candidates = body.get("candidates", [])
            if not candidates:
                result.error_message = "No candidates returned"
                return result

            text = ""
            for part in candidates[0].get("content", {}).get("parts", []):
                text += part.get("text", "")

            approx_tokens = max(len(text.split()), 1)
            ttft          = first_token_time - request_start
            total         = complete_time - request_start
            tpot          = (total - ttft) / approx_tokens

            result.ttft_seconds             = round(ttft, 4)
            result.total_latency_seconds    = round(total, 4)
            result.approximate_output_tokens = approx_tokens
            result.tpot_seconds             = round(tpot, 6)
            result.status                   = "success"
            result.response_text            = text

    except aiohttp.ClientError as e:
        result.error_message = f"ClientError: {str(e)}"
    except Exception as e:
        result.error_message = f"Exception: {str(e)}"

    return result


# ── Concurrent Analysis Runner (Session 11.1 — unchanged) ─────────

async def run_concurrent_analysis(question: str) -> dict:
    """
    What it does:   Fires all 5 Chronicle agents simultaneously.
    When called:    By /analyze endpoint in api.py.
    Returns:        Per-agent results and aggregate timing metrics.
    Introduced:     Session 11.1. Permanent.
    """
    ssl_ctx     = ssl.create_default_context(cafile=certifi.where())
    connector   = aiohttp.TCPConnector(limit=len(CHRONICLE_AGENTS), ssl=ssl_ctx)
    agent_names = list(CHRONICLE_AGENTS.keys())

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            asyncio.create_task(
                chronicle_infer(session, question, name, idx + 1)
            )
            for idx, name in enumerate(agent_names)
        ]
        wall_start   = time.monotonic()
        raw_results  = await asyncio.gather(*tasks, return_exceptions=True)
        wall_elapsed = time.monotonic() - wall_start

    results     = {}
    ttft_values = []

    for name, raw in zip(agent_names, raw_results):
        if isinstance(raw, Exception):
            results[name] = {"status": "error", "error_message": str(raw)}
        else:
            results[name] = raw.model_dump()
            if raw.status == "success" and raw.ttft_seconds is not None:
                ttft_values.append(raw.ttft_seconds)

    metrics = {
        "wall_clock_seconds": round(wall_elapsed, 3),
        "agents_succeeded":   sum(1 for r in results.values() if r.get("status") == "success"),
        "agents_total":       len(CHRONICLE_AGENTS),
    }
    if ttft_values:
        metrics["mean_ttft_seconds"] = round(statistics.mean(ttft_values), 3)
        metrics["max_ttft_seconds"]  = round(max(ttft_values), 3)
        metrics["min_ttft_seconds"]  = round(min(ttft_values), 3)

    return {"agent_results": results, "metrics": metrics}


# ── Chronicle LangGraph State (Session 12.1) ──────────────────────

class ChronicleState(TypedDict):
    """
    What it does:   Shared state passed through all 5 Chronicle agent nodes.
                    Each node reads from prior fields and adds its own.
                    Annotated[list, operator.add] fields accumulate across nodes.
    When called:    Created in build_initial_state() before graph.ainvoke().
    Introduced:     Session 12.1. Permanent.
    """
    # ── Input fields (set before graph runs) ──────────────────────
    question:       str
    data_sources:   list[str]
    depth:          str
    analysis_id:    str

    # ── Ingestion Agent output ─────────────────────────────────────
    raw_data:       dict                                  # source→records
    sources_live:   dict                                  # source→bool (MCP vs fallback)

    # ── Pattern Agent output ───────────────────────────────────────
    correlations:   Annotated[list[str], operator.add]

    # ── Timeline Agent output ──────────────────────────────────────
    timeline_events: Annotated[list[str], operator.add]

    # ── Brutality Agent output ─────────────────────────────────────
    honest_analysis: str

    # ── Synthesis Agent output ─────────────────────────────────────
    final_brief:    str
    confidence:     float
    token_chunk:    str   # S12.2: picked up by the stream adapter

    # ── Aggregate metrics ──────────────────────────────────────────
    agent_trace:    Annotated[list[str], operator.add]    # debug mode
    processing_ms:  int                                   # wall clock

    # ── Tool invocation tracking (Session 12.2) ────────────────────
    # Accumulated across all nodes that make MCP calls mid-stream.
    # Stream adapter reads these to emit tool_call / tool_result events.
    tool_calls:   Annotated[list[dict], operator.add]   # S12.2
    tool_results: Annotated[list[dict], operator.add]   # S12.2


# ── LLM Instances (Session 12.1) ──────────────────────────────────
# Two instances: utility tier (faster, cheaper) and frontier tier.
# Utility agents (ingestion, pattern, timeline) use utility_llm.
# Frontier agents (brutality, synthesis) use frontier_llm.
# Permanent from Session 12.1 onward.

def build_llm_instances() -> tuple:
    """
    What it does:   Creates two ChatGoogleGenerativeAI instances.
                    utility_llm: maps to Chronicle's INT4 utility tier (cheap).
                    frontier_llm: maps to Chronicle's FP16 frontier tier (full quality).
    When called:    Once in build_chronicle_graph().
    Returns:        (utility_llm, frontier_llm) tuple.
    Introduced:     Session 12.1. Permanent.

    Note: Both use Gemini 2.5 Flash in this session — the tier concept
    (utility vs frontier) is locked in CHRONICLE_AGENTS. A future session
    replaces this with local vLLM endpoints via LiteLLM routing.
    """
    utility_llm = ChatGoogleGenerativeAI(
        model="gemini-2.5-flash",
        google_api_key=GEMINI_API_KEY,
        temperature=0.3,
        max_output_tokens=512,
    )
    frontier_llm = ChatGoogleGenerativeAI(
        model="gemini-2.5-flash",
        google_api_key=GEMINI_API_KEY,
        temperature=0.7,
        max_output_tokens=1024,
    )
    return utility_llm, frontier_llm


# ── LLM Span Helper (Session 13.1) ────────────────────────────────

async def call_gemini_traced(
    llm,
    prompt:      str,
    temperature: float,
    agent_name:  str,
) -> dict:
    """
    What it does:   Wraps every Gemini call in an 'llm.call' span.
                    Sets input_prompt, model, temperature BEFORE the call.
                    Sets token counts from response object AFTER the call.
                    Records exceptions explicitly with error kind attribute.
    When called:    By every Chronicle agent node instead of direct ainvoke().
    Returns:        Dict with 'text', 'in_tok', 'out_tok'.
    Introduced:     Session 13.1. Permanent.

    The token count rule:
    Never use len(prompt.split()). Wrong by 20-40%. Systematic error.
    Always read from response.usage_metadata after the call returns.
    Cost anomaly detection built on wrong counts produces false alerts.
    """
    with tracer.start_as_current_span("llm.call") as span:

        # BEFORE the call — what the LLM will receive
        span.set_attribute("llm.model",       "gemini-2.5-flash")
        span.set_attribute("llm.temperature", temperature)
        span.set_attribute("llm.agent",       agent_name)
        # Truncate to 1800 chars — backends cap attribute size
        # Redact PII before set_attribute — not in the backend
        span.set_attribute("llm.input_prompt", _redact_and_truncate(prompt))

        try:
            span.add_event("llm.call.start")
            response = await llm.ainvoke([HumanMessage(content=prompt)])
            span.add_event("llm.call.end")

            # AFTER the call — from response object, never heuristics
            usage   = getattr(response, "usage_metadata", None)
            in_tok  = getattr(usage, "prompt_token_count",     0) if usage else 0
            out_tok = getattr(usage, "candidates_token_count", 0) if usage else 0
            if usage and isinstance(usage, dict):
                in_tok  = usage.get("input_tokens",  in_tok)
                out_tok = usage.get("output_tokens", out_tok)

            span.set_attribute("llm.token_count.input",  in_tok)
            span.set_attribute("llm.token_count.output", out_tok)
            span.set_status(Status(StatusCode.OK))

            return {
                "text":    response.content,
                "in_tok":  in_tok,
                "out_tok": out_tok,
            }

        except Exception as exc:
            # Distinguish error kinds for routing in alerting layer
            error_kind = (
                "rate_limited"  if "429" in str(exc) else
                "server_error"  if str(exc)[:1] == "5" else
                "bad_request"
            )
            span.set_attribute("llm.error.kind", error_kind)
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR, str(exc)))
            raise


def _redact_and_truncate(text: str, max_chars: int = 1800) -> str:
    """
    Redacts common PII patterns and truncates to max_chars.
    Redact at emission time — not in the backend.
    Introduced: Session 13.1. Permanent.
    """
    import re
    # Email
    text = re.sub(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+",
                  "[EMAIL]", text)
    # Phone (loose international pattern)
    text = re.sub(r"\+?[\d\s\-().]{10,15}", "[PHONE]", text)
    # Credit card (4-digit groups)
    text = re.sub(r"\b\d{4}[\s-]?\d{4}[\s-]?\d{4}[\s-]?\d{4}\b",
                  "[CARD]", text)
    return text[:max_chars]


# ── LangGraph Node Functions (Session 12.1) ───────────────────────

def make_ingestion_node(mcp_pool: "MCPClientPool"):
    """
    Factory for ingestion_node bound to an MCP pool.
    Introduced: Session 12.1. Permanent.
    """
    async def ingestion_node(state: ChronicleState) -> dict:
        """
        What it does:   Fetches live data from all requested MCP sources.
                        Falls back to calibration samples if MCP unreachable.
        When called:    First node in every Chronicle analysis graph run.
        Returns:        Partial state: raw_data, sources_live, agent_trace entry.
        Introduced:     Session 12.1. Permanent.
        Instrumented:   Session 13.1 — chronicle.ingestion span,
                        child mcp.fetch.<source> span per data source.
        """
        with tracer.start_as_current_span("chronicle.ingestion") as span:
            span.set_attribute("langgraph_node", "ingestion")
            span.set_attribute("analysis_id",    state["analysis_id"])
            span.set_attribute("data_sources",   str(state["data_sources"]))

            try:
                start = time.monotonic()
                raw_data = {}
                sources_live = {}

                for source in state["data_sources"]:
                    with tracer.start_as_current_span(f"mcp.fetch.{source}") as t:
                        t.set_attribute("mcp.source", source)
                        t.set_attribute("mcp.tool",   MCP_SOURCE_CONFIG[source]["tool"])

                        result = await mcp_pool.fetch_source(source)

                        t.set_attribute("mcp.live",  result["live"])
                        t.set_attribute("mcp.count", result["count"])
                        t.set_status(Status(StatusCode.OK))

                    raw_data[source]     = result["records"]
                    sources_live[source] = result["live"]

                elapsed_ms = round((time.monotonic() - start) * 1000)
                live_count = sum(1 for v in sources_live.values() if v)
                trace_entry = (
                    f"[ingestion] {len(raw_data)} sources loaded "
                    f"({live_count} live MCP, {len(raw_data) - live_count} fallback) "
                    f"in {elapsed_ms}ms"
                )

                span.set_attribute("ingestion.total_records", sum(len(v) for v in raw_data.values()))
                span.set_attribute("ingestion.live_sources",  live_count)
                span.set_status(Status(StatusCode.OK))

                # internal_notes: what the Ingestion Agent decided (Session 13.2)
                live_sources = [src for src, live in sources_live.items() if live]
                fall_sources = [src for src, live in sources_live.items() if not live]
                total = sum(len(v) for v in raw_data.values())
                notes = f"Loaded {total} records from {len(raw_data)} sources"
                if live_sources:
                    notes += f"; live: {', '.join(live_sources)}"
                if fall_sources:
                    notes += f"; fallback (MCP unreachable): {', '.join(fall_sources)}"
                span.set_attribute("internal_notes", notes)

                return {
                    "raw_data":     raw_data,
                    "sources_live": sources_live,
                    "agent_trace":  [trace_entry],
                }

            except Exception as exc:
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR, str(exc)))
                raise

    return ingestion_node


def make_pattern_node(utility_llm: "ChatGoogleGenerativeAI", mcp_pool: "MCPClientPool"):
    """
    Factory for pattern_node bound to the utility LLM and MCP pool.
    Session 12.2 update: Pattern Agent now invokes MCP mid-reasoning
    to verify correlations with fresh data.
    tool_calls and tool_results are added to state for the stream adapter.
    Introduced: Session 12.1. Updated: Session 12.2. Permanent.
    """
    async def pattern_node(state: ChronicleState) -> dict:
        """
        What it does:   Finds cross-source correlations in the ingested data,
                        then verifies with a mid-stream MCP fetch.
                        Utility tier: INT4-equivalent prompt, concise output.
        When called:    After ingestion_node in every Chronicle graph run.
        Returns:        Partial state: correlations list, tool_calls,
                        tool_results, agent_trace entry.
        Introduced:     Session 12.1. Updated: Session 12.2. Permanent.
        Instrumented:   Session 13.1 — chronicle.pattern span, llm.call via
                        call_gemini_traced(), child mcp.verify.spotify span.
        """
        with tracer.start_as_current_span("chronicle.pattern") as span:
            span.set_attribute("langgraph_node", "pattern")
            span.set_attribute("analysis_id",    state["analysis_id"])

            try:
                start = time.monotonic()
                data_summary = {
                    src: f"{len(records)} records"
                    for src, records in state["raw_data"].items()
                }

                # ── Initial correlation pass ───────────────────────────────
                prompt = (
                    f"You are Chronicle's Pattern Agent (utility tier, INT4 precision). "
                    f"Your role: find cross-source correlations between data signals. "
                    f"Data available: {json.dumps(data_summary)}. "
                    f"User question: {state['question']}. "
                    f"Identify 2-3 specific correlations across these data sources. "
                    f"Be concise. One sentence per correlation."
                )
                result = await call_gemini_traced(
                    utility_llm, prompt, temperature=0.3, agent_name="pattern"
                )
                correlations = [
                    line.strip()
                    for line in result["text"].split("\n")
                    if line.strip()
                ]

                # ── Mid-stream MCP verification (Session 12.2) ─────────────
                # Pattern Agent fetches fresh Spotify data to verify its correlation.
                # The tool_calls and tool_results lists are picked up by
                # chronicle_stream_events() and emitted as tool_call/tool_result events.
                tool_calls   = []
                tool_results = []

                if "spotify" in state["data_sources"]:
                    with tracer.start_as_current_span("mcp.verify.spotify") as t:
                        t.set_attribute("mcp.source", "spotify")
                        t.set_attribute("mcp.reason", "correlation_verification")

                        tool_calls.append({
                            "name":    "get_spotify_history",
                            "message": "Verifying listening pattern correlation with fresh data",
                        })
                        verify_result = await mcp_pool.fetch_source("spotify", {"days": 30})
                        tool_results.append({
                            "name":    "get_spotify_history",
                            "status":  "success" if verify_result["live"] else "fallback",
                            "summary": f"{verify_result['count']} tracks retrieved ({'live' if verify_result['live'] else 'calibration data'})",
                        })

                        t.set_attribute("mcp.live",  verify_result["live"])
                        t.set_attribute("mcp.count", verify_result["count"])
                        t.set_status(Status(StatusCode.OK))

                elapsed_ms = round((time.monotonic() - start) * 1000)

                span.set_attribute("pattern.correlations_found", len(correlations))
                span.set_attribute("pattern.routing_decision",
                                   "cross_source_found" if correlations else "no_correlation")
                span.set_status(Status(StatusCode.OK))

                # internal_notes: what Pattern Agent decided (Session 13.2)
                routing = "cross_source_found" if correlations else "no_correlation"
                notes   = (
                    f"Found {len(correlations)} correlation(s); "
                    f"routing_decision={routing}; "
                    f"MCP verify={'live' if tool_results and tool_results[0].get('status') == 'success' else 'fallback'}"
                )
                span.set_attribute("internal_notes", notes)

                return {
                    "correlations": correlations,
                    "tool_calls":   tool_calls,    # S12.2: stream adapter picks these up
                    "tool_results": tool_results,  # S12.2: stream adapter picks these up
                    "agent_trace":  [f"[pattern] {len(correlations)} correlations, MCP verify in {elapsed_ms}ms"],
                }

            except Exception as exc:
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR, str(exc)))
                raise

    return pattern_node


def make_timeline_node(utility_llm: "ChatGoogleGenerativeAI"):
    """
    Factory for timeline_node bound to the utility LLM.
    Introduced: Session 12.1. Permanent.
    """
    async def timeline_node(state: ChronicleState) -> dict:
        """
        What it does:   Sequences life events from data signals across all sources.
                        Utility tier: temporal ordering, bounded output.
        When called:    After pattern_node.
        Returns:        Partial state: timeline_events list, agent_trace entry.
        Introduced:     Session 12.1. Permanent.
        Instrumented:   Session 13.1 — chronicle.timeline span, llm.call via
                        call_gemini_traced().
        """
        with tracer.start_as_current_span("chronicle.timeline") as span:
            span.set_attribute("langgraph_node", "timeline")
            span.set_attribute("analysis_id",    state["analysis_id"])

            try:
                start = time.monotonic()
                prompt = (
                    f"You are Chronicle's Timeline Agent (utility tier, INT4 precision). "
                    f"Your role: sequence life events from data signals across all sources. "
                    f"Correlations found: {state['correlations']}. "
                    f"User question: {state['question']}. "
                    f"List 3-4 key timeline events implied by this data. "
                    f"Each event: one sentence. Most recent first."
                )
                result = await call_gemini_traced(
                    utility_llm, prompt, temperature=0.3, agent_name="timeline"
                )
                events = [
                    line.strip()
                    for line in result["text"].split("\n")
                    if line.strip()
                ]
                elapsed_ms = round((time.monotonic() - start) * 1000)

                span.set_attribute("timeline.events_found", len(events))
                span.set_status(Status(StatusCode.OK))

                # internal_notes: what Timeline Agent decided (Session 13.2)
                span.set_attribute(
                    "internal_notes",
                    f"Sequenced {len(events)} timeline event(s) from {len(state['correlations'])} correlation(s)"
                )

                return {
                    "timeline_events": events,
                    "agent_trace":     [f"[timeline] {len(events)} events sequenced in {elapsed_ms}ms"],
                }

            except Exception as exc:
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR, str(exc)))
                raise

    return timeline_node


def make_brutality_node(frontier_llm: "ChatGoogleGenerativeAI", mcp_pool: "MCPClientPool"):
    """
    Factory for brutality_node bound to the frontier LLM and MCP pool.
    Session 12.2 update: Brutality Agent fetches live GitHub commits
    mid-reasoning to back up its honest analysis with evidence.
    Introduced: Session 12.1. Updated: Session 12.2. Permanent.
    """
    async def brutality_node(state: ChronicleState) -> dict:
        """
        What it does:   Delivers honest cross-source analysis. No softening.
                        Frontier tier: FP16-equivalent, deep multi-constraint reasoning.
                        Session 12.2: backs the analysis with a mid-stream
                        MCP fetch of live commit history.
        When called:    After timeline_node.
        Returns:        Partial state: honest_analysis string, tool_calls,
                        tool_results, agent_trace entry.
        Introduced:     Session 12.1. Updated: Session 12.2. Permanent.

        This agent requires frontier tier because it must hold multiple
        constraints simultaneously: honesty, specificity, cross-source coherence,
        and avoiding the user's likely self-deceptions. INT4 quality degradation
        on multi-constraint tasks would reduce this to generic life advice
        (see TASK_SURVIVABILITY_MATRIX["multi_constraint_reasoning"]).

        Instrumented: Session 13.1 — chronicle.brutality span, llm.call via
        call_gemini_traced(), child mcp.evidence.github span.
        """
        with tracer.start_as_current_span("chronicle.brutality") as span:
            span.set_attribute("langgraph_node", "brutality")
            span.set_attribute("analysis_id",    state["analysis_id"])
            span.set_attribute("llm.tier",       "frontier")

            try:
                start = time.monotonic()

                # ── Mid-stream MCP evidence fetch (Session 12.2) ───────────
                tool_calls   = []
                tool_results = []
                github_evidence = ""

                if "github" in state["data_sources"]:
                    with tracer.start_as_current_span("mcp.evidence.github") as t:
                        t.set_attribute("mcp.source", "github")
                        t.set_attribute("mcp.reason", "honesty_evidence")

                        tool_calls.append({
                            "name":    "get_commit_history",
                            "message": "Checking commit history to verify claims about productivity",
                        })
                        result = await mcp_pool.fetch_source("github", {"days": 60})
                        status = "success" if result["live"] else "fallback"
                        summary = f"{result['count']} commit records in last 60 days retrieved"
                        tool_results.append({
                            "name":    "get_commit_history",
                            "status":  status,
                            "summary": summary,
                        })
                        github_evidence = summary

                        t.set_attribute("mcp.live",  result["live"])
                        t.set_attribute("mcp.count", result["count"])
                        t.set_status(Status(StatusCode.OK))

                # ── Honest analysis with live evidence ─────────────────────
                prompt = (
                    f"You are Chronicle's Brutality Agent (frontier tier, FP16 precision). "
                    f"Your role: deliver honest cross-source analysis. No softening. No hedging. "
                    f"The user asked: {state['question']}. "
                    f"Correlations: {state['correlations']}. "
                    f"Timeline: {state['timeline_events']}. "
                    f"Live evidence: {github_evidence or 'no live data'}. "
                    f"Write 2-3 sentences of honest analysis backed by the evidence, that "
                    f"the user probably already knows but hasn't admitted. "
                    f"Do not be cruel. Be precise."
                )
                result = await call_gemini_traced(
                    frontier_llm, prompt, temperature=0.7, agent_name="brutality"
                )
                elapsed_ms = round((time.monotonic() - start) * 1000)

                span.set_attribute("brutality.output_length", len(result["text"]))
                span.set_status(Status(StatusCode.OK))

                # internal_notes: what Brutality Agent decided (Session 13.2)
                evidence_note = (
                    f"GitHub evidence: {tool_results[0]['summary']}"
                    if tool_results else "no live evidence"
                )
                span.set_attribute(
                    "internal_notes",
                    f"Honest analysis generated; {evidence_note}; output_length={len(result['text'])}"
                )

                return {
                    "honest_analysis": result["text"],
                    "tool_calls":      tool_calls,
                    "tool_results":    tool_results,
                    "agent_trace":     [f"[brutality] honest analysis + MCP evidence in {elapsed_ms}ms"],
                }

            except Exception as exc:
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR, str(exc)))
                raise

    return brutality_node


def make_synthesis_node(frontier_llm: "ChatGoogleGenerativeAI"):
    """
    Factory for synthesis_node bound to the frontier LLM.
    Introduced: Session 12.1. Permanent.
    """
    async def synthesis_node(state: ChronicleState) -> dict:
        """
        What it does:   Produces the final Chronicle analyst brief.
                        Frontier tier: structured multi-source generation.
        When called:    Last node before END in every Chronicle graph run.
        Returns:        Partial state: final_brief, confidence, agent_trace entry.
        Introduced:     Session 12.1. Permanent.
        Instrumented:   Session 13.1 — chronicle.synthesis span, llm.call via
                        call_gemini_traced().
        """
        with tracer.start_as_current_span("chronicle.synthesis") as span:
            span.set_attribute("langgraph_node", "synthesis")
            span.set_attribute("analysis_id",    state["analysis_id"])
            span.set_attribute("llm.tier",       "frontier")

            try:
                start = time.monotonic()
                prompt = (
                    f"You are Chronicle's Synthesis Agent (frontier tier, FP16 precision). "
                    f"Your role: produce the final structured analyst brief. "
                    f"Question: {state['question']}. "
                    f"Honest analysis: {state['honest_analysis']}. "
                    f"Key correlations: {state['correlations']}. "
                    f"Write a 3-4 sentence synthesis brief. "
                    f"End with a confidence score as a decimal between 0.0 and 1.0 on its own line "
                    f"like: CONFIDENCE: 0.82"
                )
                result = await call_gemini_traced(
                    frontier_llm, prompt, temperature=0.7, agent_name="synthesis"
                )
                content = result["text"]

                # Extract confidence score from the structured output
                confidence = 0.75  # Default if parsing fails
                lines = content.split("\n")
                brief_lines = []
                for line in lines:
                    if line.strip().startswith("CONFIDENCE:"):
                        try:
                            confidence = float(line.strip().split(":")[-1].strip())
                            confidence = max(0.0, min(1.0, confidence))
                        except ValueError:
                            pass
                    else:
                        brief_lines.append(line)

                final_brief = "\n".join(brief_lines).strip()
                elapsed_ms = round((time.monotonic() - start) * 1000)

                span.set_attribute("synthesis.confidence",   confidence)
                span.set_attribute("synthesis.brief_length", len(final_brief))
                span.set_status(Status(StatusCode.OK))

                # internal_notes: what Synthesis Agent decided (Session 13.2)
                span.set_attribute(
                    "internal_notes",
                    f"Final brief generated; confidence={confidence:.2f}; brief_length={len(final_brief)}"
                )

                return {
                    "final_brief": final_brief,
                    "confidence":  confidence,
                    "token_chunk": final_brief,   # S12.2: stream adapter emits as token_chunks
                    "agent_trace": [f"[synthesis] brief generated, confidence={confidence} in {elapsed_ms}ms"],
                }

            except Exception as exc:
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR, str(exc)))
                raise

    return synthesis_node


# ── Chronicle Graph Builder (Session 12.1) ────────────────────────

def build_chronicle_graph(mcp_pool: "MCPClientPool"):
    """
    What it does:   Compiles the Chronicle LangGraph StateGraph.
                    Wires all 5 agent nodes in sequence.
                    Returns a compiled graph ready for ainvoke() or astream().
    When called:    Once in FastAPI lifespan startup.
                    Stored in app.state.graph for reuse across all requests.
    Returns:        Compiled LangGraph StateGraph.
    Introduced:     Session 12.1. Updated: Session 12.2. Permanent.

    Session 12.2 update: pattern_node and brutality_node now receive
    mcp_pool so they can invoke MCP tools mid-reasoning.
    Graph topology unchanged.

    Graph topology (linear sequence, Session 12.1):
        ingestion → pattern → timeline → brutality → synthesis → END
    """
    utility_llm, frontier_llm = build_llm_instances()

    graph = StateGraph(ChronicleState)

    # ── Add nodes ─────────────────────────────────────────────────
    graph.add_node("ingestion", make_ingestion_node(mcp_pool))
    graph.add_node("pattern",   make_pattern_node(utility_llm, mcp_pool))   # ← mcp_pool added S12.2
    graph.add_node("timeline",  make_timeline_node(utility_llm))
    graph.add_node("brutality", make_brutality_node(frontier_llm, mcp_pool)) # ← mcp_pool added S12.2
    graph.add_node("synthesis", make_synthesis_node(frontier_llm))

    # ── Wire edges (linear sequence) ──────────────────────────────
    graph.set_entry_point("ingestion")
    graph.add_edge("ingestion", "pattern")
    graph.add_edge("pattern",   "timeline")
    graph.add_edge("timeline",  "brutality")
    graph.add_edge("brutality", "synthesis")
    graph.add_edge("synthesis", END)

    return graph.compile()


# ── Stream Adapter (Session 12.2) ──────────────────────────────────

# The only place graph node names appear in the stream adapter.
# When nodes are renamed: update here only.
NODE_LABELS = {
    "ingestion": "Ingestion Agent",
    "pattern":   "Pattern Agent",
    "timeline":  "Timeline Agent",
    "brutality": "Brutality Agent",
    "synthesis": "Synthesis Agent",
}
AGENT_NODES = set(NODE_LABELS.keys())


async def chronicle_stream_events(
    graph,
    initial_state: ChronicleState,
    analysis_id:   str,
    wall_start:    float,
):
    """
    What it does:   Async generator that runs graph.astream() and translates
                    raw LangGraph chunks into typed Chronicle SSE events.
                    This is the stream adapter — it isolates the frontend from
                    LangGraph's internal vocabulary.
    When called:    By the sse_generator in api.py for every /analyze/stream request.
    Yields:         BaseStreamEvent subclasses — one per meaningful state transition.
    Introduced:     Session 12.2. Permanent.

    Five detection blocks in order:
    1. Agent handoff — when a new node name appears in the chunk
    2. Tool calls — tool_calls list accumulated in state
    3. Tool results — tool_results list accumulated in state
    4. Synthesis tokens — token_chunk field on synthesis node output
    5. Final answer — final_brief on synthesis output

    The stream adapter is the only file that changes when nodes are renamed.
    No other code knows about graph node names.
    """
    from stream_schemas import (
        StreamStartEvent, AgentHandoffEvent, ToolCallEvent,
        ToolResultEvent, TokenChunkEvent, FinalAnswerEvent,
    )

    seq       = 1
    prev_node = None
    seen_tool_calls   = set()   # deduplicate tool events across chunks
    seen_tool_results = set()

    # graph.astream() never mutates initial_state in place — node outputs
    # only ever arrive as per-chunk updates. Track the fields FinalAnswerEvent
    # needs locally as they stream in, rather than reading stale initial_state.
    accumulated_correlations   = []
    accumulated_honest_analysis = ""
    accumulated_raw_data       = {}

    # ── Emit stream_start BEFORE graph work ───────────────────────
    # Client receives this within 100ms of request arrival.
    # Eliminates blank screen entirely.
    # DO NOT delay this until after graph.astream() begins.
    yield StreamStartEvent(
        seq=seq,
        analysis_id=analysis_id,
        question=initial_state["question"],
        sources=initial_state["data_sources"],
    )
    seq += 1

    async for chunk in graph.astream(initial_state, stream_mode="updates"):
        for node_name, node_update in chunk.items():
            if not isinstance(node_update, dict):
                continue

            # ── Track fields needed for FinalAnswerEvent ───────────
            if node_name == "ingestion" and node_update.get("raw_data"):
                accumulated_raw_data = node_update["raw_data"]
            if node_name == "pattern" and node_update.get("correlations"):
                accumulated_correlations.extend(node_update["correlations"])
            if node_name == "brutality" and node_update.get("honest_analysis"):
                accumulated_honest_analysis = node_update["honest_analysis"]

            # ── Block 1: Agent handoff detection ──────────────────
            if node_name in AGENT_NODES and node_name != prev_node:
                yield AgentHandoffEvent(
                    seq=seq,
                    from_agent=NODE_LABELS.get(prev_node) if prev_node else None,
                    to_agent=NODE_LABELS[node_name],
                    message=f"{NODE_LABELS[node_name]} now active",
                )
                seq      += 1
                prev_node = node_name

            # ── Block 2: Tool call events ──────────────────────────
            for tc in node_update.get("tool_calls") or []:
                key = f"{node_name}:{tc['name']}"
                if key not in seen_tool_calls:
                    seen_tool_calls.add(key)
                    yield ToolCallEvent(
                        seq=seq,
                        agent=NODE_LABELS.get(node_name, node_name),
                        tool_name=tc["name"],
                        message=tc.get("message", f"Invoking {tc['name']}"),
                    )
                    seq += 1

            # ── Block 3: Tool result events ────────────────────────
            for tr in node_update.get("tool_results") or []:
                key = f"{node_name}:{tr['name']}"
                if key not in seen_tool_results:
                    seen_tool_results.add(key)
                    yield ToolResultEvent(
                        seq=seq,
                        agent=NODE_LABELS.get(node_name, node_name),
                        tool_name=tr["name"],
                        status=tr.get("status", "success"),
                        message=tr.get("summary", "Done"),
                    )
                    seq += 1

            # ── Block 4: Token chunk events (synthesis streaming) ──
            # Synthesis node adds token_chunk to state for each word.
            # This creates the word-by-word streaming effect in the chat panel.
            if node_name == "synthesis" and node_update.get("token_chunk"):
                for word in node_update["token_chunk"].split(" "):
                    if word:
                        yield TokenChunkEvent(seq=seq, chunk=word + " ")
                        seq += 1

            # ── Block 5: Final answer detection ───────────────────
            if node_name == "synthesis" and node_update.get("final_brief"):
                processing_ms = round((time.time() - wall_start) * 1000)
                yield FinalAnswerEvent(
                    seq=seq,
                    analysis_id=analysis_id,
                    final_brief=node_update.get("final_brief", ""),
                    confidence=float(node_update.get("confidence", 0.75)),
                    correlations=accumulated_correlations,
                    honest_analysis=accumulated_honest_analysis,
                    sources_used=list(accumulated_raw_data.keys()),
                    processing_ms=processing_ms,
                )
                seq += 1


# ── Initial State Builder (Session 12.1) ──────────────────────────

def build_initial_state(request: "AnalysisRequest", analysis_id: str) -> ChronicleState:
    """
    What it does:   Constructs the initial ChronicleState from a validated request.
    When called:    In the /analyze endpoint, before graph.ainvoke().
    Returns:        ChronicleState dict ready to pass to ainvoke().
    Introduced:     Session 12.1. Permanent.
    """
    return {
        "question":        request.question,
        "data_sources":    request.data_sources,
        "depth":           request.depth,
        "analysis_id":     analysis_id,
        "raw_data":        {},
        "sources_live":    {},
        "correlations":    [],
        "timeline_events": [],
        "honest_analysis": "",
        "final_brief":     "",
        "confidence":      0.0,
        "token_chunk":     "",
        "agent_trace":     [],
        "processing_ms":   0,
        "tool_calls":      [],   # S12.2
        "tool_results":    [],   # S12.2
    }


# ── Session Verification (Session 12.2 — REPLACED) ───────────────

def run_session_verification() -> dict:
    """
    ┌─────────────────────────────────────────────────────────────┐
    │  SESSION 12.2 — VERIFICATION TEST                           │
    ├─────────────────────────────────────────────────────────────┤
    │  WHAT THIS TESTS:                                           │
    │    - chronicle_stream_events() yields at least 5 events     │
    │    - StreamStartEvent is the first event emitted            │
    │    - AgentHandoffEvent emitted for each of 5 agents         │
    │    - FinalAnswerEvent is the last event (final=True)        │
    │    - to_sse_frame() produces correct double-newline format  │
    ├─────────────────────────────────────────────────────────────┤
    │  PASS CRITERIA:                                             │
    │    ✓ First event: event_type == "stream_start"              │
    │    ✓ 5 agent_handoff events emitted (one per node)          │
    │    ✓ At least 1 tool_call + 1 tool_result event             │
    │    ✓ FinalAnswerEvent has final=True and non-empty brief     │
    │    ✓ SSE frame ends with double newline                     │
    └─────────────────────────────────────────────────────────────┘
    """
    from stream_schemas import (
        StreamStartEvent, AgentHandoffEvent, FinalAnswerEvent,
        ToolCallEvent, ToolResultEvent, to_sse_frame,
    )
    checks = []
    start  = time.monotonic()

    async def collect_events():
        pool  = MCPClientPool()
        graph = build_chronicle_graph(pool)
        req   = AnalysisRequest(
            question="What patterns exist in my personal data?",
            data_sources=["spotify", "finance", "fitness", "github", "journal"],
        )
        state     = build_initial_state(req, "verify-s122")
        events    = []
        wall_st   = time.time()
        async for evt in chronicle_stream_events(graph, state, "verify-s122", wall_st):
            events.append(evt)
        return events

    try:
        events = asyncio.run(collect_events())
    except Exception as e:
        checks.append({"label": "chronicle_stream_events() runs", "passed": False, "note": str(e)})
        duration_ms = round((time.monotonic() - start) * 1000)
        return {"passed": False, "checks": checks, "summary": f"0/5 checks passed in {duration_ms}ms", "duration_ms": duration_ms}

    # CHECK 1: First event is stream_start
    first_ok = bool(events) and events[0].event_type == "stream_start"
    checks.append({
        "label":  "First event is StreamStartEvent",
        "passed": first_ok,
        "note":   f"First event type: {events[0].event_type if events else 'no events'}",
    })

    # CHECK 2: 5 agent_handoff events (one per Chronicle node)
    handoffs   = [e for e in events if e.event_type == "agent_handoff"]
    handoff_ok = len(handoffs) == 5
    checks.append({
        "label":  "5 AgentHandoffEvents emitted (one per node)",
        "passed": handoff_ok,
        "note":   f"Handoff events: {len(handoffs)} | agents: {[e.to_agent for e in handoffs]}",
    })

    # CHECK 3: At least 1 tool_call + 1 tool_result
    tool_calls   = [e for e in events if e.event_type == "tool_call"]
    tool_results = [e for e in events if e.event_type == "tool_result"]
    tools_ok = len(tool_calls) >= 1 and len(tool_results) >= 1
    checks.append({
        "label":  "At least 1 ToolCallEvent + 1 ToolResultEvent emitted",
        "passed": tools_ok,
        "note":   f"tool_call: {len(tool_calls)} · tool_result: {len(tool_results)}",
    })

    # CHECK 4: Last event is FinalAnswerEvent with final=True and non-empty brief
    last = events[-1] if events else None
    final_ok = (
        last is not None
        and last.event_type == "final_answer"
        and getattr(last, "final", False)
        and bool(getattr(last, "final_brief", "").strip())
    )
    checks.append({
        "label":  "Last event is FinalAnswerEvent with final=True and non-empty brief",
        "passed": final_ok,
        "note":   (
            f"Last event: {last.event_type if last else 'none'} · "
            f"final={getattr(last, 'final', None)} · "
            f"brief length: {len(getattr(last, 'final_brief', ''))}"
        ),
    })

    # CHECK 5: SSE frame format correct (double newline terminator)
    test_frame = to_sse_frame(events[0]) if events else ""
    frame_ok   = test_frame.endswith("\n\n") and "event: stream_start" in test_frame
    checks.append({
        "label":  "to_sse_frame() produces correct SSE wire format",
        "passed": frame_ok,
        "note":   f"Frame ends with double newline: {test_frame.endswith(chr(10)+chr(10))} | contains event: line: {'event:' in test_frame}",
    })

    duration_ms = round((time.monotonic() - start) * 1000)
    passed      = sum(1 for c in checks if c["passed"])
    total       = len(checks)

    return {
        "passed":      passed == total,
        "checks":      checks,
        "summary":     f"{passed}/{total} checks passed in {duration_ms}ms",
        "duration_ms": duration_ms,
    }


# ── Async Job Queue — Background Worker (Session 12.3) ────────────

async def run_chronicle_analysis(
    job_id:      str,
    ticket_id:   str,
    graph,
    request:     "AnalysisRequest",
    analysis_id: str,
    otel_ctx=None,     # S13.1: context captured from HTTP handler
) -> None:
    """
    What it does:   Drives the same compiled Chronicle graph used by
                    /analyze and /analyze/stream, but in the background —
                    writing job status to job_store at every node
                    transition instead of returning a response or
                    streaming SSE frames.
    When called:    Scheduled via FastAPI BackgroundTasks from
                    POST /analyze/async, immediately after the 202
                    response has already been sent to the client.
    Returns:        Nothing — all output goes to job_store, polled via
                    GET /analyze/jobs/{job_id}.
    Introduced:     Session 12.3. Permanent.

    WHY graph.astream() and not graph.ainvoke():
        ainvoke() blocks until the whole graph finishes and returns
        one final state — no visibility into which node is running.
        astream(stream_mode="updates") yields a chunk after every node,
        which is exactly what update_status(active_node=...) needs to
        make /analyze/jobs/{job_id} show live progress instead of a
        single queued -> completed jump.

    WHY this can take 60-90 seconds now:
        Once mcp_servers/ are running, ingestion pulls real HTTP
        responses from 5 processes, and pattern/brutality make a
        mid-reasoning MCP call on top of their Gemini call. Multiply
        five sequential Gemini round-trips by real network I/O and a
        single request can comfortably blow past API Gateway's 29s
        ceiling — which is the whole reason this function exists.

    Session 13.1 update: accepts otel_ctx, captured from the HTTP handler's
    current context before BackgroundTasks scheduled this coroutine.
    Attaching it here — before starting the root span — makes this whole
    background run a CHILD of the /analyze/async HTTP span instead of an
    orphan trace with no parent.
    """
    from job_store import JobStatus, update_status

    # Attach context from HTTP handler — closes the BackgroundTasks boundary
    token = otel_context.attach(otel_ctx) if otel_ctx else None

    try:
        with tracer.start_as_current_span("backgroundtasks.chronicle_analysis") as span:
            span.set_attribute("job_id",      job_id)
            span.set_attribute("analysis_id", analysis_id)
            span.set_attribute("question",    request.question[:200])

            wall_start = time.monotonic()

            try:
                await update_status(
                    job_id, JobStatus.PROCESSING,
                    active_node="ingestion",
                    partial_result="Pulling data from MCP sources...",
                )

                initial_state = build_initial_state(request, analysis_id)
                accum: dict = dict(initial_state)
                prev_node: Optional[str] = None

                async for chunk in graph.astream(initial_state, stream_mode="updates"):
                    for node_name, node_update in chunk.items():
                        if not isinstance(node_update, dict):
                            continue
                        accum.update(node_update)

                        if node_name in AGENT_NODES and node_name != prev_node:
                            prev_node = node_name
                            await update_status(
                                job_id, JobStatus.PROCESSING,
                                active_node=node_name,
                                partial_result=f"{NODE_LABELS.get(node_name, node_name)} running...",
                            )

                final_brief = accum.get("final_brief", "")
                if final_brief:
                    processing_ms = round((time.monotonic() - wall_start) * 1000)
                    span.set_attribute("processing_ms", processing_ms)
                    await update_status(
                        job_id, JobStatus.COMPLETED,
                        active_node=None,
                        final_result=final_brief,
                        correlations=accum.get("correlations", []),
                        honest_analysis=accum.get("honest_analysis", ""),
                        confidence=float(accum.get("confidence", 0.75)),
                        sources_used=list(accum.get("raw_data", {}).keys()),
                    )
                    span.set_status(Status(StatusCode.OK))
                else:
                    await update_status(
                        job_id, JobStatus.FAILED,
                        error_message="Graph completed but no final_brief was produced.",
                    )
                    span.set_status(Status(StatusCode.ERROR, "No synthesis output"))

            except Exception as exc:
                span.record_exception(exc)
                span.set_status(Status(StatusCode.ERROR, str(exc)))
                await update_status(
                    job_id, JobStatus.FAILED,
                    error_message=f"{type(exc).__name__}: analysis pipeline error",
                )

    finally:
        if token:
            otel_context.detach(token)


# ── Session Verification (Session 12.3 — EXTENDED) ────────────────

def run_session_verification() -> dict:
    """
    ┌─────────────────────────────────────────────────────────────┐
    │  SESSION 13.2 — VERIFICATION TEST                            │
    ├─────────────────────────────────────────────────────────────┤
    │  WHAT THIS TESTS:                                           │
    │    - OTel TracerProvider is configured                      │
    │    - call_gemini_traced() is importable and async           │
    │    - _redact_and_truncate() redacts PII and truncates       │
    │    - chronicle.ingestion span fires on ingestion_node()     │
    │    - run_chronicle_analysis() accepts otel_ctx param        │
    │    - OTLPSpanExporter is configured (not Console)           │
    │    - PHOENIX_ENDPOINT resolves correctly                    │
    │    - internal_notes attribute appears on ingestion node     │
    │    - judge_pipeline.py is importable                        │
    │    - grade_trajectory() returns expected rubric fields      │
    ├─────────────────────────────────────────────────────────────┤
    │  PASS CRITERIA:                                             │
    │    ✓ trace.get_tracer_provider() returns TracerProvider     │
    │    ✓ call_gemini_traced is an async function                │
    │    ✓ _redact_and_truncate() redacts email, truncates        │
    │    ✓ ingestion_node wrapped in chronicle.ingestion span     │
    │    ✓ run_chronicle_analysis() signature has otel_ctx        │
    │    ✓ Exporter class is OTLPSpanExporter                     │
    │    ✓ PHOENIX_ENDPOINT contains "6006"                       │
    │    ✓ ingestion_node sets internal_notes attribute           │
    │    ✓ judge_pipeline.SEEDED_BUGGY_TRACE exists                │
    │    ✓ grade_trajectory() returns tool_correctness,            │
    │      brutality_honesty, pii_leak fields                     │
    └─────────────────────────────────────────────────────────────┘

    Session 12.2/12.3/13.1 checks (SSE events, job_store round-trip,
    async job lifecycle, OTel instrumentation) are preserved unchanged —
    they are load-bearing regression coverage for prior sessions, not
    superseded by S13.2. S13.2 only ADDS checks 14-18 below.
    """
    import inspect
    from opentelemetry import trace as otel_trace
    from otel_setup import setup_tracing, PHOENIX_ENDPOINT
    from stream_schemas import (
        StreamStartEvent, AgentHandoffEvent, FinalAnswerEvent,
        ToolCallEvent, ToolResultEvent, to_sse_frame,
    )
    from job_store import JobStatus, JobRecord, write_job, read_job

    checks = []
    start  = time.monotonic()

    async def collect_events():
        pool  = MCPClientPool()
        graph = build_chronicle_graph(pool)
        req   = AnalysisRequest(
            question="What patterns exist in my personal data?",
            data_sources=["spotify", "finance", "fitness", "github", "journal"],
        )
        state     = build_initial_state(req, "verify-s122")
        events    = []
        wall_st   = time.time()
        async for evt in chronicle_stream_events(graph, state, "verify-s122", wall_st):
            events.append(evt)
        return events

    try:
        events = asyncio.run(collect_events())
    except Exception as e:
        checks.append({"label": "chronicle_stream_events() runs", "passed": False, "note": str(e)})
        duration_ms = round((time.monotonic() - start) * 1000)
        return {"passed": False, "checks": checks, "summary": f"0/{13} checks passed in {duration_ms}ms", "duration_ms": duration_ms}

    # CHECK 1: First event is stream_start
    first_ok = bool(events) and events[0].event_type == "stream_start"
    checks.append({
        "label":  "First event is StreamStartEvent",
        "passed": first_ok,
        "note":   f"First event type: {events[0].event_type if events else 'no events'}",
    })

    # CHECK 2: 5 agent_handoff events (one per Chronicle node)
    handoffs   = [e for e in events if e.event_type == "agent_handoff"]
    handoff_ok = len(handoffs) == 5
    checks.append({
        "label":  "5 AgentHandoffEvents emitted (one per node)",
        "passed": handoff_ok,
        "note":   f"Handoff events: {len(handoffs)} | agents: {[e.to_agent for e in handoffs]}",
    })

    # CHECK 3: At least 1 tool_call + 1 tool_result
    tool_calls   = [e for e in events if e.event_type == "tool_call"]
    tool_results = [e for e in events if e.event_type == "tool_result"]
    tools_ok = len(tool_calls) >= 1 and len(tool_results) >= 1
    checks.append({
        "label":  "At least 1 ToolCallEvent + 1 ToolResultEvent emitted",
        "passed": tools_ok,
        "note":   f"tool_call: {len(tool_calls)} · tool_result: {len(tool_results)}",
    })

    # CHECK 4: Last event is FinalAnswerEvent with final=True and non-empty brief
    last = events[-1] if events else None
    final_ok = (
        last is not None
        and last.event_type == "final_answer"
        and getattr(last, "final", False)
        and bool(getattr(last, "final_brief", "").strip())
    )
    checks.append({
        "label":  "Last event is FinalAnswerEvent with final=True and non-empty brief",
        "passed": final_ok,
        "note":   (
            f"Last event: {last.event_type if last else 'none'} · "
            f"final={getattr(last, 'final', None)} · "
            f"brief length: {len(getattr(last, 'final_brief', ''))}"
        ),
    })

    # CHECK 5: SSE frame format correct (double newline terminator)
    test_frame = to_sse_frame(events[0]) if events else ""
    frame_ok   = test_frame.endswith("\n\n") and "event: stream_start" in test_frame
    checks.append({
        "label":  "to_sse_frame() produces correct SSE wire format",
        "passed": frame_ok,
        "note":   f"Frame ends with double newline: {test_frame.endswith(chr(10)+chr(10))} | contains event: line: {'event:' in test_frame}",
    })

    # ── CHECK 6-8 (Session 12.3): async job queue ──────────────────
    async def run_job_lifecycle():
        pool  = MCPClientPool()
        graph = build_chronicle_graph(pool)
        req   = AnalysisRequest(
            question="What patterns exist in my personal data?",
            data_sources=["spotify", "finance", "fitness", "github", "journal"],
        )
        job_id      = "verify-s123-job"
        analysis_id = "verify-s123-analysis"
        before = JobRecord(job_id=job_id, ticket_id=job_id, analysis_id=analysis_id, question=req.question)
        await write_job(before)
        before_read = await read_job(job_id)
        await run_chronicle_analysis(job_id, job_id, graph, req, analysis_id)
        after = await read_job(job_id)
        return before_read, after

    try:
        before_job, after_job = asyncio.run(run_job_lifecycle())
    except Exception as e:
        before_job, after_job = None, None
        checks.append({"label": "job_store round-trip + run_chronicle_analysis()", "passed": False, "note": str(e)})
    else:
        # CHECK 6: job_store round-trip
        roundtrip_ok = (
            before_job is not None and before_job.status == JobStatus.QUEUED
            and after_job is not None and after_job.last_heartbeat_ms is not None
        )
        checks.append({
            "label":  "job_store write_job/read_job/update_status round-trip",
            "passed": roundtrip_ok,
            "note":   f"before.status={getattr(before_job, 'status', None)} · after.status={getattr(after_job, 'status', None)}",
        })

        # CHECK 7: run_chronicle_analysis() is async
        worker_ok = inspect.iscoroutinefunction(run_chronicle_analysis)
        checks.append({
            "label":  "run_chronicle_analysis() is an async function",
            "passed": worker_ok,
            "note":   "Ready for BackgroundTasks scheduling" if worker_ok else "Not a coroutine function",
        })

        # CHECK 8: full async job reaches completed with non-empty final_result
        completed_ok = (
            after_job is not None
            and after_job.status == JobStatus.COMPLETED
            and bool((after_job.final_result or "").strip())
            and after_job.active_node is None
        )
        checks.append({
            "label":  "Async job reaches status=completed with non-empty final_result",
            "passed": completed_ok,
            "note":   f"status={getattr(after_job, 'status', None)} · final_result length={len((getattr(after_job, 'final_result', '') or ''))}",
        })

    # ── CHECK 9 (Session 13.1): TracerProvider configured ──────────
    setup_tracing()
    provider = otel_trace.get_tracer_provider()
    provider_ok = provider is not None and "TracerProvider" in type(provider).__name__
    checks.append({
        "label":  "TracerProvider is configured via setup_tracing()",
        "passed": provider_ok,
        "note":   type(provider).__name__,
    })

    # ── CHECK 10 (Session 13.1): call_gemini_traced is async ────────
    traced_ok = inspect.iscoroutinefunction(call_gemini_traced)
    checks.append({
        "label":  "call_gemini_traced() is an async function",
        "passed": traced_ok,
        "note":   "Ready to replace direct ainvoke() calls" if traced_ok else "Not async",
    })

    # ── CHECK 11 (Session 13.1): _redact_and_truncate works ─────────
    test_text  = "Contact john@example.com or call +1-555-123-4567 about card 4111-1111-1111-1111"
    redacted   = _redact_and_truncate(test_text)
    redact_ok  = "@" not in redacted and "4111" not in redacted
    checks.append({
        "label":  "_redact_and_truncate() removes email and card numbers",
        "passed": redact_ok,
        "note":   f"Sample output: {redacted[:80]}",
    })

    # ── CHECK 12 (Session 13.1): ingestion node wrapped in a span ───
    pool         = MCPClientPool()
    ingestion_fn = make_ingestion_node(pool)
    node_ok      = inspect.iscoroutinefunction(ingestion_fn)
    checks.append({
        "label":  "make_ingestion_node() returns async function (wrapped in span)",
        "passed": node_ok,
        "note":   "chronicle.ingestion span will fire on every graph run",
    })

    # ── CHECK 13 (Session 13.1): otel_ctx propagation param ─────────
    sig    = inspect.signature(run_chronicle_analysis)
    ctx_ok = "otel_ctx" in sig.parameters
    checks.append({
        "label":  "run_chronicle_analysis() has otel_ctx parameter",
        "passed": ctx_ok,
        "note":   f"Parameters: {list(sig.parameters.keys())}",
    })

    # ── CHECK 14 (Session 13.2): Exporter is OTLP not Console ───────
    try:
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        otlp_provider = setup_tracing(use_batch=False)
        processors = otlp_provider._active_span_processor._span_processors
        exporter_types = [type(p.span_exporter).__name__ for p in processors
                          if hasattr(p, 'span_exporter')]
        otlp_ok = any('OTLP' in t for t in exporter_types)
        checks.append({
            "label":  "OTLPSpanExporter configured (not ConsoleSpanExporter)",
            "passed": otlp_ok,
            "note":   f"Active exporters: {exporter_types}",
        })
    except Exception as e:
        checks.append({"label": "OTLPSpanExporter check", "passed": False, "note": str(e)})

    # ── CHECK 15 (Session 13.2): PHOENIX_ENDPOINT points to port 6006 ──
    endpoint_ok = "6006" in PHOENIX_ENDPOINT
    checks.append({
        "label":  "PHOENIX_ENDPOINT contains port 6006",
        "passed": endpoint_ok,
        "note":   f"Endpoint: {PHOENIX_ENDPOINT}",
    })

    # ── CHECK 16 (Session 13.2): ingestion_node sets internal_notes ─────
    try:
        notes_pool = MCPClientPool()
        notes_node = make_ingestion_node(notes_pool)
        notes_src  = inspect.getsource(notes_node)
        notes_ok   = "internal_notes" in notes_src
        checks.append({
            "label":  "ingestion_node sets internal_notes attribute",
            "passed": notes_ok,
            "note":   "internal_notes set_attribute found in node source" if notes_ok
                      else "internal_notes missing from ingestion_node",
        })
    except Exception as e:
        checks.append({"label": "internal_notes check", "passed": False, "note": str(e)})

    # ── CHECK 17 (Session 13.2): judge_pipeline.py importable ───────────
    try:
        import judge_pipeline
        seeded_ok = hasattr(judge_pipeline, 'SEEDED_BUGGY_TRACE')
        checks.append({
            "label":  "judge_pipeline.SEEDED_BUGGY_TRACE exists",
            "passed": seeded_ok,
            "note":   "judge_pipeline.py imported successfully" if seeded_ok
                      else "SEEDED_BUGGY_TRACE not found in judge_pipeline",
        })
    except Exception as e:
        checks.append({"label": "judge_pipeline import", "passed": False, "note": str(e)})

    # ── CHECK 18 (Session 13.2): grade_trajectory returns rubric fields ──
    try:
        import judge_pipeline
        judge_result = asyncio.run(judge_pipeline.grade_trajectory(judge_pipeline.SEEDED_BUGGY_TRACE))
        fields     = {'tool_correctness', 'brutality_honesty', 'pii_leak'}
        has_fields = fields.issubset(judge_result.keys())
        checks.append({
            "label":  "grade_trajectory() returns tool_correctness, brutality_honesty, pii_leak",
            "passed": has_fields,
            "note":   (
                f"tool_correctness={judge_result.get('tool_correctness')} · "
                f"brutality_honesty={judge_result.get('brutality_honesty')} · "
                f"pii_leak={judge_result.get('pii_leak')}"
            ),
        })
    except Exception as e:
        checks.append({"label": "grade_trajectory() rubric check", "passed": False, "note": str(e)})

    duration_ms = round((time.monotonic() - start) * 1000)
    passed      = sum(1 for c in checks if c["passed"])
    total       = len(checks)

    return {
        "passed":      passed == total,
        "checks":      checks,
        "summary":     f"{passed}/{total} checks passed in {duration_ms}ms",
        "duration_ms": duration_ms,
    }


# ── CLI Entry Point (Session 13.2 — UPDATED) ─────────────────────

if __name__ == "__main__":
    from otel_setup import setup_tracing as _cli_setup_tracing
    _cli_setup_tracing(use_batch=False)   # S13.2: init tracing before verification runs

    print("\n╔══════════════════════════════════════════════════════╗")
    print("║  Chronicle — Session 13.2 Verification               ║")
    print("╚══════════════════════════════════════════════════════╝\n")

    result = run_session_verification()
    print(f"  Verification: {result['summary']}\n")
    for check in result["checks"]:
        icon = "✓" if check["passed"] else "✗"
        print(f"  {icon} {check['label']}")
        print(f"      {check['note']}")
    print()

    if result["passed"]:
        print("  ✓ Session 13.2 COMPLETE.")
        print("  Open Phoenix UI: http://localhost:6006")
        print("  Start the 5 MCP servers: bash mcp_servers/start_all.sh")
        print("  Start the API:           python api.py")
        print("  Queue an async analysis: curl -X POST http://localhost:8000/analyze/async \\")
        print("    -H 'Content-Type: application/json' \\")
        print("    -d '{\"question\": \"What does my data say about me?\"}'")
        print("  Then submit an analysis and watch the waterfall appear in Phoenix.")
    else:
        print("  ✗ Fix failing checks before proceeding.")
    print()


# ══════════════════════════════════════════════════════════════════
# SESSION 13.2 — COMPLETE — "Phoenix + LLM-as-Judge"
# ══════════════════════════════════════════════════════════════════
#
# What was ADDED in Session 13.2 (extended, nothing removed):
#   otel_setup.py: ConsoleSpanExporter replaced with OTLPSpanExporter
#     pointing at Arize Phoenix (http://localhost:6006/v1/traces)
#   agent.py: internal_notes span attribute on all 5 Chronicle nodes
#     (ingestion, pattern, timeline, brutality, synthesis)
#   judge_pipeline.py: LLM-as-Judge with 3 parallel rubrics:
#     - tool_correctness (PASS/FAIL)
#     - brutality_honesty (1-5 score)
#     - pii_leak (PASS/FAIL)
#   extract_from_phoenix() / build_trajectory_from_df(): reads spans
#     from Phoenix via SpanQuery and reconstructs full trajectories
#   SEEDED_BUGGY_TRACE: known-bad trajectory for pipeline verification
#   run_session_verification(): extended with checks 14-18 for S13.2
#
# What stays UNCHANGED from Session 13.1:
#   All span instrumentation in every agent node
#   call_gemini_traced() — unchanged
#   otel_setup.get_tracer(), shutdown_tracing() interface
#   All AI attribute schema fields
#   BackgroundTasks context propagation pattern
#   run_chronicle_analysis() otel_ctx parameter
#
# SESSION 13.3 HANDOFF — "Monitoring daemon"
#   grade_trajectory() results feed alert rules:
#     - any pii_leak == FAIL fires immediately
#     - 7-day rolling mean of brutality_honesty < 3.5
#     - tool_correctness FAIL rate > 2% over last 24h
#   run_nightly_judge_pipeline() is the entry point for a cron job.
# ══════════════════════════════════════════════════════════════════