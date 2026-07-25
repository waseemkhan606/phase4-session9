"""
job_store.py — Chronicle Async Job State
Session 12.3. Permanent from this session onward.

In-memory job store backed by asyncio.Lock. In production this dict
swaps for a Redis HASH; write_job()/read_job()/update_status() keep
the exact same signatures either way, so api.py and agent.py never
need to change when the backing store changes.

State machine:
  queued -> processing -> completed
                        -> failed
                        -> cancelled
"""

import time
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class JobStatus(str, Enum):
    QUEUED     = "queued"
    PROCESSING = "processing"
    COMPLETED  = "completed"
    FAILED     = "failed"
    CANCELLED  = "cancelled"


class JobRecord(BaseModel):
    """
    Persisted record for one Chronicle async analysis.
    Written by run_chronicle_analysis() (agent.py), read by
    GET /analyze/jobs/{job_id} (api.py).

    ticket_id mirrors job_id — kept as a separate field because the
    client-facing "ticket" and the internal queue key are conceptually
    distinct even though they're equal today (a future queue backend
    may split them, e.g. if job_id becomes a Redis stream entry ID).
    Introduced: Session 12.3. Permanent.
    """
    job_id:            str
    ticket_id:         str
    analysis_id:       str
    question:          str
    status:            JobStatus = JobStatus.QUEUED
    active_node:       Optional[str] = None       # which Chronicle agent is running
    partial_result:    Optional[str] = None       # latest progress note
    final_result:      Optional[str] = None       # synthesis brief when completed
    correlations:      list[str] = []
    honest_analysis:   str = ""
    confidence:        float = 0.0
    sources_used:      list[str] = []
    error_message:     Optional[str] = None
    submitted_at_ms:   int = Field(default_factory=lambda: int(time.time() * 1000))
    updated_at_ms:     int = Field(default_factory=lambda: int(time.time() * 1000))
    completed_at_ms:   Optional[int] = None
    last_heartbeat_ms: Optional[int] = None
    processing_ms:     Optional[int] = None


class JobAcceptedResponse(BaseModel):
    """202 Accepted response body. Introduced: Session 12.3. Permanent."""
    status:                       str = "queued"
    job_id:                       str
    ticket_id:                    str
    analysis_id:                  str
    poll_url:                     str
    estimated_completion_seconds: int = 75
    retry_after_seconds:          int = 5


# ── In-memory store (dev/demo backend — swap for Redis in production) ──
import asyncio

_lock:      asyncio.Lock   = asyncio.Lock()
_job_store: dict[str, str] = {}   # job_id -> JobRecord JSON string


async def write_job(record: JobRecord) -> None:
    """Write or overwrite a job record atomically."""
    async with _lock:
        record.updated_at_ms = int(time.time() * 1000)
        _job_store[record.job_id] = record.model_dump_json()


async def read_job(job_id: str) -> Optional[JobRecord]:
    """Return a JobRecord, or None if job_id is unknown."""
    async with _lock:
        raw = _job_store.get(job_id)
        if raw is None:
            return None
        return JobRecord.model_validate_json(raw)


_UNSET = object()  # sentinel distinguishing "not provided" from "explicitly None"


async def update_status(
    job_id:          str,
    status:          JobStatus,
    active_node             = _UNSET,   # Optional[str]; pass None to explicitly clear
    partial_result:  Optional[str] = None,
    final_result:    Optional[str] = None,
    correlations:    Optional[list] = None,
    honest_analysis: Optional[str] = None,
    confidence:      Optional[float] = None,
    sources_used:    Optional[list] = None,
    error_message:   Optional[str] = None,
) -> None:
    """
    Partial update — only overwrites explicitly passed fields.
    Called by run_chronicle_analysis() at every Chronicle node transition.

    active_node uses a sentinel default (not None) because completion
    needs to explicitly CLEAR active_node to None — if None meant
    "not provided" here, run_chronicle_analysis()'s final
    update_status(..., active_node=None, ...) call would silently
    leave the last node's name stuck in the record forever.
    """
    record = await read_job(job_id)
    if record is None:
        return

    record.status            = status
    record.last_heartbeat_ms = int(time.time() * 1000)

    if active_node is not _UNSET: record.active_node    = active_node
    if partial_result is not None: record.partial_result = partial_result
    if final_result   is not None:
        record.final_result    = final_result
        record.completed_at_ms = int(time.time() * 1000)
        record.processing_ms   = record.completed_at_ms - record.submitted_at_ms
    if correlations     is not None: record.correlations     = correlations
    if honest_analysis  is not None: record.honest_analysis  = honest_analysis
    if confidence        is not None: record.confidence      = confidence
    if sources_used      is not None: record.sources_used    = sources_used
    if error_message     is not None: record.error_message   = error_message

    await write_job(record)
