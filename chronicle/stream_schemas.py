"""
stream_schemas.py — Chronicle SSE Event Contract
Session 12.2. Permanent from this session onward.

These are the 7 event types emitted by /analyze/stream.
They are the public API between the server and every client.
Any rename of event_type values or field names is a breaking change.

Session 13.1 adds: span_id, trace_id fields to each event (OTel).
Session 14.1 adds: cache_hit bool to FinalAnswerEvent.
All additions are additive — existing fields never removed.
"""

from pydantic import BaseModel, Field
from typing import Optional, Literal
import time


class BaseStreamEvent(BaseModel):
    """
    Common fields across all Chronicle SSE events.
    seq: monotonically increasing per-stream sequence number.
    ts_ms: server-side timestamp in milliseconds.
    Introduced: Session 12.2. Permanent.
    """
    event_type: str
    seq:        int
    ts_ms:      int = Field(default_factory=lambda: int(time.time() * 1000))
    session:    str = "12.2"


class StreamStartEvent(BaseStreamEvent):
    """
    Emitted BEFORE any graph work begins.
    Client receives this within 100ms of POST /analyze/stream.
    Eliminates the blank-screen problem from the first frame.
    Introduced: Session 12.2. Permanent.
    """
    event_type:  Literal["stream_start"] = "stream_start"
    analysis_id: str
    question:    str
    sources:     list[str]


class AgentHandoffEvent(BaseStreamEvent):
    """
    Emitted when a new LangGraph node becomes active.
    from_agent is None for the first handoff (start → ingestion).
    Introduced: Session 12.2. Permanent.
    """
    event_type:  Literal["agent_handoff"] = "agent_handoff"
    from_agent:  Optional[str]
    to_agent:    str
    message:     str


class ToolCallEvent(BaseStreamEvent):
    """
    Emitted when an agent invokes an MCP tool mid-reasoning.
    Introduced: Session 12.2. Permanent.
    Pattern Agent and Brutality Agent emit these in S12.2.
    """
    event_type: Literal["tool_call"] = "tool_call"
    agent:      str
    tool_name:  str
    message:    str


class ToolResultEvent(BaseStreamEvent):
    """
    Emitted when an MCP tool returns a result mid-reasoning.
    status: "success" | "error" | "fallback"
    Introduced: Session 12.2. Permanent.
    """
    event_type: Literal["tool_result"] = "tool_result"
    agent:      str
    tool_name:  str
    status:     str
    message:    str


class TokenChunkEvent(BaseStreamEvent):
    """
    Emitted for each word/chunk during Synthesis Agent streaming.
    Client appends chunk to the chat panel text buffer.
    Introduced: Session 12.2. Permanent.
    """
    event_type: Literal["token_chunk"] = "token_chunk"
    chunk:      str


class FinalAnswerEvent(BaseStreamEvent):
    """
    Emitted after Synthesis Agent completes.
    final: True signals the client to close the stream and stop polling.
    Introduced: Session 12.2. Permanent.
    """
    event_type:     Literal["final_answer"] = "final_answer"
    analysis_id:    str
    final_brief:    str
    confidence:     float
    correlations:   list[str]
    honest_analysis: str
    sources_used:   list[str]
    processing_ms:  int
    final:          bool = True


class ErrorStreamEvent(BaseStreamEvent):
    """
    Emitted on any unrecoverable error during streaming.
    Client shows error card and stops reading the stream.
    Introduced: Session 12.2. Permanent.
    """
    event_type:  Literal["error"] = "error"
    error_code:  str
    message:     str
    final:       bool = True


def to_sse_frame(event: BaseStreamEvent, event_name: str = None) -> str:
    """
    What it does:   Serialises a Pydantic event to an SSE wire frame.
    Format:         event: <type>\\ndata: <json>\\n\\n
    The double newline is the MANDATORY SSE frame terminator.
    Without it: browser buffers frames indefinitely and never dispatches.
    When called:    By the sse_generator in api.py before every yield.
    Introduced:     Session 12.2. Permanent.
    """
    name = event_name or event.event_type
    return f"event: {name}\ndata: {event.model_dump_json()}\n\n"


def keepalive_frame() -> str:
    """
    Returns an SSE comment line — browser ignores, proxy stays alive.
    Emitted every 15 seconds during long analyses.
    Introduced: Session 12.2. Permanent.
    """
    return f": keepalive {int(time.time())}\n\n"