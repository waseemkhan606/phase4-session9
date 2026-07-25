"""
otel_setup.py — Chronicle OpenTelemetry Configuration
Session 13.2. Permanent from this session onward.

Session 13.1: ConsoleSpanExporter (stdout JSON)
Session 13.2: OTLPSpanExporter → Arize Phoenix  ← THIS SESSION

All instrumentation code in agent.py and api.py stays identical.
Only this file changes when the backend changes.
"""

import os
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    SimpleSpanProcessor,
    BatchSpanProcessor,
)
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource, SERVICE_NAME
from opentelemetry.trace import Status, StatusCode

# ── Resource — service identity stamped on every span ────────────
# Every span emitted from Chronicle inherits these attributes.
# Change service.version when deploying a new release.
CHRONICLE_RESOURCE = Resource.create({
    SERVICE_NAME:                  "chronicle",
    "service.version":             "13.2.0",
    "deployment.environment":      os.getenv("ENV", "development"),
})

# ── Phoenix collector endpoint — overridable via env var ──────────
PHOENIX_ENDPOINT = os.getenv(
    "PHOENIX_COLLECTOR_ENDPOINT",
    "http://localhost:6006/v1/traces",
)

# ── TracerProvider — created once, used everywhere ────────────────
_tracer_provider: TracerProvider = None


def setup_tracing(use_batch: bool = True) -> TracerProvider:
    """
    What it does:   Creates and configures the global TracerProvider.
                    Called once at FastAPI lifespan startup.
    Args:           use_batch=True for production (async export).
                    use_batch=False for dev (synchronous, immediate).
    Returns:        TracerProvider (also stored in _tracer_provider).
    Introduced:     Session 13.1. Updated: Session 13.2 (OTLP exporter).

    Session 13.2 change: ConsoleSpanExporter → OTLPSpanExporter pointed
    at Phoenix. That is the only behavioral change from Session 13.1 —
    everything downstream (FastAPIInstrumentor, agent node spans,
    call_gemini_traced, BackgroundTask context propagation) is unchanged.
    """
    global _tracer_provider

    exporter = OTLPSpanExporter(endpoint=PHOENIX_ENDPOINT)

    processor = (
        BatchSpanProcessor(exporter)  if use_batch
        else SimpleSpanProcessor(exporter)
    )

    provider = TracerProvider(resource=CHRONICLE_RESOURCE)
    provider.add_span_processor(processor)

    trace.set_tracer_provider(provider)
    _tracer_provider = provider
    return provider


def get_tracer(name: str = "chronicle") -> trace.Tracer:
    """
    What it does:   Returns a named tracer for use in a module.
    When called:    Once per module at import time.
    Usage:          tracer = get_tracer("chronicle.agents")
    Introduced:     Session 13.1. Permanent.
    """
    return trace.get_tracer(name)


def shutdown_tracing() -> None:
    """
    What it does:   Flushes all buffered spans before process exit.
    When called:    In FastAPI lifespan finally-block.
    Why critical:   BatchSpanProcessor buffers in memory.
                    Fast shutdown drops the buffer.
                    Without this: the last ~500ms of spans are lost
                    on every deployment.
    Introduced:     Session 13.1. Permanent.
    """
    global _tracer_provider
    if _tracer_provider:
        _tracer_provider.shutdown()


# Re-export Status and StatusCode so callers import from one place
__all__ = [
    "setup_tracing",
    "get_tracer",
    "shutdown_tracing",
    "Status",
    "StatusCode",
]