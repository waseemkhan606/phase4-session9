"""
Chronicle MCP Server — Fitness
Port: 3003
Tool: get_fitness_data

Returns Apple Health-style fitness data with embedded patterns:
- Step count drops on high-commit days (cross-source correlation)
- HRV lowest after late-night coding sessions
- Gym attendance collapses after February
- Sleep average ~5.7h, deep sleep short

Run: uvicorn mcp_servers.fitness_server:app --port 3003
"""

import random
from datetime import datetime, timedelta
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(title="Chronicle MCP — Fitness", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


def generate_fitness_data(days: int = 30) -> list[dict]:
    records = []
    now     = datetime.now()

    for day_offset in range(days):
        date    = now - timedelta(days=day_offset)
        weekday = date.weekday()

        heavy_work_day = weekday < 5 and random.random() < 0.6

        steps = (
            random.randint(800, 2500)   if heavy_work_day
            else random.randint(3000, 7000)
        )
        active_calories = round(steps * 0.045 + random.uniform(-20, 20), 1)
        sleep_hours     = round(random.uniform(4.5, 6.2) if heavy_work_day
                                else random.uniform(6.0, 8.5), 1)
        deep_sleep_min  = round(sleep_hours * random.uniform(0.08, 0.14) * 60, 0)
        resting_hr      = random.randint(68, 80) if heavy_work_day else random.randint(58, 72)
        hrv_ms          = random.randint(32, 48) if heavy_work_day else random.randint(45, 68)
        bedtime_hour    = random.choice([0, 1, 2]) if heavy_work_day else random.choice([22, 23])

        month           = date.month
        gym_session     = (month == 1 and weekday < 5 and random.random() < 0.55) or \
                          (month == 2 and weekday < 5 and random.random() < 0.25) or \
                          (month == 3 and weekday < 5 and random.random() < 0.07)

        records.append({
            "date":              date.date().isoformat(),
            "steps":             steps,
            "active_calories":   active_calories,
            "sleep_hours":       sleep_hours,
            "deep_sleep_min":    deep_sleep_min,
            "resting_hr_bpm":    resting_hr,
            "hrv_ms":            hrv_ms,
            "bedtime_hour":      bedtime_hour,
            "gym_session":       gym_session,
            "heavy_work_day":    heavy_work_day,
            "stand_alerts_dismissed": random.randint(5, 15) if heavy_work_day else random.randint(0, 5),
        })

    return sorted(records, key=lambda x: x["date"], reverse=True)


class MCPRequest(BaseModel):
    jsonrpc: str = "2.0"
    method: str
    params: dict = {}
    id: int = 1


@app.post("/mcp")
async def handle_mcp(request: MCPRequest) -> dict:
    if request.method == "tools/list":
        return {
            "jsonrpc": "2.0", "id": request.id,
            "result": {"tools": [{"name": "get_fitness_data",
                "description": "Returns fitness data for Chronicle analysis",
                "inputSchema": {"type": "object",
                    "properties": {"days": {"type": "integer", "default": 30}}}}]}
        }

    if request.method == "tools/call" and request.params.get("name") == "get_fitness_data":
        days    = int(request.params.get("arguments", {}).get("days", 30))
        records = generate_fitness_data(days)

        avg_steps   = round(sum(r["steps"] for r in records) / max(len(records), 1))
        avg_sleep   = round(sum(r["sleep_hours"] for r in records) / max(len(records), 1), 1)
        avg_hrv     = round(sum(r["hrv_ms"] for r in records) / max(len(records), 1), 1)
        gym_count   = sum(1 for r in records if r["gym_session"])
        low_hrv_days = sum(1 for r in records if r["hrv_ms"] < 40)

        return {
            "jsonrpc": "2.0", "id": request.id,
            "result": {
                "content": records,
                "summary": {
                    "avg_daily_steps":  avg_steps,
                    "avg_sleep_hours":  avg_sleep,
                    "avg_hrv_ms":       avg_hrv,
                    "gym_sessions":     gym_count,
                    "low_hrv_days":     low_hrv_days,
                    "target_steps":     8000,
                    "steps_gap_pct":    round((8000 - avg_steps) / 8000 * 100, 1),
                },
                "source": "fitness_mcp_server",
                "live":   True,
            }
        }

    return {"jsonrpc": "2.0", "id": request.id, "error": {"code": -32601, "message": "Method not found"}}


@app.get("/health")
async def health():
    return {"status": "ok", "server": "fitness_mcp", "port": 3003}
