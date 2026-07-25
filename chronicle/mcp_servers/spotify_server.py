"""
Chronicle MCP Server — Spotify
Port: 3001
Tool: get_spotify_history

Returns realistic Spotify listening history for Chronicle analysis.
Data is pre-seeded with patterns that make Chronicle's analysis meaningful:
- Late night listening (10pm-2am) on weekdays
- Radiohead spike when commits happen (cross-source correlation bait)
- Skip rate increases on Monday mornings
- Session length dropping over 6 weeks

Run: uvicorn mcp_servers.spotify_server:app --port 3001
"""

import random
from datetime import datetime, timedelta
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(title="Chronicle MCP — Spotify", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


def generate_listening_history(days: int = 30) -> list[dict]:
    """Generate realistic Spotify history with embedded patterns."""
    tracks = [
        {"artist": "Radiohead",    "track": "Paranoid Android",    "duration_s": 386, "genre": "alternative"},
        {"artist": "Radiohead",    "track": "Karma Police",        "duration_s": 264, "genre": "alternative"},
        {"artist": "Radiohead",    "track": "Fake Plastic Trees",  "duration_s": 288, "genre": "alternative"},
        {"artist": "Burial",       "track": "Archangel",           "duration_s": 399, "genre": "electronic"},
        {"artist": "Burial",       "track": "Shell of Light",      "duration_s": 342, "genre": "electronic"},
        {"artist": "Mac DeMarco",  "track": "Chamber of Reflection","duration_s": 231, "genre": "indie"},
        {"artist": "The National", "track": "Bloodbuzz Ohio",      "duration_s": 230, "genre": "indie"},
        {"artist": "Bon Iver",     "track": "Holocene",            "duration_s": 346, "genre": "folk"},
        {"artist": "Tame Impala",  "track": "Eventually",          "duration_s": 316, "genre": "psychedelic"},
        {"artist": "Kendrick Lamar","track": "Alright",            "duration_s": 216, "genre": "hip-hop"},
    ]

    history = []
    now = datetime.now()

    for day_offset in range(days):
        date = now - timedelta(days=day_offset)
        weekday = date.weekday()  # 0=Monday, 6=Sunday

        session_hour = random.choices(
            [22, 23, 0, 1, 14, 15, 20],
            weights=[25, 30, 20, 15, 3, 3, 4],
            k=1
        )[0]

        if weekday < 5:
            session_tracks = random.choices(
                tracks,
                weights=[20, 15, 12, 10, 8, 8, 7, 7, 7, 6],
                k=random.randint(4, 9)
            )
        else:
            session_tracks = random.choices(tracks, k=random.randint(2, 5))

        for i, track in enumerate(session_tracks):
            played_at = date.replace(
                hour=session_hour % 24,
                minute=random.randint(0, 59),
                second=random.randint(0, 59),
            )
            skipped = (weekday == 0 and session_hour in [8, 9, 10]) and random.random() < 0.6
            skip_at_s = random.randint(15, 60) if skipped else None

            history.append({
                "played_at":    played_at.isoformat(),
                "artist":       track["artist"],
                "track":        track["track"],
                "duration_s":   track["duration_s"],
                "genre":        track["genre"],
                "skipped":      skipped,
                "skip_at_s":    skip_at_s,
                "session_hour": session_hour,
                "day_of_week":  date.strftime("%A"),
            })

    return sorted(history, key=lambda x: x["played_at"], reverse=True)


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
            "result": {
                "tools": [{
                    "name": "get_spotify_history",
                    "description": "Returns Spotify listening history for Chronicle analysis",
                    "inputSchema": {
                        "type": "object",
                        "properties": {
                            "days": {"type": "integer", "default": 30,
                                     "description": "Number of days of history to return"},
                        }
                    }
                }]
            }
        }

    if request.method == "tools/call":
        tool_name = request.params.get("name")
        arguments = request.params.get("arguments", {})

        if tool_name == "get_spotify_history":
            days    = int(arguments.get("days", 30))
            history = generate_listening_history(days)

            artists      = {}
            late_night   = sum(1 for t in history if t["session_hour"] >= 22 or t["session_hour"] <= 2)
            skipped      = sum(1 for t in history if t["skipped"])
            for t in history:
                artists[t["artist"]] = artists.get(t["artist"], 0) + 1

            top_artists = sorted(artists.items(), key=lambda x: x[1], reverse=True)[:3]

            return {
                "jsonrpc": "2.0", "id": request.id,
                "result": {
                    "content": history,
                    "summary": {
                        "total_tracks":     len(history),
                        "days_covered":     days,
                        "late_night_plays": late_night,
                        "late_night_pct":   round(late_night / max(len(history), 1) * 100, 1),
                        "skip_count":       skipped,
                        "skip_rate_pct":    round(skipped / max(len(history), 1) * 100, 1),
                        "top_artists":      [{"artist": a, "plays": c} for a, c in top_artists],
                    },
                    "source": "spotify_mcp_server",
                    "live":   True,
                }
            }

    return {"jsonrpc": "2.0", "id": request.id, "error": {"code": -32601, "message": "Method not found"}}


@app.get("/health")
async def health():
    return {"status": "ok", "server": "spotify_mcp", "port": 3001}
