"""
Chronicle MCP Server — GitHub
Port: 3004
Tool: get_commit_history

Returns GitHub commit history with embedded patterns:
- Most commits between 9pm and 1am
- Occasional force pushes to main
- 4 active repos, several abandoned
- Commit message quality declining late at night
- Zero commits on weekends

Run: uvicorn mcp_servers.github_server:app --port 3004
"""

import random
from datetime import datetime, timedelta
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(title="Chronicle MCP — GitHub", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

REPOS = [
    {"name": "chronicle",           "active": True,  "last_commit_days_ago": 1,   "commits": 234},
    {"name": "side-project-alpha",  "active": False, "last_commit_days_ago": 243, "commits": 847},
    {"name": "side-project-beta",   "active": False, "last_commit_days_ago": 312, "commits": 3},
    {"name": "personal-site",       "active": True,  "last_commit_days_ago": 7,   "commits": 56},
    {"name": "ml-experiments",      "active": True,  "last_commit_days_ago": 14,  "commits": 189},
    {"name": "abandoned-saas",      "active": False, "last_commit_days_ago": 487, "commits": 1247},
]

COMMIT_MESSAGES_GOOD  = ["feat: add user auth", "fix: resolve race condition", "refactor: extract service layer", "docs: update API reference"]
COMMIT_MESSAGES_BAD   = ["fix", "wip", "asdfgh", "final", "final2", "FINAL", "ok", "test", "changes", "update stuff"]
COMMIT_MESSAGES_FORCE = ["fix broken merge", "revert disaster", "undo last commit pls", "force push sorry"]

def generate_commits(days: int = 90) -> list[dict]:
    commits = []
    now     = datetime.now()

    for day_offset in range(days):
        date    = now - timedelta(days=day_offset)
        weekday = date.weekday()

        if weekday >= 5:
            continue

        n_commits = random.choices(
            [0, 1, 2, 3, 5, 8, 12, 15],
            weights=[20, 15, 20, 15, 12, 10, 5, 3], k=1
        )[0]

        for _ in range(n_commits):
            hour = random.choices(
                [22, 23, 0, 1, 21, 20, 14, 10],
                weights=[25, 28, 18, 12, 8, 5, 2, 2], k=1
            )[0]

            repo          = random.choice(REPOS)
            is_force_push = random.random() < 0.025

            if hour >= 22 or hour <= 1:
                msg = random.choice(COMMIT_MESSAGES_BAD + COMMIT_MESSAGES_FORCE if is_force_push
                                    else COMMIT_MESSAGES_BAD)
            else:
                msg = random.choice(COMMIT_MESSAGES_GOOD)

            commits.append({
                "timestamp":   date.replace(hour=hour % 24, minute=random.randint(0, 59)).isoformat(),
                "repo":        repo["name"],
                "message":     msg,
                "hour":        hour % 24,
                "day_of_week": date.strftime("%A"),
                "is_force_push": is_force_push,
                "late_night":  hour >= 22 or hour <= 2,
                "lines_added": random.randint(1, 180),
                "lines_removed": random.randint(0, 80),
            })

    return sorted(commits, key=lambda x: x["timestamp"], reverse=True)


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
            "result": {"tools": [{"name": "get_commit_history",
                "description": "Returns commit history for Chronicle GitHub analysis",
                "inputSchema": {"type": "object",
                    "properties": {"days": {"type": "integer", "default": 90},
                                   "repo": {"type": "string"}}}}]}
        }

    if request.method == "tools/call" and request.params.get("name") == "get_commit_history":
        args        = request.params.get("arguments", {})
        days        = int(args.get("days", 90))
        repo_filter = args.get("repo")

        commits = generate_commits(days)
        if repo_filter:
            commits = [c for c in commits if c["repo"] == repo_filter]

        total         = len(commits)
        late_night    = sum(1 for c in commits if c["late_night"])
        force_pushes  = sum(1 for c in commits if c["is_force_push"])
        bad_messages  = sum(1 for c in commits if c["message"] in COMMIT_MESSAGES_BAD + COMMIT_MESSAGES_FORCE)
        weekend_commits = 0

        return {
            "jsonrpc": "2.0", "id": request.id,
            "result": {
                "content": commits,
                "repos":   REPOS,
                "summary": {
                    "total_commits":       total,
                    "late_night_commits":  late_night,
                    "late_night_pct":      round(late_night / max(total, 1) * 100, 1),
                    "force_pushes":        force_pushes,
                    "bad_commit_messages": bad_messages,
                    "message_quality_pct": round((total - bad_messages) / max(total, 1) * 100, 1),
                    "weekend_commits":     weekend_commits,
                    "active_repos":        sum(1 for r in REPOS if r["active"]),
                    "abandoned_repos":     sum(1 for r in REPOS if not r["active"]),
                },
                "source": "github_mcp_server",
                "live":   True,
            }
        }

    return {"jsonrpc": "2.0", "id": request.id, "error": {"code": -32601, "message": "Method not found"}}


@app.get("/health")
async def health():
    return {"status": "ok", "server": "github_mcp", "port": 3004}
