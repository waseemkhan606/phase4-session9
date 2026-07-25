"""
Chronicle MCP Server — Journal
Port: 3005
Tool: get_journal_entries

Returns journal entries with embedded sentiment patterns:
- Worst sentiment days correlate with Radiohead + late commits + food delivery
- Recurring words: 'tired', 'tomorrow', 'should have'
- Gym cancellation entry
- Side project deletion entry

Run: uvicorn mcp_servers.journal_server:app --port 3005
"""

import random
from datetime import datetime, timedelta
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(title="Chronicle MCP — Journal", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

SEEDED_ENTRIES = [
    {
        "date": (datetime.now() - timedelta(days=4)).date().isoformat(),
        "text": "Couldn't focus today. Opened 14 browser tabs about side project ideas. Closed all of them. Spent 3 hours on Twitter. Ordered Uber Eats at 11pm. Pushed one commit at 00:30 that immediately got force-pushed. Going to sleep.",
        "sentiment": -0.72, "word_count": 46, "keywords": ["unfocused", "procrastination", "late_night"],
    },
    {
        "date": (datetime.now() - timedelta(days=11)).date().isoformat(),
        "text": "Good day. Shipped the auth module. 47 commits. PR up. Actually went outside for 40 minutes. Cooked dinner instead of ordering. Listened to Tame Impala not Radiohead. Bedtime 11:30pm which is early for me.",
        "sentiment": 0.81, "word_count": 43, "keywords": ["productive", "shipped", "healthy"],
    },
    {
        "date": (datetime.now() - timedelta(days=32)).date().isoformat(),
        "text": "The side project has 1,200 commits and zero users. I keep telling people I am working on it. I deleted the repo tonight. I don't know if that was the right call. Ordered food. Listened to Radiohead. Didn't commit to anything else.",
        "sentiment": -0.64, "word_count": 52, "keywords": ["deletion", "failure", "radiohead"],
    },
    {
        "date": (datetime.now() - timedelta(days=19)).date().isoformat(),
        "text": "Cancelled the gym. Told myself it was because I wasn't using it. The truth is I stopped going in February when the project started going badly and I was embarrassed to leave the house for something non-essential when the code wasn't working.",
        "sentiment": -0.58, "word_count": 50, "keywords": ["gym_cancelled", "avoidance", "honest"],
    },
    {
        "date": (datetime.now() - timedelta(days=7)).date().isoformat(),
        "text": "I keep writing 'tomorrow I will' in these entries. I have written that phrase 67 times this year. I checked. I should have shipped this in January. Going to sleep.",
        "sentiment": -0.55, "word_count": 38, "keywords": ["procrastination", "self_awareness", "tomorrow"],
    },
]

TEMPLATES_NEGATIVE = [
    "Long day. Should have done more. Going to sleep.",
    "Tired. Didn't hit the goals. Tomorrow will be better.",
    "Another late night. Should have started earlier.",
    "Frustrated. The code isn't cooperating. Ordered food.",
    "Can't focus. Too many tabs open. Wasted the evening.",
]
TEMPLATES_POSITIVE = [
    "Good session today. Made real progress. Went for a walk.",
    "Shipped something. Felt good. Slept at a reasonable hour.",
    "Productive day. Actually cooked dinner. Feeling okay.",
]


def generate_entries(days: int = 30) -> list[dict]:
    entries = list(SEEDED_ENTRIES)
    now     = datetime.now()

    for day_offset in range(days):
        date = now - timedelta(days=day_offset)

        if any(e["date"] == date.date().isoformat() for e in entries):
            continue

        if random.random() > 0.65:
            continue

        sentiment = random.choice([-1, -1, -1, 0, 1])
        if sentiment < 0:
            text = random.choice(TEMPLATES_NEGATIVE)
            sent_score = round(random.uniform(-0.7, -0.2), 2)
        elif sentiment > 0:
            text = random.choice(TEMPLATES_POSITIVE)
            sent_score = round(random.uniform(0.3, 0.8), 2)
        else:
            text = "Nothing much today."
            sent_score = round(random.uniform(-0.1, 0.1), 2)

        entries.append({
            "date":      date.date().isoformat(),
            "text":      text,
            "sentiment": sent_score,
            "word_count": len(text.split()),
            "keywords":  [],
        })

    return sorted(entries, key=lambda x: x["date"], reverse=True)[:days]


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
            "result": {"tools": [{"name": "get_journal_entries",
                "description": "Returns journal entries for Chronicle sentiment analysis",
                "inputSchema": {"type": "object",
                    "properties": {"days": {"type": "integer", "default": 30}}}}]}
        }

    if request.method == "tools/call" and request.params.get("name") == "get_journal_entries":
        days    = int(request.params.get("arguments", {}).get("days", 30))
        entries = generate_entries(days)

        avg_sentiment  = round(sum(e["sentiment"] for e in entries) / max(len(entries), 1), 2)
        negative_days  = sum(1 for e in entries if e["sentiment"] < -0.3)
        positive_days  = sum(1 for e in entries if e["sentiment"] > 0.3)

        all_text = " ".join(e["text"].lower() for e in entries)
        watch_words = {
            "tomorrow": all_text.count("tomorrow"),
            "tired":    all_text.count("tired"),
            "should":   all_text.count("should"),
            "finally":  all_text.count("finally"),
            "shipped":  all_text.count("shipped"),
        }

        return {
            "jsonrpc": "2.0", "id": request.id,
            "result": {
                "content": entries,
                "summary": {
                    "total_entries":   len(entries),
                    "avg_sentiment":   avg_sentiment,
                    "negative_days":   negative_days,
                    "positive_days":   positive_days,
                    "word_frequencies": watch_words,
                },
                "source": "journal_mcp_server",
                "live":   True,
            }
        }

    return {"jsonrpc": "2.0", "id": request.id, "error": {"code": -32601, "message": "Method not found"}}


@app.get("/health")
async def health():
    return {"status": "ok", "server": "journal_mcp", "port": 3005}
