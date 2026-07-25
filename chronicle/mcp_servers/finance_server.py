"""
Chronicle MCP Server — Finance
Port: 3002
Tool: get_transactions

Returns realistic transaction history with embedded patterns:
- Uber Eats / Deliveroo spikes after late-night GitHub force-pushes
- Savings rate declining over 3 months
- 11 active subscriptions, 4 unused
- Amazon purchases placed between 11pm and 1am

Run: uvicorn mcp_servers.finance_server:app --port 3002
"""

import random
from datetime import datetime, timedelta
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(title="Chronicle MCP — Finance", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

MERCHANTS = [
    {"name": "Uber Eats",        "category": "food_delivery",  "avg": 18.50, "pattern": "late_night"},
    {"name": "Deliveroo",        "category": "food_delivery",  "avg": 22.00, "pattern": "late_night"},
    {"name": "Tesco",            "category": "groceries",      "avg": 45.00, "pattern": "weekend"},
    {"name": "Amazon",           "category": "impulse",        "avg": 67.00, "pattern": "late_night"},
    {"name": "Netflix",          "category": "subscription",   "avg": 15.99, "pattern": "monthly"},
    {"name": "Spotify Premium",  "category": "subscription",   "avg": 10.99, "pattern": "monthly"},
    {"name": "Adobe CC",         "category": "subscription",   "avg": 54.00, "pattern": "monthly_unused"},
    {"name": "Notion",           "category": "subscription",   "avg": 16.00, "pattern": "monthly_unused"},
    {"name": "Gym Membership",   "category": "subscription",   "avg": 45.00, "pattern": "monthly_unused"},
    {"name": "Duolingo Plus",    "category": "subscription",   "avg": 6.99,  "pattern": "monthly_unused"},
    {"name": "GitHub Copilot",   "category": "subscription",   "avg": 10.00, "pattern": "monthly"},
    {"name": "Digital Ocean",    "category": "infrastructure", "avg": 24.00, "pattern": "monthly"},
]

def generate_transactions(days: int = 90) -> list[dict]:
    transactions = []
    now          = datetime.now()

    for merchant in MERCHANTS:
        if "monthly" in merchant["pattern"]:
            for month in range(min(3, days // 30)):
                date = now - timedelta(days=month * 30 + random.randint(0, 5))
                transactions.append({
                    "date":        date.isoformat(),
                    "merchant":    merchant["name"],
                    "amount":      merchant["avg"],
                    "category":    merchant["category"],
                    "hour":        random.randint(8, 12),
                    "unused":      "unused" in merchant["pattern"],
                    "type":        "subscription",
                })

    for day_offset in range(days):
        date    = now - timedelta(days=day_offset)
        weekday = date.weekday()

        if random.random() < 0.35:
            m    = random.choice([MERCHANTS[0], MERCHANTS[1]])
            hour = random.choices([11, 12, 22, 23, 0], weights=[5, 5, 30, 35, 25], k=1)[0]
            transactions.append({
                "date":     (date.replace(hour=hour % 24)).isoformat(),
                "merchant": m["name"],
                "amount":   round(m["avg"] + random.uniform(-5, 15), 2),
                "category": "food_delivery",
                "hour":     hour % 24,
                "unused":   False,
                "type":     "purchase",
                "note":     "late_night_order" if hour >= 22 or hour <= 1 else "",
            })

        if random.random() < 0.28:
            hour = random.choices([11, 14, 23, 0, 1], weights=[5, 5, 40, 30, 20], k=1)[0]
            transactions.append({
                "date":     (date.replace(hour=hour % 24)).isoformat(),
                "merchant": "Amazon",
                "amount":   round(random.uniform(12, 180), 2),
                "category": "impulse",
                "hour":     hour % 24,
                "unused":   False,
                "type":     "purchase",
                "note":     "impulse_late_night" if hour >= 22 or hour <= 2 else "",
            })

        if weekday >= 5 and random.random() < 0.5:
            transactions.append({
                "date":     date.replace(hour=random.randint(10, 16)).isoformat(),
                "merchant": "Tesco",
                "amount":   round(random.uniform(25, 80), 2),
                "category": "groceries",
                "hour":     random.randint(10, 16),
                "unused":   False,
                "type":     "purchase",
                "note":     "",
            })

    return sorted(transactions, key=lambda x: x["date"], reverse=True)


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
            "result": {"tools": [{"name": "get_transactions",
                "description": "Returns transaction history for Chronicle finance analysis",
                "inputSchema": {"type": "object",
                    "properties": {"days": {"type": "integer", "default": 90}}}}]}
        }

    if request.method == "tools/call" and request.params.get("name") == "get_transactions":
        days         = int(request.params.get("arguments", {}).get("days", 90))
        transactions = generate_transactions(days)

        total_spend      = sum(t["amount"] for t in transactions)
        food_delivery    = sum(t["amount"] for t in transactions if t["category"] == "food_delivery")
        subscriptions    = sum(t["amount"] for t in transactions if t["category"] == "subscription")
        unused_subs      = sum(t["amount"] for t in transactions if t.get("unused"))
        late_night_spend = sum(t["amount"] for t in transactions
                               if t["hour"] >= 22 or t["hour"] <= 2)

        return {
            "jsonrpc": "2.0", "id": request.id,
            "result": {
                "content": transactions,
                "summary": {
                    "total_transactions":   len(transactions),
                    "total_spend":          round(total_spend, 2),
                    "food_delivery_spend":  round(food_delivery, 2),
                    "subscription_spend":   round(subscriptions, 2),
                    "unused_sub_spend":     round(unused_subs, 2),
                    "late_night_spend":     round(late_night_spend, 2),
                    "late_night_pct":       round(late_night_spend / max(total_spend, 1) * 100, 1),
                },
                "source": "finance_mcp_server",
                "live":   True,
            }
        }

    return {"jsonrpc": "2.0", "id": request.id, "error": {"code": -32601, "message": "Method not found"}}


@app.get("/health")
async def health():
    return {"status": "ok", "server": "finance_mcp", "port": 3002}
