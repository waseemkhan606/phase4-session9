#!/bin/bash
# Start all 5 Chronicle MCP servers in the background
# Run from the chronicle/ directory: bash mcp_servers/start_all.sh

echo "Starting Chronicle MCP servers..."

uvicorn mcp_servers.spotify_server:app  --port 3001 --log-level warning &
uvicorn mcp_servers.finance_server:app  --port 3002 --log-level warning &
uvicorn mcp_servers.fitness_server:app  --port 3003 --log-level warning &
uvicorn mcp_servers.github_server:app   --port 3004 --log-level warning &
uvicorn mcp_servers.journal_server:app  --port 3005 --log-level warning &

sleep 2

echo ""
echo "MCP server health checks:"
for port in 3001 3002 3003 3004 3005; do
    status=$(curl -s http://localhost:$port/health | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['server'])" 2>/dev/null)
    if [ -n "$status" ]; then
        echo "  ✓ Port $port — $status"
    else
        echo "  ✗ Port $port — not responding"
    fi
done

echo ""
echo "All MCP servers running. Test one:"
echo "  curl -X POST http://localhost:3001/mcp \\"
echo "    -H 'Content-Type: application/json' \\"
echo "    -d '{\"jsonrpc\":\"2.0\",\"method\":\"tools/call\",\"params\":{\"name\":\"get_spotify_history\",\"arguments\":{\"days\":7}},\"id\":1}'"
echo ""
echo "Stop all: kill \$(lsof -ti:3001,3002,3003,3004,3005)"
