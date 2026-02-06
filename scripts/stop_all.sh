#!/bin/bash
set -e

# Change to the directory of the script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$DIR/.."

echo "🛑 Stopping Fabric Monitoring System..."

# 1. Stop Docker Containers
if [ -f "$PROJECT_ROOT/lineage_explorer/docker-compose.yml" ]; then
    echo "🐳 Stopping Docker containers..."
    cd "$PROJECT_ROOT/lineage_explorer"
    docker compose down
    cd "$DIR"
else
    echo "⚠️  docker-compose.yml not found in lineage_explorer/"
fi

# 2. Kill Python Server (server.py or run_lineage_explorer.py)
echo "🐍 Stopping Python server(s)..."
# Kill by known script names
pids=$(pgrep -f "server.py|run_lineage_explorer.py")
if [ -n "$pids" ]; then
    echo "Found server processes: $pids"
    kill $pids 2>/dev/null || true
    echo "Killed python server processes"
fi

# Kill by port 8000 (just in case)
port_pid=$(lsof -t -i:8000)
if [ -n "$port_pid" ]; then
    echo "Found process on port 8000: $port_pid"
    kill $port_pid 2>/dev/null || true
    echo "Killed process on port 8000"
fi

# 3. Kill make process (make lineage-explorer)
make_pids=$(pgrep -f "make lineage-explorer")
if [ -n "$make_pids" ]; then
    echo "Found make lineage-explorer processes: $make_pids"
    kill $make_pids
    echo "Killed make process"
else
    echo "No make lineage-explorer process found."
fi

echo "✅ All systems stopped."
