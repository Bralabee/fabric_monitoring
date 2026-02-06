#!/bin/bash
set -e

# Change to the directory of the script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$DIR/.."

echo "🚀 Starting Fabric Monitoring System..."

# 1. Start Docker Containers (Neo4j)
if [ -f "$PROJECT_ROOT/lineage_explorer/docker-compose.yml" ]; then
    echo "🐳 Starting Neo4j container..."
    cd "$PROJECT_ROOT/lineage_explorer"
    docker compose up -d
    cd "$PROJECT_ROOT"
    
    # Wait for Neo4j to be healthy
    echo "⏳ Waiting for Neo4j to be ready..."
    sleep 5
    
    # Check container status
    status=$(docker ps --filter "name=fabric-lineage-neo4j" --format "{{.Status}}")
    echo "   Neo4j status: $status"
else
    echo "⚠️  docker-compose.yml not found in lineage_explorer/"
fi

# 2. Start Python Server (Lineage Explorer API)
echo "🐍 Starting Lineage Explorer API..."
cd "$PROJECT_ROOT"

# Check if already running
if lsof -t -i:8000 > /dev/null 2>&1; then
    echo "⚠️  Port 8000 already in use. Server may already be running."
    echo "   Use 'scripts/stop_all.sh' to stop first, or access http://127.0.0.1:8000"
else
    # Start the server in the background
    conda run --no-capture-output -n fabric-monitoring python scripts/run_lineage_explorer.py &
    
    # Wait for it to start
    sleep 3
    
    if lsof -t -i:8000 > /dev/null 2>&1; then
        echo "✅ Lineage Explorer API started on http://127.0.0.1:8000"
    else
        echo "⚠️  Server may have failed to start. Check logs."
    fi
fi

echo ""
echo "📍 Services:"
echo "   • Lineage Explorer: http://127.0.0.1:8000"
echo "   • Neo4j Browser:    http://127.0.0.1:7474"
echo ""
echo "✅ System started. Use 'scripts/stop_all.sh' to stop all services."
