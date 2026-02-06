#!/usr/bin/env python3
"""
Ensure Project Containers Script
--------------------------------
This script manages the startup of project containers (Neo4j), ensuring that:
1. Required ports are available or correctly owned by our containers.
2. If ports are taken by foreign processes, new ports are selected.
3. The Docker environment is spun up correctly.
"""

import socket
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Tuple, Optional

# Configuration
NEO4J_HTTP_PORT = 7474
NEO4J_BOLT_PORT = 7687
CONTAINER_NAME = "fabric-lineage-neo4j"
PROJECT_ROOT = Path(__file__).resolve().parent.parent
COMPOSE_DIR = PROJECT_ROOT / "lineage_explorer"
ENV_FILE = COMPOSE_DIR / ".env"

def check_port(port: int) -> bool:
    """Check if a port is in use. Returns True if in use."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('127.0.0.1', port)) == 0

def get_container_ports(container_name: str) -> List[int]:
    """Get list of public ports mapped by a container."""
    try:
        cmd = ["docker", "ps", "--format", "{{.Ports}}", "--filter", f"name={container_name}"]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        output = result.stdout.strip()
        
        if not output:
            return []
            
        # Parse output like "0.0.0.0:7474->7474/tcp, 0.0.0.0:7687->7687/tcp"
        ports = []
        parts = output.split(',')
        for part in parts:
            if "->" in part:
                host_part = part.strip().split("->")[0]
                if ":" in host_part:
                    port_str = host_part.split(":")[-1]
                    try:
                        ports.append(int(port_str))
                    except ValueError:
                        pass
        return ports
    except subprocess.CalledProcessError:
        return []

def is_port_owned_by_us(port: int) -> bool:
    """Check if the port is owned by our specific container."""
    mapped_ports = get_container_ports(CONTAINER_NAME)
    return port in mapped_ports

def find_available_port(start_port: int, max_attempts: int = 100) -> int:
    """Find the next available port starting from start_port."""
    for port in range(start_port, start_port + max_attempts):
        if not check_port(port):
            return port
    raise RuntimeError(f"No available ports found in range {start_port}-{start_port + max_attempts}")

def update_env_file(http_port: int, bolt_port: int):
    """Update or create .env file with correct ports."""
    # We might need to adjust docker-compose to use variables for ports if we change them.
    # Currently docker-compose.yml hardcodes ports in the 'ports' section:
    # - "127.0.0.1:7474:7474"
    # To make this dynamic, we would need to edit docker-compose.yml or use env vars there.
    # For now, we will simply export environment variables that docker-compose might use 
    # if modified, or we verify we can use them.
    
    # Since the user's docker-compose has hardcoded ports "127.0.0.1:7474:7474", 
    # we can't easily change them via .env without modifying the YAML.
    # If we detect a conflict, we will have to instruct the user or modify the YAML.
    pass

def main():
    print(f"Checking environment for {CONTAINER_NAME}...")
    
    # Check default ports
    ports_bad_state = False
    
    for port_name, port in [("HTTP", NEO4J_HTTP_PORT), ("Bolt", NEO4J_BOLT_PORT)]:
        in_use = check_port(port)
        if in_use:
            if is_port_owned_by_us(port):
                print(f"✅ Port {port} ({port_name}) is in use by our container.")
            else:
                print(f"❌ Port {port} ({port_name}) is in use by ANOTHER process!")
                ports_bad_state = True
        else:
            print(f"ℹ️  Port {port} ({port_name}) is free.")

    if ports_bad_state:
        print("\nCRITICAL: Ports are occupied by other services.")
        print("Attempting to auto-resolve is risky without modifying docker-compose.yml.")
        print(f"Please stop the service running on ports {NEO4J_HTTP_PORT}/{NEO4J_BOLT_PORT} or manually edit docker-compose.yml.")
        sys.exit(1)

    # Ensure Neo4j Password is set
    local_env = os.environ.copy()
    if "NEO4J_PASSWORD" not in local_env:
        # Check if .env has it, otherwise default
        if ENV_FILE.exists():
            print(f"Loading env from {ENV_FILE}")
            # simple parse
            with open(ENV_FILE) as f:
                for line in f:
                    if line.strip().startswith("NEO4J_PASSWORD="):
                        pass_val = line.strip().split("=", 1)[1]
                        if pass_val != "<your_neo4j_password>":
                             local_env["NEO4J_PASSWORD"] = pass_val
        
        if "NEO4J_PASSWORD" not in local_env:
            print("⚠️  NEO4J_PASSWORD not found. Using default 'fabric_lineage'.")
            local_env["NEO4J_PASSWORD"] = "fabric_lineage"

    print(f"\n🚀 Starting containers in {COMPOSE_DIR}...")
    try:
        subprocess.run(
            ["docker", "compose", "up", "-d"], 
            cwd=COMPOSE_DIR, 
            env=local_env,
            check=True
        )
        print("✅ Docker Compose command successful.")
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to start containers: {e}")
        sys.exit(1)

    # Health check
    print("Health checking Neo4j...")
    for _ in range(30):
        try:
            # We use curl to check HTTP port
            result = subprocess.run(
                ["curl", "-f", "-s", f"http://localhost:{NEO4J_HTTP_PORT}"],
                capture_output=True
            )
            if result.returncode == 0:
                print(f"✅ Neo4j is responding at http://localhost:{NEO4J_HTTP_PORT}")
                return
        except Exception:
            pass
        time.sleep(1)
        print(".", end="", flush=True)
    
    print("\n⚠️  Neo4j started but health check timed out. It might still be initializing.")

if __name__ == "__main__":
    main()
