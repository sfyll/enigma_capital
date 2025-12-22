#!/usr/bin/env zsh

# Find project root by locating pyproject.toml
SCRIPT_DIR="$(cd "$(dirname "${(%):-%x}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"
while [[ "$PROJECT_ROOT" != "/" ]]; do
    if [[ -f "$PROJECT_ROOT/pyproject.toml" ]]; then
        break
    fi
    PROJECT_ROOT="$(dirname "$PROJECT_ROOT")"
done

if [[ ! -f "$PROJECT_ROOT/pyproject.toml" ]]; then
    echo "Error: Could not find project root (pyproject.toml not found)"
    exit 1
fi

# Path to the JSON file
JSON_PATH="$PROJECT_ROOT/account_data_fetcher/config/port_number_pairing.json"

kill_process_on_port() {
  local port=$1
  # Use `awk` to process each line from `lsof` output separately
  sudo lsof -iTCP -sTCP:LISTEN -n -P | grep ":$port" | awk '{print $2}' | while read -r pid; do
    if [ -n "$pid" ]; then
      echo "Killing process with PID $pid on port $port"
      sudo kill -9 "$pid"
    else
      echo "No process found listening on port $port"
    fi
  done
}

# Extract port numbers using grep and awk, and handle multiple ports per line
ports=$(grep -Eo '"[^"]+": [0-9]+' "$JSON_PATH" | awk '{print $2}')

for port in $ports; do
  kill_process_on_port $port
done

