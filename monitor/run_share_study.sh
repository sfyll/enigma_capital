#!/usr/bin/env bash

# Determine the script's directory (works in bash, zsh if explicitly invoked)
if [ -n "$BASH_VERSION" ]; then
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
elif [ -n "$ZSH_VERSION" ]; then
    SCRIPT_DIR="$(cd "$(dirname "${(%):-%x}")" && pwd)"
else
    SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
fi

# Define a function to find the project root
find_project_root() {
    local dir="$1"
    while [ "$dir" != "/" ]; do
        if [ -f "$dir/pyproject.toml" ]; then
            echo "$dir"
            return
        fi
        dir="$(dirname "$dir")"
    done
    echo ""
}

# Find the project root based on the current or script directory
PROJECT_ROOT=$(find_project_root "$SCRIPT_DIR")

if [ -z "$PROJECT_ROOT" ]; then
    echo "Project root not found. Please ensure pyproject.toml exists."
    exit 1
fi

# Navigate to the portfolio_monitor directory if not already there
if [ "$(pwd)" != "$PROJECT_ROOT/monitor/portfolio_monitor" ]; then
    cd "$PROJECT_ROOT/monitor/portfolio_monitor" || exit
fi

# Run the Python script using Poetry
cd "$PROJECT_ROOT" && poetry run python monitor/portfolio_monitor/share_study.py "$@"


