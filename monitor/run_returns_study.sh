#!/usr/bin/env bash

# Determine the script's directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Define a function to find the project root
find_project_root() {
    local dir="$1"
    while [ "$dir" != "/" ]; do
        if [ -d "$dir/env" ]; then
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
    echo "Project root not found. Please ensure the virtual environment is properly set up."
    exit 1
fi

# Define the virtual environment directory
VENV_DIR="$PROJECT_ROOT/env"

# Check if the virtual environment exists
if [ ! -d "$VENV_DIR" ]; then
    echo "Virtual environment not found in $VENV_DIR. Please set it up first."
    exit 1
fi

# Activate the virtual environment
source "$VENV_DIR/bin/activate"

# Navigate to the portfolio_monitor directory if not already there
if [ "$(pwd)" != "$PROJECT_ROOT/monitor/portfolio_monitor" ]; then
    cd "$PROJECT_ROOT/monitor/portfolio_monitor" || exit
fi

# Run the Python script
python3 returns_study.py "$@"

# Deactivate the virtual environment
deactivate

