#!/usr/bin/env zsh

case "$1" in
    -d|--daemon)
        $0 < /dev/null &> /dev/null & disown
        exit 0
        ;;
    *)
        ;;
esac

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

cd "$PROJECT_ROOT"

poetry run python -m monitor.runner --log-file ~/log/uranium_thesis_monitoring.log --seconds 259200 --request-type URANIUM -v -q
