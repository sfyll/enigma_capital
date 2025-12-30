#!/usr/bin/env bash

case "$1" in
    -d|--daemon)
        $0 < /dev/null &> /dev/null & disown
        exit 0
        ;;
    *)
        ;;
esac

# Find project root by locating pyproject.toml
# Shell-agnostic way to get script directory (works in bash, zsh if explicitly invoked)
if [ -n "$BASH_VERSION" ]; then
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
elif [ -n "$ZSH_VERSION" ]; then
    SCRIPT_DIR="$(cd "$(dirname "${(%):-%x}")" && pwd)"
else
    SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
fi

PROJECT_ROOT="$SCRIPT_DIR"
while [ "$PROJECT_ROOT" != "/" ]; do
    if [ -f "$PROJECT_ROOT/pyproject.toml" ]; then
        break
    fi
    PROJECT_ROOT="$(dirname "$PROJECT_ROOT")"
done

if [ ! -f "$PROJECT_ROOT/pyproject.toml" ]; then
    echo "Error: Could not find project root (pyproject.toml not found)"
    exit 1
fi

cd "$PROJECT_ROOT"

oetry run python -m account_data_fetcher.launcher.runner --log-file ~/log/account_data_fetcher.log --seconds 86400 -v -q --exchanges Binance Ethereum IB_flex Kraken kucoin hyperliquid --writers csv gsheet
