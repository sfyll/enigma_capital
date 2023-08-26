#!/usr/bin/env zsh

case "$1" in
    -d|--daemon)
        $0 < /dev/null &> /dev/null & disown
        exit 0
        ;;
    *)
        ;;
esac

cd $ENIGMA

source env/bin/activate

python3 -m account_data_fetcher.launcher.runner --log-file ~/log/runner.log --seconds 0 -vvv -q --exchanges BYBIT DYDX Ethereum IB_flex Kraken Rsk --writers csv #TradeStation