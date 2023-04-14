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

python3 -m account_data_fetcher.runner --log-file ~/log/balance_and_position_fetcher.log --seconds 1800 --request-type FLEX -v -q --manual-balance true --exchange Binance BYBIT DYDX Ethereum IB TradeStation Kraken Rootstock