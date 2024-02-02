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

python3 -m account_data_fetcher.launcher.runner --log-file ~/log/account_data_fetcher.log --seconds 86400 -v -q --exchanges Binance BYBIT Ethereum TradeStation IB_flex Kraken Rsk KUCOIN --writers csv gsheet
