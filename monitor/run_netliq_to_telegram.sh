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

python3 -m monitor.runner --log-file ~/log/netliq_to_telegram.log --seconds 3600 --request-type NETLIQ -v -q
