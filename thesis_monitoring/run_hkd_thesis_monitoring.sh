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

python3 -m thesis_monitoring.runner --log-file ~/log/hkd_thesis_monitoring.log --seconds 604800 --request-type HKD -v -q --starting-date 2022-01-01
