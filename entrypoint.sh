#!/bin/sh

set -e # exit inmediately on command execution failure
set -x # show commands when executing

CRAWLER_SCRIPT='/srv/mwmbl/crawler-script'

is_positive_integer ()
{
	VALUE="$1"

	if [ "$VALUE" -gt 0 2> /dev/null ]
	then
		return 0
	else
		echo "The threads variable must contain a positive integer."
		return 1
	fi
}

if [ ! -n "${THREADS}" ]
then
	THREADS=1
else
	if ! is_positive_integer "$THREADS"
	then
		exit 1
	fi
fi

. "$CRAWLER_SCRIPT"/venv/bin/activate
exec python "$CRAWLER_SCRIPT"/main.py -j "$THREADS"
