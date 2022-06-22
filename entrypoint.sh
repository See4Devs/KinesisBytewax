#!/bin/sh

/venv/bin/python /bytewax/app.py

echo 'Process ended.'

if [ true ]
then
    echo 'Keeping container alive...';
    while :; do sleep 1; done
fi
