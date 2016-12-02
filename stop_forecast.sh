#! /bin/bash

ps -ef | grep forecast_server.py | grep -v grep | awk '{print $2}' | xargs kill
