#!/bin/sh
sleep 15
DJANGO_READ_DOT_ENV_FILE=True python koku/manage.py runserver 0.0.0.0:8000
