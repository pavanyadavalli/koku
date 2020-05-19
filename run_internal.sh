#!/bin/sh
sleep 15
# python koku/manage.py migrate_schemas
DJANGO_READ_DOT_ENV_FILE=True gunicorn 'koku.wsgi:application' --bind=0.0.0.0:8080 --config gunicorn.py
