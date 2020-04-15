#!/bin/sh
DJANGO_READ_DOT_ENV_FILE=True RUNNING_MIGRATIONS=True python koku/manage.py migrate_schemas
