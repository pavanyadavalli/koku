#!/bin/sh
sleep 5
python manage.py migrate_schemas
#mkdir -p /testing/profile/
#python manage.py runprofileserver 0.0.0.0:8080 --use-cprofile --prof-path=/koku/koku
#DJANGO_READ_DOT_ENV_FILE=True python koku/manage.py runserver 0.0.0.0:8000
DJANGO_READ_DOT_ENV_FILE=True gunicorn 'koku.wsgi:application' --bind=0.0.0.0:8080 --config gunicorn.py
