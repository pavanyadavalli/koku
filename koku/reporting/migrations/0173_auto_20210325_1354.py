# Generated by Django 3.1.7 on 2021-03-25 13:54
import pkgutil

from django.db import connection
from django.db import migrations


def add_gcp_storage_views(apps, schema_editor):
    """Create the GCP Materialized views from files."""

    for view in ("", "_by_project", "_by_account", "_by_service", "_by_region"):
        view_sql = pkgutil.get_data("reporting.provider.gcp", f"sql/views/reporting_gcp_storage_summary{view}.sql")
        view_sql = view_sql.decode("utf-8")
        with connection.cursor() as cursor:
            cursor.execute(view_sql)


class Migration(migrations.Migration):

    dependencies = [("reporting", "0172_auto_20210318_1514")]

    operations = [migrations.RunPython(add_gcp_storage_views)]
