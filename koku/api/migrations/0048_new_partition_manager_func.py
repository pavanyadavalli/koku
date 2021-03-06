# Generated by Django 3.1.10 on 2021-05-17 19:48
import os

from django.db import migrations

from koku import migration_sql_helpers as msh


def apply_public_func_updates(apps, schema_editor):
    path = msh.find_db_functions_dir()
    for funcfile in (
        # trigger func is the same as original apply (interface not changed) but does not have the drop in the file.
        "partitioned_manager_trigger_function.sql",
    ):
        msh.apply_sql_file(schema_editor, os.path.join(path, funcfile), literal_placeholder=True)


class Migration(migrations.Migration):

    dependencies = [("api", "0047_update_django_migration_sequences")]

    operations = [migrations.RunPython(code=apply_public_func_updates)]
