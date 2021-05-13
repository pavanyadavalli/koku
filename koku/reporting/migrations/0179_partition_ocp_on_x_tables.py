# Generated by Django 3.1.10 on 2021-05-13 16:46
from django.db import migrations

from koku import migration_sql_helpers as msh
from koku import pg_partition as ppart


def update_partitioned_tables_trigger(apps, schema_editor):
    path = msh.find_db_functions_dir()
    for funcfile in "partitioned_tables_manage_trigger.sql":
        msh.apply_sql_file(schema_editor, os.path.join(path, funcfile))


def convert_table_to_partitioned(apps, schema_editor, table_name):
    # Resolve the current schema name
    target_schema = ppart.resolve_schema(ppart.CURRENT_SCHEMA)
    # This is the table we will model from
    source_table = table_name
    # This is the target table's name (it will be renamed during the conversion to the source table name)
    target_table = f"p_{source_table}"

    # We want to change the target tables's 'id' column default
    target_identity_col = ppart.ColumnDefinition(
        target_schema, target_table, "uuid", default=ppart.Default("uuid_generate_v4()")
    )
    # We also need to include the identity col as part of the primary key definition
    new_pk = ppart.PKDefinition(f"{target_table}_pkey", ["usage_start", "uuid"])

    # Init the converter
    p_converter = ppart.ConvertToPartition(
        source_table,
        "usage_start",
        target_table_name=target_table,
        partition_type=ppart.PARTITION_RANGE,
        pk_def=new_pk,
        col_def=[target_identity_col],
        target_schema=target_schema,
        source_schema=target_schema,
    )

    # Push the button, Frank.
    p_converter.convert_to_partition()


def convert_ocpaws_table_to_partitioned(apps, schema_editor):
    source_table = "reporting_ocpawscostlineitem_daily_summary"
    convert_table_to_partitioned(apps, schema_editor, source_table)


def convert_ocpaws_project_table_to_partitioned(apps, schema_editor):
    source_table = "reporting_ocpawscostlineitem_project_daily_summary"
    convert_table_to_partitioned(apps, schema_editor, source_table)


def convert_ocpazure_table_to_partitioned(apps, schema_editor):
    source_table = "reporting_ocpazurecostlineitem_daily_summary"
    convert_table_to_partitioned(apps, schema_editor, source_table)


def convert_ocpazure_project_table_to_partitioned(apps, schema_editor):
    source_table = "reporting_ocpazurecostlineitem_project_daily_summary"
    convert_table_to_partitioned(apps, schema_editor, source_table)


class Migration(migrations.Migration):

    dependencies = [("reporting", "0178_auto_20210511_1851")]

    operations = [
        migrations.RunSQL("alter table partitioned_tables drop trigger tr_attach_date_range_partition ;"),
        migrations.AlterModelOptions(name="ocpawscostlineitemdailysummary", options={"managed": False}),
        migrations.AlterModelOptions(name="ocpawscostlineitemprojectdailysummary", options={"managed": False}),
        migrations.AlterModelOptions(name="ocpawscostlineitemdailysummary", options={"managed": False}),
        migrations.AlterModelOptions(name="ocpazurecostlineitemprojectdailysummary", options={"managed": False}),
        migrations.RunPython(code=convert_ocpaws_table_to_partitioned),
        migrations.RunPython(code=convert_ocpaws_project_table_to_partitioned),
        migrations.RunPython(code=convert_ocpazure_table_to_partitioned),
        migrations.RunPython(code=convert_ocpazure_project_table_to_partitioned),
    ]
