# Generated by Django 3.1.7 on 2021-03-18 15:14
from django.db import migrations
from django.db import models


class Migration(migrations.Migration):

    dependencies = [("cost_models", "0001_initial")]

    operations = [
        migrations.AlterField(
            model_name="costmodel",
            name="source_type",
            field=models.CharField(
                choices=[
                    ("AWS", "AWS"),
                    ("OCP", "OCP"),
                    ("Azure", "Azure"),
                    ("GCP", "GCP"),
                    ("IBM", "IBM"),
                    ("AWS-local", "AWS-local"),
                    ("Azure-local", "Azure-local"),
                    ("GCP-local", "GCP-local"),
                    ("IBM-local", "IBM-local"),
                ],
                max_length=50,
            ),
        ),
        migrations.AlterField(
            model_name="costmodelaudit",
            name="source_type",
            field=models.CharField(
                choices=[
                    ("AWS", "AWS"),
                    ("OCP", "OCP"),
                    ("Azure", "Azure"),
                    ("GCP", "GCP"),
                    ("IBM", "IBM"),
                    ("AWS-local", "AWS-local"),
                    ("Azure-local", "Azure-local"),
                    ("GCP-local", "GCP-local"),
                    ("IBM-local", "IBM-local"),
                ],
                max_length=50,
            ),
        ),
    ]
