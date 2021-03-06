#
# Copyright 2021 Red Hat Inc.
# SPDX-License-Identifier: Apache-2.0
#
"""Models for OCP on AWS tables."""
from django.contrib.postgres.fields import ArrayField
from django.contrib.postgres.indexes import GinIndex
from django.db import models
from django.db.models import JSONField

VIEWS = (
    "reporting_ocpallcostlineitem_daily_summary",
    "reporting_ocpallcostlineitem_project_daily_summary",
    "reporting_ocpall_compute_summary",
    "reporting_ocpall_storage_summary",
    "reporting_ocpall_cost_summary",
    "reporting_ocpall_cost_summary_by_account",
    "reporting_ocpall_cost_summary_by_region",
    "reporting_ocpall_cost_summary_by_service",
    "reporting_ocpall_database_summary",
    "reporting_ocpall_network_summary",
)


class OCPAllCostLineItemDailySummary(models.Model):
    """A summarized view of OCP on All infrastructure cost."""

    class Meta:
        """Meta for OCPAllCostLineItemDailySummary."""

        db_table = "reporting_ocpallcostlineitem_daily_summary"
        managed = False

        indexes = [
            models.Index(fields=["usage_start"], name="ocpall_usage_idx"),
            models.Index(fields=["namespace"], name="ocpall_namespace_idx"),
            models.Index(fields=["node"], name="ocpall_node_idx", opclasses=["varchar_pattern_ops"]),
            models.Index(fields=["resource_id"], name="ocpall_resource_idx"),
            GinIndex(fields=["tags"], name="ocpall_tags_idx"),
            models.Index(fields=["product_family"], name="ocpall_product_family_idx"),
            models.Index(fields=["instance_type"], name="ocpall_instance_type_idx"),
            # A GIN functional index named "ocpall_product_code_ilike" was created manually
            # via RunSQL migration operation
            # Function: (upper(product_code) gin_trgm_ops)
            # A GIN functional index named "ocpall_product_family_ilike" was created manually
            # via RunSQL migration operation
            # Function: (upper(product_family) gin_trgm_ops)
        ]

    id = models.IntegerField(primary_key=True)

    # The infrastructure provider type
    source_type = models.TextField()

    # OCP Fields
    cluster_id = models.CharField(max_length=50, null=True)

    cluster_alias = models.CharField(max_length=256, null=True)

    # Kubernetes objects by convention have a max name length of 253 chars
    namespace = ArrayField(models.CharField(max_length=253, null=False))

    node = models.CharField(max_length=253, null=True)

    resource_id = models.CharField(max_length=253, null=True)

    usage_start = models.DateField(null=False)

    usage_end = models.DateField(null=False)

    # Infrastructure source fields
    usage_account_id = models.CharField(max_length=50, null=False)

    account_alias = models.ForeignKey("AWSAccountAlias", on_delete=models.SET_NULL, null=True)

    product_code = models.CharField(max_length=50, null=False)

    product_family = models.CharField(max_length=150, null=True)

    instance_type = models.CharField(max_length=50, null=True)

    region = models.CharField(max_length=50, null=True)

    availability_zone = models.CharField(max_length=50, null=True)

    tags = JSONField(null=True)

    usage_amount = models.DecimalField(max_digits=24, decimal_places=9, null=True)

    unit = models.CharField(max_length=63, null=True)

    # Cost breakdown can be done by cluster, node, project, and pod.
    # Cluster and node cost can be determined by summing the AWS unblended_cost
    # with a GROUP BY cluster/node.
    # Project cost is a summation of pod costs with a GROUP BY project
    # The cost of un-utilized resources = sum(unblended_cost) - sum(project_cost)
    unblended_cost = models.DecimalField(max_digits=30, decimal_places=15, null=True)

    markup_cost = models.DecimalField(max_digits=30, decimal_places=15, null=True)

    currency_code = models.CharField(max_length=10, null=True)

    # This is a count of the number of projects that share an AWS resource
    # It is used to divide cost evenly among projects
    shared_projects = models.IntegerField(null=False, default=1)

    source_uuid = models.UUIDField(unique=False, null=True)

    tags_hash = models.TextField(max_length=512)


# Materialized Views for UI Reporting
class OCPAllCostSummary(models.Model):
    """A MATERIALIZED VIEW specifically for UI API queries.
    This table gives a daily breakdown of total cost.
    """

    class Meta:
        """Meta for OCPAllCostSummary."""

        db_table = "reporting_ocpall_cost_summary"
        managed = False

    id = models.IntegerField(primary_key=True)

    usage_start = models.DateField(null=False)

    usage_end = models.DateField(null=False)

    cluster_id = models.CharField(max_length=50, null=True)

    cluster_alias = models.CharField(max_length=256, null=True)

    unblended_cost = models.DecimalField(max_digits=24, decimal_places=9, null=True)

    markup_cost = models.DecimalField(max_digits=24, decimal_places=9, null=True)

    currency_code = models.CharField(max_length=10)

    source_uuid = models.UUIDField(unique=False, null=True)


class OCPAllCostSummaryByAccount(models.Model):
    """A MATERIALIZED VIEW specifically for UI API queries.
    This table gives a daily breakdown of total cost by account.
    """

    class Meta:
        """Meta for OCPAllCostSummaryByAccount."""

        db_table = "reporting_ocpall_cost_summary_by_account"
        managed = False

    id = models.IntegerField(primary_key=True)

    usage_start = models.DateField(null=False)

    usage_end = models.DateField(null=False)

    cluster_id = models.CharField(max_length=50, null=True)

    cluster_alias = models.CharField(max_length=256, null=True)

    usage_account_id = models.CharField(max_length=50, null=False)

    account_alias = models.ForeignKey("AWSAccountAlias", on_delete=models.DO_NOTHING, null=True)

    unblended_cost = models.DecimalField(max_digits=24, decimal_places=9, null=True)

    markup_cost = models.DecimalField(max_digits=24, decimal_places=9, null=True)

    currency_code = models.CharField(max_length=10)

    source_uuid = models.UUIDField(unique=False, null=True)


class OCPAllCostSummaryByService(models.Model):
    """A MATERIALIZED VIEW specifically for UI API queries.
    This table gives a daily breakdown of total cost by account.
    """

    class Meta:
        """Meta for OCPAllCostSummaryByService."""

        db_table = "reporting_ocpall_cost_summary_by_service"
        managed = False

    id = models.IntegerField(primary_key=True)

    usage_start = models.DateField(null=False)

    usage_end = models.DateField(null=False)

    cluster_id = models.CharField(max_length=50, null=True)

    cluster_alias = models.CharField(max_length=256, null=True)

    usage_account_id = models.CharField(max_length=50, null=False)

    account_alias = models.ForeignKey("AWSAccountAlias", on_delete=models.DO_NOTHING, null=True)

    product_code = models.CharField(max_length=50, null=False)

    product_family = models.CharField(max_length=150, null=True)

    unblended_cost = models.DecimalField(max_digits=24, decimal_places=9, null=True)

    markup_cost = models.DecimalField(max_digits=24, decimal_places=9, null=True)

    currency_code = models.CharField(max_length=10)

    source_uuid = models.UUIDField(unique=False, null=True)


class OCPAllCostSummaryByRegion(models.Model):
    """A MATERIALIZED VIEW specifically for UI API queries.
    This table gives a daily breakdown of total cost by region.
    """

    class Meta:
        """Meta for OCPAllCostSummaryByRegion."""

        db_table = "reporting_ocpall_cost_summary_by_region"
        managed = False

    id = models.IntegerField(primary_key=True)

    usage_start = models.DateField(null=False)

    usage_end = models.DateField(null=False)

    cluster_id = models.CharField(max_length=50, null=True)

    cluster_alias = models.CharField(max_length=256, null=True)

    usage_account_id = models.CharField(max_length=50, null=False)

    account_alias = models.ForeignKey("AWSAccountAlias", on_delete=models.DO_NOTHING, null=True)

    region = models.CharField(max_length=50, null=True)

    availability_zone = models.CharField(max_length=50, null=True)

    unblended_cost = models.DecimalField(max_digits=24, decimal_places=9, null=True)

    markup_cost = models.DecimalField(max_digits=24, decimal_places=9, null=True)

    currency_code = models.CharField(max_length=10)

    source_uuid = models.UUIDField(unique=False, null=True)


class OCPAllComputeSummary(models.Model):
    """A summarized view of OCP on All infrastructure cost for products in the compute service category."""

    class Meta:
        """Meta for OCPAllComputeSummary."""

        db_table = "reporting_ocpall_compute_summary"
        managed = False

    id = models.IntegerField(primary_key=True)

    cluster_id = models.CharField(max_length=50, null=True)

    cluster_alias = models.CharField(max_length=256, null=True)

    usage_account_id = models.CharField(max_length=50, null=False)

    account_alias = models.ForeignKey("AWSAccountAlias", on_delete=models.DO_NOTHING, null=True)

    usage_start = models.DateField(null=False)

    usage_end = models.DateField(null=False)

    product_code = models.CharField(max_length=50, null=False)

    instance_type = models.CharField(max_length=50)

    resource_id = models.CharField(max_length=253)

    usage_amount = models.DecimalField(max_digits=30, decimal_places=15, null=True)

    unit = models.CharField(max_length=63, null=True)

    unblended_cost = models.DecimalField(max_digits=30, decimal_places=15, null=True)

    markup_cost = models.DecimalField(max_digits=30, decimal_places=15, null=True)

    currency_code = models.CharField(max_length=10, null=True)

    source_uuid = models.UUIDField(unique=False, null=True)


class OCPAllDatabaseSummary(models.Model):
    """A summarized view of OCP on All infrastructure cost for products in the database service category."""

    class Meta:
        """Meta for OCPAllDatabaseSummary."""

        db_table = "reporting_ocpall_database_summary"
        managed = False

    id = models.IntegerField(primary_key=True)

    # OCP Fields
    cluster_id = models.CharField(max_length=50, null=True)

    cluster_alias = models.CharField(max_length=256, null=True)

    usage_account_id = models.CharField(max_length=50, null=False)

    account_alias = models.ForeignKey("AWSAccountAlias", on_delete=models.DO_NOTHING, null=True)

    usage_start = models.DateField(null=False)

    usage_end = models.DateField(null=False)

    product_code = models.CharField(max_length=50, null=False)

    usage_amount = models.DecimalField(max_digits=30, decimal_places=15, null=True)

    unit = models.CharField(max_length=63, null=True)

    unblended_cost = models.DecimalField(max_digits=30, decimal_places=15, null=True)

    markup_cost = models.DecimalField(max_digits=30, decimal_places=15, null=True)

    currency_code = models.CharField(max_length=10, null=True)

    source_uuid = models.UUIDField(unique=False, null=True)


class OCPAllNetworkSummary(models.Model):
    """A summarized view of OCP on All infrastructure cost for products in the network service category."""

    class Meta:
        """Meta for OCPAllNetworkSummary."""

        db_table = "reporting_ocpall_network_summary"
        managed = False

    id = models.IntegerField(primary_key=True)

    cluster_id = models.CharField(max_length=50, null=True)

    cluster_alias = models.CharField(max_length=256, null=True)

    usage_account_id = models.CharField(max_length=50, null=False)

    account_alias = models.ForeignKey("AWSAccountAlias", on_delete=models.DO_NOTHING, null=True)

    usage_start = models.DateField(null=False)

    usage_end = models.DateField(null=False)

    product_code = models.CharField(max_length=50, null=False)

    usage_amount = models.DecimalField(max_digits=30, decimal_places=15, null=True)

    unit = models.CharField(max_length=63, null=True)

    unblended_cost = models.DecimalField(max_digits=30, decimal_places=15, null=True)

    markup_cost = models.DecimalField(max_digits=30, decimal_places=15, null=True)

    currency_code = models.CharField(max_length=10, null=True)

    source_uuid = models.UUIDField(unique=False, null=True)


class OCPAllStorageSummary(models.Model):
    """A summarized view of OCP on All infrastructure cost for products in the storage service category."""

    class Meta:
        """Meta for OCPAllStorageSummary."""

        db_table = "reporting_ocpall_storage_summary"
        managed = False

    id = models.IntegerField(primary_key=True)

    # OCP Fields
    cluster_id = models.CharField(max_length=50, null=True)

    cluster_alias = models.CharField(max_length=256, null=True)

    usage_account_id = models.CharField(max_length=50, null=False)

    account_alias = models.ForeignKey("AWSAccountAlias", on_delete=models.DO_NOTHING, null=True)

    usage_start = models.DateField(null=False)

    usage_end = models.DateField(null=False)

    product_family = models.CharField(max_length=150, null=True)

    product_code = models.CharField(max_length=50, null=False)

    usage_amount = models.DecimalField(max_digits=30, decimal_places=15, null=True)

    unit = models.CharField(max_length=63, null=True)

    unblended_cost = models.DecimalField(max_digits=30, decimal_places=15, null=True)

    markup_cost = models.DecimalField(max_digits=30, decimal_places=15, null=True)

    currency_code = models.CharField(max_length=10, null=True)

    source_uuid = models.UUIDField(unique=False, null=True)


class OCPAllCostLineItemProjectDailySummary(models.Model):
    """A summarized view of OCP on AWS cost by OpenShift project."""

    class Meta:
        """Meta for OCPAllCostLineItemProjectDailySummary."""

        db_table = "reporting_ocpallcostlineitem_project_daily_summary"
        managed = False

        indexes = [
            models.Index(fields=["usage_start"], name="ocpall_proj_usage_idx"),
            models.Index(fields=["namespace"], name="ocpall_proj_namespace_idx"),
            models.Index(fields=["node"], name="ocpall_proj_node_idx"),
            models.Index(fields=["resource_id"], name="ocpall_proj_resource_idx"),
            GinIndex(fields=["pod_labels"], name="ocpall_proj_pod_labels_idx"),
            models.Index(fields=["product_family"], name="ocpall_proj_prod_fam_idx"),
            models.Index(fields=["instance_type"], name="ocpall_proj_inst_type_idx"),
        ]

    id = models.IntegerField(primary_key=True)

    # The infrastructure provider type
    source_type = models.TextField()

    # OCP Fields
    cluster_id = models.CharField(max_length=50, null=True)

    cluster_alias = models.CharField(max_length=256, null=True)

    # Whether the data comes from a pod or volume report
    data_source = models.CharField(max_length=64, null=True)

    # Kubernetes objects by convention have a max name length of 253 chars
    namespace = models.CharField(max_length=253, null=False)

    node = models.CharField(max_length=253, null=True)

    pod_labels = JSONField(null=True)

    resource_id = models.CharField(max_length=253, null=True)

    usage_start = models.DateField(null=False)

    usage_end = models.DateField(null=False)

    # AWS Fields
    usage_account_id = models.CharField(max_length=50, null=False)

    account_alias = models.ForeignKey("AWSAccountAlias", on_delete=models.SET_NULL, null=True)

    product_code = models.CharField(max_length=50, null=False)

    product_family = models.CharField(max_length=150, null=True)

    instance_type = models.CharField(max_length=50, null=True)

    region = models.CharField(max_length=50, null=True)

    availability_zone = models.CharField(max_length=50, null=True)

    # Need more precision on calculated fields, otherwise there will be
    # Rounding errors
    usage_amount = models.DecimalField(max_digits=30, decimal_places=15, null=True)

    unit = models.CharField(max_length=63, null=True)

    unblended_cost = models.DecimalField(max_digits=30, decimal_places=15, null=True)

    project_markup_cost = models.DecimalField(max_digits=30, decimal_places=15, null=True)

    pod_cost = models.DecimalField(max_digits=30, decimal_places=15, null=True)

    currency_code = models.CharField(max_length=10, null=True)

    source_uuid = models.UUIDField(unique=False, null=True)
