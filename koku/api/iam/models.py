#
# Copyright 2018 Red Hat, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
"""Models for identity and access management."""
import logging
import os
import pkgutil
from uuid import uuid4

from django.core.exceptions import ValidationError
from django.db import connection as conn
from django.db import models
from django.db import transaction
from tenant_schemas.models import TenantMixin
from tenant_schemas.postgresql_backend.base import _is_valid_schema_name
from tenant_schemas.utils import schema_exists

from koku.database import CloneSchemaError
from koku.tasks import create_tenant_schema
from masu.processor.tasks import TENANT_CREATE_QUEUE


LOG = logging.getLogger(__name__)


class CloneSchemaTemplateMissing(CloneSchemaError):
    pass


class Customer(models.Model):
    """A Koku Customer.

    A customer is an organization of N-number of users

    """

    date_created = models.DateTimeField(auto_now_add=True)
    date_updated = models.DateTimeField(auto_now_add=True)
    uuid = models.UUIDField(default=uuid4, editable=False, unique=True, null=False)
    account_id = models.CharField(max_length=150, blank=False, null=True, unique=True)
    schema_name = models.TextField(unique=True, null=False, default="public")

    class Meta:
        ordering = ["schema_name"]


class User(models.Model):
    """A Koku User."""

    uuid = models.UUIDField(default=uuid4, editable=False, unique=True, null=False)
    username = models.CharField(max_length=150, unique=True)
    email = models.EmailField(blank=True)
    date_created = models.DateTimeField(auto_now_add=True)
    is_active = models.BooleanField(default=True, null=True)
    customer = models.ForeignKey("Customer", null=True, on_delete=models.CASCADE)

    def __init__(self, *args, **kwargs):
        """Initialize non-persisted user properties."""
        super().__init__(*args, **kwargs)
        self.admin = False
        self.access = {}
        self.identity_header = None
        self.beta = False

    class Meta:
        ordering = ["username"]


class Tenant(TenantMixin):
    """The model used to create a tenant schema."""

    # Sometimes the Tenant model can seemingly return funky results,
    # so the template schema name is going to get more inline with the
    # customer account schema names
    _TEMPLATE_SCHEMA = os.environ.get("TEMPLATE_SCHEMA", "template0")

    schema_created = models.BooleanField(default=False)
    schema_create_running = models.BooleanField(default=False)

    # Override the mixin domain url to make it nullable, non-unique
    domain_url = None

    # Delete all schemas when a tenant is removed
    auto_drop_schema = True

    def _verify_template(self, verbosity=1):
        LOG.info(f'Verify that template schema "{self._TEMPLATE_SCHEMA}" exists')
        # This is using the teanant table data as the source of truth which can be dangerous.
        # If this becomes unreliable, then the database itself should be the source of truth
        # and extra code must be written to handle the sync of the table data to the state of
        # the database.
        template_schema = self.__class__.objects.get_or_create(schema_name=self._TEMPLATE_SCHEMA)

        # Strict check here! Both the record and the schema *should* exist!
        res = bool(template_schema) and schema_exists(self._TEMPLATE_SCHEMA)
        LOG.info(f"{str(res)}")

        return res

    def _clone_schema(self):
        LOG.info("Loading create script from koku_tenant_create.sql file.")
        create_sql_buff = pkgutil.get_data("api.iam", "sql/koku_tenant_create.sql").decode("utf-8")
        LOG.info(f'Cloning template schema "{self._TEMPLATE_SCHEMA}" to "{self.schema_name}"')
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{self.schema_name}" AUTHORIZATION current_user ;')
            cur.execute(f'SET search_path = "{self.schema_name}", public ;')
            cur.execute(create_sql_buff)
            cur.execute("SET search_path = public ;")
        return True

    def create_schema(self, check_if_exists=True, sync_schema=True, verbosity=1):
        """
        If schema is "public" or matches _TEMPLATE_SCHEMA, then use the superclass' create_schema() method.
        Else, verify the template and inputs and use the database clone function.
        """
        if self.schema_name in ("public", self._TEMPLATE_SCHEMA):
            LOG.info(f'Using superclass for "{self.schema_name}" schema creation')
            return super().create_schema(check_if_exists=True, sync_schema=sync_schema, verbosity=verbosity)

        # Verify name structure
        if not _is_valid_schema_name(self.schema_name):
            exc = ValidationError(f'Invalid schema name: "{self.schema_name}"')
            LOG.error(f"{exc.__class__.__name__}:: {''.join(exc)}")
            raise exc

        # Enqueue message for worker to process schema
        create_tenant_schema.s().set(queue=TENANT_CREATE_QUEUE)

        with transaction.atomic():
            ret = self._verify_template(verbosity=verbosity)
            if not ret:
                errmsg = f'Template schema "{self._TEMPLATE_SCHEMA}" does not exist'
                LOG.critical(errmsg)
                raise CloneSchemaTemplateMissing(errmsg)

            # Always check to see if the schema exists!
            LOG.info(f"Check if target schema {self.schema_name} already exists")
            if schema_exists(self.schema_name):
                LOG.warning(f'Schema "{self.schema_name}" already exists. Exit with False.')
                return False

            # Clone the schema. The database function will check
            # that the source schema exists and the destination schema does not.
            try:
                self._clone_schema()
            except Exception as dbe:
                db_exc = dbe
                LOG.error(
                    f"""Exception {dbe.__class__.__name__} cloning"""
                    + f""" "{self._TEMPLATE_SCHEMA}" to "{self.schema_name}": {str(dbe)}"""
                )
                LOG.info("Setting transaction to exit with ROLLBACK")
                transaction.set_rollback(True)  # Set this transaction context to issue a rollback on exit
            else:
                LOG.info(f'Successful clone of "{self._TEMPLATE_SCHEMA}" to "{self.schema_name}"')

        # Set schema to public (even if there was an exception)
        with transaction.atomic():
            LOG.info("Reset DB search path to public")
            conn.set_schema_to_public()

        if db_exc:
            raise db_exc

        return True
