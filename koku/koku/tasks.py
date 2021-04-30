#
# Copyright 2021 Red Hat, Inc.
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
"""Tasks for sources-client."""
import json
import logging
import os

from django.conf import settings
from django.db import connection
from django.db import transaction

from .database import CloneSchemaError
from .database import dbfunc_not_exists
from .migration_sql_helpers import apply_sql_file
from .migration_sql_helpers import find_db_functions_dir
from koku import celery_app
from masu.processor.tasks import TENANT_CREATE_QUEUE

# from masu.processor.worker_cache import WorkerCache


class CloneSchemaFuncMissing(CloneSchemaError):
    pass


LOG = logging.getLogger(__name__)
_TEMPLATE_SCHEMA = getattr(settings, "TEMPLATE_SCHEMA", os.environ.get("TEMPLATE_SCHEMA", "template0"))
_READ_SCHEMA_FUNC_FILENAME = os.path.join(find_db_functions_dir(), "read_schema.sql")
_READ_SCHEMA_FUNC_SCHEMA = "public"
_READ_SCHEMA_FUNC_NAME = "read_schema"
_READ_SCHEMA_FUNC_SIG = (
    f"{_READ_SCHEMA_FUNC_SCHEMA}.{_READ_SCHEMA_FUNC_NAME}(" "source_schema text, " "_verbose boolean DEFAULT false" ")"
)
_CREATE_SCHEMA_FUNC_FILENAME = os.path.join(find_db_functions_dir(), "create_schema.sql")
_CREATE_SCHEMA_FUNC_SCHEMA = "public"
_CREATE_SCHEMA_FUNC_NAME = "create_schema"
_CREATE_SCHEMA_FUNC_SIG = (
    f"{_CREATE_SCHEMA_FUNC_SCHEMA}.{_CREATE_SCHEMA_FUNC_NAME}("
    "source_schema text, source_structure jsonb, "
    "new_schemata text[], "
    "copy_data boolean DEFAULT false, "
    "_verbose boolean DEFAULT false"
    ")"
)
_CREATE_SCHEMA_FUNC_FILENAME = os.path.join(find_db_functions_dir(), "create_schema.sql")
_CLONE_SCHEMA_FUNC_FILENAME = os.path.join(find_db_functions_dir(), "clone_schema.sql")
_CLONE_SCHEMA_FUNC_SCHEMA = "public"
_CLONE_SHEMA_FUNC_NAME = "clone_schema"
_CLONE_SCHEMA_FUNC_SIG = (
    f"{_CLONE_SCHEMA_FUNC_SCHEMA}.{_CLONE_SHEMA_FUNC_NAME}("
    "source_schema text, dest_schema text, "
    "copy_data boolean DEFAULT false, "
    "_verbose boolean DEFAULT false"
    ")"
)


@celery_app.task(name="koku.tasks.create_tenant_schema", queue=TENANT_CREATE_QUEUE)
def create_tenant_schema():
    with transaction.atomic():
        ret = _check_clone_func()
        if ret:
            errmsg = "Missing clone_schema function even after re-applying the function SQL file."
            LOG.critical(errmsg)
            raise CloneSchemaFuncMissing(errmsg)

        LOG.debug(f"Reading structure for template schema {_TEMPLATE_SCHEMA}")
        template_structure = _read_template_structure()

    schema_batch = [None]

    while schema_batch:
        with transaction.atomic():
            LOG.info("Getting tenant batch")
            schema_batch = _get_tenant_batch()
            if schema_batch:
                LOG.info(f"Got {len(schema_batch)} tenants to process")
                res = _create_schema(template_structure, schema_batch)

                created = [s[1:] for s in res if s.startswith("+")]
                skipped = [s[1:] for s in res if s.startswith("!")]
                _set_tenants_as_created(created)

                LOG.info(f"Created schemata {', '.join(created)}")
                if skipped:
                    LOG.info(f"Skipped existing schema: {', '.join(skipped)}")


def _read_template_structure():
    sql = """
SELECT public.read_schema(%s);
"""
    with connection.cursor() as cur:
        cur.execute(sql, (_TEMPLATE_SCHEMA,))
        data = cur.fetchone()
        if data:
            if isinstance(data[0], str):
                data = json.loads(data[0])
            else:
                data = data[0]

    return data


def _create_schema(template_structure, schema_batch):
    sql = """
select public.create_schema(%s, %s, %s, copy_data=>true);
"""
    with connection.cursor() as cur:
        cur.execute(sql, (_TEMPLATE_SCHEMA, template_structure, schema_batch))
        data = cur.fetchone()
        if data:
            data = data[0]

    return data


def _get_tenant_batch():
    sql = """
UPDATE public.api_tenant tt
   SET schema_create_running = true,
       schema_created = false
  FROM (
         SELECT schema_name
           FROM public.api_tenant
          WHERE schema_created = false
            AND schema_create_running = false
            FOR UPDATE
           SKIP LOCKED
       ) as lt
 WHERE tt.schema_name = lt.schema_name
RETURNING tt.schema_name;
"""
    with connection.cursor() as cur:
        cur.execute(sql)
        data = [t[0] for t in cur.fetchall()]

    return data


def _set_tenants_as_created(tenant_batch):
    sql = """
UPDATE public.api_tenant
   SET schema_create_running = false,
       schema_created = true
 WHERE schema_name = any(%s);
"""
    with connection.cursor() as cur:
        cur.execute(sql, (tenant_batch,))


def _check_clone_func():
    clone_func_map = {
        _CLONE_SCHEMA_FUNC_SCHEMA: {
            _CLONE_SHEMA_FUNC_NAME: _CLONE_SCHEMA_FUNC_SIG,
            _READ_SCHEMA_FUNC_NAME: _READ_SCHEMA_FUNC_SIG,
            _CREATE_SCHEMA_FUNC_NAME: _CREATE_SCHEMA_FUNC_SIG,
        }
    }
    func_file_map = {
        _CLONE_SHEMA_FUNC_NAME: _CLONE_SCHEMA_FUNC_FILENAME,
        _READ_SCHEMA_FUNC_NAME: _READ_SCHEMA_FUNC_FILENAME,
        _CREATE_SCHEMA_FUNC_NAME: _CREATE_SCHEMA_FUNC_FILENAME,
    }

    LOG.info("Verify that clone function(s) exists")
    res = dbfunc_not_exists(connection, clone_func_map)
    if res:
        LOG.warning("Clone function(s) missing")
        for schema in res:
            connection.cursor().execute(f"SET SEARCH_PATH = {schema} ;")
            for func in res[schema]:
                LOG.info(f'Creating clone function "{func}"')
                apply_sql_file(connection.schema_editor(), func_file_map[func], literal_placeholder=True)

        res = dbfunc_not_exists(connection, clone_func_map)
        if res:
            missing_functions = [f"{s}.{f}" for s in res for f in res[s]]
            LOG.error(f"Clone functions {', '.join(missing_functions)} still missing after application.")
            raise CloneSchemaFuncMissing(missing_functions)
    else:
        LOG.info("Clone functions exist")

    return res
