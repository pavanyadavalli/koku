#
# Copyright 2018 Red Hat, Inc.
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
"""API application configuration module."""
import os

from django.apps import AppConfig
from django.db import connections
from django.db import DEFAULT_DB_ALIAS
from django.db.migrations.executor import MigrationExecutor


def check_migrations():
    """
    Check the status of database migrations.

    The koku API server is responsible for running all database migrations.  This method
    will return the state of the database and whether or not all migrations have been completed.

    Hat tip to the Stack Overflow contributor: https://stackoverflow.com/a/31847406

    Returns:
        Boolean - True if database is available and migrations have completed.  False otherwise.

    """
    connection = connections[DEFAULT_DB_ALIAS]
    connection.prepare_database()
    executor = MigrationExecutor(connection)
    targets = executor.loader.graph.leaf_nodes()
    return not executor.migration_plan(targets)


class ApiConfig(AppConfig):
    """API application configuration."""

    name = "api"

    def ready(self):
        RUNNING_MIGRATIONS = False if os.getenv("RUNNING_MIGRATIONS", "False") == "False" else True
        print("API ready called. RUNNING_MIGRATIONS: ", str(RUNNING_MIGRATIONS))
        if not RUNNING_MIGRATIONS:
            if check_migrations():
                print("Migations are ready")
            else:
                print("Migrations are not ready")
                exit(1)
