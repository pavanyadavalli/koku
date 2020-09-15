# flake8: noqa
#
# Copyright 2020 Red Hat, Inc.
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
# insert the block below at line 20 of
# <VENV_PATH>/lib/python3.8/site-packages/tenant_schemas/management/commands/migrate_schemas.py
# providing there's not a way to ensure that the parser argument is not added at the start of
# the processing for migrate_schemas

for anum in range(len(parser._actions)):
    if "--skip-checks" in parser._actions[anum].option_strings:
        for ostr in parser._actions[anum].option_strings:
            del parser._option_string_actions[ostr]

        parser._actions.pop(anum)
        break
