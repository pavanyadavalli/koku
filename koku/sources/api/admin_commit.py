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
"""View for admin committing kafka messages."""
import logging

from rest_framework.decorators import api_view
from rest_framework.decorators import permission_classes
from rest_framework.decorators import renderer_classes
from rest_framework.permissions import AllowAny
from rest_framework.response import Response
from rest_framework.settings import api_settings

from sources.kafka_listener import queue_force_commit_kafka_msg

LOG = logging.getLogger(__name__)


@api_view(http_method_names=["POST"])
@permission_classes((AllowAny,))
@renderer_classes(tuple(api_settings.DEFAULT_RENDERER_CLASSES))
def admin_commit(request):
    """Packages response for class-based view."""
    offset = request.query_params.get("offset")
    partition = request.query_params.get("partition")
    response = {"query_params": request.query_params, "offset": offset, "partition": partition}
    queue_force_commit_kafka_msg(request.query_params)
    return Response(response)
