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
import logging

from django.core.exceptions import ObjectDoesNotExist

from api.provider.models import Sources
from koku import celery_app
from masu.processor.tasks import PRIORITY_QUEUE
from masu.processor.tasks import REMOVE_EXPIRED_DATA_QUEUE
from sources.api.source_status import SourceStatus
from sources.sources_provider_coordinator import SourcesProviderCoordinator
from sources.storage import load_providers_to_delete

LOG = logging.getLogger(__name__)


@celery_app.task(name="sources.tasks.delete_source", queue=PRIORITY_QUEUE)
def delete_source(source_id, auth_header, koku_uuid):
    """Delete Provider and Source."""
    LOG.info(f"Deleting Provider {koku_uuid} for Source ID: {source_id}")
    coordinator = SourcesProviderCoordinator(source_id, auth_header)
    coordinator.destroy_account(koku_uuid)


@celery_app.task(name="sources.tasks.delete_source_beat", queue=REMOVE_EXPIRED_DATA_QUEUE)
def delete_source_beat():
    providers = load_providers_to_delete()
    for p in providers:
        provider = p.get("provider")
        delete_source.delay(provider.source_id, provider.auth_header, provider.koku_uuid)


@celery_app.task(name="sources.tasks.source_status_beat", queue=PRIORITY_QUEUE)
def source_status_beat():
    """Source Status push."""
    sources_query = Sources.objects.filter(source_id__isnull=False).all()
    for source in sources_query:
        try:
            status_pusher = SourceStatus(source.source_id)
            LOG.info("Delivering source status for Source ID: %s", source.source_id)
            status_pusher.push_status()
        except ObjectDoesNotExist:
            LOG.info(f"Source status not pushed.  Unable to find Source ID: {source.source_id}")
