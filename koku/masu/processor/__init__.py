#
# Copyright 2021 Red Hat Inc.
# SPDX-License-Identifier: Apache-2.0
#
"""Masu Processor."""
import logging

from django.conf import settings

from koku.feature_flags import UNLEASH_CLIENT
from masu.external import GZIP_COMPRESSED
from masu.external import UNCOMPRESSED

LOG = logging.getLogger(__name__)

ALLOWED_COMPRESSIONS = (UNCOMPRESSED, GZIP_COMPRESSED)


def enable_trino_processing(source_uuid, source_type, account):  # noqa
    """Helper to determine if source is enabled for Trino."""
    if account and not account.startswith("acct"):
        account = f"acct{account}"

    context = {"schema": account, "source-type": source_type, "sourceUUID": source_uuid}

    LOG.debug(f"enable_trino_processing({source_uuid}, {source_type}, {account})")
    return settings.ENABLE_PARQUET_PROCESSING or UNLEASH_CLIENT.is_enabled("trino-processor", context)
