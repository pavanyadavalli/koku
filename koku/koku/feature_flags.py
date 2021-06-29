#
# Copyright 2021 Red Hat Inc.
# SPDX-License-Identifier: Apache-2.0
#
"""Create Unleash Client."""
from django.conf import settings
from UnleashClient import UnleashClient
from UnleashClient.strategies import Strategy


class SourceTypesStrategy(Strategy):
    def load_provisioning(self) -> list:
        return [x.strip() for x in self.parameters["sourceTypes"].split(",")]

    def apply(self, context):
        default_value = False
        if "source-type" in context.keys():
            default_value = context["source-type"] in self.parsed_provisioning
        return default_value


class TrinoAccountsStrategy(Strategy):
    def load_provisioning(self) -> list:
        return [x.strip() for x in self.parameters["schemaName"].split(",")]

    def apply(self, context):
        default_value = False
        if "schema" in context.keys():
            default_value = context["schema"] in self.parsed_provisioning
        return default_value


class TrinoSourcesStrategy(Strategy):
    def load_provisioning(self) -> list:
        return [x.strip() for x in self.parameters["trinoSources"].split(",")]

    def apply(self, context):
        default_value = False
        if "sourceUUID" in context.keys():
            default_value = context["sourceUUID"] in self.parsed_provisioning
        return default_value


strategies = {
    "sourceTypes": SourceTypesStrategy,
    "trinoAccounts": TrinoAccountsStrategy,
    "trinoSources": TrinoSourcesStrategy,
}
UNLEASH_CLIENT = UnleashClient(settings.UNLEASH_URL, "koku", custom_strategies=strategies)
# UNLEASH_CLIENT = UnleashClient(f"http://{host}:{port}/api", "koku", custom_strategies=strategies)
UNLEASH_CLIENT.initialize_client()
