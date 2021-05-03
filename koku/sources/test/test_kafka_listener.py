#
# Copyright 2019 Red Hat, Inc.
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
"""Test the Sources Kafka Listener handler."""
# import asyncio
import json
import queue
from unittest.mock import patch
from uuid import uuid4

from django.db import IntegrityError
from django.db import InterfaceError
from django.db import OperationalError
from django.db.models.signals import post_save
from django.test.utils import override_settings
from faker import Faker
from kafka.errors import KafkaError
from rest_framework.exceptions import ValidationError

import sources.kafka_listener as source_integration
from api.iam.test.iam_test_case import IamTestCase
from api.provider.models import Provider
from api.provider.models import Sources
from api.provider.provider_builder import ProviderBuilder
from api.provider.provider_builder import ProviderBuilderError
from koku.middleware import IdentityHeaderMiddleware
from masu.prometheus_stats import WORKER_REGISTRY
from providers.provider_access import ProviderAccessor
from providers.provider_errors import SkipStatusPush
from sources import storage
from sources.config import Config
from sources.kafka_listener import PROCESS_QUEUE
from sources.kafka_listener import process_synchronize_sources_msg
from sources.kafka_listener import SourcesIntegrationError
from sources.kafka_listener import storage_callback
from sources.sources_http_client import SourcesHTTPClient
from sources.sources_provider_coordinator import SourcesProviderCoordinator
from sources.test.test_sources_http_client import COST_MGMT_APP_TYPE_ID

# import requests_mock
# from requests.exceptions import RequestException
# from sources.sources_http_client import SourceNotFoundError
# from sources.sources_http_client import SourcesHTTPClientError

faker = Faker()
SOURCES_APPS = "http://www.sources.com/api/v1.0/applications?filter[application_type_id]={}&filter[source_id]={}"


def raise_source_manager_error(param_a, param_b, param_c, param_d, param_e):
    """Raise ProviderBuilderError"""
    raise ProviderBuilderError()


def raise_validation_error(param_a, param_b, param_c, param_d, param_e):
    """Raise ValidationError"""
    raise ValidationError()


def raise_provider_manager_error(param_a):
    """Raise ProviderBuilderError"""
    raise ProviderBuilderError("test exception")


class ConsumerRecord:
    """Test class for kafka msg."""

    def __init__(self, topic, offset, event_type, value, auth_header=None, partition=0):
        """Initialize Msg."""
        self._topic = topic
        self._offset = offset
        self._partition = partition
        if auth_header:
            self._headers = (
                ("event_type", bytes(event_type, encoding="utf-8")),
                ("x-rh-identity", bytes(auth_header, encoding="utf-8")),
            )
        else:
            self._headers = (("event_type", bytes(event_type, encoding="utf-8")),)
        self._value = value

    def topic(self):
        return self._topic

    def offset(self):
        return self._offset

    def partition(self):
        return self._partition

    def value(self):
        return self._value

    def headers(self):
        return self._headers


class MsgDataGenerator:
    """Test class to create msg_data."""

    def __init__(self, event_type, test_topic=None, value=None):
        """Initialize MsgDataGenerator."""
        self.test_topic = test_topic or "platform.sources.event-stream"
        self.test_offset = 5
        self.cost_management_app_type = COST_MGMT_APP_TYPE_ID
        self.test_auth_header = Config.SOURCES_FAKE_HEADER
        self.event_type = event_type
        if value:
            self.test_value = json.dumps(value)
        else:
            self.test_value = '{"id":1,"source_id":1,"application_type_id":2}'
        self.msg = ConsumerRecord(
            topic=self.test_topic,
            offset=self.test_offset,
            event_type=self.event_type,
            auth_header=self.test_auth_header,
            value=bytes(self.test_value, encoding="utf-8"),
        )


class MockKafkaConsumer:
    def __init__(self, preloaded_messages=["hi", "world"]):
        self.preloaded_messages = preloaded_messages

    async def start(self):
        pass

    async def stop(self):
        pass

    async def commit(self):
        self.preloaded_messages.pop()

    async def seek(self, topic_partition):
        # This isn't realistic... But it's one way to stop the consumer for our needs.
        raise KafkaError("Seek to commited. Closing...")

    async def getone(self):
        for msg in self.preloaded_messages:
            return msg
        raise KafkaError("Closing Mock Consumer")

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self.getone()


class SourcesKafkaMsgHandlerTest(IamTestCase):
    """Test Cases for the Sources Kafka Listener."""

    @classmethod
    def setUpClass(cls):
        """Set up the test class."""
        super().setUpClass()
        post_save.disconnect(storage_callback, sender=Sources)
        account = "12345"
        IdentityHeaderMiddleware.create_customer(account)

    def setUp(self):
        """Setup the test method."""
        super().setUp()
        self.aws_source = {
            "source_id": 10,
            "source_uuid": uuid4(),
            "name": "ProviderAWS",
            "source_type": "AWS",
            "authentication": {"credentials": {"role_arn": "arn:aws:iam::111111111111:role/CostManagement"}},
            "billing_source": {"data_source": {"bucket": "fake-bucket"}},
            "auth_header": Config.SOURCES_FAKE_HEADER,
            "account_id": "acct10001",
            "offset": 10,
        }
        self.aws_local_source = {
            "source_id": 11,
            "source_uuid": uuid4(),
            "name": "ProviderAWS Local",
            "source_type": "AWS-local",
            "authentication": {"credentials": {"role_arn": "arn:aws:iam::111111111111:role/CostManagement"}},
            "billing_source": {"data_source": {"bucket": "fake-local-bucket"}},
            "auth_header": Config.SOURCES_FAKE_HEADER,
            "account_id": "acct10001",
            "offset": 11,
        }
        self.azure_local_source = {
            "source_id": 12,
            "source_uuid": uuid4(),
            "name": "ProviderAzure Local",
            "source_type": "Azure-local",
            "authentication": {"credentials": {"role_arn": "arn:aws:iam::111111111111:role/CostManagement"}},
            "billing_source": {"data_source": {"bucket": "fake-local-bucket"}},
            "auth_header": Config.SOURCES_FAKE_HEADER,
            "account_id": "acct10001",
            "offset": 12,
        }
        self.gcp_source = {
            "source_id": 13,
            "source_uuid": uuid4(),
            "name": "Provider GCP",
            "source_type": "GCP",
            "authentication": {"credentials": {"project_id": "test_project"}},
            "billing_source": {"data_source": {"dataset": "test_dataset", "table_id": "test_table_id"}},
            "auth_header": Config.SOURCES_FAKE_HEADER,
            "account_id": "acct10001",
            "offset": 12,
        }

    def test_execute_koku_provider_op_create(self):
        """Test to execute Koku Operations to sync with Sources for creation."""
        source_id = self.aws_source.get("source_id")
        provider = Sources(**self.aws_source)
        provider.save()

        msg = {"operation": "create", "provider": provider, "offset": provider.offset}
        with patch.object(SourcesHTTPClient, "set_source_status"):
            with patch.object(ProviderAccessor, "cost_usage_source_ready", returns=True):
                source_integration.execute_koku_provider_op(msg)
        self.assertIsNotNone(Sources.objects.get(source_id=source_id).koku_uuid)
        self.assertFalse(Sources.objects.get(source_id=source_id).pending_update)
        self.assertEqual(Sources.objects.get(source_id=source_id).koku_uuid, str(provider.source_uuid))

    @override_settings(CELERY_TASK_ALWAYS_EAGER=True)
    def test_execute_koku_provider_op_destroy(self):
        """Test to execute Koku Operations to sync with Sources for destruction."""
        source_id = self.aws_source.get("source_id")
        provider = Sources(**self.aws_source)
        provider.save()

        msg = {"operation": "destroy", "provider": provider, "offset": provider.offset}
        with patch.object(SourcesHTTPClient, "set_source_status"):
            source_integration.execute_koku_provider_op(msg)
        self.assertEqual(Sources.objects.filter(source_id=source_id).exists(), False)

    @override_settings(CELERY_TASK_ALWAYS_EAGER=True)
    def test_execute_koku_provider_op_destroy_provider_not_found(self):
        """Test to execute Koku Operations to sync with Sources for destruction with provider missing.

        First, raise ProviderBuilderError. Check that provider and source still exists.
        Then, re-call provider destroy without exception, then see both source and provider are gone.

        """
        source_id = self.aws_source.get("source_id")
        provider = Sources(**self.aws_source)
        provider.save()
        # check that the source exists
        self.assertTrue(Sources.objects.filter(source_id=source_id).exists())

        with patch.object(ProviderAccessor, "cost_usage_source_ready", returns=True):
            builder = SourcesProviderCoordinator(source_id, provider.auth_header)
            builder.create_account(provider)

        self.assertTrue(Provider.objects.filter(uuid=provider.source_uuid).exists())
        provider = Sources.objects.get(source_id=source_id)

        msg = {"operation": "destroy", "provider": provider, "offset": provider.offset}
        with patch.object(SourcesHTTPClient, "set_source_status"):
            with patch.object(ProviderBuilder, "destroy_provider", side_effect=raise_provider_manager_error):
                source_integration.execute_koku_provider_op(msg)
                self.assertTrue(Provider.objects.filter(uuid=provider.source_uuid).exists())
                self.assertTrue(Sources.objects.filter(source_uuid=provider.source_uuid).exists())
                self.assertTrue(Sources.objects.filter(koku_uuid=provider.source_uuid).exists())

        with patch.object(SourcesHTTPClient, "set_source_status"):
            source_integration.execute_koku_provider_op(msg)
        self.assertFalse(Provider.objects.filter(uuid=provider.source_uuid).exists())

    def test_execute_koku_provider_op_update(self):
        """Test to execute Koku Operations to sync with Sources for update."""

        def set_status_helper(*args, **kwargs):
            """helper to clear update flag."""
            storage.clear_update_flag(source_id)

        source_id = self.aws_source.get("source_id")
        provider = Sources(**self.aws_source)
        provider.save()

        msg = {"operation": "create", "provider": provider, "offset": provider.offset}
        with patch.object(SourcesHTTPClient, "set_source_status"):
            with patch.object(ProviderAccessor, "cost_usage_source_ready", returns=True):
                source_integration.execute_koku_provider_op(msg)

        builder = SourcesProviderCoordinator(source_id, provider.auth_header)

        source = storage.get_source_instance(source_id)
        uuid = source.koku_uuid

        with patch.object(ProviderAccessor, "cost_usage_source_ready", returns=True):
            builder.update_account(source)

        self.assertEqual(
            Provider.objects.get(uuid=uuid).billing_source.data_source,
            self.aws_source.get("billing_source").get("data_source"),
        )

        provider.billing_source = {"data_source": {"bucket": "new-bucket"}}
        provider.koku_uuid = uuid
        provider.pending_update = True
        provider.save()

        msg = {"operation": "update", "provider": provider, "offset": provider.offset}
        with patch.object(SourcesHTTPClient, "set_source_status", side_effect=set_status_helper):
            with patch.object(ProviderAccessor, "cost_usage_source_ready", returns=True):
                source_integration.execute_koku_provider_op(msg)
        response = Sources.objects.get(source_id=source_id)
        self.assertEqual(response.pending_update, False)
        self.assertEqual(response.billing_source, {"data_source": {"bucket": "new-bucket"}})

        response = Provider.objects.get(uuid=uuid)
        self.assertEqual(response.billing_source.data_source.get("bucket"), "new-bucket")

    def test_execute_koku_provider_op_skip_status(self):
        """Test to execute Koku Operations to sync with Sources and not push status."""
        source_id = self.aws_source.get("source_id")
        provider = Sources(**self.aws_source)
        provider.save()

        msg = {"operation": "create", "provider": provider, "offset": provider.offset}
        with patch.object(SourcesHTTPClient, "set_source_status"):
            with patch.object(ProviderAccessor, "cost_usage_source_ready", side_effect=SkipStatusPush):
                source_integration.execute_koku_provider_op(msg)
        self.assertEqual(Sources.objects.get(source_id=source_id).status, {})

    def test_collect_pending_items(self):
        """Test to load the in-progress queue."""
        aws_source = Sources(
            source_id=1,
            auth_header=Config.SOURCES_FAKE_HEADER,
            offset=1,
            name="AWS Source",
            source_type=Provider.PROVIDER_AWS,
            authentication="fakeauth",
            billing_source="s3bucket",
        )
        aws_source.save()

        aws_source_incomplete = Sources(
            source_id=2,
            auth_header=Config.SOURCES_FAKE_HEADER,
            offset=2,
            name="AWS Source 2",
            source_type=Provider.PROVIDER_AWS,
        )
        aws_source_incomplete.save()

        ocp_source = Sources(
            source_id=3,
            auth_header=Config.SOURCES_FAKE_HEADER,
            authentication="fakeauth",
            offset=3,
            name="OCP Source",
            source_type=Provider.PROVIDER_OCP,
        )
        ocp_source.save()

        ocp_source_complete = Sources(
            source_id=4,
            auth_header=Config.SOURCES_FAKE_HEADER,
            offset=4,
            name="Complete OCP Source",
            source_type=Provider.PROVIDER_OCP,
            koku_uuid=faker.uuid4(),
        )
        ocp_source_complete.save()
        source_delete = Sources.objects.get(source_id=4)
        source_delete.pending_delete = True
        source_delete.save()

        response = source_integration._collect_pending_items()
        self.assertEqual(len(response), 3)

    @patch("time.sleep", side_effect=None)
    @patch("sources.kafka_listener.check_kafka_connection", side_effect=[bool(0), bool(1)])
    def test_kafka_connection_metrics_listen_for_messages(self, mock_start, mock_sleep):
        """Test check_kafka_connection increments kafka connection errors on KafkaError."""
        connection_errors_before = WORKER_REGISTRY.get_sample_value("kafka_connection_errors_total")
        source_integration.is_kafka_connected()
        connection_errors_after = WORKER_REGISTRY.get_sample_value("kafka_connection_errors_total")
        self.assertEqual(connection_errors_after - connection_errors_before, 1)

    # @patch.object(Config, "SOURCES_API_URL", "http://www.sources.com")
    # @patch("sources.kafka_listener.sources_network_info", returns=None)
    # def test_process_message_application_create(self, mock_sources_network_info):
    #     """Test the process_message function."""
    #     test_application_id = 2

    #     def _expected_application_create(msg_data, test, source_network_info_mock):
    #         source_name = test.get("source_name")
    #         query_results = Sources.objects.filter(source_id=msg_data.get("source_id"))
    #         if source_name in ["amazon", "ocp"]:
    #             self.assertTrue(query_results.exists())
    #             self.assertEqual(query_results.first().auth_header, msg_data.get("auth_header"))
    #             source_network_info_mock.assert_called()

    #     test_matrix = [
    #         {
    #             "event": source_integration.KAFKA_APPLICATION_CREATE,
    #             "value": {"id": 1, "source_id": 1, "application_type_id": test_application_id},
    #             "source_name": "ocp",
    #             "expected_fn": _expected_application_create,
    #         },
    #         {
    #             "event": source_integration.KAFKA_APPLICATION_CREATE,
    #             "value": {"id": 1, "source_id": 1, "application_type_id": test_application_id},
    #             "source_name": "amazon",
    #             "expected_fn": _expected_application_create,
    #         },
    #     ]
    #     for test in test_matrix:
    #         msg_data = MsgDataGenerator(event_type=test.get("event"), value=test.get("value")).get_data()
    #         with patch.object(SourcesHTTPClient, "get_source_details", return_value={"source_type_id": "1"}):
    #             with patch.object(SourcesHTTPClient, "get_source_type_name", return_value=test.get("source_name")):
    #                 # process_message(test_application_id, msg_data)
    #                 test.get("expected_fn")(msg_data, test, mock_sources_network_info)

    # @patch.object(Config, "SOURCES_API_URL", "http://www.sources.com")
    # def test_process_message_application_unsupported_source_type(self):
    #     """Test the process_message function with an unsupported source type."""
    #     test_application_id = 2

    #     test = {
    #         "event": source_integration.KAFKA_APPLICATION_CREATE,
    #         "value": {"id": 1, "source_id": 1, "application_type_id": test_application_id},
    #     }
    #     msg_data = MsgDataGenerator(event_type=test.get("event"), value=test.get("value")).get_data()
    #     with patch.object(
    #         SourcesHTTPClient, "get_source_details", return_value={"name": "my ansible", "source_type_id": 2}
    #     ):
    #         with patch.object(SourcesHTTPClient, "get_application_settings", return_value={}):
    #             with patch.object(SourcesHTTPClient, "get_source_type_name", return_value="ansible-tower"):
    #                 # self.assertIsNone(process_message(test_application_id, msg_data))
    #                 self.assertIsNone(msg_data)
    #                 pass

    # @patch.object(Config, "SOURCES_API_URL", "http://www.sources.com")
    # @patch("sources.kafka_listener.sources_network_info", returns=None)
    # @patch("sources.kafka_listener.save_auth_info", returns=None)
    # def test_process_message_authentication_create(self, mock_save_auth_info, mock_sources_network_info):
    #     """Test the process_message function for authentication create."""
    #     test_application_id = 2

    #     def _expected_authentication_create(msg_data, test, save_auth_info_mock):
    #         expected_cost_mgmt_match = test.get("expected_cost_mgmt_match")
    #         query_results = Sources.objects.filter(source_id=test.get("value").get("source_id"))
    #         if expected_cost_mgmt_match:
    #             self.assertTrue(query_results.exists())
    #             self.assertEqual(query_results.first().auth_header, msg_data.get("auth_header"))
    #             save_auth_info_mock.assert_called()
    #         else:
    #             self.assertFalse(query_results.exists())
    #             save_auth_info_mock.assert_not_called()

    #     test_matrix = [
    #         {
    #             "event": source_integration.KAFKA_AUTHENTICATION_CREATE,
    #             "value": {
    #                 "id": 1,
    #                 "source_id": 1,
    #                 "resource_type": "Application",
    #                 "resource_id": "1",
    #                 "application_type_id": test_application_id,
    #             },
    #             "expected_cost_mgmt_match": False,
    #             "expected_fn": _expected_authentication_create,
    #         },
    #         {
    #             "event": source_integration.KAFKA_AUTHENTICATION_CREATE,
    #             "value": {
    #                 "id": 1,
    #                 "source_id": 1,
    #                 "resource_type": "Application",
    #                 "resource_id": "1",
    #                 "application_type_id": test_application_id,
    #             },
    #             "expected_cost_mgmt_match": True,
    #             "expected_fn": _expected_authentication_create,
    #         },
    #         {
    #             "event": source_integration.KAFKA_AUTHENTICATION_CREATE,
    #             "value": {
    #                 "id": 1,
    #                 "source_id": 1,
    #                 "resource_type": "Application",
    #                 "resource_id": "1",
    #                 "application_type_id": test_application_id,
    #             },
    #             "expected_cost_mgmt_match": True,
    #             "expected_fn": _expected_authentication_create,
    #         },
    #         {
    #             "event": source_integration.KAFKA_AUTHENTICATION_UPDATE,
    #             "value": {
    #                 "id": 1,
    #                 "source_id": 1,
    #                 "resource_type": "Application",
    #                 "resource_id": "1",
    #                 "application_type_id": test_application_id,
    #             },
    #             "expected_cost_mgmt_match": True,
    #             "expected_fn": _expected_authentication_create,
    #         },
    #     ]

    #     for test in test_matrix:
    #         msg_data = MsgDataGenerator(event_type=test.get("event"), value=test.get("value")).get_data()
    #         with patch.object(
    #             SourcesHTTPClient,
    #             "get_application_type_is_cost_management",
    #             return_value=test.get("expected_cost_mgmt_match"),
    #         ):
    #             with patch.object(
    #                 SourcesHTTPClient,
    #                 "get_source_id_from_applications_id",
    #                 return_value=test.get("expected_cost_mgmt_match"),
    #             ):
    #                 with patch.object(SourcesHTTPClient, "get_source_details", return_value={"source_type_id": "1"}):
    #                     with patch.object(SourcesHTTPClient, "get_source_type_name", return_value="amazon"):
    #                         # process_message(test_application_id, msg_data)
    #                         test.get("expected_fn")(msg_data, test, mock_save_auth_info)

    # @patch.object(Config, "SOURCES_API_URL", "http://www.sources.com")
    # def test_process_message_destroy(self):
    #     """Test the process_message function for application and source destroy."""
    #     test_application_id = 2

    #     def _expected_destroy(msg_data):
    #         query_results = Sources.objects.filter(source_id=msg_data.get("source_id"))
    #         self.assertTrue(query_results.exists())
    #         self.assertTrue(query_results.first().pending_delete)

    #     test_matrix = [
    #         {
    #             "event": source_integration.KAFKA_APPLICATION_DESTROY,
    #             "value": {"id": 1, "source_id": 1, "application_type_id": test_application_id},
    #             "expected_fn": _expected_destroy,
    #         },
    #         {
    #             "event": source_integration.KAFKA_SOURCE_DESTROY,
    #             "value": {"id": 1, "source_id": 1, "application_type_id": test_application_id},
    #             "expected_fn": _expected_destroy,
    #         },
    #     ]

    #     for test in test_matrix:
    #         storage.create_source_event(test.get("value").get("source_id"), Config.SOURCES_FAKE_HEADER, 3)
    #         msg_data = MsgDataGenerator(event_type=test.get("event"), value=test.get("value")).get_data()
    #         with patch.object(SourcesHTTPClient, "get_source_details", return_value={"source_type_id": "1"}):
    #             with patch.object(SourcesHTTPClient, "get_source_type_name", return_value="amazon"):
    #                 # process_message(test_application_id, msg_data)
    #                 test.get("expected_fn")(msg_data)
    #         Sources.objects.all().delete()

    # @patch.object(Config, "SOURCES_API_URL", "http://www.sources.com")
    # @patch("sources.kafka_listener.sources_network_info", returns=None)
    # def test_process_message_update(self, mock_sources_network_info):
    #     """Test the process_message function for authentication and source update."""
    #     test_application_id = 2

    #     def _expected_update(test):
    #         query_results = Sources.objects.filter(source_id=test.get("value").get("source_id"))
    #         self.assertTrue(query_results.exists())
    #         self.assertTrue(query_results.first().pending_update)

    #     test_matrix = [
    #         {
    #             "event": source_integration.KAFKA_AUTHENTICATION_UPDATE,
    #             "value": {
    #                 "id": 1,
    #                 "source_id": 1,
    #                 "resource_type": "Application",
    #                 "resource_id": "1",
    #                 "application_type_id": test_application_id,
    #             },
    #             "expected_cost_mgmt_match": True,
    #             "expected_fn": _expected_update,
    #         }
    #     ]

    #     for test in test_matrix:
    #         test_source = Sources(
    #             source_id=test.get("value").get("source_id"),
    #             koku_uuid="testkokuid",
    #             auth_header=Config.SOURCES_FAKE_HEADER,
    #             offset=4,
    #         )
    #         test_source.save()
    #         msg_data = MsgDataGenerator(event_type=test.get("event"), value=test.get("value")).get_data()
    #         with patch.object(
    #             SourcesHTTPClient,
    #             "get_application_type_is_cost_management",
    #             return_value=test.get("expected_cost_mgmt_match"),
    #         ):
    #             with patch.object(SourcesHTTPClient, "get_source_details", return_value={"source_type_id": "1"}):
    #                 with patch.object(SourcesHTTPClient, "get_source_type_name", return_value="amazon"):
    #                     with patch.object(SourcesHTTPClient, "get_source_id_from_applications_id", return_value=1):
    #                         # process_message(test_application_id, msg_data)
    #                         self.assertIsNone(msg_data)
    #                         test.get("expected_fn")(test)
    #                         Sources.objects.all().delete()

    # @patch("sources.kafka_listener.process_message")
    # def test_listen_for_messages(self, mock_process_message):
    #     """Test to listen for kafka messages."""
    #     future_mock = asyncio.Future()
    #     future_mock.set_result("test result")
    #     mock_process_message.return_value = future_mock

    #     cost_management_app_type = 2

    #     test_matrix = [
    #         {
    #             "test_value": '{"id": 1, "source_id": 1, "application_type_id": 2',
    #             "operation": "Application.create",
    #             "expected_process": False,
    #         },
    #         {
    #             "test_value": json.dumps({"id": 1, "source_id": 1, "application_type_id": 2}),
    #             "operation": "Source.update",
    #             "expected_process": False,
    #         },
    #         {
    #             "test_value": json.dumps({"id": 1, "source_id": 1, "application_type_id": 2}),
    #             "operation": "Application.create",
    #             "expected_process": True,
    #         },
    #     ]
    #     for test in test_matrix:
    #         msg = ConsumerRecord(
    #             topic="platform.sources.event-stream",
    #             offset=5,
    #             event_type=test.get("operation"),
    #             auth_header="testheader",
    #             value=bytes(test.get("test_value"), encoding="utf-8"),
    #         )

    #         mock_consumer = MockKafkaConsumer([msg])

    #         source_integration.listen_for_messages(msg, mock_consumer, cost_management_app_type)

    #         if test.get("expected_process"):
    #             mock_process_message.assert_called()
    #         else:
    #             mock_process_message.assert_not_called()

    # @patch("sources.kafka_listener.process_message")
    # def test_listen_for_messages_db_error(self, mock_process_message):
    #     """Test to listen for kafka messages with database errors."""
    #     future_mock = asyncio.Future()
    #     future_mock.set_result("test result")

    #     cost_management_app_type = 2

    #     test_matrix = [
    #         {
    #             "test_value": json.dumps({"id": 1, "source_id": 1, "application_type_id": 2}),
    #             "side_effect": InterfaceError,
    #         },
    #         {
    #             "test_value": json.dumps({"id": 1, "source_id": 1, "application_type_id": 2}),
    #             "side_effect": OperationalError,
    #         },
    #     ]

    #     for test in test_matrix:
    #         with self.subTest(test=test):
    #             msg = ConsumerRecord(
    #                 topic="platform.sources.event-stream",
    #                 offset=5,
    #                 event_type="Application.create",
    #                 auth_header="testheader",
    #                 value=bytes(test.get("test_value"), encoding="utf-8"),
    #             )

    #             mock_consumer = MockKafkaConsumer([msg])

    #             mock_process_message.side_effect = test.get("side_effect")
    #             with patch("sources.kafka_listener.close_and_set_db_connection") as close_mock:
    #                 with patch.object(Config, "RETRY_SECONDS", 0):
    #                     source_integration.listen_for_messages(msg, mock_consumer, cost_management_app_type)
    #                     close_mock.assert_called()

    # @patch("sources.kafka_listener.process_message")
    # def test_listen_for_messages_other_errors(self, mock_process_message):
    #     """Test to listen for kafka messages with network errors and source not found."""
    #     future_mock = asyncio.Future()
    #     future_mock.set_result("test result")

    #     cost_management_app_type = 2

    #     test_matrix = [
    #         {
    #             "test_value": json.dumps({"id": 1, "source_id": 1, "application_type_id": 2}),
    #             "side_effect": SourcesHTTPClientError,
    #         },
    #         {
    #             "test_value": json.dumps({"id": 1, "source_id": 1, "application_type_id": 2}),
    #             "side_effect": SourceNotFoundError,
    #         },
    #     ]

    #     for test in test_matrix:
    #         msg = ConsumerRecord(
    #             topic="platform.sources.event-stream",
    #             offset=5,
    #             event_type="Application.create",
    #             auth_header="testheader",
    #             value=bytes(test.get("test_value"), encoding="utf-8"),
    #         )

    #         mock_consumer = MockKafkaConsumer([msg])

    #         mock_process_message.side_effect = test.get("side_effect")
    #         with patch("sources.kafka_listener.close_and_set_db_connection") as close_mock:
    #             with patch.object(Config, "RETRY_SECONDS", 0):
    #                 source_integration.listen_for_messages(msg, mock_consumer, cost_management_app_type)
    #                 close_mock.assert_not_called()

    @patch("sources.kafka_listener.execute_koku_provider_op")
    def test_process_synchronize_sources_msg_db_error(self, mock_process_message):
        """Test processing synchronize messages with database errors."""
        provider = Sources.objects.create(**self.aws_source)
        provider.save()

        test_queue = queue.PriorityQueue()

        test_matrix = [
            {"test_value": {"operation": "update", "provider": provider}, "side_effect": InterfaceError},
            {"test_value": {"operation": "update", "provider": provider}, "side_effect": OperationalError},
        ]

        for i, test in enumerate(test_matrix):
            mock_process_message.side_effect = test.get("side_effect")
            with patch("sources.kafka_listener.close_and_set_db_connection") as close_mock:
                with patch.object(Config, "RETRY_SECONDS", 0):
                    process_synchronize_sources_msg((i, test["test_value"]), test_queue)
                    close_mock.assert_called()
        for i in range(2):
            priority, _ = test_queue.get_nowait()
            self.assertEqual(priority, i)

    @patch("sources.kafka_listener.execute_koku_provider_op")
    def test_process_synchronize_sources_msg_integration_error(self, mock_process_message):
        """Test processing synchronize messages with database errors."""
        provider = Sources.objects.create(**self.aws_source)
        provider.save()

        test_queue = queue.PriorityQueue()

        test_matrix = [
            {"test_value": {"operation": "update", "provider": provider}, "side_effect": IntegrityError},
            {"test_value": {"operation": "update", "provider": provider}, "side_effect": SourcesIntegrationError},
        ]

        for i, test in enumerate(test_matrix):
            mock_process_message.side_effect = test.get("side_effect")
            with patch.object(Config, "RETRY_SECONDS", 0):
                process_synchronize_sources_msg((i, test["test_value"]), test_queue)
        for i in range(2):
            priority, _ = test_queue.get_nowait()
            self.assertEqual(priority, i)

    # @patch("sources.kafka_listener.execute_koku_provider_op")
    # def test_process_synchronize_sources_msg(self, mock_process_message):
    #     """Test processing synchronize messages."""
    #     provider = Sources(**self.aws_source)

    #     test_queue = queue.PriorityQueue()

    #     messages = [
    #         {"operation": "create", "provider": provider, "offset": provider.offset},
    #         {"operation": "update", "provider": provider},
    #     ]

    #     for msg in messages:
    #         with patch("sources.storage.clear_update_flag") as mock_clear_flag:
    #             process_synchronize_sources_msg((0, msg), test_queue)
    #             mock_clear_flag.assert_called()

    #     msg = {"operation": "destroy", "provider": provider}
    #     with patch("sources.storage.clear_update_flag") as mock_clear_flag:
    #         process_synchronize_sources_msg((0, msg), test_queue)
    #         mock_clear_flag.assert_not_called()

    def test_storage_callback_create(self):
        """Test storage callback puts create task onto queue."""
        local_source = Sources(**self.aws_local_source, pending_update=True)
        local_source.save()

        with patch("sources.kafka_listener.execute_process_queue"):
            storage_callback("", local_source)
            _, msg = PROCESS_QUEUE.get_nowait()
            self.assertEqual(msg.get("operation"), "create")

    def test_storage_callback_update(self):
        """Test storage callback puts update task onto queue."""
        uuid = self.aws_local_source.get("source_uuid")
        local_source = Sources(**self.aws_local_source, koku_uuid=uuid, pending_update=True)
        local_source.save()

        with patch("sources.kafka_listener.execute_process_queue"), patch(
            "sources.storage.screen_and_build_provider_sync_create_event", return_value=False
        ):
            storage_callback("", local_source)
            _, msg = PROCESS_QUEUE.get_nowait()
            self.assertEqual(msg.get("operation"), "update")

    def test_storage_callback_update_and_delete(self):
        """Test storage callback only deletes on pending update and delete."""
        uuid = self.aws_local_source.get("source_uuid")
        local_source = Sources(**self.aws_local_source, koku_uuid=uuid, pending_update=True, pending_delete=True)
        local_source.save()

        with patch("sources.kafka_listener.execute_process_queue"), patch(
            "sources.storage.screen_and_build_provider_sync_create_event", return_value=False
        ):
            storage_callback("", local_source)
            _, msg = PROCESS_QUEUE.get_nowait()
            self.assertEqual(msg.get("operation"), "destroy")
