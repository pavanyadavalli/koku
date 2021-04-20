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
"""Test the common util functions."""
import gzip
import json
import types
from datetime import date
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from os.path import exists

from dateutil import parser
from django.test import TestCase
from tenant_schemas.utils import schema_context

import masu.util.common as common_utils
from api.models import Provider
from api.utils import DateHelper
from masu.config import Config
from masu.external import LISTEN_INGEST
from masu.external import POLL_INGEST
from masu.test import MasuTestCase
from reporting.provider.aws.models import AWSCostEntryBill


class CommonUtilTests(MasuTestCase):
    """Test Common Masu functions."""

    def test_extract_uuids_from_string(self):
        """Test that a uuid is extracted from a string."""
        assembly_id = "882083b7-ea62-4aab-aa6a-f0d08d65ee2b"
        cur_key = f"/koku/20180701-20180801/{assembly_id}/koku-1.csv.gz"

        uuids = common_utils.extract_uuids_from_string(cur_key)
        self.assertEqual(len(uuids), 1)
        self.assertEqual(uuids.pop(), assembly_id)

    def test_extract_uuids_from_string_capitals(self):
        """Test that a uuid is extracted from a string with capital letters."""
        assembly_id = "882083B7-EA62-4AAB-aA6a-f0d08d65Ee2b"
        cur_key = f"/koku/20180701-20180801/{assembly_id}/koku-1.csv.gz"

        uuids = common_utils.extract_uuids_from_string(cur_key)
        self.assertEqual(len(uuids), 1)
        self.assertEqual(uuids.pop(), assembly_id)

    def test_stringify_json_data_list(self):
        """Test that each element of JSON is returned as a string."""
        data = [{"datetime": datetime.utcnow(), "float": 1.2, "int": 1, "str": "string"}, {"Decimal": Decimal("1.2")}]

        with self.assertRaises(TypeError):
            json.dumps(data)

        result = common_utils.stringify_json_data(data)

        self.assertIsInstance(result[0]["datetime"], str)
        self.assertIsInstance(result[0]["float"], str)
        self.assertIsInstance(result[0]["int"], str)
        self.assertIsInstance(result[0]["str"], str)
        self.assertIsInstance(result[1]["Decimal"], str)

    def test_stringify_json_data_dict(self):
        """Test that the dict block is covered."""
        data = {"datetime": datetime.utcnow(), "float": 1.2, "int": 1, "str": "string", "Decimal": Decimal("1.2")}

        with self.assertRaises(TypeError):
            json.dumps(data)

        result = common_utils.stringify_json_data(data)

        self.assertIsInstance(result["datetime"], str)
        self.assertIsInstance(result["float"], str)
        self.assertIsInstance(result["int"], str)
        self.assertIsInstance(result["str"], str)
        self.assertIsInstance(result["Decimal"], str)

    def test_ingest_method_type(self):
        """Test that the correct ingest method is returned for provider type."""
        test_matrix = [
            {"provider_type": Provider.PROVIDER_AWS, "expected_ingest": POLL_INGEST},
            {"provider_type": Provider.PROVIDER_AWS_LOCAL, "expected_ingest": POLL_INGEST},
            {"provider_type": Provider.PROVIDER_OCP, "expected_ingest": LISTEN_INGEST},
            {"provider_type": Provider.PROVIDER_AZURE_LOCAL, "expected_ingest": POLL_INGEST},
            {"provider_type": "NEW_TYPE", "expected_ingest": None},
        ]

        for test in test_matrix:
            ingest_method = common_utils.ingest_method_for_provider(test.get("provider_type"))
            self.assertEqual(ingest_method, test.get("expected_ingest"))

    def test_month_date_range_tuple(self):
        """Test month_date_range_tuple returns first of the month and first of next month."""
        test_date = datetime(year=2018, month=12, day=15)
        expected_start_month = datetime(year=2018, month=12, day=1)
        expected_start_next_month = datetime(year=2019, month=1, day=1)

        start_month, first_next_month = common_utils.month_date_range_tuple(test_date)

        self.assertEquals(start_month, expected_start_month)
        self.assertEquals(first_next_month, expected_start_next_month)

    def test_date_range(self):
        """Test that a date range generator is returned."""
        start_date = "2020-01-01"
        end_date = "2020-02-29"

        date_generator = common_utils.date_range(start_date, end_date)

        start_date = parser.parse(start_date)
        end_date = parser.parse(end_date)

        self.assertIsInstance(date_generator, types.GeneratorType)

        first_date = next(date_generator)
        self.assertEqual(first_date, start_date.date())
        for day in date_generator:
            self.assertIsInstance(day, date)
            self.assertGreater(day, start_date.date())
            self.assertLessEqual(day, end_date.date())
        self.assertEqual(day, end_date.date())

    def test_date_range_pair_date_args(self):
        """Test that start and end dates are returned by this generator with date args passed instead of str."""
        start_date = date(2020, 1, 1)
        end_date = date(2020, 2, 29)
        step = 3

        date_generator = common_utils.date_range_pair(start_date, end_date, step=step)

        start_date = datetime(start_date.year, start_date.month, start_date.day)
        end_date = datetime(end_date.year, end_date.month, end_date.day)

        self.assertIsInstance(date_generator, types.GeneratorType)

        first_start, first_end = next(date_generator)
        self.assertEqual(first_start, start_date.date())
        self.assertEqual(first_end, start_date.date() + timedelta(days=step))

        for start, end in date_generator:
            self.assertIsInstance(start, date)
            self.assertIsInstance(end, date)
            self.assertGreater(start, start_date.date())
            self.assertLessEqual(end, end_date.date())
        self.assertEqual(end, end_date.date())

    def test_date_range_pair(self):
        """Test that start and end dates are returned by this generator."""
        start_date = "2020-01-01"
        end_date = "2020-02-29"
        step = 3

        date_generator = common_utils.date_range_pair(start_date, end_date, step=step)

        start_date = parser.parse(start_date)
        end_date = parser.parse(end_date)

        self.assertIsInstance(date_generator, types.GeneratorType)

        first_start, first_end = next(date_generator)
        self.assertEqual(first_start, start_date.date())
        self.assertEqual(first_end, start_date.date() + timedelta(days=step))

        for start, end in date_generator:
            self.assertIsInstance(start, date)
            self.assertIsInstance(end, date)
            self.assertGreater(start, start_date.date())
            self.assertLessEqual(end, end_date.date())
        self.assertEqual(end, end_date.date())

    def test_date_range_pair_one_day(self):
        """Test that generator works for a single day."""
        start_date = "2020-01-01"
        end_date = start_date
        step = 3

        date_generator = common_utils.date_range_pair(start_date, end_date, step=step)

        start_date = parser.parse(start_date)
        end_date = parser.parse(end_date)

        self.assertIsInstance(date_generator, types.GeneratorType)

        first_start, first_end = next(date_generator)
        self.assertEqual(first_start, start_date.date())
        self.assertEqual(first_end, end_date.date())
        with self.assertRaises(StopIteration):
            next(date_generator)

    def test_safe_float(self):
        """Test the safe_float method handles good and bad inputs."""
        out = common_utils.safe_float("foo")
        self.assertEqual(out, float(0))

        out = common_utils.safe_float("1.1")
        self.assertEqual(out, float("1.1"))

    def test_safe_dict(self):
        """Test the safe_dict method handles good and bad inputs."""
        out = common_utils.safe_dict(1)
        self.assertEqual(out, "{}")

        expected = '{"a": "b", "c": "d"}'
        out = common_utils.safe_dict(expected)
        self.assertEqual(out, expected)

    def test_get_path_prefix(self):
        """Test that path prefix is returned."""
        account = "10001"
        provider_type = Provider.PROVIDER_AWS
        provider_uuid = self.aws_provider_uuid
        start_date = datetime.utcnow().date()
        year = start_date.strftime("%Y")
        month = start_date.strftime("%m")
        expected_path_prefix = f"{Config.WAREHOUSE_PATH}/{Config.PARQUET_DATA_TYPE}"
        expected_path = (
            f"{expected_path_prefix}/{account}/{provider_type}/" f"source={provider_uuid}/year={year}/month={month}"
        )

        path = common_utils.get_path_prefix(account, provider_type, provider_uuid, start_date, "parquet")
        self.assertEqual(path, expected_path)

        # Test with report_type
        report_type = "pod_report"
        expected_path = (
            f"{expected_path_prefix}/{account}/{provider_type}/{report_type}/"
            f"source={provider_uuid}/year={year}/month={month}"
        )
        path = common_utils.get_path_prefix(
            account, provider_type, provider_uuid, start_date, "parquet", report_type=report_type
        )
        self.assertEqual(path, expected_path)

    def test_get_hive_table_path(self):
        """Test that we resolve the path for a Hive table."""
        account = "10001"
        provider_type = Provider.PROVIDER_AWS

        expected_path_prefix = f"{Config.WAREHOUSE_PATH}/{Config.PARQUET_DATA_TYPE}"
        expected_path = f"{expected_path_prefix}/{account}/{provider_type}"

        path = common_utils.get_hive_table_path(account, provider_type)
        self.assertEqual(path, expected_path)

        # Test with report_type
        report_type = "pod_report"
        expected_path = f"{expected_path_prefix}/{account}/{provider_type}/{report_type}"
        path = common_utils.get_hive_table_path(account, provider_type, report_type=report_type)
        self.assertEqual(path, expected_path)

    def test_determine_if_full_summary_update_needed(self):
        """Test that we process full month under the correct conditions."""
        dh = DateHelper()

        with schema_context(self.schema):
            bills = AWSCostEntryBill.objects.all()
            current_month_bill = bills.filter(billing_period_start=dh.this_month_start).first()
            last_month_bill = bills.filter(billing_period_start=dh.last_month_start).first()

            # Current month, previously summarized
            self.assertFalse(common_utils.determine_if_full_summary_update_needed(current_month_bill))
            # Previous month
            self.assertTrue(common_utils.determine_if_full_summary_update_needed(last_month_bill))

            current_month_bill.summary_data_creation_datetime = None
            current_month_bill.save()

            # Current month, has not been summarized before
            self.assertTrue(common_utils.determine_if_full_summary_update_needed(current_month_bill))

    def test_split_alphanumeric_string(self):
        """Test the alpha-numeric split function."""
        s = "4 GiB"

        expected = ["4 ", "GiB"]
        result = list(common_utils.split_alphanumeric_string(s))
        self.assertEqual(result, expected)

    def test_strip_characters_from_column_name(self):
        """Test that column names are converted properly."""
        bad_str = r"column\one:two-three four,five/six_seven"
        expected = "column_one_two_three_four_five_six_seven"

        result = common_utils.strip_characters_from_column_name(bad_str)
        self.assertEqual(result, expected)


class NamedTemporaryGZipTests(TestCase):
    """Tests for NamedTemporaryGZip."""

    def test_temp_gzip_is_removed(self):
        """Test that the gzip file is removed."""
        with common_utils.NamedTemporaryGZip() as temp_gzip:
            file_name = temp_gzip.name
            self.assertTrue(exists(file_name))

        self.assertFalse(exists(file_name))

    def test_gzip_is_readable(self):
        """Test the the written gzip file is readable."""
        test_data = "Test Read Gzip"
        with common_utils.NamedTemporaryGZip() as temp_gzip:

            temp_gzip.write(test_data)
            temp_gzip.close()

            with gzip.open(temp_gzip.name, "rt") as f:
                read_data = f.read()

        self.assertEquals(test_data, read_data)
