# Copyright 2023 Datametica Solutions Pvt. Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import argparse
import os
import unittest

from apache_beam.options.pipeline_options import PipelineOptions

from options.ingestion_options import IngestionOptions
from utility.basic_utilities import read_yaml_from_local, parse_schema_as_dict_local

path = os.path.dirname(os.path.dirname(__file__))


class CommonUtilityTest(unittest.TestCase):

    def setUp(self) -> None:
        argv = [f'--job_prop_file={path}/resources/CSVProcessor/configFiles/csv_job.yml',
                f'--task_prop_file={path}/resources/CSVProcessor/configFiles/csv_task.yml',
                '--code_bucket=ssedelivery',
                '--data_bucket=ssedelivery',
                '--log_level=INFO',
                '--job_name=unit_test']
        parser = argparse.ArgumentParser()
        known_args, pipeline_args = parser.parse_known_args(argv)
        pipeline_options = PipelineOptions(pipeline_args, save_main_session=False)
        self.options = pipeline_options.view_as(IngestionOptions)

    def test_parse_schema_as_dict(self):
        schema_file_path = f'{path}/resources/error_table.json'
        actual_schema_dict = parse_schema_as_dict_local(schema_file_path)
        expected_schema_dict = {'fields': [{'name': 'job_name', 'type': 'String'}, {'name': 'job_id', 'type': 'String'},
                                           {'name': 'error_record', 'type': 'String'},
                                           {'name': 'error_type', 'type': 'String'},
                                           {'name': 'error_desc', 'type': 'String'},
                                           {'name': 'target_table', 'type': 'String'},
                                           {'name': 'source_name', 'type': 'String'},
                                           {'name': 'insert_ts', 'type': 'timestamp'}]}

        self.assertDictEqual(expected_schema_dict, actual_schema_dict)

    def test_read_task_prop_from_local(self):
        actual_yaml_config = read_yaml_from_local(self.options.task_prop_file)
        expected_yaml_config = {'tasks': [
            {'task_name': 'test_load', 'job_name': 'csv-ingestion-dif-1', 'input_processor': 'CSVProcessor',
             'dataflow_writer': 'BigQueryStreamWriter', 'is_filename_req_in_target': False,
             'job_prop_file': 'resources/csv/csv_job.yml', 'data_file': 'test.csv',
             'source_schema_validation': False, 'delimited_file_props': {'delimiter': ',', 'header_exists': True},
             'targets': [{'bigquery_stream': {'target_table': 'Employee_Details',
                                              'write_disposition': 'write_append'}}]}]}
        self.assertDictEqual(actual_yaml_config, expected_yaml_config)
