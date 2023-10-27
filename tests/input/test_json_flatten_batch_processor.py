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

import os
import unittest

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from input.json_flatten_batch_processor import JsonFlattenBatchProcessor

path = os.path.dirname(os.path.dirname(__file__))


def remove_ele(element, tag):
    if tag in element:
        del element[tag]
    return element


class JSONFlattenBatchProcessorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.task = {'source': 'json', 'job_name': 'json-flatten-ingestion-dif-test',
                     'data_file': f'{path}/resources/JSONProcessor/dataFiles/json_flatten_batch.json',
                     'target': {'schema_file': f'{path}/resources/JSONProcessor/schemaFiles/Employee_schema.json'},
                     'log_level': 'INFO',
                     'column_name_map': f'{path}/resources/JSONProcessor/dataFiles/colNameMap.json'}

        self.task_for_nest = {'source': 'json', 'job_name': 'json-flatten-ingestion-dif-test',
                              'data_file': f'{path}/resources/JSONProcessor/dataFiles/json_flatten_nest.json',
                              'target': {
                                  'schema_file': f'{path}/resources/JSONProcessor/schemaFiles/Employee_schema.json'},
                              'log_level': 'INFO',
                              'column_name_map': f'{path}/resources/JSONProcessor/dataFiles/colNameMapNest.json'}

        self.expected_dict_valid = [
            {'emp_code': 1, 'manager_code': 'RNnZuTIcIAETRGooYubw',
             'senior_manager_code': 'YBaVnrfSYQyPFBFykheI', 'lead_manager_code': 'LQoKeHCTCLNBvEpPwFhk',
             'company_code': 'sVvqUSCISXbnxhsdvyCf',
             'FileName': f'{path}/resources/JSONProcessor/dataFiles/json_flatten_batch.json',
             'source_name': f'{path}/resources/JSONProcessor/dataFiles/json_flatten_batch.json'}]

        self.expected_dict_valid_nest = [
            {"id": 3485, "address_line1": "HlTIaghlYiUIRRSvPNtH", "address_line2": "qcFnCrEJEQFTwbwssZZS",
             "pincode": 8622,
             'FileName': f'{path}/resources/JSONProcessor/dataFiles/json_flatten_nest.json',
             'source_name': f'{path}/resources/JSONProcessor/dataFiles/json_flatten_nest.json'}]

        self.pipeline_options = PipelineOptions.from_dictionary(self.task)

    def test_JSON_processor_valid(self):
        with TestPipeline(options=self.pipeline_options) as p:
            data = p | JsonFlattenBatchProcessor(self.task)
            passed_rec = data["valid"] | beam.Map(remove_ele, "RawMessageId")
            data["invalid"] | beam.Map(print)
            assert_that(passed_rec, equal_to(self.expected_dict_valid))

    def test_nest_JSON_processor_valid(self):
        with TestPipeline(options=self.pipeline_options) as p:
            data = p | JsonFlattenBatchProcessor(self.task_for_nest)
            passed_rec = data["valid"] | beam.Map(remove_ele, "RawMessageId")
            assert_that(passed_rec, equal_to(self.expected_dict_valid_nest))
