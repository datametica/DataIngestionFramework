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
from collections import OrderedDict

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from input.json_processor import JsonProcessor as JSONProcessor

path = os.path.dirname(os.path.dirname(__file__))


class JSONProcessorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.task = {'source': 'json', 'job_name': 'parquet-ingestion-dif-test',
                     'data_file': f'{path}/resources/JSONProcessor/dataFiles/Employee.json',
                     'target': {'schema_file': f'{path}/resources/JSONProcessor/schemaFiles/Employee_schema.json'},
                     'log_level': 'INFO'}

        self.expected_dict_valid = [OrderedDict(
            [('emp_code', 1), ('manager_code', 'RNnZuTIcIAETRGooYubw'),
             ('senior_manager_code', 'YBaVnrfSYQyPFBFykheI'),
             ('lead_manager_code', 'LQoKeHCTCLNBvEpPwFhk'),
             ('company_code', 'sVvqUSCISXbnxhsdvyCf'),
             ('source_name',
              f'{path}/resources/JSONProcessor/dataFiles/Employee.json')])]
        self.pipeline_options = PipelineOptions.from_dictionary(self.task)

    def test_JSON_processor_valid(self):
        with TestPipeline(options=self.pipeline_options) as p:
            data = p | JSONProcessor(self.task)
            passed_rec = data["valid"]
            assert_that(passed_rec, equal_to(self.expected_dict_valid))
