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

from input.csv_processor import CSVProcessor
from p_transforms.select_req_columns import SelectRequiredColumns

path = os.path.dirname(os.path.dirname(__file__))


class SelectRequiredColumnsTest(unittest.TestCase):
    def setUp(self) -> None:
        self.expected_dict_valid = [OrderedDict([('col1', 'val1'), ('col2', 'val2'), ('source_name',
                                                                                      f'{path}\\resources/CSVProcessor/dataFiles/Student_Data.csv')]),
                                    OrderedDict([('col1', 'val1'), ('col2', 'val2'), ('source_name',
                                                                                      f'{path}\\resources/CSVProcessor/dataFiles/Student_Data.csv')])]

        self.task = {'source': 'delimited_file', 'jobid': 'test-job', 'job_name': 'csv-ingestion-dif-test',
                     'data_file': f'{path}\\resources/CSVProcessor/dataFiles/Student_Data.csv',
                     'delimited_file_props': {'delimiter': ',', 'header_exists': True}, 'log_level': 'info',
                     'required_columns': ['col1', 'col2']}

        self.pipeline_options = PipelineOptions.from_dictionary(self.task)

    def test_select_req_columns_valid(self):
        with TestPipeline(options=self.pipeline_options) as pipeline:
            data = pipeline | CSVProcessor(self.task)

            passed_rec = data["valid"] | SelectRequiredColumns(self.task)

            assert_that(passed_rec, equal_to(self.expected_dict_valid))
