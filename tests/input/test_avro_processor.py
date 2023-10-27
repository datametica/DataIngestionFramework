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

from input.avro_processor import AvroProcessor

path = os.path.dirname(os.path.dirname(__file__))


class AvroProcessorTest(unittest.TestCase):
    def setUp(self) -> None:
        self.expected_dict_valid = [OrderedDict([('catalog_service_internal_identifier', 11), ('source_name',
                                                                                               f'{path}/resources/AvroProcessor/dataFiles/avro_test_data_test.avro'),
                                                 ('title', 'title'), ('asin', 'asin')])]
        self.task = {'source': 'avro', 'job_name': 'avro-ingestion-dif-test',
                     'data_file': f'{path}/resources/AvroProcessor/dataFiles/avro_test_data_test.avro',
                     'target': {'schema_file': f'{path}/resources/AvroProcessor/schema/avroSchemaPython.json'}}

        self.pipeline_options = PipelineOptions.from_dictionary(self.task)

    def test_avro_processor_valid(self):
        with TestPipeline(options=self.pipeline_options) as pipeline:
            data = pipeline | AvroProcessor(self.task)

            assert_that(data["valid"], equal_to(self.expected_dict_valid))
