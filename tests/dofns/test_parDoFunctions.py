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

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from dofns.parDoFunctions import ProcessJsonWithoutFlatten, ConvertToJson, FileOperations, ReadFileContent, \
    Triaging

path = os.path.dirname(os.path.dirname(__file__))


def remove_ts(element, tag):
    if tag in element:
        del element[tag]
    return element


class ProcessJsonWithoutFlattenTest(unittest.TestCase):

    def setUp(self) -> None:
        self.task = {}
        self.pipeline_options = PipelineOptions.from_dictionary(self.task)

    def test_valid(self):
        test_data = [("id", """{"col1": "val1", "col2": "val2"}""")]
        expected = [{'col1': 'val1', 'col2': 'val2'}]

        with TestPipeline(options=self.pipeline_options) as pipeline:
            data = pipeline | beam.Create(test_data)

            passed_rec = data | beam.ParDo(ProcessJsonWithoutFlatten(self.task)).with_outputs(
                "valid", "invalid")

            assert_that(passed_rec["valid"], equal_to(expected))

    def test_invalid(self):
        test_data = [("id", {"col1": "val1", "col2": "val2"})]
        expected = [{'job_id': '', 'error_record': "{'col1': 'val1', 'col2': 'val2'}", 'error_type': 'Invalid Json',
                     'error_desc': 'the JSON object must be str, bytes or bytearray, not dict'}]

        with TestPipeline(options=self.pipeline_options) as pipeline:
            data = pipeline | beam.Create(test_data)

            passed_rec = data | beam.ParDo(ProcessJsonWithoutFlatten(self.task)).with_outputs(
                "valid", "invalid")

            data_to_assert = passed_rec["invalid"] | beam.Map(remove_ts, "insert_ts")

            assert_that(data_to_assert, equal_to(expected))


class ConvertToJsonTest(unittest.TestCase):

    def setUp(self) -> None:
        self.task = {}
        self.pipeline_options = PipelineOptions.from_dictionary(self.task)

    def test_valid(self):
        test_data = [("id", """<bookstore>
  <book category="Science Fiction">
    <title>Snow Crash</title>
    <author>Neal Stephenson</author>
    <price>14.99</price>
  </book>
  <book category="Mystery">
    <title>The Girl with the Dragon Tattoo</title>
    <author>Stieg Larsson</author>
    <price>9.99</price>
  </book>
  <book category="Romance">
    <title>The Notebook</title>
    <author>Nicholas Sparks</author>
    <price>7.99</price>
  </book>
</bookstore>
""", "filename")]
        expected = [('id', {'bookstore': {'book': [
            {'@category': 'Science Fiction', 'title': 'Snow Crash', 'author': 'Neal Stephenson', 'price': '14.99'},
            {'@category': 'Mystery', 'title': 'The Girl with the Dragon Tattoo', 'author': 'Stieg Larsson',
             'price': '9.99'},
            {'@category': 'Romance', 'title': 'The Notebook', 'author': 'Nicholas Sparks', 'price': '7.99'}]}},
                     'filename')]

        with TestPipeline(options=self.pipeline_options) as pipeline:
            data = pipeline | beam.Create(test_data)

            passed_rec = data | beam.ParDo(ConvertToJson("job-name", "output-table")).with_outputs(
                "valid", "invalid")

            assert_that(passed_rec["valid"], equal_to(expected))

    def test_invalid(self):
        test_data = [("id", {"col1": "val1", "col2": "val2"}, "filename")]
        expected = [OrderedDict(
            [('job_name', 'job-name'), ('job_id', ''), ('error_record', "{'col1': 'val1', 'col2': 'val2'}"),
             ('error_type', 'Invalid Xml'), ('error_desc', "a bytes-like object is required, not 'dict'"),
             ('target_table', 'output-table'), ('source_name', 'filename')])]

        with TestPipeline(options=self.pipeline_options) as pipeline:
            data = pipeline | beam.Create(test_data)

            passed_rec = data | beam.ParDo(ConvertToJson("job-name", "output-table")).with_outputs(
                "valid", "invalid")

            data_to_assert = passed_rec["invalid"] | beam.Map(remove_ts, "insert_ts")

            assert_that(data_to_assert, equal_to(expected))


class FileMetaData:
    def __init__(self, path):
        self.path = path


class FileOperationsTest(unittest.TestCase):

    def setUp(self) -> None:
        self.task = {}
        self.pipeline_options = PipelineOptions.from_dictionary(self.task)

    def test_valid(self):
        file_meta_data = FileMetaData('/path/to/file')
        test_data = [(file_meta_data, [{"col1": "val1", "col2": "val2"}, {"col1": "val1", "col2": "val2"}])]
        expected = [('/path/to/file', {'col1': 'val1', 'col2': 'val2'}),
                    ('/path/to/file', {'col1': 'val1', 'col2': 'val2'})]

        with TestPipeline(options=self.pipeline_options) as pipeline:
            data = pipeline | beam.Create(test_data)

            passed_rec = data | beam.ParDo(FileOperations())

            assert_that(passed_rec, equal_to(expected))


class ReadFileContentTest(unittest.TestCase):

    def setUp(self) -> None:
        self.task = {}
        self.pipeline_options = PipelineOptions.from_dictionary(self.task)

    def test_valid(self):
        test_data = [('/path/to/file', [{"col1": "val1", "col2": "val2"}, {"col1": "val1", "col2": "val2"}])]
        expected = [('/path/to/file', {'col1': 'val1', 'col2': 'val2'}),
                    ('/path/to/file', {'col1': 'val1', 'col2': 'val2'})]

        with TestPipeline(options=self.pipeline_options) as pipeline:
            data = pipeline | beam.Create(test_data)

            passed_rec = data | beam.ParDo(ReadFileContent())

            assert_that(passed_rec, equal_to(expected))


class TriagingTest(unittest.TestCase):

    def setUp(self) -> None:
        self.task = {}
        self.pipeline_options = PipelineOptions.from_dictionary(self.task)

    def test_valid(self):
        test_data = [["key", "value"]]
        expected = ["key\u0001value"]

        with TestPipeline(options=self.pipeline_options) as pipeline:
            data = pipeline | beam.Create(test_data)

            passed_rec = data | beam.ParDo(Triaging())

            assert_that(passed_rec, equal_to(expected))
