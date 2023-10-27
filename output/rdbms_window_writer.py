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

"""This module contains the implementation of a RDBMS window writer that can be used to write data to RDBMS.
"""
import typing
from abc import ABC

import apache_beam as beam
from apache_beam import coders
from apache_beam.io.jdbc import WriteToJdbc
from apache_beam.transforms.window import FixedWindows
from google.cloud import bigquery

from dofns.remove_source_name import RemoveSourceName
from output.base_output_writer import BaseOutputWriter
from utility.basic_utilities import bigquery_schema_to_typing


class AddColumns(beam.DoFn):
    def __init__(self, Er):
        self.Er = Er

    def process(self, element):
        for i in self.Er:
            if i not in element.keys():
                element[i] = ""
        for e in element:
            if element[e] == "None":
                element[e] = ""
            element[e] = str(element[e])
        yield element


class RDBMSWindowWriter(BaseOutputWriter, ABC):
    def __init__(self, task, prop_dict):
        self.task = prop_dict
        self.schema = bigquery.Client(project=task["project"]).get_table(task["tablename"]).schema
        self.ExampleRow = typing.NamedTuple('ExampleRow', bigquery_schema_to_typing(self.schema))

    def expand(self, p_input):
        if "is_filename_req_in_target" in self.task:
            if str(self.task["is_filename_req_in_target"]).lower() == "false":
                p_input = (
                        p_input
                        | f"Remove Source Name {str(self.task['targets']).split('.')[-1]}"
                        >> beam.ParDo(RemoveSourceName(self.task))
                )

        coders.registry.register_coder(self.ExampleRow, coders.RowCoder)

        data = p_input | beam.ParDo(AddColumns(self.ExampleRow._fields)) | beam.Map(
            lambda x: self.ExampleRow(**x)).with_output_types(
            self.ExampleRow) | beam.WindowInto(FixedWindows(60)) | f"Write to RDBMS" >> WriteToJdbc(
            table_name=self.task["tablename"],
            driver_class_name=self.task['driver_class_name'],
            jdbc_url=self.task['jdbc_url'],
            username=self.task['username'],
            password=self.task['password'],
            classpath=[self.task['classpath']])

        return data
