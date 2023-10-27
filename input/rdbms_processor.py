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

"""This module contains the implementation of a RDBMS processor that can be used to read data from a RDBMS.
"""

import abc

import apache_beam as beam
from apache_beam.io.jdbc import ReadFromJdbc
from apache_beam.typehints.schemas import LogicalType, MillisInstant

from dofns.parDoFunctions import DebugDoFn
from input.base_source_processor import BaseSourceProcessor


# Define a custom logical type for database strings
@LogicalType.register_logical_type
class db_str(LogicalType):
    @classmethod
    def urn(cls):
        return "beam:logical_type:javasdk:v1"

    @classmethod
    def language_type(cls):
        return str

    def to_language_type(self, value):
        return str(value)

    def to_representation_type(self, value):
        return str(value)


# Define an RdbmsProcessor class that extends the BaseSourceProcessor class
class RdbmsProcessor(BaseSourceProcessor, abc.ABC):
    """
        The JDBCProcessor class is a component within a data ingestion framework that handles
        the processing of data using the apache beam's JDBCIO from diffrent RDBMS
    """

    def __init__(self, task):
        self.task = task
        LogicalType.register_logical_type(MillisInstant)

    # Define an expand method that reads data from a JDBC source and outputs valid or invalid records
    def expand(self, p_input):
        result = (p_input | 'Read from jdbc' >> ReadFromJdbc(
            output_parallelization=True,
            table_name=self.task['source_db']['rdbms_table'],
            driver_class_name=self.task['rdbms_props']['driver_class_name'],
            jdbc_url=self.task['rdbms_props']['jdbc_url'],
            username=self.task['rdbms_props']['username'],
            password=self.task['rdbms_props']['password'],
            query=self.task['source_db']['query'],
            classpath=self.task['rdbms_props']['classpath']))

        # Output valid and invalid records
        data = result | "DebugRows" >> beam.ParDo(DebugDoFn()).with_outputs("valid",
                                                                            "invalid")

        return data
