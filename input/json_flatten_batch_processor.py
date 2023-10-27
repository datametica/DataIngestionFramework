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

"""This module contains the implementation of a JSON processor that can be used to read JSON files.
"""

import abc

import apache_beam as beam
from apache_beam.io import fileio

from dofns.parDoFunctions import addUID, FileMetaData, FileOperations, FlattenJson
from input.base_source_processor import BaseSourceProcessor


class JsonFlattenBatchProcessor(BaseSourceProcessor, abc.ABC):
    """
    The JSONProcessor class is a component within a data ingestion framework
    that processes JSON data by flattening the nested structure and renaming
    columns based on a provided column name map in batch mode.
    """

    def __init__(self, task):
        self.task = task

    def expand(self, p):
        colName = p | "Column Name Map" >> beam.io.ReadFromText(self.task["column_name_map"])

        read = (
                p | "Read Json From GCS Continuously." >> fileio.MatchFiles(self.task['data_file'])
                | "Read Matches" >> fileio.ReadMatches()
        )

        file_content = read | "Fetch Meta Data From Files" >> beam.ParDo(FileMetaData())

        file_operation = file_content | "File Operations" >> beam.ParDo(FileOperations())

        add_unique_id = file_operation | "Adding Unique Ids for msg Triaging" >> beam.ParDo(addUID())

        converted_json = add_unique_id | "Flattening Json" >> beam.ParDo(FlattenJson(self.task),
                                                                         ColumnNameMap=beam.pvalue.AsList(
                                                                             colName)).with_outputs("valid",
                                                                                                    "invalid")

        return converted_json
