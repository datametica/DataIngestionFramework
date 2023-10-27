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

"""This module contains the implementation of a delimited processor that can be used to read delimited files.
"""

import csv
from abc import ABC

import apache_beam as beam
import gcsfs
from apache_beam.io import fileio
from apache_beam.pvalue import TaggedOutput

from input.base_source_processor import BaseSourceProcessor
from utility.basic_utilities import parse_schema_as_dict, generate_error_record


class FileMetaData(beam.DoFn):
    """
    The FileMetaData class is a component within a data ingestion framework
    that handles the processing of delimited files by default (Comma-Separated Values) data. It is
    responsible for reading CSV files
    """

    def __init__(self, task_prop):

        self.task_prop = task_prop

        if "delimiter" in task_prop["delimited_file_props"]:
            self.delimiter = task_prop["delimited_file_props"]["delimiter"]
        else:
            self.delimiter = ","

        if "null_markers" in task_prop["delimited_file_props"]:
            self.null_markers = task_prop["delimited_file_props"]["null_markers"].split(",")
        else:
            self.null_markers = ["-1", "Null"]

        if "escapechar" in task_prop["delimited_file_props"]:
            self.escapechar = task_prop["delimited_file_props"]["escapechar"]
        else:
            self.escapechar = "\\"

        self.header_exist = bool(task_prop["delimited_file_props"]["header_exists"])

        if not self.header_exist:
            self.schema_dict = parse_schema_as_dict(
                task_prop["schema_file"], gcsfs.GCSFileSystem(project=task_prop["project"])
            )

    def process(self, element):
        try:
            filedata = element.read_utf8().splitlines()
            if "delimited_file_props" in self.task_prop:
                if "skip_rows" in self.task_prop["delimited_file_props"]:
                    filedata = filedata[int(self.task_prop["delimited_file_props"]["skip_rows"]):]
            csvFile = list(csv.reader(filedata, escapechar=self.escapechar, delimiter=self.delimiter))

            if self.header_exist:
                records = csvFile[1:]
                headers = csvFile[0]
            else:
                records = csvFile
                headers = list(self.schema_dict.keys())

            for record in records:
                record_dict = {}
                for i, value in enumerate(record):
                    header = headers[i] if i < len(headers) else f"column_{i + 1}"
                    record_dict[header] = value
                record_dict["source_name"] = element.metadata.path

                for k, v in record_dict.items():
                    if v in self.null_markers:
                        record_dict[k] = None

                yield TaggedOutput("valid", record_dict)

        except Exception as e:
            error_record = generate_error_record(element=element, exception=e, job_name=self.task_prop["job_name"],
                                                 target=self.task_prop["targets"], src=element.metadata.path,
                                                 err_type="Invalid Record", job_id=self.task_prop["job_id"])
            yield TaggedOutput("invalid", error_record)


class CSVProcessor(BaseSourceProcessor, ABC):
    """
    The CSVProcessor class is a component within a data ingestion framework
    that handles the processing of delimited files by default (Comma-Separated Values) data. It is
    responsible for reading delimited files
    """

    def __init__(self, prop_dict):
        self.task = prop_dict

    def expand(self, p_input):
        read_data = p_input | "Read" >> fileio.MatchFiles(
            self.task["data_file"]) | "Read Matches" >> fileio.ReadMatches()

        file_content = read_data | "Fetch Meta Data From Files" >> beam.ParDo(
            FileMetaData(self.task)).with_outputs("valid",
                                                  "invalid")

        return file_content
