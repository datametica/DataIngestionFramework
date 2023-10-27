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

import json
from abc import ABC
from datetime import datetime

import apache_beam as beam
from apache_beam.io.fileio import ReadableFile
from apache_beam.io.gcp import gce_metadata_util
from apache_beam.pvalue import TaggedOutput

from input.base_source_processor import BaseSourceProcessor


# Define a JsonReader class that extends the beam.DoFn class
class JsonReader(beam.DoFn, ABC):
    def __init__(self, prop_dict):
        self.task = prop_dict

    # Define a process method that reads JSON data and yields valid or invalid records
    def process(self, element):
        try:
            data = json.loads(element)

            # If a root key is specified in the task configuration, extract data from that key
            if "root" in self.task.keys():
                data = data[self.task["root"]]

            # If the data is a list, yield each row as a valid record
            if type(data) == list:
                for row in data:
                    row["source_name"] = self.task["data_file"]
                    yield TaggedOutput("valid", row)
            else:
                # If the data is not a list, yield it as a valid record
                data["source_name"] = self.task["data_file"]
                yield TaggedOutput("valid", data)

        except Exception as e:
            # If an exception occurs, yield an invalid record with error details
            error_record = {"job_id": gce_metadata_util.fetch_dataflow_job_id(), "error_record": str(element),
                            "error_type": "Invalid Record", "error_desc": str(e), "source_name": self.task["data_file"],
                            "insert_ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
            yield TaggedOutput("invalid", error_record)


# Define a JsonProcessor class that extends the BaseSourceProcessor class
class JsonProcessor(BaseSourceProcessor):
    """
    The JSONProcessor class is a component within a data ingestion framework
    that handles the processing of JSON data. It is responsible for parsing
    and extracting relevant information from JSON documents
    """

    def __init__(self, prop_dict):
        self.prop_dict = prop_dict
        self.task = prop_dict

    # Define an expand method that reads data from a JSON file and outputs valid or invalid records
    def expand(self, p_input):
        data = (p_input
                | 'Reading data from input file' >> beam.io.fileio.MatchFiles(file_pattern=self.task["data_file"])
                | beam.io.fileio.ReadMatches()
                | beam.Map(lambda file: file.read_utf8())
                | beam.ParDo(JsonReader(self.prop_dict)).with_outputs("valid",
                                                                      "invalid")
                )
        return data
