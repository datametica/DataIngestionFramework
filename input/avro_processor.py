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

"""This module contains the implementation of an Avro processor that can be used to read Avro data.
It also contains the implementation of a method that converts Avro data to a format that can be written to BigQuery.
"""
import collections
from abc import ABC
from datetime import datetime

import apache_beam as beam
from apache_beam.io.gcp import gce_metadata_util
from apache_beam.pvalue import TaggedOutput

# Import the BaseSourceProcessor class and the parse_schema_as_dict functions
from input.base_source_processor import BaseSourceProcessor


class ConvertToOrderedDictionary(beam.DoFn, ABC):
    """
    This class contains the implementation of a DoFn that converts Avro data to a format that can be written to BigQuery.
    """

    def __init__(self, task_dict):
        self.task_dict = task_dict

    # Define a process method that converts Avro data to an ordered dictionary format
    def process(self, element):
        try:
            rows = element
            dict_ = {}
            for item in rows:
                column_name = item
                column_value = rows[item]
                if isinstance(column_value, dict):
                    self.readDictionary(column_value, dict_, column_name)
                else:
                    dict_[column_name] = column_value
                    dict_["source_name"] = self.task_dict['data_file']
            yield TaggedOutput("valid", dict_)
        except Exception as e:
            error_record = {"job_id": gce_metadata_util.fetch_dataflow_job_id(), "error_record": str(element),
                            "error_type": "Invalid Record", "error_desc": str(e),
                            "source_name": self.task_dict['data_file'],
                            "insert_ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
            yield TaggedOutput("invalid", error_record)

    # Define a readDictionary method that recursively reads nested dictionaries
    def readDictionary(self, column_value, dict_, column_name):
        """This method recursively reads nested dictionaries.
        """
        innerDict_ = collections.OrderedDict()
        inner_rows = collections.OrderedDict(column_value.items())
        for innerItem in inner_rows:
            inner_column_name = innerItem
            inner_column_value = inner_rows[innerItem]
            if isinstance(inner_column_value, dict):
                self.readDictionary(inner_column_value, innerDict_, inner_column_name)
            else:
                innerDict_[inner_column_name] = inner_column_value
        dict_[column_name] = innerDict_


class FetchData(beam.DoFn):
    def process(self, element):
        yield TaggedOutput("valid", element)


# Define an AvroProcessor class that extends the BaseSourceProcessor class
class AvroProcessor(BaseSourceProcessor, ABC):
    """
    The AvroProcessor class is a component in a data ingestion framework that handles the
    fetching of Avro data."""

    def __init__(self, prop_dict):
        self.task = prop_dict

    # Define an expand method that reads data from an Avro file and converts it to a format that can be written to BigQuery
    def expand(self, p_input):
        read_avro = (
                p_input
                | "Read data from avro file"
                >> beam.io.ReadFromAvro(self.task["data_file"])
                | "Convert to BQ format"
                >>
                beam.ParDo(ConvertToOrderedDictionary(self.task)).with_outputs("valid", "invalid")
        )
        return read_avro
