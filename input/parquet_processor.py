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

"""This module contains the implementation of a Parquet processor that can be used to read Parquet files.
"""
import collections
from abc import ABC
from datetime import datetime

import apache_beam as beam
from apache_beam.io.gcp import gce_metadata_util
from apache_beam.pvalue import TaggedOutput

from input.base_source_processor import BaseSourceProcessor
from utility.logger_setup import _get_logger


# Define a ConvertToOrderedDictionary class that extends the beam.DoFn class
class ConvertToOrderedDictionary(beam.DoFn, ABC):
    def __init__(self, task_dict):
        self.task_dict = task_dict
        self.logger = _get_logger(__name__, self.task_dict['log_level'])

    # Define a process method that converts a Parquet record to an ordered dictionary
    def process(self, element):
        try:
            rows = collections.OrderedDict(element)
            dict_ = collections.OrderedDict()
            for item in rows:
                column_name = item
                column_value = rows[item]
                if isinstance(column_value, dict):
                    self.readRecord(column_value, dict_, column_name)
                else:
                    dict_[column_name] = column_value
                    dict_["source_name"] = self.task_dict["data_file"]
            yield TaggedOutput("valid", dict_)
        except Exception as e:
            # If an exception occurs, yield an invalid record with error details
            error_record = {"job_id": gce_metadata_util.fetch_dataflow_job_id(), "error_record": str(element),
                            "error_type": "Invalid Record", "error_desc": str(e),
                            "source_name": self.task_dict["data_file"],
                            "insert_ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
            yield TaggedOutput("invalid", error_record)

    # Define a readRecord method that recursively reads nested records
    def readRecord(self, column_value, dict_, column_name):
        innerDict_ = collections.OrderedDict()
        inner_rows = collections.OrderedDict(column_value.items())
        for innerItem in inner_rows:
            inner_column_name = innerItem
            inner_column_value = inner_rows[innerItem]
            if isinstance(inner_column_value, dict):
                self.readRecord(inner_column_value, innerDict_, inner_column_name)
            else:
                innerDict_[inner_column_name] = inner_column_value
        dict_[column_name] = innerDict_


# Define a ParquetProcessor class that extends the BaseSourceProcessor class
class ParquetProcessor(BaseSourceProcessor):
    """
    The ParquetProcessor class is a component within a data ingestion framework
    that handles the processing of Parquet data
    """

    def __init__(self, prop_dict):
        self.prop_dict = prop_dict
        self.task = prop_dict

    # Define an expand method that reads data from a Parquet file and outputs valid or invalid records
    def expand(self, p_input):
        read_parquet = (
                p_input
                | "Read data from parquet file" >> beam.io.ReadFromParquet(self.task["data_file"])
                | "Convert to BQ format" >> beam.ParDo(ConvertToOrderedDictionary(self.task)).with_outputs("valid",
                                                                                                           "invalid")
        )
        return read_parquet
