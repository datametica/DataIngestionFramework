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

"""This module contains the implementation of an API processor that fetches data from an API endpoint
and processes it using Apache Beam.
"""
import abc
from datetime import datetime

import apache_beam as beam
import requests
from apache_beam.io.gcp import gce_metadata_util
from apache_beam.pvalue import TaggedOutput

from input.base_source_processor import BaseSourceProcessor
from utility.flatten import *
from utility.logger_setup import _get_logger


class FetchApiData(beam.DoFn, abc.ABC):
    """
    This class contains the implementation of a DoFn that fetches data from an API endpoint.
    """

    def __init__(self, task):
        self.task = task
        self.logger = _get_logger("ApiProcessor", self.task["log_level"])
        self.base_api_path = self.task["api_path"]
        self.query_params = None
        if "query_params" in self.task:
            self.query_params = self.task["query_params"]

    def process(self, element, *args, **kwargs):
        resolved_api_path = None
        try:
            if self.query_params is None:
                resolved_api_path = self.base_api_path
            else:
                resolved_api_path = self.base_api_path + "?"
                for i in range(len(self.query_params)):
                    if i != len(self.query_params) - 1:
                        resolved_api_path = resolved_api_path + self.query_params[i] + "&"
                    else:
                        resolved_api_path = resolved_api_path + self.query_params[i]

            self.logger.info(f"Resolved Api Path: {resolved_api_path}")
            # Resolve Auth Part
            if (
                    "authentication" in self.task
                    and str(self.task["authentication"]["is_public_api"]).lower() == "true"
            ):
                headers = None
            else:
                headers = self.task["authentication"]["headers"]

            # Send GET request to API endpoint
            response = requests.get(resolved_api_path, headers=headers)
            json_data = response.json()
            if "root" in self.task:
                json_data = response.json()[self.task["root"]]

            flatten_ = flatsplode(json_data)
            # Perform Flattening If there is a Nested structure
            for data in flatten_:
                data["source_name"] = resolved_api_path
                # Tag output as valid and yield flattened data
                yield TaggedOutput("valid", data)
        except Exception as e:
            # If there is an exception, tag output as invalid and yield error record
            error_record = {"job_id": gce_metadata_util.fetch_dataflow_job_id(), "error_record": str(element),
                            "error_type": "Invalid Record", "error_desc": str(e), "source_name": resolved_api_path,
                            "insert_ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
            yield TaggedOutput("invalid", error_record)


class ApiProcessor(BaseSourceProcessor, abc.ABC):
    """
    This class contains the implementation of an API processor that fetches data from an API endpoint
    and processes it using Apache Beam.
    """

    def __init__(self, task):
        self.task = task

    def expand(self, p_input):
        # Create a single-element collection and fetch data from API using FetchApiData
        data = (
                p_input
                | "Single Element Collection" >> beam.Create(["Single Element"])
                | "Fetch Data From Api" >> beam.ParDo(FetchApiData(self.task)).with_outputs("valid",
                                                                                            "invalid")
        )
        return data
