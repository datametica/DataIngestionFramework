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

"""This file contains code for converting JSON data to a dictionary format."""

import json
from abc import ABC

import apache_beam as beam
from apache_beam.pvalue import TaggedOutput

from utility.basic_utilities import generate_error_record
from utility.logger_setup import _get_logger


# Class to convert JSON data to a dictionary format
class ConvertJsonDataToDict(beam.DoFn, ABC):
    """
    The ConvertJsonDataToDict class is a component within a data ingestion framework
    that handles the processing of JSON data. It is responsible for converting JSON
    data to a dictionary format
    """

    def __init__(self, task):
        self.task = task
        self.logger = _get_logger(__name__, self.task["log_level"])

    # Function to convert JSON data to a dictionary format
    def process(self, key_value, window=beam.DoFn.WindowParam):
        """
        :param key_value: Tuple of (shard_id, batch)
        :param window: Window parameter
        :return: Yields a dictionary record
        """

        try:
            shard_id, batch = key_value
            for message_body, publish_time in batch:
                # Convert the message body to a dictionary format
                data = json.loads(str(message_body.decode("UTF-8")))
                yield TaggedOutput("valid", data)
        except Exception as e:
            error_record = generate_error_record(element=key_value, exception=e, job_name=self.task["job_name"],
                                                 target=self.task["targets"], src=self.task["source"],
                                                 err_type="Invalid Record", job_id=self.task["job_id"])
            yield TaggedOutput("invalid", error_record)
