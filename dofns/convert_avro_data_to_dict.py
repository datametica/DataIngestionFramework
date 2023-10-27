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

"""This module contains the DoFn to convert Avro data to a dictionary format
"""
import io
from abc import ABC

import apache_beam as beam
from apache_beam.pvalue import TaggedOutput
from avro.io import DatumReader, BinaryDecoder

from utility.basic_utilities import generate_error_record
from utility.logger_setup import _get_logger


class ConvertAvroDataToDict(beam.DoFn, ABC):
    """
    The ConvertAvroDataToDict class is a component within a data ingestion framework
    that handles the processing of Avro data. It is responsible for converting Avro
    data to a dictionary format
    """

    def __init__(self, schema, task):
        self.schema = schema
        self.task = task
        self.reader = DatumReader(self.schema)
        self.logger = _get_logger(__name__, self.task["log_level"])

    # Function to convert Avro data to a dictionary format
    def process(self, key_value, window=beam.DoFn.WindowParam):
        """
        :param key_value: Tuple of (shard_id, batch)
        :param window: Window parameter
        :return: Yields a dictionary record
        """

        shard_id, batch = key_value
        for message_body, publish_time in batch:
            message_bytes = io.BytesIO(message_body)
            decoder = BinaryDecoder(message_bytes)
            try:
                event_dict = self.reader.read(decoder)
                self.logger.debug(f"dict_record: {event_dict}")
                yield TaggedOutput("valid", event_dict)
            except Exception as e:
                error_record = generate_error_record(element=key_value, exception=e, job_name=self.task["job_name"],
                                                     target=self.task["targets"], src="kafka",
                                                     err_type="Invalid Record", job_id=self.task["job_id"])
                yield TaggedOutput("invalid", error_record)
