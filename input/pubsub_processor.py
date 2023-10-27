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

"""This module contains the implementation of a PubSub processor that can be used to read JSON/Avro messages from a Pub/Sub topic.
"""

from abc import ABC

import apache_beam as beam
import avro.schema
import gcsfs
from apache_beam.io.gcp.pubsub import ReadFromPubSub

from dofns.convert_avro_data_to_dict import ConvertAvroDataToDict
from dofns.convert_json_data_to_dict import ConvertJsonDataToDict
from dofns.group_by_fixed_windows import GroupMessagesByFixedWindows
from input.base_source_processor import BaseSourceProcessor


# Define a PubsubProcessor class that extends the BaseSourceProcessor class
class PubsubProcessor(BaseSourceProcessor, ABC):
    """
    The PubSubProcessor class is a component within a data ingestion framework
    that handles the processing of Avro and JSON data from a Pub/Sub messaging
    system. It is responsible for receiving Avro and JSON messages from the
    Pub/Sub topic.
    """

    def __init__(self, task):
        self.task = task

    # Define an expand method that reads data from a PubSub subscription, groups messages into fixed windows,
    # and converts the data to a dictionary based on the input format (JSON/Avro)
    def expand(self, p_input):
        input_data_format = self.task["source_data"]["format"]

        # Read data from PubSub subscription
        if str(input_data_format).lower() == "avro":
            source_data = p_input | "Read From Pubsub" >> ReadFromPubSub(
                subscription=self.task["input_subscription"]
            ).with_output_types(bytes)
        else:
            source_data = p_input | "Read From Pubsub" >> ReadFromPubSub(
                subscription=self.task["input_subscription"]
            )

        # Group messages into a fixed window
        windowing_time = 1
        if "windowing_time" in self.task:
            windowing_time = self.task["windowing_time"]
        windowed_data = (
                source_data
                | "Create Batch Of Windows" >> GroupMessagesByFixedWindows(windowing_time)
        )

        # Convert data to dictionary based on input types (JSON/Avro)
        if str(input_data_format).lower() == "json":
            return windowed_data | "Convert to Dictionary" >> beam.ParDo(
                ConvertJsonDataToDict(self.task)
            ).with_outputs("valid",
                           "invalid")
        else:
            gcs_fs = gcsfs.GCSFileSystem(project=self.task["project_id"])
            schema_file = self.task["avro"]["avsc_schema_file"]
            schema = avro.schema.parse(gcs_fs.open(schema_file, "r").read())
            return windowed_data | "Convert to Dictionary" >> beam.ParDo(
                ConvertAvroDataToDict(schema, self.task)
            ).with_outputs("valid",
                           "invalid")
