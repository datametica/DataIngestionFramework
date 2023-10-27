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

"""This module contains the implementation of a Kafka processor that can be used to read JSON messages from a Kafka topic.
"""

from abc import ABC

import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka

from dofns.parDoFunctions import ProcessJsonWithoutFlatten
from input.base_source_processor import BaseSourceProcessor


class KafkaProcessor(BaseSourceProcessor, ABC):
    """
    The KafkaProcessor class is a component within a data ingestion framework
    that handles the processing of JSON data from a Kafka topic. It is
    responsible for consuming JSON messages from the Kafka topic, parsing and
    extracting the relevant information, and performing any required data
    transformations or validations. The KafkaProcessor enables the integration
    of JSON data from Kafka into downstream systems
    """

    def __init__(self, prop_dict):
        self.prop_dict = prop_dict
        self.consumer_config = prop_dict['consumer_config']
        self.topic = prop_dict['topic_name']

    def expand(self, p_input):
        read = p_input | "Read Json from Kafka" >> ReadFromKafka(
            consumer_config=self.consumer_config,
            topics=[self.topic],
            commit_offset_in_finalize=False,
            with_metadata=False
        )

        return read | "Process JSON" >> beam.ParDo(
            ProcessJsonWithoutFlatten(self.prop_dict)).with_outputs("valid", "invalid")
