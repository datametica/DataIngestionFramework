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

"""This module contains the implementation of a Kafka processor that can be used to read Kafka messages and flatten those messages.
"""

import abc

import apache_beam as beam
from apache_beam.io.kafka import ReadFromKafka

from dofns.parDoFunctions import addKafkaUID as addUID, FlattenJson
from input.base_source_processor import BaseSourceProcessor


class KafkaFlattenProcessor(BaseSourceProcessor, abc.ABC):
    """
    The KafkaFlattenProcessor class is a component within a data ingestion framework
    that processes encoded JSON strings from a Kafka topic. It performs similar
    operations as the JSONProcessor by flattening the nested data structure and
    renaming columns based on a provided column name map. This allows for efficient
    extraction of relevant data from the JSON strings and facilitates further processing
    """

    def __init__(self, task):
        self.task = task

    def expand(self, p):
        colName = p | "Column Name Map" >> beam.io.ReadFromText(self.task["column_name_map"])

        read = p | "Read Json from Kafka" >> ReadFromKafka(
            consumer_config={"bootstrap.servers": self.task['bootstrap_server'],
                             "group.id": f"{self.task['topic_name']}Group",
                             "auto.offset.reset": "latest",
                             "enable.auto.commit": "true"},
            topics=[self.task['topic_name']],
            commit_offset_in_finalize=False,
            with_metadata=False
        )

        add_unique_id = read | "Adding Unique Ids for msg Triaging" >> beam.ParDo(addUID())

        converted_json = add_unique_id | "Flattening Json" >> beam.ParDo(FlattenJson(self.task),
                                                                         ColumnNameMap=beam.pvalue.AsList(
                                                                             colName)).with_outputs("valid",
                                                                                                    "invalid")

        return converted_json
