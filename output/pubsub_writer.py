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

"""This module contains the implementation of a Pubsub writer that can be used to write data to Pubsub.
"""
import json
from abc import ABC

import apache_beam as beam
from apache_beam.io import WriteToPubSub

from output.base_output_writer import BaseOutputWriter


class PubsubWriter(BaseOutputWriter, ABC):
    def __init__(self, task, prop_dict):
        self.task = task
        self.prop_dict = prop_dict

    def expand(self, p_input):
        return p_input | 'Convert dict to byte string for pubsub' >> beam.Map(
            lambda x: json.dumps(x).encode('utf-8')) \
            | f"Write to Pubsub" >> WriteToPubSub(topic=self.prop_dict["topic_name"])
