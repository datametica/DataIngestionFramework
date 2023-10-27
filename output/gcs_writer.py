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

"""This module contains the implementation of a GCS writer that can be used to write data to GCS.
"""

from abc import ABC

import apache_beam as beam

from dofns.parDoFunctions import GroupMessagesByFixedWindowsContinuous, WriteToGcsStreaming
from output.base_output_writer import BaseOutputWriter


class GCSWriter(BaseOutputWriter, ABC):
    def __init__(self, task, prop_dict):
        self.task = task
        self.prop_dict = prop_dict

    def expand(self, p_input):
        if not self.task["streaming"]:
            return p_input | "Write Messages to GCS" >> beam.io.WriteToText(self.prop_dict["output_path"])

        return (p_input | "Window into" >> GroupMessagesByFixedWindowsContinuous(10)
                | "Write Messages to GCS file" >> beam.ParDo(
                    WriteToGcsStreaming(self.prop_dict["output_path"]))
                )
