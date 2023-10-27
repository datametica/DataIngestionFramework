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

"""
This module contains the transform that can be used to select the required columns from the data.
"""
import abc
from collections import OrderedDict

import apache_beam as beam
from apache_beam.transforms.ptransform import PTransform

from utility.logger_setup import _get_logger


class RemoveNotRequiredColumn(beam.DoFn, abc.ABC):
    """This class contains the method that can be used to select the required columns from the data.
    """

    def __init__(self, task):
        self.task = task
        self.logger = _get_logger(__name__, self.task["log_level"])
        self.req_cols = [col.lower() for col in self.task["required_columns"]]

    def process(self, element, *args, **kwargs):
        data = OrderedDict()
        for key, value in element.items():
            if (str(key) == "source_name") or (str(key).lower() in self.req_cols):
                data[key] = value
        yield data


class SelectRequiredColumns(PTransform, abc.ABC):
    """This class contains the method that can be used to select the required columns from the data.
    """

    def __init__(self, task):
        self.task = task

    def expand(self, p_input):
        return p_input | "Remove Not Req cols" >> beam.ParDo(
            RemoveNotRequiredColumn(self.task)
        )
