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

""" This module contains the DoFn to remove the source_name from the dictionary. """
from abc import ABC
from collections import OrderedDict

import apache_beam as beam


class RemoveSourceName(beam.DoFn, ABC):
    def __init__(self, task):
        self.task = task

    def process(self, element, *args, **kwargs):
        """
        :param element: Dictionary
        :return: Dictionary
        """
        file_name_removed_dict = OrderedDict()
        for key, value in element.items():
            if str(key).lower() == "source_name":
                pass
            else:
                file_name_removed_dict[key] = value
        yield file_name_removed_dict
