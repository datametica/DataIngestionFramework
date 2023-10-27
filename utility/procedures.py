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
This module contains the functions that can be used to perform data validation.
"""

import datetime
import logging


def long_to_timestamp(input_timestamp, column_name=None):
    """
    This method converts a long value to timestamp.
    Args:
        input_timestamp: Long value to be converted to timestamp.
        column_name: Column name.
    Returns:
        Timestamp.
    """
    if input_timestamp is None:
        return input_timestamp
    epoch_timestamp = datetime.datetime(1970, 1, 1, 0, 0, 0)
    delta = datetime.timedelta(seconds=input_timestamp / 1000)
    ts = epoch_timestamp + delta
    output_timestamp = ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    return output_timestamp


def not_null(input_data, column_name=None):
    """
    This method checks if the value is null.
    Args:
        input_data: Value to be validated.
        column_name: Column name.
    Returns:
        True if the value is not null, False otherwise.
    """
    if input_data is None:
        raise Exception(f"{column_name} column cannot be Null")
    return input_data


def default_fn(element, column_name=None):
    """
    This method is the default function.
    Args:
        element: Value to be validated.
        column_name: Column name.
    Returns:
        Value.
    """
    logging.error(f"Function Not Found for {column_name}")
