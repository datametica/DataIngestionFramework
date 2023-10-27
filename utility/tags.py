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

"""This module contains the tags that can be used to tag the data in the pipeline.
"""


class Tags:
    DATA = "data"
    SCHEMA = "schema"
    OUTPUT_REC = "output_records"
    VALID_RECORDS = "valid_records"
    INVALID_RECORDS = "invalid_records"
    DATA_TRANS_VALID_RECORDS = "data_trans_valid_records"
    DATA_TRANS_INVALID_RECORDS = "data_trans_invalid_records"
    DATA_QUALITY_VALID_RECORDS = "data_quality_valid_records"
    DATA_QUALITY_INVALID_RECORDS = "data_quality_invalid_records"
