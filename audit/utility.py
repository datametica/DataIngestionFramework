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

"""This module contains the utility functions for the audit pipeline.
"""


def targets(ls):
    """This method returns the targets as a string.
    Args:
        ls: List of targets.
    Returns:
        Comma separated string of targets.  
    """
    targets = []
    for target in ls:
        target_type = list(target.keys())[0]
        if target_type == "bigquery_stream":
            targets.append(f'{{"bigquery": {target[target_type]["target_table"]}}}')
        elif target_type == "bigquery":
            targets.append(f'{{"bigquery": {target[target_type]["target_table"]}}}')
        elif target_type == "pubsub":
            targets.append(f'{{"pubsub": {target[target_type]["topic_name"]}}}')
        elif target_type == "kafka":
            targets.append(f'{{"kafka": {target[target_type]["topic_name"]}}}')
        elif target_type == "rdbms":
            targets.append(f'{{"rdbms": {target[target_type]["tablename"]}}}')
        elif target_type == "rdbms":
            targets.append(f'{{"rdbms": {target[target_type]["tablename"]}}}')
        else:
            targets.append(str(target))

    return ",".join(targets)
