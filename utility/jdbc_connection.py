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

"""This module contains the connection function for JDBC connection
"""


def connection(task):
    """
    This method creates a JDBC connection.
    Args:
        task: Task containing the JDBC connection properties.
    Returns:
        JDBC connection object.
    """

    import jaydebeapi

    con = jaydebeapi.connect(jclassname=task["rdbms_props"]["driver_class_name"],
                             url=task["rdbms_props"]["jdbc_url"],
                             driver_args=[task["rdbms_props"]["username"],
                                          task["rdbms_props"]["password"]],
                             jars=[task["rdbms_props"]["jar_path"]])
    return con
