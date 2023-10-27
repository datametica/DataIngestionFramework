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

"""This module contains the utility functions for RDBMS.
These functions are used to create incremental queries.
"""
import jaydebeapi

from audit.utility import targets
from utility.bqutility import run_query


def get_max_delta(task):
    """This method returns the maximum value of the incremental column.
    Args:
        task: Dictionary containing the properties of the task.
    Returns:
        Maximum value of the incremental column.
    """
    table_name = f"{task['source_db']['schema']}.{task['source_db']['rdbms_table']}"
    inc_col = task["source_db"]["incremental_mode"]["column_name"]

    max_delta_query = (
        f"select max({inc_col}) as max_val from {table_name} where {inc_col} is not null"
    )

    conn = jaydebeapi.connect(jclassname=task["rdbms_props"]["driver_class_name"],
                              url=task["rdbms_props"]["jdbc_url"],
                              driver_args=[task["rdbms_props"]["username"],
                                           task["rdbms_props"]["password"]],
                              jars=["jars/ojdbc8-23.2.0.0.jar"]) #add the local jar paths A/C to your RDBMS
    cursor = conn.cursor()
    cursor.execute(max_delta_query)
    max_value = cursor.fetchone()[0]
    return str(max_value)


def create_incremental_query(task):
    """This method creates an incremental query.
    Args:
        task: Dictionary containing the properties of the task.
    Returns:
        Start value, end value and the incremental query.
    """
    audit_table_name = task["audit"]["job_audit_table"]
    job_name = task["job_name"]
    task_name = task["task_name"]
    target_name = targets(task["targets"])
    inc_col = task["source_db"]["incremental_mode"]["column_name"]
    datatype = task["source_db"]["incremental_mode"]["column_datatype"]
    max_delta = get_max_delta(task)

    audit_query = f"""select delta_start_val, delta_end_val from {audit_table_name}
                      where end_ts is not null  and job_name= '{job_name}' and task_name='{task_name}' and
                      target_name='{target_name}' and status ='Completed'  order by end_ts desc limit 1  """
    audit_df = run_query(audit_query, task["project_id"])
    delta_start_val = None
    delta_end_val = None
    if len(audit_df) >= 1:
        delta_start_val = audit_df.iloc[0]["delta_start_val"]
        delta_end_val = audit_df.iloc[0]["delta_end_val"]

    if delta_start_val is None and delta_end_val is None:
        return delta_start_val, max_delta, task["source_db"]["query"]
    else:
        base_query = task["source_db"]["query"]
        if str(datatype).lower() in ["date", "timestamp"]:
            delta_end_val = f"TO_TIMESTAMP('{delta_end_val}', 'YYYY-MM" \
                            f"-DD:HH24:MI:SS')"
        whereClause = f" where {inc_col} > {delta_end_val}"
        query = base_query + whereClause
        delta_start_val = delta_end_val
        delta_end_val = max_delta
        return delta_start_val, delta_end_val, query
