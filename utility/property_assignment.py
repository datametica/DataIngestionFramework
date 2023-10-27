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

"""This module contains functions to read property files and tables.
The property files and tables are used to configure the pipeline.
Merging of the property files and tables is also done in this module.
"""

import json

from .basic_utilities import read_yaml_from_local, read_yaml_from_gcs
from .bqutility import run_query


def job_config(task, project=None):
    """This method reads the property file and returns the properties as a dictionary.
    Args:
        task: Dictionary containing the properties of the task.
        project: Project ID of the BigQuery instance.
    Returns:
        Dictionary containing the properties of the task.
    """
    if 'job_prop_file' in task:
        if str(task['job_prop_file']).startswith("gs://"):
            job_prop = read_yaml_from_gcs(task['job_prop_file'], project)
        else:
            job_prop = read_yaml_from_local(task['job_prop_file'])
    elif 'job_prop_table' in task:
        if task['additional_input']:
            additional_task_input = json.loads(task.pop("additional_input"))
            task.update(additional_task_input)
        job_prop = run_query(
            f"select * from {task['job_prop_table']['table_id']} where job_name like \"{task['job_prop_table']['job_name']}\"",
            project).to_dict(orient="records")[0]
        if job_prop['additional_input']:
            additional_job_input = json.loads(job_prop.pop("additional_input"))
            job_prop.update(additional_job_input)

    job_prop.update(task)
    if "dataflow_prop" in job_prop:
        dataflow_prop = job_prop.pop("dataflow_prop")
        job_prop.update(dataflow_prop)

    if "is_filename_req_in_target" not in job_prop:
        job_prop["is_filename_req_in_target"] = False

    job_prop['use_public_ips'] = False
    return job_prop


def prop_config(options):
    """This method reads the property file and returns the properties as a dictionary.
    Args:
        options: Dictionary containing the properties of the task.
    Returns:
        Dictionary containing the properties of the task.
    """
    if options['task_prop_file']:
        if str(options['task_prop_file']).startswith("gs://"):
            config_dict = read_yaml_from_gcs(options['task_prop_file'], options['project'])
        else:
            config_dict = read_yaml_from_local(options['task_prop_file'])

        task_list = config_dict['tasks']
    elif options['task_prop_table']:
        table = options['task_prop_table']
        task_id = options['task_id']

        task_list = run_query(f"select * from {table} where task_id like \"{task_id}\"", options['project']).to_dict(
            orient="records")

    else:
        raise FileNotFoundError("Please Provide Property file or table id")

    prop_map = []

    for task in task_list:
        job_prop = job_config(task, options['project'])
        for i in options:
            if options[i]:
                job_prop[i] = options[i]
        prop_map.append(job_prop)

    return prop_map
