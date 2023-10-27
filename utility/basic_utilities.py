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
Module contains basic utility (helper) functions which are used to
do some trivial task such as reading property files, resolving those
properties, get DQ and DT rules. 
"""

import datetime
import json
from typing import Dict, Any

import gcsfs
import requests
import yaml
from apache_beam.io.gcp.gcsfilesystem import GCSFileSystem
from google.cloud import bigquery
from requests.auth import HTTPBasicAuth
from yaml import SafeLoader

from utility.secret_manager import access_secret_version


def read_yaml_from_local(file):
    """Read Yaml Property files stored locally
    Args:
        file(str): local file path
    Returns:
        (dict): Key-Value pair of property name and property value
    """
    with open(file, "r") as f:
        data = yaml.load(f, Loader=SafeLoader)
    return data


def read_yaml_from_gcs(file, project):
    """Read Yaml Property files stored on GCS
    Args:
        file(str): GCS file path starting with gs://
        project(str): GCP project name
    Returns:
        (dict): Key-Value pair of property name and property value
    """
    gcs_fs = gcsfs.GCSFileSystem(project=project)
    data = yaml.safe_load(gcs_fs.open(file))
    return data


def read_file_as_string_from_gcs(file, project):
    """Read files stored on GCS as string
    Args:
        file(str): GCS file path starting with gs://
        project(str): GCP project name
    Returns:
        (str): returns data as string
    """
    gcs_fs = gcsfs.GCSFileSystem(project=project)
    data = gcs_fs.open(file).read()
    return data


def run_query(query, project_id):
    """Run query on BigQuery and return the fetched data as df
    Args:
        query(str): query which needs to be executed
        project_id(Optional[str]): GCP project name
    Returns:
        pandas.DataFrame: records from tables as dataframe
    """
    client = bigquery.Client(project=project_id)
    df = client.query(query).to_dataframe()
    return df


def resolve_property(task, options):
    """If the values are kept as variables in the property file, resolve them
    Args:
        task(dict): task from property file
        options: pipeline options
    Returns:
        (dict): resolved task
    """
    if "audit" in task:
        if "job_audit_table" in task["audit"]:
            if task["audit"]["job_audit_table"]:
                task["audit"]["job_audit_table"] = str(task["audit"]["job_audit_table"]).replace(
                    "{GCP_PROJECT}", options.project_id
                )
        if "file_audit_table" in task["audit"]:
            task["audit"]["file_audit_table"] = str(
                task["audit"]["file_audit_table"]
            ).replace("{GCP_PROJECT}", options.project_id)

    if "data_file" in task and task["data_file"]:
        task["data_file"] = str(task["data_file"]).replace("{DATA_BUCKET}", options.data_bucket)

    # if source is snowflake. Fetch keys from secret manager
    if str(task["source"]).lower() == "snowflake":
        secret_manager = task["secret_manager"]
        snowflake_props = access_secret_version(secret_manager["gcp_project_id"], secret_manager["secret_id"],
                                                secret_manager["version_id"], )
        task["snowflake_props"] = snowflake_props

    if "snowflake_props" in task:
        if "staging_bucket_name" in task["snowflake_props"]:
            task["snowflake_props"]["staging_bucket_name"] = str(
                task["snowflake_props"]["staging_bucket_name"]).replace("{CODE_BUCKET}", options.code_bucket)

    if "transformation_table" in task:
        task["transformation_table"] = str(task["transformation_table"]).replace("{GCP_PROJECT}", options.project_id)

    if "data_quality_rule_table" in task:
        task["data_quality_rule_table"] = str(task["data_quality_rule_table"]).replace("{GCP_PROJECT}",
                                                                                       options.project_id)

    if "source_db" in task:
        if "schema" not in task["source_db"]:
            task["source_db"]["schema"] = ""
        if "table" not in task["source_db"]:
            task["source_db"]["table"] = ""

    if options.resolve_variables:
        variables = options.resolve_variables.replace("\\", '"')
        variables = json.loads(variables)
        for key, value in variables.items():
            task["source_db"]["query"] = str(task["source_db"]["query"]).replace(f"##{key}##", value)
            task["source_db"]["rdbms_table"] = str(task["source_db"]["rdbms_table"]).replace(f"##{key}##", value)
    return task


def parse_schema_as_dict(schema_file, gcs_fs: GCSFileSystem):
    """Parse schema file and return schema as dict
    Args:
        schema_file(str): schema file path
        gcs_fs(GCSFileSystem): GCSFileSystem object
    Returns:
        (dict): schema as dict
    """

    file_pointer = gcs_fs.open(schema_file, "r")
    try:
        fields = json.load(file_pointer)["fields"]
    except:
        fields = json.load(file_pointer)
    return {"fields": fields}


def parse_schema_as_dict_local(schema_file):
    """Parse schema file and return schema as dict
    Args:
        schema_file(str): schema file path
    Returns:
        (dict): schema as dict
    """

    with open(schema_file, 'r') as f:
        file_pointer = f.read()
    try:
        fields = json.loads(file_pointer)["fields"]
    except:
        fields = json.loads(file_pointer)
    return {"fields": fields}


def schemaFetchBQ(table_id, project, schema_file=None):
    """Fetch schema of a table from BigQuery
    Args:
        table_id(str): table id in format project:dataset.table
        project(str): GCP project name
    Returns:
        (dict): schema as dict
    """
    if schema_file:
        return parse_schema_as_dict(schema_file, gcsfs.GCSFileSystem(project=project))

    client = bigquery.Client(project=project)
    table = client.get_table(table_id.replace(":", "."))
    schema = table.to_api_repr()["schema"]
    return schema


def bigquery_schema_to_typing(schema):
    """Convert BigQuery schema to typing
    Args:
        schema(list): BigQuery schema
    Returns:
        (dict): schema as dict
    """
    final = []
    for field in schema:
        field_name = field.name
        field_type = field.field_type

        if field_type == "STRING":
            typing_type = str
        elif field_type == "BYTES":
            typing_type = bytes
        elif field_type == "INTEGER":
            typing_type = int
        elif field_type == "FLOAT":
            typing_type = float
        elif field_type == "BOOLEAN":
            typing_type = bool
        elif field_type == "TIMESTAMP":
            typing_type = str
        elif field_type == "DATE":
            typing_type = str
        elif field_type == "TIME":
            typing_type = str
        elif field_type == "DATETIME":
            typing_type = str
        elif field_type == "RECORD":
            # Recursively call this function to get the nested schema
            nested_schema = bigquery_schema_to_typing(field.fields)
            typing_type = Dict[str, Any]
            for key, value in nested_schema.items():
                typing_type[key] = value
        final.append((field_name, typing_type))
    final.append(("source_name", str))
    return final


def generate_error_record(element, exception, job_name, target, src, err_type, job_id):
    return {"job_name": job_name, "job_id": job_id, "error_record": str(element), "error_type": err_type,
            "error_desc": str(exception), "target_table": str(target), "source_name": src,
            "insert_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}


def fetch_access_token(task):
    token_params = task["authentication"]["token_params"]
    token_endpoint = task["authentication"]["token_endpoint"]

    response = requests.post(token_endpoint,
                             auth=HTTPBasicAuth(token_params['client_id'], token_params['client_secret']),
                             data=token_params)
    if response.status_code == 200:
        return response.json()['access_token']
    else:
        raise ValueError('Token request failed with status code:', response.status_code)
