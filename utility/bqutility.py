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

"""This module contains the implementation of a BigQuery 
utility that can be used to run queries on BigQuery.
"""

from google.cloud import bigquery, storage


def run_query(query, project_id):
    """This method runs a query on BigQuery and returns the result as a dataframe.
    Args:
        query: SQL query to run on BigQuery.
        project_id: Project ID of the BigQuery instance.
    Returns:
        Dataframe containing the result of the query.
    """
    client = bigquery.Client(project=project_id)
    df = client.query(query).to_dataframe()

    return df


def copy_blob(bucket_name, blob_name, destination_bucket_name, destination_blob_name, move=False):
    """Moves a blob from one bucket to another with a new name."""
    action = "copied"
    storage_client = storage.Client()
    source_bucket = storage_client.bucket(bucket_name)
    source_blob = source_bucket.blob(blob_name)
    destination_bucket = storage_client.bucket(destination_bucket_name)
    destination_generation_match_precondition = 0

    blob_copy = source_bucket.copy_blob(
        source_blob, destination_bucket, destination_blob_name,
        if_generation_match=destination_generation_match_precondition,
    )
    if move:
        action = "moved"
        source_bucket.delete_blob(blob_name)

    return (
        "Blob {} in bucket {} {} to blob {} in bucket {}.".format(
            source_blob.name,
            source_bucket.name,
            action,
            blob_copy.name,
            destination_bucket.name,
        )
    )


def delete_blob(bucket_name, blob_name):
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    generation_match_precondition = blob.generation

    blob.delete(if_generation_match=generation_match_precondition)

    return f"Blob {blob_name} deleted."
