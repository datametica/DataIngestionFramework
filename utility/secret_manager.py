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

"""This module contains the function to access the secret manager.
"""
import json

from google.cloud import secretmanager


def access_secret_version(project_id, secret_id, version_id):
    """
    Access the payload for the given secret version if one exists. The version
    can be a version number as a string (e.g. "5") or an alias (e.g. "latest").
    Args:
        project_id: Project ID of the BigQuery instance.
        secret_id: ID of the secret.
        version_id: Version of the secret.
    Returns:
        Payload of the secret.
    """
    # Import the Secret Manager client library.
    client = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version.
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version.
    response = client.access_secret_version(name=name)

    payload = json.loads(response.payload.data.decode("UTF-8"))
    return payload


def access_secrets_from_pattern(project, pattern):
    secret_dictionary = {}
    client = secretmanager.SecretManagerServiceClient()
    parent = f"projects/{project}"
    secrets = client.list_secrets(request={"parent": parent})

    for secret in secrets:
        if f"/secrets/{pattern}" in secret.name:
            secret_name = secret.name.split('/')[-1]
            secret_version = "latest"

            response = client.access_secret_version(
                request={"name": f"{parent}/secrets/{secret_name}/versions/{secret_version}"})
            payload = response.payload.data.decode("UTF-8")

            secret_dictionary[secret_name] = payload

    return secret_dictionary


def access_secret_from_uri(name, task):
    name = name.replace("{PROJECT_ID}", task["project"])
    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(name=name)
    payload = response.payload.data.decode("UTF-8")
    return str(payload).strip()
