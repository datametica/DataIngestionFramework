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

import setuptools

REQUIRED_PACKAGES = [
    "gcsfs",
    "db-dtypes",
    "google-cloud-secret-manager",
    "snowflake-connector-python",
    "ndjson",
    "sendgrid",
    "xmltodict",
    "pyodbc",
    "teradatasql",
    "cx_Oracle",
    "JayDeBeApi"
]
PACKAGE_NAME = "data_ingestion"
PACKAGE_VERSION = "0.0.1"
setuptools.setup(
    name=PACKAGE_NAME,
    version=PACKAGE_VERSION,
    description="Data Ingestion",
    install_requires=REQUIRED_PACKAGES,
    packages=setuptools.find_packages(),
    maintainer="N.A.",
    maintainer_email="N.A.",
    url="N.A."
)