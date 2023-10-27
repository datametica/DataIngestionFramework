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

"""This module contains the implementation of a Snowflake processor that can be used to read data from Snowflake.
"""

import json
from abc import ABC

import apache_beam as beam
import gcsfs
from apache_beam import TaggedOutput
from apache_beam.io.snowflake import ReadFromSnowflake

from input.base_source_processor import BaseSourceProcessor


# Define a null_handling function that handles null values based on the datatype
def null_handling(value, datatype):
    if str(datatype).lower() != "string" and str(value).lower() == "n":
        return None
    else:
        return value


class FetchData(beam.DoFn):
    def process(self, element):
        yield TaggedOutput("valid", element)


# Define a SnowFlakeProcessor class that extends the BaseSourceProcessor class
class SnowFlakeIOProcessor(BaseSourceProcessor, ABC):
    """
    The SnowflakeProcessor class is a component within a data ingestion framework
    that handles the processing and integration of data into Snowflake,
    a cloud-based data warehouse.
    """

    def __init__(self, prop_dict):
        self.prop_dict = prop_dict
        self.gcs_fs = gcsfs.GCSFileSystem(project=self.prop_dict["project_id"])
        self.schema = json.load(self.gcs_fs.open(self.prop_dict["schema_file"]))

    # Define a getJsonRow method that converts a list of strings to a JSON object based on the schema
    def getJsonRow(self, mylist):
        schemaList = self.schema.get("fields")
        headerList = []
        datatype = []
        jsonDict = {}
        for ele in schemaList:
            headerList.append(ele.get("name"))
            datatype.append(ele.get("type"))
        cnt = 0
        for ele in mylist:
            value = null_handling(str(ele.decode()), datatype[cnt])
            jsonDict.update({headerList[cnt]: value})
            cnt += 1
        source_name = f"{self.prop_dict['snowflake_props']['schema']}.{self.prop_dict['snowflake_props']['database']}.{self.prop_dict['source_db']['snowflake_table']}"
        jsonDict.update({"source_name": source_name})
        return dict(jsonDict)

    # Define a csv_mapper method that maps a list of strings to a JSON object
    def csv_mapper(self, strings_array):
        return self.getJsonRow(strings_array)

    # Define an expand method that reads data from Snowflake and outputs valid or invalid records
    def expand(self, p_input):

        if (
                "delta_start" in self.prop_dict
                and "delta_end" in self.prop_dict
                and "query" in self.prop_dict
        ):
            return p_input | "Reading From SnowFlake" >> ReadFromSnowflake(
                server_name=self.prop_dict["snowflake_props"]["server_name"],
                schema=self.prop_dict["snowflake_props"]["schema"],
                database=self.prop_dict["snowflake_props"]["database"],
                staging_bucket_name=self.prop_dict["snowflake_props"][
                    "staging_bucket_name"
                ],
                storage_integration_name=self.prop_dict["snowflake_props"][
                    "storage_integration"
                ],
                csv_mapper=self.csv_mapper,
                username=self.prop_dict["snowflake_props"]["user"],
                password=self.prop_dict["snowflake_props"]["password"],
                query=self.prop_dict["query"],
                role=self.prop_dict["snowflake_props"]["role"],
                warehouse=self.prop_dict["snowflake_props"]["warehouse"],
            ) | beam.ParDo(FetchData()).with_outputs("valid", "invalid")
        else:
            read_data = p_input | "Reading From SnowFlake" >> ReadFromSnowflake(
                server_name=self.prop_dict["snowflake_props"]["server_name"],
                schema=self.prop_dict["snowflake_props"]["schema"],
                database=self.prop_dict["snowflake_props"]["database"],
                staging_bucket_name=self.prop_dict["snowflake_props"][
                    "staging_bucket_name"
                ],
                storage_integration_name=self.prop_dict["snowflake_props"][
                    "storage_integration"
                ],
                csv_mapper=self.csv_mapper,
                username=self.prop_dict["snowflake_props"]["user"],
                password=self.prop_dict["snowflake_props"]["password"],
                query=self.prop_dict["snowflake_props"]["query"],
                role=self.prop_dict["snowflake_props"]["role"],
                warehouse=self.prop_dict["snowflake_props"]["warehouse"],
            ) | beam.ParDo(FetchData()).with_outputs("valid", "invalid")

            return read_data
