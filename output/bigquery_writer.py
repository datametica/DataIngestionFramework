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

import base64

"""This module contains the implementation of the BigQuery writer.
"""

from abc import ABC

import apache_beam as beam

from dofns.remove_source_name import RemoveSourceName
from output.base_output_writer import BaseOutputWriter
from utility.basic_utilities import schemaFetchBQ


class PrepareData(beam.DoFn):
    def process(self, element):
        d = {}
        for key, value in element.items():
            if isinstance(value, bytes):
                d[key] = base64.b64encode(value)
            else:
                d[key] = value
        yield d


class BigQueryWriter(BaseOutputWriter, ABC):
    def __init__(self, task, prop_dict):
        self.task = task
        self.prop_dict = prop_dict
        self.bq_table_id = str(self.prop_dict["target_table"]).replace(":", ".")
        self.project = self.bq_table_id.split(".")[0]
        self.bq_dataset = self.bq_table_id.split(".")[1]
        self.bq_table = self.bq_table_id.split(".")[2]

        schema_file = None
        if "schema_file" in task:
            schema_file = task["schema_file"]

        self.table_schema = schemaFetchBQ(self.bq_dataset + "." + self.bq_table,
                                          project=self.project, schema_file=schema_file)

        self.write_disposition = prop_dict["write_disposition"]

    def expand(self, p_input):
        additional_bq_parameters = {}
        if str(self.task["is_filename_req_in_target"]).lower() == "false":
            p_input = (
                    p_input
                    | f"Remove Source Name {str(self.task['targets']).split('.')[-1]}"
                    >> beam.ParDo(RemoveSourceName(self.task))
            )
        return p_input \
            | "Prepare Data" >> beam.ParDo(PrepareData()) \
            | "Write To Bigquery" >> beam.io.WriteToBigQuery(
                table=self.bq_table,
                dataset=self.bq_dataset,
                project=self.project,
                additional_bq_parameters=additional_bq_parameters,
                create_disposition="CREATE_IF_NEEDED",
                schema=self.table_schema,
                write_disposition=self.write_disposition,
            )
