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

"""This module contains the AuditHelper class which is used to insert audit records into BigQuery
"""

from abc import ABC
from datetime import datetime

import apache_beam as beam
from google.cloud import bigquery

from audit.utility import targets
from utility.logger_setup import _get_logger


class AuditHelper(beam.DoFn, ABC):
    """
    This class is used to insert audit records into BigQuery
    """

    def start_bundle(self):
        # Initialize the BigQuery client
        self.bqClient = bigquery.Client(self.task_prop["project_id"])

    def __init__(self, task_prop):
        self.bqClient = None
        self.task_prop = task_prop
        self.logger = _get_logger(__name__, self.task_prop["log_level"])

    def process(self, element, source_count, passed_count, failed_count):
        # Initialize variables
        pass_count = passed_count
        failed_count = failed_count
        source_count = source_count
        audit_table_id = str(self.task_prop["audit"]["job_audit_table"]).replace(":", ".")
        start_ts = self.task_prop["start_ts"]
        end_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        jobid = self.task_prop["jobid"]

        # Get source name based on source type
        if self.task_prop["source"] == "snowflake":
            source_name = f"{self.task_prop['source_db']['schema']}.{self.task_prop['source_db']['database']}.{self.task_prop['source_db']['snowflake_table']}"
        else:
            source_name = self.task_prop["data_file"]
        task_name = self.task_prop["task_name"]
        # Get target name based on target system
        target = self.task_prop["target_sys"]  # from property
        target_name = targets(self.task_prop["targets"])
        # Create rows to insert into BigQuery
        rows_to_insert = [
            {
                "job_id": jobid,
                "job_name": self.task_prop["job_name"],
                "task_name": task_name,
                "source_name": source_name,
                "target": target,
                "target_name": target_name,
                "start_ts": start_ts,
                "end_ts": end_ts,
                "src_rec_count": source_count,
                "tgt_rec_count": pass_count,
                "err_rec_count": failed_count,
                "status": "COMPLETED",
            }
        ]
        # Insert rows into BigQuery
        errors = self.bqClient.insert_rows_json(audit_table_id, rows_to_insert)
        if len(errors) == 0:
            self.logger.debug(
                f"Audit start for job id {str(jobid)} has been inserted in {audit_table_id}"
            )
        else:
            self.logger.error(
                f"{str(len(errors))} errors found while inserting records with jobid {str(jobid)}"
            )
            raise Exception(
                f"{str(len(errors))} errors found while inserting records with jobid {str(jobid)}"
            )
