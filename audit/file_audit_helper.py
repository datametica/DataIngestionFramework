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

"""This module contains FileLevelAudit class which is used to insert audit records
into BigQuery for file level processing.
"""

from abc import ABC
from datetime import datetime

import apache_beam as beam
from google.cloud import bigquery

from audit.utility import targets
from utility.logger_setup import _get_logger


class FileLevelAudit(beam.DoFn, ABC):
    def __init__(self, task_prop):
        # Initialize the FileLevelAudit class with the task properties
        self.bqClient = None
        self.task_prop = task_prop
        self.logger = _get_logger(__name__, self.task_prop["log_level"])

    def start_bundle(self):
        # Start the bundle
        self.bqClient = bigquery.Client(self.task_prop["project_id"])

    def process(self, element, *args, **kwargs):
        # Process the element
        file_name = str(element[0])
        passed_record_view = element[1]["passed_record_view"]
        failed_record_view = element[1]["error_record_view"]
        source_record_view = element[1]["source_record_view"]
        if len(passed_record_view) == 1:
            pass_count = element[1]["passed_record_view"][0]
        else:
            pass_count = 0
        if len(failed_record_view) == 1:
            failed_count = element[1]["error_record_view"][0]
        else:
            failed_count = 0
        if len(source_record_view) == 1:
            source_count = element[1]["source_record_view"][0]
        else:
            source_count = 0

        if file_name == "source_name":
            failed_count = 0

        audit_table_id = str(self.task_prop["audit"]["file_audit_table"]).replace(
            ":", "."
        )
        load_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        jobid = self.task_prop["jobid"]
        source_name = file_name
        task_name = self.task_prop["task_name"]
        target = "Bigquery"
        target_name = targets(self.task_prop["targets"])
        if source_name == "CSVProcessor":
            source_name = "delimited_file"
        # Create rows to insert into BigQuery
        rows_to_insert = [
            {
                "job_id": jobid,
                "job_name": self.task_prop["job_name"],
                "task_name": task_name,
                "source_name": source_name,
                "target": target,
                "target_name": target_name,
                "src_rec_count": source_count,
                "tgt_rec_count": pass_count,
                "err_rec_count": failed_count,
                "load_ts": load_ts,
            }
        ]
        # Insert rows into BigQuery
        errors = self.bqClient.insert_rows_json(audit_table_id, rows_to_insert)
        if len(errors) == 0:
            self.logger.info(
                f"Audit start for job id {str(jobid)} has been inserted in {audit_table_id}"
            )
        else:
            self.logger.error(
                f"{str(len(errors))} errors found while inserting records with jobid {str(jobid)} "
            )
            raise Exception(
                f"{str(len(errors))} errors found while inserting records with jobid {str(jobid)} {str(errors)}"
            )
