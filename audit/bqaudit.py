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

"""This module contains the exception class for transformation.
"""

from datetime import datetime

from google.cloud import bigquery

from audit.utility import targets
from utility.logger_setup import _get_logger


class BQAudit:
    """This class is used to insert audit records into BigQuery
    """

    def __init__(self, task_prop):
        # Initialize the BQAudit class with the task properties
        self.task_prop = task_prop
        self.project_id = self.task_prop["project_id"]
        self.logger = _get_logger(__name__, self.task_prop["log_level"])
        self.bqClient = bigquery.Client(project=self.project_id)

    def start_audit(self):
        # Start the audit process
        self.logger.info(f"Starting Auditing For Job Id {self.task_prop['jobid']}")
        audit_table_id = self.task_prop["audit"]["job_audit_table"].replace(":", ".")
        jobid = self.task_prop["jobid"]
        start_ts = self.task_prop["start_ts"]
        end_ts = None
        job_name = self.task_prop["job_name"]
        task_name = self.task_prop["task_name"]
        target_name = targets(self.task_prop["targets"])
        # Create rows to insert into BigQuery
        rows_to_insert = [
            {
                "job_id": jobid,
                "job_name": job_name,
                "start_ts": start_ts,
                "end_ts": end_ts,
                "task_name": task_name,
                "status": "In-Progress",
                "source": self.task_prop["source"],
                "source_name": self.task_prop["source"],
                "target": target_name,
                "target_name": target_name

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
                f"{str(len(errors))} errors found while inserting records with jobid {str(jobid)}"
            )
            raise Exception(errors)

    def insert_on_job_failed(self):
        # Insert audit record when job fails
        self.logger.info(
            f"Inserting Audit Record For Job Id {self.task_prop['jobid']} after failure"
        )
        audit_table_id = self.task_prop["audit"]["job_audit_table"].replace(":", ".")
        jobid = self.task_prop["jobid"]
        start_ts = None
        end_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        job_name = self.task_prop["job_name"]
        task_name = self.task_prop["task_name"]
        # Create rows to insert into BigQuery
        rows_to_insert = [
            {
                "job_id": jobid,
                "job_name": job_name,
                "start_ts": start_ts,
                "end_ts": end_ts,
                "task_name": task_name,
                "status": "FAILED",
                "source": self.task_prop["source"]
            }
        ]
        # Insert rows into BigQuery
        errors = self.bqClient.insert_rows_json(audit_table_id, rows_to_insert)
        if len(errors) == 0:
            self.logger.info(f"Inserted Failed Job id {jobid} in Audit table")
        else:
            self.logger.error(
                f"{str(len(errors))} errors found while inserting records with jobid {str(jobid)}"
            )
            raise Exception(
                f"{str(len(errors))} errors found while inserting records with jobid {str(jobid)}"
            )
