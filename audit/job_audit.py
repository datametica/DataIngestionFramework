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

"""This module contains the JobAudit class which is used to insert audit records into BigQuery for job-level processing
"""

import abc
from abc import ABC
from datetime import datetime

import apache_beam
import apache_beam as beam
from apache_beam import pvalue
from apache_beam.transforms.ptransform import PTransform
from google.cloud import bigquery

from audit.utility import targets
from utility.logger_setup import _get_logger
from utility.rdbms_utility import get_max_delta


class AuditHelper(beam.DoFn, ABC):
    def start_bundle(self):
        # Start the bundle
        self.bqClient = bigquery.Client(self.task_prop["project_id"])

    def __init__(self, task_prop):
        # Initialize the AuditHelper class with the task properties
        self.bqClient = None
        self.task_prop = task_prop
        self.logger = _get_logger(__name__, self.task_prop["log_level"])

    def process(self, element, source_count, passed_count, failed_count):
        # Process the element
        if type(source_count) == apache_beam.pvalue.EmptySideInput:
            source_count = 0
        if type(passed_count) == apache_beam.pvalue.EmptySideInput:
            passed_count = 0
        if type(failed_count) == apache_beam.pvalue.EmptySideInput:
            failed_count = 0

        pass_count = passed_count
        failed_count = failed_count
        source_count = source_count
        audit_table_id = str(self.task_prop["audit"]["job_audit_table"]).replace(":", ".")
        start_ts = self.task_prop["start_ts"]
        end_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        jobid = self.task_prop["jobid"]
        delta_start = None
        delta_end = None
        if "delta_start" in self.task_prop:
            delta_start = self.task_prop["delta_start"]
        if "delta_end" in self.task_prop:
            delta_end = self.task_prop["delta_end"]

        if self.task_prop["source"] == "snowflake":
            source_name = f"{self.task_prop['source_db']['schema']}.{self.task_prop['source_db']['database']}.{self.task_prop['source_db']['snowflake_table']}"
            delta_end = get_max_delta(self.task_prop)
        elif self.task_prop["source"] == "rdbms":
            source_name = f"{self.task_prop['source_db']['schema']}.{self.task_prop['source_db']['rdbms_table']}"
        elif self.task_prop["source"] == "api":
            source_name = f"{self.task_prop['api_path']}"
        else:
            source_name = self.task_prop["source"]
        task_name = self.task_prop["task_name"]
        source = self.task_prop["source"]
        target_name = targets(self.task_prop["targets"])
        rows_to_insert = [
            {
                "job_id": jobid,
                "job_name": self.task_prop["job_name"],
                "task_name": task_name,
                "source": source,
                "source_name": source_name,
                "target": target_name,
                "target_name": target_name,
                "delta_start_val": delta_start,
                "delta_end_val": delta_end,
                "start_ts": start_ts,
                "end_ts": end_ts,
                "src_rec_count": source_count,
                "tgt_rec_count": pass_count,
                "err_rec_count": failed_count,
                "status": "COMPLETED",
            }
        ]

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


class JobAudit(PTransform, abc.ABC):
    def __init__(self, processed_data, passed_records, merged_error_records, task):
        # Initialize the JobAudit class with processed data, passed records, merged error records and task properties
        self.processed_data = processed_data
        self.passed_records = passed_records
        self.merged_error_records = merged_error_records
        self.task = task

    def expand(self, p_input):
        # Expand the input
        source_count_view = (
                self.processed_data
                | "Source Count" >> beam.combiners.Count.Globally().without_defaults()
        )
        # get pass count
        valid_count_view = (
                self.passed_records
                | "Valid Count" >> beam.combiners.Count.Globally().without_defaults()
        )
        # get fail count
        error_count_view = (
                self.merged_error_records
                | "Error Count" >> beam.combiners.Count.Globally().without_defaults()
        )

        return (
                p_input
                | beam.Create(["Single Element"])
                | "Auditing For Complete Job"
                >> beam.ParDo(
            AuditHelper(self.task),
            source_count=pvalue.AsSingleton(source_count_view),
            passed_count=pvalue.AsSingleton(valid_count_view),
            failed_count=pvalue.AsSingleton(error_count_view),
        )
        )
