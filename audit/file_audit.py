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

"""This module contains the FileAudit and FileLevelAudit classes which are used to insert audit records into BigQuery for file-level processing
"""

import abc
from abc import ABC
from datetime import datetime

import apache_beam as beam
from apache_beam.transforms.ptransform import PTransform
from google.cloud import bigquery

from audit.utility import targets
from utility.logger_setup import _get_logger
from utility.mailer import send_mail


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

        audit_table_id = str(self.task_prop["audit"]["file_audit_table"]).replace(
            ":", "."
        )
        load_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        jobid = self.task_prop["jobid"]
        source_name = file_name
        task_name = self.task_prop["task_name"]
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
                "target": target_name,
                "target_name": target_name,
                "src_rec_count": source_count,
                "tgt_rec_count": pass_count,
                "err_rec_count": failed_count,
                "load_ts": load_ts,
            }
        ]
        # Send email
        send_mail(rows_to_insert)
        # Insert rows into BigQuery
        errors = self.bqClient.insert_rows_json(audit_table_id, rows_to_insert)
        if len(errors) == 0:
            self.logger.info(
                f"Audit for job id {str(jobid)} has been inserted in {audit_table_id}"
            )
        else:
            self.logger.error(
                f"{str(len(errors))} errors found while inserting records with jobid {str(jobid)} "
            )
            raise Exception(
                f"{str(len(errors))} errors found while inserting records with jobid {str(jobid)} {str(errors)}"
            )


class FileAudit(PTransform, abc.ABC):
    def __init__(self, processed_data, passed_records, merged_error_records, task):
        # Initialize the FileAudit class with the processed data, passed records, merged error records, and task properties
        self.processed_data = processed_data
        self.passed_records = passed_records
        self.merged_error_records = merged_error_records
        self.task = task

    def expand(self, p_input):
        # Get source record view for source count
        source_record_view = (
                self.processed_data
                | "Get Source Name For Source Count"
                >> beam.FlatMap((lambda x: {x["source_name"]: 1}))
                | "source_name per element for source" >> beam.combiners.Count.PerElement()
        )

        # Get source record view for valid count
        passed_record_view = (
                self.passed_records
                | "Get Source Name For Valid Count"
                >> beam.FlatMap((lambda x: {x["source_name"]: 1}))
                | "source_name per element" >> beam.combiners.Count.PerElement()
        )

        # Get error record view
        try:
            error_record_view = (
                    self.merged_error_records
                    | "Get Error Name" >> beam.FlatMap((lambda x: {x["source_name"]: 1}))
                    | beam.combiners.Count.PerElement()
            )
        except Exception:
            error_record_view = p_input | beam.Create([])

        # Merge file count collection
        file_count_collection = (
                                    {
                                        "source_record_view": source_record_view,
                                        "passed_record_view": passed_record_view,
                                        "error_record_view": error_record_view,
                                    }
                                ) | "Merge" >> beam.CoGroupByKey()

        # Perform file level audit
        file_count_collection | "File Level Audit" >> beam.ParDo(
            FileLevelAudit(self.task)
        )

        return p_input
