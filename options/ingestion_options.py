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

"""This module contains the options class for ingestion pipeline.
"""
from apache_beam.options.pipeline_options import PipelineOptions


class IngestionOptions(PipelineOptions):
    """This class defines the options for ingestion pipeline.
    """

    @classmethod
    def _add_argparse_args(cls, parser):
        """This method adds the arguments to the parser.
        Args:
            parser: Argument parser.
        """
        parser.add_argument("--job_id", type=str, help="Job Id ")
        parser.add_argument(
            "--project_id", type=str, help="project ID of GCP project", default=None
        )

        parser.add_argument(
            "--job_prop_file", type=str, help="Gcs File containing job properties", default=None
        )
        parser.add_argument(
            "--task_prop_file", type=str, help="Gcs File containing Task properties", default=None
        )
        parser.add_argument(
            "--code_bucket",
            type=str,
            default="\u0001\u0001\u0001\u0001\u0001",
            help="GCS bucket name which holds all schema files",
            required=False,
        )
        parser.add_argument(
            "--data_bucket",
            type=str,
            default="\u0001\u0001\u0001\u0001\u0001",
            help="GCS bucket name which holds data files",
            required=False,
        )
        parser.add_argument("--log_level", type=str, help="Gcs Schema File Path", default="INFO")
        parser.add_argument(
            "--task_prop_table", type=str, help="Gcs File containing job properties", default=None
        )
        parser.add_argument(
            "--task_id", type=str, help="Gcs File containing job properties", default=None
        )
        parser.add_argument(
            "--targets"
        )
        parser.add_argument(
            "--resolve_variables", type=str, help="airflow variables which need to be resolved", default=None
        )
        parser.add_argument(
            "--bq_project",
            default="\u0001\u0001\u0001\u0001\u0001"
        )
        parser.add_argument(
            "--bq_hook"
        )
        parser.add_argument(
            "--adhoc_run", type=str, default=False
        )
        parser.add_argument(
            "--delta_start_val"
        )
        parser.add_argument(
            "--delta_end_val"
        )
        parser.add_argument(
            "--max_delta"
        )
