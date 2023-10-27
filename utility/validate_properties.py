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

"""This module contains the class that can be used to validate the properties of the pipeline.
"""
from exceptions.property_validation_exception import PropertyValidationException
from utility.logger_setup import _get_logger


class ValidateProperties:
    """This class contains the methods that can be used to validate the properties of the pipeline.
    """

    def __init__(self, prop_map, options):
        self.prop_map = prop_map
        self.options = options
        if options and options.log_level:
            self.logger = _get_logger(__name__, options.log_level)
        else:
            self.logger = _get_logger(__name__, "INFO")

    def validate_pipeline_options(self):
        if self.options.project is None:
            raise PropertyValidationException(f"project id should be present")

        if self.options.task_prop_file is None:
            raise PropertyValidationException(f"Task property file should be present")

    def validate_properties(self):
        for task in self.prop_map:
            if "source" not in task:
                raise PropertyValidationException(
                    "source must be specified either at job_level or task_level"
                )
            if "targets" not in task:
                raise PropertyValidationException(
                    "target must be specified either at job_level or task_level"
                )

            if "data_trans_enabled" in task:
                if str(task["data_trans_enabled"]).lower() == "true":
                    if "transformation_table" not in task:
                        raise PropertyValidationException(
                            """transformation_table not specified. 
                        It must be specified when data_trans_enabled is true """
                        )
            if "data_quality_enabled" in task:
                if str(task["data_quality_enabled"]).lower() == "true":
                    if "data_quality_rule_table" not in task:
                        raise PropertyValidationException(
                            """data_quality_rule_table not specified. 
                        It must be specified when data_quality_enabled is true """
                        )
            if "error" in task:

                error_properties = task["error"]
                if "insert_error_rec" not in error_properties:
                    self.logger.info(
                        "insert_error_rec property not found. Error Records will not be Processed"
                    )
                else:
                    if "error_table" not in error_properties:
                        raise PropertyValidationException(
                            "error_table not specified. It must be present when insert_error_rec"
                        )
                    else:
                        if len(str(error_properties["error_table"]).split(".")) != 2:
                            raise PropertyValidationException(
                                f"Invalid Table Name {str(error_properties['error_table'])}. It should be of the "
                                f"form <project.dataset.table>"
                            )

            # Validate Properties For Snowflake Source
            if str(task["source"]).lower() == "snowflake":
                # Validating Job level properties
                if "secret_manager" not in task:
                    raise PropertyValidationException(
                        "secret_manager should be specified"
                    )
                else:
                    secret_manager = task["secret_manager"]
                    if "gcp_project_id" not in secret_manager:
                        raise PropertyValidationException(
                            "gcp_project_id should be specified in secret_manager"
                        )
                    if "secret_id" not in secret_manager:
                        raise PropertyValidationException(
                            "secret_id should be specified in secret_manager"
                        )
                    if "version_id" not in secret_manager:
                        raise PropertyValidationException(
                            "version_id should be specified in secret_manager"
                        )

                # Validating task level properties
                if "source_db" not in task:
                    raise PropertyValidationException(
                        """source_db must be specified , if source is snowflake"""
                    )
                else:
                    # print(f"{str(task['source'])}")
                    if str(task["source"]).lower() == "snowflake":
                        source = task["source_db"]
                        if "schema" not in source:
                            raise PropertyValidationException(
                                """schema must be specified , if source is snowflake"""
                            )
                        if "database" not in source:
                            raise PropertyValidationException(
                                """database must be specified , if source is snowflake"""
                            )
                        if "snowflake_table" not in source:
                            raise PropertyValidationException(
                                """snowflake_table must be specified , if source is snowflake"""
                            )

                if "target" not in task:
                    raise PropertyValidationException("""target must be specified""")
                else:
                    target = task["target"]
                    if "target_table" not in target:
                        raise PropertyValidationException(
                            """target_table must be specified"""
                        )
                    else:
                        table_id = target["target_table"]
                        if len(str(table_id).split(".")) < 2:
                            raise PropertyValidationException(
                                f"""Invalid Table name {table_id} must be specified"""
                            )
                    if "schema_file" not in target:
                        raise PropertyValidationException(
                            f"""schema_file must be specified"""
                        )
                    else:
                        if not str(target["schema_file"]).startswith("gs://"):
                            raise PropertyValidationException(
                                f"""Invalid Schema File {str(target['schema_file'])}. It must be a valid GCS file"""
                            )
                    if "write_disposition" in target:
                        write_disposition = target["write_disposition"]
                        if str(write_disposition).upper() not in [
                            "WRITE_TRUNCATE",
                            "WRITE_APPEND",
                            "WRITE_EMPTY",
                        ]:
                            raise PropertyValidationException(
                                f"Invalid value {write_disposition} for write_disposition property"
                            )

                    if "schema_file" not in target:
                        raise PropertyValidationException(
                            """schema_file must be specified"""
                        )
                    if "write_disposition" not in target:
                        raise PropertyValidationException(
                            """write_disposition must be specified"""
                        )

            # Validate Properties For delimited_file Source
            if str(task["source"]).lower() == "delimited_file":
                if "input_processor" not in task:
                    raise PropertyValidationException("Input Processor Must be Specified")
                if "data_file" not in task:
                    raise PropertyValidationException("Input File Must be Specified")
                else:
                    if not str(task["data_file"]).startswith("gs://"):
                        raise PropertyValidationException(
                            f"Invalid GCS Path {str(task['input_file'])}"
                        )
                if "delimited_file_props" in task:
                    if "delimiter" not in task["delimited_file_props"]:
                        self.logger.debug(
                            "delimiter not specified. By default ',' will be used to split records"
                        )

            # Validate Properties For Api Source
            if str(task["source"]).lower() == "api":
                if "api_path" not in task:
                    raise PropertyValidationException(
                        "A url path must be specified , if source is api"
                    )
                if "authentication" not in task:
                    raise PropertyValidationException(
                        "Authentication must be specified, if source is api"
                    )
                # Check if not public api, ways to authenticate
