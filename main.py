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

""" This module is the main module for the pipeline. It contains the main function that is the entry point for the pipeline.
It is responsible for reading the property file, validating the properties, setting the pipeline options, and running the pipeline.
"""

from Imports.imports import *

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    argv = sys.argv
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)
    options = pipeline_options.view_as(IngestionOptions)

    if options.log_level:
        logger = get_logger(__name__, options.log_level)
    else:
        logger = get_logger(__name__, "INFO")
    # Reading the Property Files as dict
    prop_map = prop_config(pipeline_options.get_all_options())
    # Validation the Properties
    ValidateProperties(prop_map, options)
    # Setting the Pipeline Options from the Property File
    pipeline_options = PipelineOptions.from_dictionary(prop_map[0])
    p_options = pipeline_options.get_all_options()

    pipeline = beam.Pipeline(options=pipeline_options)

    for task in prop_map:
        task["streaming"] = streaming_flag = pipeline_options.get_all_options().get("streaming")
        # This Job ID is used for auditing purposes
        if p_options['job_id']:
            job_id = str(options.job_id)
        else:
            job_id = str(uuid.uuid4())

        logger.info(f"Starting Job with {job_id}")
        task["project_id"] = pipeline_options.get_all_options()["project"]
        task["jobid"] = job_id

        # Resolve project and bucket
        task = resolve_property(task, options)
        task["start_ts"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        audit_processor = None
        audit_processor_instance = None
        # Setting Audit Classes if enabled. Streaming Jobs are not Audited.
        if "audit" in task and not streaming_flag:
            if str(task["audit"]["required"]).lower() == "true":
                audit_processor = globals()["BQAudit"]
                if str(task["audit"]["audit_target"]).lower() == "rdbms":
                    audit_processor = globals()["RdbmsAudit"]

        # Inserting the Job Start Audit
        if audit_processor is not None:
            audit_processor_instance = audit_processor(task)
            audit_processor_instance.start_audit()

        input_processor = globals()[task["input_processor"]]
        # if this is an incremental load. Derive query and respective start and end
        if "source_db" in task:
            if "incremental_mode" in task["source_db"]:
                logger.info("Incremental Job")
                delta_start, delta_end, query = create_incremental_query(task)
                logger.info(f"query: {str(query)}")
                task["delta_start"] = delta_start
                task["delta_end"] = delta_end
                task["source_db"]["query"] = query

        # Apply Processor to the input . Required Processor will be derived from the property file.
        processed_data = (pipeline | f"{task['input_processor']}_{uuid.uuid4()}" >> input_processor(task))

        passed_records = processed_data["valid"]
        processed_data_cnt = processed_data["valid"]
        failed_records = processed_data["invalid"]

        # if only "required_columns" is enabled and column selection needs to be done
        if "required_columns" in task:
            required_cols = task["required_columns"]
            passed_records = (passed_records | f"Select_Only_Req_Columns" >> SelectRequiredColumns(task))

        passed_records_bq = passed_records

        # Writing valid records to multiple targets one by one
        for target in task["targets"]:
            target_type = list(target.keys())[0]
            target_processor = globals()[target_map[target_type]]
            passed_records_bq | f"Write_to_{target_type}_{uuid.uuid4()}" >> target_processor(task, target[target_type])

        # Job Level Audit
        if "audit" in task:
            if str(task["audit"]["required"]).lower() == "true" and not streaming_flag:
                if str(task["audit"]["audit_target"]).lower() == "bigquery":
                    pipeline | f"Job_Level_Audit" >> JobAudit(processed_data_cnt, passed_records, failed_records,
                                                              task)

            # Check If file_level_auditing is enabled
            if "enable_file_level_auditing" in task["audit"] and not streaming_flag:
                if str(task["audit"]["enable_file_level_auditing"]).lower() == "true":
                    pipeline | f"File_Level_Audit" >> FileAudit(processed_data_cnt, passed_records,
                                                                failed_records,
                                                                task)
    # Running the Pipeline
    pipeline.run()
