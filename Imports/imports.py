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

"""This module contains the imports for the data ingestion framework.
"""
import argparse
import datetime
import sys
import uuid

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from audit.file_audit import FileAudit
from audit.job_audit import JobAudit
from options.ingestion_options import IngestionOptions
from output.base_output_writer import target_map
from p_transforms.select_req_columns import SelectRequiredColumns
from utility.basic_utilities import resolve_property
from utility.logger_setup import _get_logger as get_logger
from utility.property_assignment import prop_config
from utility.rdbms_utility import create_incremental_query
from utility.tags import Tags
from apache_beam.transforms import window
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.transforms.trigger import AfterProcessingTime, AccumulationMode, Repeatedly
from dofns.parDoFunctions import PerformJoins, ReadRepeated


from input.api_processor import ApiProcessor
from input.kafka_processor import KafkaProcessor
from input.rdbms_processor import RdbmsProcessor
from input.pubsub_processor import PubsubProcessor
from input.parquet_processor import ParquetProcessor
from input.ndjson_processor import NdJsonProcessor
from input.json_processor import JsonProcessor
from input.csv_processor import CSVProcessor
from input.snowflake_io_processor import SnowFlakeIOProcessor
from input.avro_processor import AvroProcessor
from input.json_flatten_streaming_processor import JsonFlattenStreamingProcessor
from input.kafka_flatten_processor import KafkaFlattenProcessor
from input.json_flatten_batch_processor import JsonFlattenBatchProcessor
from input.bigquery_processor import BigQueryProcessor

from output.pubsub_writer import PubsubWriter
from output.kafka_writer import KafkaWriter
from output.bigquery_writer import BigQueryWriter
from output.bigquery_stream_writer import BigQueryStreamWriter
from output.rdbms_writer import RDBMSWriter
from output.rdbms_window_writer import RDBMSWindowWriter
from output.gcs_writer import GCSWriter

from audit.bqaudit import BQAudit


from utility.validate_properties import ValidateProperties

