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

""" This file contains all the parDo functions used in the pipeline """

import datetime
import json
import random
import uuid
from collections import OrderedDict

import apache_beam as beam
import pytz
from apache_beam import ParDo, WindowInto, WithKeys
from apache_beam.io.gcp import gce_metadata_util
from apache_beam.metrics.metric import Metrics
from apache_beam.pvalue import TaggedOutput
from apache_beam.transforms.window import FixedWindows
from google.cloud import bigquery

from utility import procedures
from utility.basic_utilities import generate_error_record
from utility.flatten import flatsplode


class ProcessJsonWithoutFlatten(beam.DoFn):
    def __init__(self, task):
        self.task = task
        self.msg_pushed_bq = Metrics.counter("msg_pushed_to_bq", "msg_pushed_to_bq")

    def process(self, elementIn):
        """
        This function is used to flatten the json data
        :param elementIn: json data
        :return: flattened json data
        """
        self.msg_pushed_bq.inc()
        raw_message_id, element = elementIn
        try:
            result = json.loads(element)
            yield TaggedOutput("valid", result)
        except Exception as e:
            error_record = {"job_id": gce_metadata_util.fetch_dataflow_job_id(), "error_record": str(element),
                            "error_type": "Invalid Json", "error_desc": str(e),
                            "insert_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
            yield TaggedOutput("invalid", error_record)


class addUID(beam.DoFn):
    def __init__(self):
        self.source_msg_count = Metrics.counter("source_msg_count", "source_msg_count")

    def process(self, element):
        """
        This function is used to add unique id to each element
        :param element: json data
        :return: unique id and json data
        """
        self.source_msg_count.inc()
        filename, element = element
        yield f"{uuid.uuid4()}_{datetime.datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S.%f')}", element, filename


class addKafkaUID(beam.DoFn):
    def process(self, element):
        """
        This function is used to add unique id to each element
        :param element: json data
        :return: unique id and json data
        """
        token, element = element
        uid = filename = f"{uuid.uuid4()}_{datetime.datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S.%f')}"
        try:
            element = element.decode("UTF-8")
        except:
            element = element
        yield uid, element, filename


class addBatchUID(beam.DoFn):
    def __init__(self, prop_dict):
        self.source_msg_count = Metrics.counter("source_msg_count", "source_msg_count")
        self.path = prop_dict['data_file'].strip("*")
        self.org_path = prop_dict['original_path'].strip("/")

    def process(self, element):
        """
        This function is used to add unique id to each element
        :param element: json data
        :return: unique id and json data
        """
        self.source_msg_count.inc()
        filename, element = element
        filename = filename.replace(self.path, self.org_path)
        yield f"{uuid.uuid4()}_{datetime.datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S.%f')}", element, filename


class ConvertToJson(beam.DoFn):
    def __init__(self, job_name, output_table):
        self.job_name = job_name
        self.output_table = output_table

    def process(self, elementIn):
        """
        This function is used to convert xml data to json
        :param elementIn: xml data
        :return: json data
        """
        import xmltodict
        uid, element, filename = elementIn
        try:
            json_from_xml = xmltodict.parse(element, xml_attribs=True)
            yield TaggedOutput("valid", (uid, json_from_xml, filename))
        except Exception as e:
            error_record = OrderedDict()
            error_record["job_name"] = self.job_name
            error_record["job_id"] = gce_metadata_util.fetch_dataflow_job_id()
            error_record["error_record"] = str(element)
            error_record["error_type"] = "Invalid Xml"
            error_record["error_desc"] = str(e)
            error_record["target_table"] = self.output_table
            error_record["source_name"] = filename
            error_record["insert_ts"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            yield TaggedOutput("invalid", error_record)


class GroupMessagesByFixedWindowsContinuous(beam.PTransform):

    def __init__(self, window_size, num_shards=5):
        self.window_size = int(window_size * 60)
        self.num_shards = num_shards

    def expand(self, pcoll):
        """
        This function is used to group the messages by fixed window
        :param pcoll: json data
        :return: windowed json data
        """
        return (pcoll | "Window into fixed intervals" >> WindowInto(
            FixedWindows(self.window_size)) | "Add timestamp to windowed elements" >> ParDo(
            AddTimestamp()) | "Group by key" >> beam.GroupIntoBatches(1000,
                                                                      max_buffering_duration_secs=30 + self.window_size))


class AddTimestamp(beam.DoFn):
    def process(self, element, publish_time=beam.DoFn.TimestampParam):
        """
        This function is used to add timestamp to each element
        :param element: json data
        :param publish_time: timestamp
        """
        try:
            element = element.decode("utf-8")
        except:
            element = element

        yield (datetime.datetime.utcfromtimestamp(float(publish_time)).strftime("%Y-%m-%d %H:%M:%S.%f"), element)


class WriteToGCSContinuous(beam.DoFn):
    def __init__(self, output_path):
        self.msg_pushed_gcs = Metrics.counter("msg_files_created", "msg_files_created")
        self.output_path = output_path.rstrip("/") + "/"

    def process(self, key_value, window=beam.DoFn.WindowParam):
        """
        This function is used to write data to GCS
        :param key_value: json data
        :param window: windowed data
        """
        self.msg_pushed_gcs.inc()
        ts_format = "%Y_%m_%d_%H_%M"
        window_start = window.start.to_utc_datetime().strftime(ts_format)
        window_end = window.end.to_utc_datetime().strftime(ts_format)
        shard_id, batch = key_value

        filename = self.output_path + "-".join([window_start, window_end, str(uuid.uuid4())])

        with beam.io.gcsio.GcsIO().open(filename=filename, mode="w") as f:
            for message_body in batch:
                f.write(f"{message_body}\n".encode("utf-8"))


class WriteToGcsStreaming(beam.DoFn):
    def __init__(self, output_path):
        if not output_path.endswith("/"):
            output_path += "/"
        self.output_path = output_path

    def process(self, key_value, window=beam.DoFn.WindowParam):
        """
        This function is used to write data to GCS
        :param key_value: json data
        :param window: windowed data
        """
        ts_format = "%Y_%m_%d_%H_%M"
        window_start = window.start.to_utc_datetime().strftime(ts_format)
        window_end = window.end.to_utc_datetime().strftime(ts_format)
        shard_id, batch = key_value

        filename = self.output_path + "-".join([window_start, window_end, str(uuid.uuid4())])

        with beam.io.gcsio.GcsIO().open(filename=filename, mode="w") as f:
            for message_body in batch:
                f.write(f"{message_body}\n".encode("utf-8"))


class ReadRepeated(beam.DoFn):
    def __init__(self, task):
        self.query = task["read_properties"]['query']
        self.project_id = task["read_properties"]['project']

    def process(self, element):
        """
        This function is used to read data from bigquery
        :param element: json data
        :return: json data
        """
        client = bigquery.Client(project=self.project_id)
        df = client.query(self.query).to_dataframe().to_json(orient="records")
        return json.loads(df)


class PerformJoins(beam.DoFn):
    def __init__(self, task):
        self.task = task
        self.join_column = task["filter_properties"]["join_column"]

    def process(self, element, joinData):
        """
        This function is used to perform joins
        :param element: json data
        :param joinData: json data
        :return: json data
        """
        for data in joinData:
            if all(element[src_col] == data[join_col] for src_col, join_col in self.join_column.items() if
                   src_col in element and join_col in data):
                element.update(data)
        yield element


class FlattenJson(beam.DoFn):
    def __init__(self, task):
        self.task = task
        self.json_count = Metrics.counter("preflat_msg_count", "preflat_msg_count")
        self.flattened_json_count = Metrics.counter("postflat_msg_count", "postflat_msg_count")

    def process(self, element, ColumnNameMap):
        """
        This function is used to flatten json data
        :param element: json data
        :param ColumnNameMap: json data
        :return: json data
        """
        self.json_count.inc()
        records = []
        uid, element, filename = element
        ColumnNameMap = json.loads("".join(ColumnNameMap))
        col_map = ColumnNameMap["flat_table_mapping"]
        try:
            element = json.loads(element)
            flat_json = flatsplode(element)
            for json_data in flat_json:
                ts_now = str(datetime.datetime.now(pytz.timezone('US/Mountain')))
                self.flattened_json_count.inc()
                updated_dict = {}
                for key in col_map.values():
                    if key.split("|")[0] not in json_data.keys():
                        json_data[key.split("|")[0]] = None

                for key_src, value_src in json_data.items():
                    for key_map, value_map in col_map.items():
                        proc_check = value_map.split("|")
                        value_map = proc_check[0]
                        if key_src == value_map:
                            if len(proc_check) > 1:
                                for function in "|".join(proc_check[1:]).split(","):
                                    fn = getattr(procedures, function, procedures.default_fn)
                                    value_src = fn(value_src, key_map)

                            updated_dict[key_map] = value_src

                updated_dict["FileName"] = updated_dict["source_name"] = filename
                updated_dict["RawMessageId"] = uid

                for k, v in updated_dict.items():
                    if v == "CURRENT_TIMESTAMP":
                        updated_dict[k] = ts_now
                    elif v == "KAFKA_TOPIC_NAME":
                        updated_dict[k] = self.task['topic_name']
                    elif v == "ALTERNATE_KAFKA_TOPIC_ID":
                        updated_dict[k] = f"{self.task['topic_name']}_{ts_now}"
                records.append(TaggedOutput("valid", updated_dict))

            return records
        except Exception as e:
            error_record = {"job_id": gce_metadata_util.fetch_dataflow_job_id(), "error_record": str(element),
                            "error_type": "Invalid Record", "error_desc": str(e), "source_name": filename,
                            "insert_ts": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
            return [TaggedOutput("invalid", error_record)]


class GroupMessagesByFixedWindows(beam.PTransform):

    def __init__(self, window_size, num_shards=5):
        self.window_size = int(window_size * 60)
        self.num_shards = num_shards

    def expand(self, pcoll):
        """
        This function is used to group messages by fixed windows
        :param pcoll: json data
        :return: json data
        """
        return (pcoll | "Window into fixed intervals" >> WindowInto(
            FixedWindows(self.window_size)) | "Add timestamp to windowed elements" >> ParDo(
            AddTimestamp()) | "Add key" >> WithKeys(
            lambda _: random.randint(0, self.num_shards - 1)) | "Group by key" >> beam.GroupByKey())


class FileOperations(beam.DoFn):
    def process(self, element):
        """
        This function is used to read file content
        :param element: json data
        :return: json data
        """
        file_meta_data, filedata = element
        filename = file_meta_data.path
        for i in filedata:
            yield filename, i


class FileMetaData(beam.DoFn):
    def process(self, element):
        """
        This function is used to read file metadata
        :param element: json data
        :return: json data
        """
        file_meta_data = element.metadata
        filedata = element.read_utf8().splitlines()
        yield file_meta_data, filedata


class ReadFileContent(beam.DoFn):
    def process(self, element):
        """
        This function is used to read file content
        :param element: json data
        :return: json data
        """
        filename, filedata = element
        for i in filedata:
            yield filename, i


class Triaging(beam.DoFn):
    def process(self, element):
        """
        This function is used to triage data
        :param element: json data
        :return: json data
        """
        yield f"{element[0]}\u0001{element[1]}"


class DebugDoFn(beam.DoFn):
    def process(self, element):
        """
        This function is used to print data
        :param element: json data
        :return: json data
        """
        try:
            rec = {}
            for i in element._fields:
                val = getattr(element, i)
                if val == "None":
                    val = None
                if str(val).startswith("Timestamp"):
                    val = val.to_utc_datetime().strftime("%Y-%m-%d_%H:%M:%S.%f").replace("_", " ")
                rec[i] = val
            yield TaggedOutput("valid", rec)
        except Exception as e:
            error_record = generate_error_record(element=element, exception=e, job_name="",
                                                 target="", src="",
                                                 err_type="Invalid Record", job_id=None)
            yield TaggedOutput("invalid", error_record)
