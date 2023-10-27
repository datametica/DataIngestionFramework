from abc import ABC

import apache_beam as beam
from apache_beam import TaggedOutput
from apache_beam.io.gcp.internal.clients import bigquery as bq_client

from input.base_source_processor import BaseSourceProcessor


class FetchData(beam.DoFn):
    def process(self, element):
        yield TaggedOutput("valid", element)


class BigQueryProcessor(BaseSourceProcessor, ABC):
    """
    The BigQueryProcessor class is a component within a data ingestion framework
    that handles the processing of data from BigQuery.
    """

    def __init__(self, task):
        self.task = task

    def expand(self, p_input):
        data = p_input | "Read from BigQuery" >> beam.io.ReadFromBigQuery(
            query=self.task["source_db"]["query"],
            project=self.task["bq_project"],
            use_standard_sql=True,
            temp_dataset=bq_client.DatasetReference(
                projectId=self.task["bq_project"], datasetId=self.task["source_db"]["temp_dataset"])
        ) | beam.ParDo(FetchData()).with_outputs("valid",
                                                 "invalid")

        return data
