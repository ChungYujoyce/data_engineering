import argparse
import csv
import numpy
import json
import os
import torch
from typing import Tuple
from google.cloud import bigquery
from google.oauth2 import service_account

import apache_beam as beam
from apache_beam.io.gcp.bigquery import ReadFromBigQuery
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.dataframe.convert import to_pcollection
from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerTensor
from apache_beam.ml.inference.pytorch_inference import PytorchModelHandlerKeyedTensor
from apache_beam.options.pipeline_options import PipelineOptions

from model import LinearRegression
import warnings
warnings.filterwarnings('ignore')

"""
RunInference for predictions
"""
save_model_dir_multiply_five = 'five_times_table_torch.pt'
save_model_dir_multiply_ten = 'ten_times_table_torch.pt'

value_to_predict = numpy.array([20, 40, 60, 90], dtype=numpy.float32).reshape(-1, 1)

torch_five_times_model_handler = PytorchModelHandlerTensor(
    state_dict_path=save_model_dir_multiply_five,
    model_class=LinearRegression,
    model_params={'input_dim': 1,
                  'output_dim': 1}
                  )
pipeline = beam.Pipeline()

class PredictionProcessor(beam.DoFn):
    """
    A processor to format the output of the RunInference transform.
    """
    def process(self, element: PredictionResult):
        input_value = element.example
        output_value = element.inference
        yield (f"input is {input_value.item()} output is {output_value.item()}")

class PredictionWithKeyProcessor(beam.DoFn):
    def __init__(self):
        beam.DoFn.__init__(self)

    def process(self, element: Tuple[str, PredictionResult]):
        key = element[0]
        input_value = element[1].example
        output_value = element[1].inference
        yield (f"key: {key}, input: {input_value.item()} output: {output_value.item()}" )

with pipeline as p:
      (
      p 
      | "ReadInputData" >> beam.Create(value_to_predict)
      | "ConvertNumpyToTensor" >> beam.Map(torch.Tensor)
      | "RunInferenceTorch" >> RunInference(torch_five_times_model_handler)
      | "PostProcessPredictions" >> beam.ParDo(PredictionProcessor())
      | beam.Map(print)
      )

# Bigquery

credentials = service_account.Credentials.from_service_account_file('./your/path/to/.json')
project = "mlops-384602"

client = bigquery.Client(credentials=credentials, project=project)

# Make sure the dataset_id is unique in your project.
dataset_id = '{project}.maths'.format(project=project)
dataset = bigquery.Dataset(dataset_id)

# Modify the location based on your project configuration.
dataset.location = 'US'
dataset = client.create_dataset(dataset, exists_ok=True)

# Table name in the BigQuery dataset.
table_name = 'maths_problems_1'

query = """
    CREATE OR REPLACE TABLE
      {project}.maths.{table} ( key STRING OPTIONS(description="A unique key for the maths problem"),
    value FLOAT64 OPTIONS(description="Our maths problem" ) );
    INSERT INTO maths.{table}
    VALUES
      ("first_question", 105.00),
      ("second_question", 108.00),
      ("third_question", 1000.00),
      ("fourth_question", 1013.00)
""".format(project=project, table=table_name)

create_job = client.query(query)
print(create_job.result())

# To read keyed data, use BigQuery as the pipeline source.
# Run command: python beam.py --noauth_local_webserver

bucket = "math_model"

pipeline_options = PipelineOptions().from_dictionary({'temp_location':f'gs://{bucket}/tmp'})
                                                      
pipeline = beam.Pipeline(options=pipeline_options)

keyed_torch_five_times_model_handler = KeyedModelHandler(torch_five_times_model_handler)

table_name = 'maths_problems_1'
table_spec = f'{project}:maths.{table_name}'

with pipeline as p:
      (
      p
      | "ReadFromBQ" >> beam.io.ReadFromBigQuery(table=table_spec) 
      | "PreprocessData" >> beam.Map(lambda x: (x['key'], x['value']))
      | "ConvertNumpyToTensor" >> beam.Map(lambda x: (x[0], torch.Tensor([x[1]])))
      | "RunInferenceTorch" >> RunInference(keyed_torch_five_times_model_handler)
      | "PostProcessPredictions" >> beam.ParDo(PredictionWithKeyProcessor())
      | beam.Map(print)
      )
