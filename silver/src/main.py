import os

from pyspark import SparkFiles
from pyspark.sql import SparkSession
from meter_values_request_transformer import MeterValuesRequestTransformer
from start_transaction_request_transformer import StartTransactionRequestTransformer
from start_transaction_response_transformer import StartTransactionResponseTransformer
from stop_transaction_request_transformer import StopTransactionRequestTransformer

url = "https://github.com/data-derp/exercise-ev-databricks/raw/main/batch-processing-bronze/output/part-00000-tid-5639432049181042996-9b69459e-aeff-43e0-8e41-01d3b2c6f5d5-37-1-c000.snappy.parquet"
spark = SparkSession.builder.appName("silver").getOrCreate()
spark.sparkContext.addFile(url)
df = spark.read.parquet("file://" + SparkFiles.get("part-00000-tid-5639432049181042996-9b69459e-aeff-43e0-8e41-01d3b2c6f5d5-37-1-c000.snappy.parquet"))

dir_path = os.path.dirname(os.path.realpath(__file__))

MeterValuesRequestTransformer().run(input_df=df, output_directory=f"{dir_path}/../output/MeterValuesRequest")
StartTransactionRequestTransformer().run(input_df=df, output_directory=f"{dir_path}/../output/StartTransactionRequest")
StartTransactionResponseTransformer().run(input_df=df, output_directory=f"{dir_path}/../output/StartTransactionResponse")
StopTransactionRequestTransformer().run(input_df=df, output_directory=f"{dir_path}/../output/StopTransactionRequest")