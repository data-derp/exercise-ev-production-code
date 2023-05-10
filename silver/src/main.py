from pyspark import SparkFiles
from pyspark.sql import SparkSession
from meter_values_request_transformer import MeterValuesRequestTransformer
from start_transaction_request_transformer import StartTransactionRequestTransformer
from start_transaction_response_transformer import StartTransactionResponseTransformer
from stop_transaction_request_transformer import StopTransactionRequestTransformer

url = "https://github.com/data-derp/exercise-ev-databricks/raw/main/batch-processing-bronze/output/part-00000-tid-5639432049181042996-9b69459e-aeff-43e0-8e41-01d3b2c6f5d5-37-1-c000.snappy.parquet"
spark = SparkSession.builder.appName("test").getOrCreate()
spark.sparkContext.addFile(url)
df = spark.read.parquet("file://" + SparkFiles.get("part-00000-tid-5639432049181042996-9b69459e-aeff-43e0-8e41-01d3b2c6f5d5-37-1-c000.snappy.parquet"))

MeterValuesRequestTransformer().run(input_df=df, output_directory="../output/MeterValuesRequest")
StartTransactionRequestTransformer().run(input_df=df, output_directory="../output/StartTransactionRequest")
StartTransactionResponseTransformer().run(input_df=df, output_directory="../output/StartTransactionResponse")
StopTransactionRequestTransformer().run(input_df=df, output_directory="../output/StopTransactionRequest")