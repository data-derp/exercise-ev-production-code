from pyspark import SparkFiles
from pyspark.sql import SparkSession
from transformer import Transformer

meter_values_request_url = "https://github.com/data-derp/exercise-ev-databricks/raw/main/batch-processing-silver/output/MeterValuesRequest/part-00000-tid-468425781006758111-f9d48bc3-3b4c-497e-8e9c-77cf63db98f8-207-1-c000.snappy.parquet"
start_transaction_request_url = "https://github.com/data-derp/exercise-ev-databricks/raw/main/batch-processing-silver/output/StartTransactionRequest/part-00000-tid-9191649339140138460-0a4f58e5-1397-41cc-a6a1-f6756f3332b6-218-1-c000.snappy.parquet"
start_transaction_response_url = "https://github.com/data-derp/exercise-ev-databricks/raw/main/batch-processing-silver/output/StartTransactionResponse/part-00000-tid-5633887168695670016-762a6dfa-619c-412d-b7b8-158ee41df1b2-185-1-c000.snappy.parquet"
stop_transaction_request_url = "https://github.com/data-derp/exercise-ev-databricks/raw/main/batch-processing-silver/output/StopTransactionRequest/part-00000-tid-5108689541678827436-b76f4703-dabf-439a-825d-5343aabc03b6-196-1-c000.snappy.parquet"
spark = SparkSession.builder.appName("test").getOrCreate()
spark.sparkContext.addFile(meter_values_request_url)
spark.sparkContext.addFile(start_transaction_request_url)
spark.sparkContext.addFile(start_transaction_response_url)
spark.sparkContext.addFile(stop_transaction_request_url)
meter_values_request_df = spark.read.parquet("file://" + SparkFiles.get("part-00000-tid-468425781006758111-f9d48bc3-3b4c-497e-8e9c-77cf63db98f8-207-1-c000.snappy.parquet"))
start_transaction_request_df = spark.read.parquet("file://" + SparkFiles.get("part-00000-tid-9191649339140138460-0a4f58e5-1397-41cc-a6a1-f6756f3332b6-218-1-c000.snappy.parquet"))
start_transaction_response_df = spark.read.parquet("file://" + SparkFiles.get("part-00000-tid-5633887168695670016-762a6dfa-619c-412d-b7b8-158ee41df1b2-185-1-c000.snappy.parquet"))
stop_transaction_request_df = spark.read.parquet("file://" + SparkFiles.get("part-00000-tid-5108689541678827436-b76f4703-dabf-439a-825d-5343aabc03b6-196-1-c000.snappy.parquet"))


Transformer().run(meter_values_request_df=meter_values_request_df, start_transaction_request_df=start_transaction_request_df, start_transaction_response_df=start_transaction_response_df, stop_transaction_request_df=stop_transaction_request_df, output_directory="../output")

