from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StringType, IntegerType, StructType, StructField


class StartTransactionRequestTransformer:
    def _start_transaction_request_filter(self, input_df: DataFrame):
        action = "StartTransaction"
        message_type = 2
        return input_df.filter((input_df.action == action) & (input_df.message_type == message_type))

    def _start_transaction_request_unpack_json(self, input_df: DataFrame):
        body_schema = StructType([
            StructField("connector_id", IntegerType(), True),
            StructField("id_tag", StringType(), True),
            StructField("meter_start", IntegerType(), True),
            StructField("timestamp", StringType(), True),
            StructField("reservation_id", IntegerType(), True),
        ])
        return input_df.withColumn("new_body", from_json(col("body"), body_schema))

    def _start_transaction_request_flatten(self, input_df: DataFrame):
        return input_df.\
            withColumn("connector_id", input_df.new_body.connector_id).\
            withColumn("id_tag", input_df.new_body.id_tag).\
            withColumn("meter_start", input_df.new_body.meter_start).\
            withColumn("timestamp", input_df.new_body.timestamp).\
            withColumn("reservation_id", input_df.new_body.reservation_id).\
            drop("new_body").\
            drop("body")

    def _start_transaction_request_cast(self, input_df: DataFrame) -> DataFrame:
        return input_df.withColumn("timestamp", to_timestamp(col("timestamp")))

    def _write_start_transaction_request(self, input_df: DataFrame, output_directory: str):
        input_df. \
            write. \
            mode("overwrite"). \
            parquet(output_directory)

    def run(self, input_df: DataFrame, output_directory: str):
        self._write_start_transaction_request(input_df. \
                                              transform(self._start_transaction_request_filter). \
                                              transform(self._start_transaction_request_unpack_json). \
                                              transform(self._start_transaction_request_flatten). \
                                              transform(self._start_transaction_request_cast), output_directory)
