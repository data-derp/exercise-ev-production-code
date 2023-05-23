from pyspark.sql import DataFrame
from pyspark.sql.functions import to_timestamp, col, from_json
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, ArrayType


class StopTransactionRequestTransformer:

    def _stop_transaction_request_filter(self, input_df: DataFrame):
        action = "StopTransaction"
        message_type = 2
        return input_df.filter((input_df.action == action) & (input_df.message_type == message_type))

    def _stop_transaction_request_unpack_json(self, input_df: DataFrame):
        body_schema = StructType([
            StructField("meter_stop", IntegerType(), True),
            StructField("timestamp", StringType(), True),
            StructField("transaction_id", IntegerType(), True),
            StructField("reason", StringType(), True),
            StructField("id_tag", StringType(), True),
            StructField("transaction_data", ArrayType(StringType()), True)
        ])
        return input_df.withColumn("new_body", from_json(col("body"), body_schema))

    def _stop_transaction_request_flatten(self, input_df: DataFrame):
        return input_df. \
            withColumn("meter_stop", input_df.new_body.meter_stop). \
            withColumn("timestamp", input_df.new_body.timestamp). \
            withColumn("transaction_id", input_df.new_body.transaction_id). \
            withColumn("reason", input_df.new_body.reason). \
            withColumn("id_tag", input_df.new_body.id_tag). \
            withColumn("transaction_data", input_df.new_body.transaction_data). \
            drop("new_body"). \
            drop("body")

    def _stop_transaction_request_cast(self, input_df: DataFrame) -> DataFrame:
        return input_df.withColumn("timestamp", to_timestamp(col("timestamp")))

    def _write_stop_transaction_request(self, input_df: DataFrame, output_directory: str):
        input_df. \
            write. \
            mode("overwrite"). \
            parquet(output_directory)

    def run(self, input_df: DataFrame, output_directory: str):
        self._write_stop_transaction_request(
            input_df. \
            transform(self._stop_transaction_request_filter). \
            transform(self._stop_transaction_request_unpack_json). \
            transform(self._stop_transaction_request_flatten). \
            transform(self._stop_transaction_request_cast), output_directory
        )
