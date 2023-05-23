from pyspark.sql import DataFrame
from pyspark.sql.functions import explode, to_timestamp, col, from_json, round
from pyspark.sql.types import StructType, StringType, StructField, ArrayType, IntegerType, DoubleType


class MeterValuesRequestTransformer:

    def _meter_values_request_filter(self, input_df: DataFrame):
        action = "MeterValues"
        message_type = 2
        return input_df.filter((input_df.action == action) & (input_df.message_type == message_type))

    def _meter_values_request_unpack_json(self, input_df: DataFrame):
        sampled_value_schema = StructType([
            StructField("value", StringType()),
            StructField("context", StringType()),
            StructField("format", StringType()),
            StructField("measurand", StringType()),
            StructField("phase", StringType()),
            StructField("unit", StringType()),
        ])

        meter_value_schema = StructType([
            StructField("timestamp", StringType()),
            StructField("sampled_value", ArrayType(sampled_value_schema)),
        ])

        body_schema = StructType([
            StructField("connector_id", IntegerType()),
            StructField("transaction_id", IntegerType()),
            StructField("meter_value", ArrayType(meter_value_schema)),
        ])
        return input_df.withColumn("new_body", from_json(col("body"), body_schema))

    def _meter_values_request_flatten(self, input_df: DataFrame):
        return input_df. \
            select("*", explode("new_body.meter_value").alias("meter_value")). \
            select("*", explode("meter_value.sampled_value").alias("sampled_value")). \
            withColumn("timestamp", to_timestamp(col("meter_value.timestamp"))). \
            withColumn("measurand", col("sampled_value.measurand")). \
            withColumn("phase", col("sampled_value.phase")). \
            withColumn("value", round(col("sampled_value.value").cast(DoubleType()), 2)). \
            select("message_id", "message_type", "charge_point_id", "action", "write_timestamp",
                   col("new_body.transaction_id").alias("transaction_id"),
                   col("new_body.connector_id").alias("connector_id"), "timestamp", "measurand", "phase", "value")

    def _write_meter_values_request(self, input_df: DataFrame, output_directory: str):
        input_df. \
            write. \
            mode("overwrite"). \
            parquet(output_directory)

    def run(self, input_df: DataFrame, output_directory: str):
        self._write_meter_values_request(input_df. \
                                         transform(self._meter_values_request_filter). \
                                         transform(self._meter_values_request_unpack_json). \
                                         transform(self._meter_values_request_flatten), output_directory)