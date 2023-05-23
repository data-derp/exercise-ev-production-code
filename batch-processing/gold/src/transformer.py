from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import when, sum, abs, first, last, lag, col, round
from pyspark.sql.types import DoubleType


class Transformer:
    def _match_start_transaction_requests_with_responses(self, input_df: DataFrame, join_df: DataFrame) -> DataFrame:
        return input_df.\
            join(join_df, input_df.message_id == join_df.message_id, "left").\
            select(
                input_df.charge_point_id.alias("charge_point_id"),
                input_df.transaction_id.alias("transaction_id"),
                join_df.meter_start.alias("meter_start"),
                join_df.timestamp.alias("start_timestamp")
            )


    def _join_with_start_transaction_responses(self, input_df: DataFrame, join_df: DataFrame) -> DataFrame:
        return input_df. \
            join(join_df, input_df.transaction_id == join_df.transaction_id, "left"). \
            select(
            join_df.charge_point_id,
            join_df.transaction_id,
            join_df.meter_start,
            input_df.meter_stop.alias("meter_stop"),
            join_df.start_timestamp,
            input_df.timestamp.alias("stop_timestamp")
        )


    def _calculate_total_time(self, input_df: DataFrame) -> DataFrame:
        seconds_in_one_hour = 3600
        return input_df. \
            withColumn("total_time", col("stop_timestamp").cast("long") / seconds_in_one_hour - col("start_timestamp").cast(
            "long") / seconds_in_one_hour). \
            withColumn("total_time", round(col("total_time").cast(DoubleType()), 2))

    def _calculate_total_energy(self, input_df: DataFrame) -> DataFrame:
        return input_df \
            .withColumn("total_energy", (col("meter_stop") - col("meter_start"))/1000) \
            .withColumn("total_energy", round(col("total_energy").cast(DoubleType()),2))


    def _calculate_total_parking_time(self, input_df: DataFrame) -> DataFrame:
        window_by_transaction = Window.partitionBy("transaction_id").orderBy(col("timestamp").asc())
        window_by_transaction_group = Window.partitionBy(["transaction_id", "charging_group"]).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        return input_df.\
            withColumn("charging", when(col("value") > 0, 1).otherwise(0)).\
            withColumn("boundary", abs(col("charging")-lag(col("charging"), 1, 0).over(window_by_transaction))).\
            withColumn("charging_group", sum("boundary").over(window_by_transaction)).\
            select(col("transaction_id"), "timestamp", "value", "charging", "boundary", "charging_group").\
            withColumn("first", first('timestamp').over(window_by_transaction_group).alias("first_id")).\
            withColumn("last", last('timestamp').over(window_by_transaction_group).alias("last_id")).\
            filter(col("charging") == 0).\
            groupBy("transaction_id", "charging_group").agg(
                first((col("last").cast("long") - col("first").cast("long"))).alias("group_duration")
            ).\
            groupBy("transaction_id").agg(
                round((sum(col("group_duration"))/3600).cast(DoubleType()), 2).alias("total_parking_time")
            )

    def _join_and_shape(self, input_df: DataFrame, joined_df: DataFrame) -> DataFrame:
        return input_df.\
            join(joined_df, on=input_df.transaction_id == joined_df.transaction_id, how="left").\
            select(
                input_df.charge_point_id,
                input_df.transaction_id,
                input_df.meter_start,
                input_df.meter_stop,
                input_df.start_timestamp,
                input_df.stop_timestamp,
                input_df.total_time,
                input_df.total_energy,
                joined_df.total_parking_time
            )


    def _write_to_parquet(self, input_df: DataFrame, output_directory: str):
        input_df.\
            write.\
            mode("overwrite").\
            parquet(output_directory)


    def run(self, stop_transaction_request_df: DataFrame, start_transaction_response_df: DataFrame, start_transaction_request_df: DataFrame, meter_values_request_df: DataFrame, output_directory: str):
        self._write_to_parquet(stop_transaction_request_df. \
                         transform(
            self._join_with_start_transaction_responses,
            start_transaction_response_df. \
                transform(self._match_start_transaction_requests_with_responses, start_transaction_request_df)
        ). \
                         transform(self._calculate_total_time). \
                         transform(self._calculate_total_energy). \
                         transform(self._join_and_shape, meter_values_request_df.filter(
            (col("measurand") == "Power.Active.Import") & (col("phase").isNull())). \
                                   transform(self._calculate_total_parking_time)
                                   ), output_directory)