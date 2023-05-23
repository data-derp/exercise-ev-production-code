from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp, year, month, dayofmonth, hour, minute, concat, lpad


class Bronze:

    def set_partitioning_cols(self, input_df: DataFrame):
        return input_df. \
            withColumn("write_timestamp_new", to_timestamp(col("write_timestamp"))). \
            withColumn("year", year(col("write_timestamp"))). \
            withColumn("month", month(col("write_timestamp"))). \
            withColumn("day", dayofmonth(col("write_timestamp"))). \
            withColumn("hour", hour(col("write_timestamp"))). \
            withColumn("minute", minute(col("write_timestamp"))). \
            withColumn("partition_col", concat(
                col("year"),
                lpad(col("month"), 2, "0"),
                lpad(col("day"), 2, "0"),
                lpad(col("hour"), 2, "0"),
                lpad(col("minute"), 2, "0"))). \
            drop("write_timestamp_new")