from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

class Transformer:

    def _create_dataframe(self, spark, filepath: str) -> DataFrame:
        custom_schema = StructType([
            StructField("message_id", StringType(), True),
            StructField("message_type", IntegerType(), True),
            StructField("charge_point_id", StringType(), True),
            StructField("action", StringType(), True),
            StructField("write_timestamp", StringType(), True),
            StructField("body", StringType(), True),
        ])

        df = spark.read.format("csv") \
            .option("header", True) \
            .option("delimiter", ",") \
            .option("escape", "\\") \
            .schema(custom_schema) \
            .load(filepath)
        return df

    def _write(self, input_df: DataFrame, output_dir: str):
        input_df. \
            write. \
            mode("overwrite"). \
            parquet(output_dir)

    def run(self, spark, filepath: str, output_dir: str):
        self._write(self._create_dataframe(spark, filepath), output_dir)