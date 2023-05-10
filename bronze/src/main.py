from pyspark import SparkFiles
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def create_dataframe(filepath: str) -> DataFrame:
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


def write(input_df: DataFrame, output_dir: str):
    input_df. \
        write. \
        mode("overwrite"). \
        parquet(output_dir)

url = "https://raw.githubusercontent.com/kelseymok/charge-point-simulator-v1.6/main/out/1680355141.csv.gz"
spark = SparkSession.builder.appName("test").getOrCreate()
spark.sparkContext.addFile(url)
df = create_dataframe("file://" + SparkFiles.get("1680355141.csv.gz"))
df.show(10)
write(df, output_dir="../output/")