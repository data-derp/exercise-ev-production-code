from pyspark import SparkFiles
from pyspark.sql import SparkSession
from transformer import Transformer

url = "https://raw.githubusercontent.com/kelseymok/charge-point-simulator-v1.6/main/out/1680355141.csv.gz"
spark = SparkSession.builder.appName("test").getOrCreate()
spark.sparkContext.addFile(url)

Transformer().run(spark=spark, filepath="file://" + SparkFiles.get("1680355141.csv.gz"), output_dir="../output")