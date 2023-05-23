from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, StructType, StructField, IntegerType


class StartTransactionResponseTransformer:

    def _start_transaction_response_filter(self, input_df: DataFrame):
        action = "StartTransaction"
        message_type = 3
        return input_df.filter((input_df.action == action) & (input_df.message_type == message_type))

    def _start_transaction_response_unpack_json(self, input_df: DataFrame):
        id_tag_info_schema = StructType([
            StructField("status", StringType(), True),
            StructField("parent_id_tag", StringType(), True),
            StructField("expiry_date", StringType(), True),
        ])

        body_schema = StructType([
            StructField("transaction_id", IntegerType(), True),
            StructField("id_tag_info", id_tag_info_schema, True)
        ])
        return input_df.withColumn("new_body", from_json(col("body"), body_schema))

    def _start_transaction_response_flatten(self, input_df: DataFrame):
        return input_df. \
            withColumn("transaction_id", input_df.new_body.transaction_id). \
            withColumn("id_tag_info_status", input_df.new_body.id_tag_info.status). \
            withColumn("id_tag_info_parent_id_tag", input_df.new_body.id_tag_info.parent_id_tag). \
            withColumn("id_tag_info_expiry_date", input_df.new_body.id_tag_info.expiry_date). \
            drop("new_body"). \
            drop("body")

    def _write_start_transaction_response(self, input_df: DataFrame, output_directory: str):
        input_df. \
            write. \
            mode("overwrite"). \
            parquet(output_directory)



    def run(self, input_df: DataFrame, output_directory: str):
        self._write_start_transaction_response(input_df. \
                                               transform(self._start_transaction_response_filter). \
                                               transform(self._start_transaction_response_unpack_json). \
                                               transform(self._start_transaction_response_flatten), output_directory)
