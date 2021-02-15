import sys
from datetime import datetime
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import (
    regexp_extract,
    col,
    input_file_name,
    unix_timestamp,
    from_unixtime,
    get_json_object,
    date_format,
    col,
    max,
    date_format,
)
from pyspark.sql.types import StructType, StructField, StringType

# define spark session
sparkSession = (
    SparkSession.builder.config("spark.driver.maxResultSize", "2000m")
    .config("spark.sql.shuffle.partitions", 4)
    .getOrCreate()
)


def process_data(path: str, execution_date: datetime):
    """
    To load data and join two dataframes to create output
    :param path: folder in which data is kept
    :param execution_date: Airflow execution date
    """

    file_date = str(
        (datetime.strptime(execution_date, '%Y-%m-%d') + timedelta(days=-1)).strftime(
            '%Y%m%d'
        )
    )

    # define file_paths and names
    input_path_1 = path + "/input_source_1/data_" + file_date + ".json"
    input_path_2 = path + "/input_source_2/engagement_" + file_date + ".csv"

    # Read first source
    try:
        df_source_2 = (
            sparkSession.read.option("header", "true").format("csv").load(input_path_2)
        )
    except FileNotFoundError as fnf_error:
        print(fnf_error)
    else:
        df_source_2 = df_source_2.withColumn(
            "date", regexp_extract(input_file_name(), "\\d{1,2}\\d{1,2}\\d{4}", 0)
        )
        df_source_2 = df_source_2.withColumn(
            'date', from_unixtime(unix_timestamp('date', 'yyyyMMdd')).alias('date')
        )

    # define schema for json file
    schema_json = StructType(
        [
            StructField("message", StringType(), True),
            StructField("post_id", StringType(), True),
            StructField("shares", StringType(), True),
        ]
    )

    # Read second source
    try:
        df_source_1 = sparkSession.read.json(
            path=input_path_1, schema=schema_json
        ).select(
            "message",
            "post_id",
            "shares",
            get_json_object('shares', "$.count").alias('shares_count'),
        )
    except FileNotFoundError as fnf_error:
        print(fnf_error)
    else:
        df_output = df_source_2.join(
            df_source_1.select("post_id", "shares_count"), on=["post_id"], how="inner"
        )
        df_output = df_output.withColumnRenamed("shares_count", "shares")

        df_output.coalesce(1).write.format("csv").save(
            path + "/output/output_" + file_date + ".csv", header='true'
        )
    finally:
        sparkSession.stop()
        print("Session Closed")


def main():
    path = sys.argv[1]
    execution_date = sys.argv[2]
    process_data(path, execution_date)


if __name__ == '__main__':
    main()
