import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf
from pyspark import SparkConf

# TODO Create a schema for incoming resources
schema = StructType([
    StructField("crime_id", StringType(), False),
    StructField("original_crime_type_name", StringType(), False),
    StructField("report_date", DateType(), False),
    StructField("call_date", DateType(), False),
    StructField("offense_date", DateType(), False),
    StructField("call_time", StringType(), False),
    StructField("call_date_time", TimestampType(), False),
    StructField("disposition", StringType(), False),
    StructField("address", StringType(), False),
    StructField("city", StringType(), False),
    StructField("state", StringType(), False),
    StructField("agency_id", StringType(), False),
    StructField("address_type", StringType(), False),
    StructField("common_location", StringType(), True)
])


def run_spark_job(spark):
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "com.sanfrancisco.police.calls_for_service.v1") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 120000) \
        .option("maxRatePerPartition", 60000) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # Take only value and convert it to String
    kafka_df = df.selectExpr("cast(value as string) as value")

    service_table = kafka_df \
        .select(psf.from_json(psf.col('value'), schema).alias("DF")) \
        .select("DF.*")

    # TODO select original_crime_type_name and disposition
    distinct_table = service_table.select(['original_crime_type_name', 'disposition', 'call_date_time']).withWatermark(
        'call_date_time', '15 minutes')

    # count the number of original crime type
    agg_df = distinct_table.groupBy(['original_crime_type_name', 'disposition']).count()


    # query = agg_df.writeStream \
    #    .trigger(processingTime="1 minute") \
    #    .outputMode('Complete') \
    #    .format('console') \
    #    .option("truncate", "false") \
    #    .start()

    # query.awaitTermination()

    radio_code_json_filepath = "radio_code.json"
    radio_code_df = spark.read.option("multiline", "true").json(radio_code_json_filepath)

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    radio_code_df = radio_code_df.withColumnRenamed("disposition_code", "disposition")
    join_query = agg_df.join(radio_code_df, "disposition").writeStream \
        .trigger(processingTime="10 seconds") \
        .outputMode('Complete') \
        .format('console') \
        .start()

    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
