from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from settings import RIDE_SCHEMA,CONSUME_TOPIC_RIDES_CSV


def read_from_kafka(consume_topic: str):
    # Spark Streaming DataFrame, connect to Kafka topic served at host in bootrap.servers option
    df_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092,broker:29092") \
        .option("subscribe", consume_topic) \
        .option("startingOffsets", "earliest") \
        .option("checkpointLocation", "checkpoint") \
        .load()
    return df_stream


def parse_ride_from_kafka_message(df, schema):
    """ take a Spark Streaming df and parse value col based on <schema>, return streaming df cols in schema """
    assert df.isStreaming is True, "DataFrame doesn't receive streaming data"

    df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    # split attributes to nested array in one Column
    col = F.split(df['value'], ', ')

    # expand col to multiple top-level columns
    for idx, field in enumerate(schema):
        df = df.withColumn(field.name, col.getItem(idx).cast(field.dataType))
    return df.select([field.name for field in schema])

def sink_console(df, output_mode: str = 'complete', processing_time: str = '5 seconds'):
    write_query = df.writeStream \
        .outputMode(output_mode) \
        .trigger(processingTime=processing_time) \
        .format("console") \
        .option("truncate", False) \
        .start()
    return write_query  # pyspark.sql.streaming.StreamingQuery


def op_groupby(df, column_names):
    df_aggregation = df.groupBy(column_names).count()
    return df_aggregation

def sink_memory(df, query_name, query_template):
    query_df = df \
        .writeStream \
        .queryName(query_name) \
        .format("memory") \
        .start()
    query_str = query_template.format(table_name=query_name)
    query_results = spark.sql(query_str)
    return query_results, query_df


if __name__ == "__main__":
    spark = SparkSession.builder.appName('streaming-rides').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

        # read_streaming data
    df_consume_stream = read_from_kafka(consume_topic=CONSUME_TOPIC_RIDES_CSV)
    print(df_consume_stream.printSchema())

    # parse streaming data
    df_rides = parse_ride_from_kafka_message(df_consume_stream, RIDE_SCHEMA)
    print(df_rides.printSchema())


    df_tripcount_by_pulocation_id = op_groupby(df_rides, ['PULocationID'])

    #print data groups by pulocationid
    sink_console(df_tripcount_by_pulocation_id)

    #sink memory
    query_name= 'PULocationID popularity'
    query_template = 'select PULocationID , count(*) as popularity from {table_name} group by PULocationID order by popularity'
    q_res, df_puloactionid_popularity = sink_memory(df_rides, query_name=query_name, query_template=query_template)
    print(df_puloactionid_popularity.shape)

    spark.streams.awaitAnyTermination()




    
