import pyspark
import pyspark.sql.types as T

INPUT_FHV_DATA_PATH = './resources/fhv_tripdata_2019-01.csv'
INPUT_GREEN_DATA_PATH = './resources/green_tripdata_2019-01.csv'
BOOTSTRAP_SERVERS = 'localhost:9092'

TOPIC_WINDOWED_PULOCATION_ID_COUNT = 'pulocation_counts_windowed'

PRODUCE_TOPIC_RIDES_CSV = CONSUME_TOPIC_RIDES_CSV = 'rides_csv'

RIDE_SCHEMA = T.StructType(
    [T.StructField("PUlocationID", T.IntegerType()),
     T.StructField('pickup_datetime', T.TimestampType()),
     T.StructField('dropOff_datetime', T.TimestampType()),
     T.StructField("DUlocationID", T.IntegerType())
     ])
