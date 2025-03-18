from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import from_json, col, window, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, FloatType, BooleanType
from constants import kafka_config

conf = SparkConf() \
    .setAppName('SparkApp') \
    .setMaster('spark://localhost:7077') \
    .set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .set("spark.sql.shuffle.partitions", "10")
 

sc = SparkContext.getOrCreate(conf=conf)

# Créer un SQLContext pour les opérations SQL
sql_context = SQLContext(sc)

schema = StructType([
    StructField("sens", IntegerType(), True),
    StructField("terminus", StringType(), True),
    StructField("infotrafic", BooleanType(), True),
    StructField("temps", StringType(), True),
    StructField("tempsReel", StringType(), True),
    StructField("stop", StringType(), True),
    StructField("numLigne", StringType(), True),
])

# Read raw data from Kafka
raw_stream = sql_context.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"]) \
    .option("subscribe", "bus_position") \
    .option("startingOffsets", "earliest") \
    .load()

parsed_stream = raw_stream \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select(
        col("data.sens").alias("sens"),
        col("data.terminus").alias("terminus"),
        col("data.infotrafic").alias("infotrafic"),
        col("data.temps").alias("temps"),
        col("data.tempsReel").alias("tempsReel"),
        col("data.stop").alias("stop"),
        col("data.numLigne").alias("numLigne")
    )

query = parsed_stream.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()