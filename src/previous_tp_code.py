import time
import json
from datetime import datetime, timedelta

import requests
from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import from_json, col, window, avg
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
    FloatType,
)
from pyspark.sql import SQLContext
import seaborn as sns
import matplotlib.dates as md


def send_thingspeak_to_kafka(topic, url, fields={}):
    # Kafka configuration
    kafka_config = {
        "bootstrap_servers": "kafka1:9092",  # Update with your Kafka broker
    }

    # Initialize Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=kafka_config["bootstrap_servers"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    # Fetch data from ThingSpeak

    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        feeds = data.get("feeds", [])

        # Publish each entry to Kafka
        for feed in feeds:
            for field in fields:
                feed[fields[field]] = feed.pop(field)
            producer.send(topic, value=feed)
            # print(f"Sent: {feed}")

        # Ensure all messages are sent
        producer.flush()
        print(f"Sent {len(feeds)} records.")
    else:
        print(f"Failed to fetch data: {response.status_code}")


# https://thingspeak.mathworks.com/channels/1785844 : Wind Power Smart Monitor
fields = {
    "field1": "wind_speed",
    "field2": "wind_power_density",
    "field3": "wind_power",
    "field4": "wind_air_density",
    "field5": "wind_temperature",
    "field6": "wind_pressure",
}

api_url = "https://thingspeak.mathworks.com/channels/1785844/feed.json?start={}&end={}"

# date initiale
initial_date_str = "2025-01-25 16:00:00"
# durée de récupération (minutes)
step = 60

date_format = "%Y-%m-%d %H:%M:%S"
initial_date = datetime.strptime(initial_date_str, date_format)
end_date = initial_date + timedelta(minutes=step)

start = initial_date.strftime(date_format)
end = end_date.strftime(date_format)


for i in range(10):
    print(f"{start}-{end}")
    send_thingspeak_to_kafka("wind", api_url.format(start, end), fields)
    time.sleep(5)
    initial_date = end_date
    end_date = initial_date + timedelta(minutes=step)
    start = initial_date.strftime(date_format)
    end = end_date.strftime(date_format)


conf = (
    SparkConf()
    .setAppName("SparkApp")
    .setMaster("spark://spark:7077")
    .set(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
    )
    .set("spark.sql.shuffle.partitions", "10")
)


sc = SparkContext.getOrCreate(conf=conf)

# Créer un SQLContext pour les opérations SQL
sql_context = SQLContext(sc)


# Kafka configuration
kafka_broker = "kafka1:9092"
kafka_topic = "wind"

# Define schema for the Kafka message
schema = StructType(
    [
        StructField("created_at", StringType(), True),
        StructField("entry_id", IntegerType(), True),
        StructField(
            "wind_speed", StringType(), True
        ),  # Voltage is initially a string
    ]
)

# Read raw data from Kafka
raw_stream = (
    sql_context.read.format("kafka")
    .option("kafka.bootstrap.servers", kafka_broker)
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "earliest")
    .load()
)

# Parse Kafka messages
# dropDuplicates : retirer les éléments ayant la même valeur
parsed_stream = (
    raw_stream.selectExpr("CAST(value AS STRING) AS message")
    .select(from_json(col("message"), schema).alias("data"))
    .select(
        col("data.created_at")
        .cast(TimestampType())
        .alias("created_at"),  # Convert timestamp to Spark TimestampType
        col("data.entry_id").alias("entry_id"),
        col("data.wind_speed")
        .cast(FloatType())
        .alias("wind_speed"),  # Convert voltage to FloatType for aggregation
    )
    .dropDuplicates(["entry_id"])
    .withWatermark("created_at", "5 minutes")
)

parsed_stream = parsed_stream.filter(col("data.wind_speed").isNotNull())


# Compute rolling average over a 5-minute window
rolling_average = (
    parsed_stream.groupBy(window(col("created_at"), "5 minutes"))
    .agg(avg("wind_speed").alias("rolling_avg_wind_speed"))
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("rolling_avg_wind_speed"),
    )
)  # Explicit ordering by window start

# Collect the result as a Pandas DataFrame
pandas_df = rolling_average.toPandas()


g = sns.lineplot(data=pandas_df, x="window_start", y="rolling_avg_wind_speed")
g.xaxis.set_major_formatter(md.DateFormatter("%d/%m\n%H:%M"))


# Kafka configuration
kafka_broker = "kafka1:9092"
kafka_topic = "wind"

# Define schema for the Kafka message
schema = StructType(
    [
        StructField("created_at", StringType(), True),
        StructField("entry_id", IntegerType(), True),
        StructField(
            "wind_speed", StringType(), True
        ),  # Voltage is initially a string
    ]
)

# Read raw data from Kafka
raw_stream = (
    sql_context.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_broker)
    .option("subscribe", kafka_topic)
    .option("startingOffsets", "earliest")
    .load()
)

# Parse Kafka messages
parsed_stream = (
    raw_stream.selectExpr("CAST(value AS STRING) AS message")
    .select(from_json(col("message"), schema).alias("data"))
    .select(
        col("data.created_at")
        .cast(TimestampType())
        .alias("created_at"),  # Convert timestamp to Spark TimestampType
        col("data.entry_id").alias("entry_id"),
        col("data.wind_speed")
        .cast(FloatType())
        .alias("wind_speed"),  # Convert voltage to FloatType for aggregation
    )
    .dropDuplicates(["entry_id"])
    .withWatermark("created_at", "5 minutes")
)

parsed_stream = parsed_stream.filter(col("data.wind_speed").isNotNull())


# Compute rolling average over a 5-minute window
rolling_average = (
    parsed_stream.groupBy(window(col("created_at"), "5 minutes"))
    .agg(avg("wind_speed").alias("rolling_avg_wind_speed"))
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("rolling_avg_wind_speed"),
    )
)  # Explicit ordering by window start

# Output rolling average to the console
query = (
    rolling_average.writeStream.outputMode("append")
    .format("console")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()
time.sleep(5)
query.stop()
