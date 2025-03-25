from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import (
    from_json,
    col,
    to_timestamp,
    udf,
    explode,
    avg,
    window,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType,
    FloatType,
)


def main():
    kafka_config = {
        "bootstrap_servers": "kafka1:9092",
    }

    conf = (
        SparkConf()
        .setAppName("BikeStationApp")
        .setMaster("spark://spark:7077")
        .set(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
        )
        .set("spark.sql.shuffle.partitions", "10")
    )
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    base_schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("number", StringType(), True),
            StructField("address", StringType(), True),
            StructField(
                "position",
                StructType(
                    [
                        StructField("lon", FloatType(), True),
                        StructField("lat", FloatType(), True),
                    ]
                ),
                True,
            ),
            StructField("available_bikes", StringType(), True),
            StructField("available_bike_stands", StringType(), True),
            StructField("bike_stands", IntegerType(), True),
            StructField("last_update", StringType(), True),
        ]
    )
    array_schema = ArrayType(base_schema)

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"])
        .option("subscribe", "bike_stations")
        .option("startingOffsets", "latest")
        .load()
    )

    raw_json = raw_stream.select(
        from_json(col("value").cast("string"), array_schema).alias("data")
    )

    exploded = raw_json.select(explode(col("data")).alias("record"))

    parsed_stream = exploded.select(
        col("record.name").alias("name"),
        col("record.number").cast("integer").alias("number"),
        col("record.address").alias("address"),
        col("record.position").alias("position"),
        col("record.available_bikes").cast("integer").alias("available_bikes"),
        col("record.available_bike_stands")
        .cast("integer")
        .alias("available_bike_stands"),
        col("record.bike_stands").alias("bike_stands"),
        to_timestamp(
            col("record.last_update"), "yyyy-MM-dd'T'HH:mm:ssXXX"
        ).alias("last_update"),
    )

    # Group events into 1-minute windows per station (name & address)
    windowed_stream = (
        parsed_stream.withWatermark("last_update", "10 seconds")
        .groupBy(
            col("name"), col("address"), window(col("last_update"), "1 minute")
        )
        .agg(
            avg("available_bikes").alias("available_bikes"),
            avg("available_bike_stands").alias("available_bike_stands"),
            avg("bike_stands").alias("bike_stands"),
        )
    )

    # UDF to compute the formatted bar (station info)
    def compute_bar(available_bikes, bike_stands, available_bike_stands):
        if (
            available_bikes is None
            or available_bike_stands is None
            or bike_stands is None
        ):
            return "[No data]"
        else:
            available_bikes = int(round(available_bikes))
            available_bike_stands = int(round(available_bike_stands))
            bike_stands = int(round(bike_stands))
            bike_info = "#" * available_bikes
            stand_info = "-" * available_bike_stands
            discrepancy = ""
            if (available_bikes + available_bike_stands) != bike_stands:
                diff = bike_stands - (available_bikes + available_bike_stands)
                discrepancy = "?" * abs(diff)
            return f"{bike_info}{stand_info}{discrepancy}"

    compute_bar_udf = udf(compute_bar, StringType())

    # Create a new column "bar" for the station info
    result_stream = windowed_stream.withColumn(
        "bar",
        compute_bar_udf(
            col("available_bikes"),
            col("bike_stands"),
            col("available_bike_stands"),
        ),
    )

    # Select separate columns: window start, window end, station, address, and bar.
    # Order by window start and then station name.
    final_stream = result_stream.select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("name").alias("station"),
        col("address"),
        col("bar"),
    ).orderBy("window_start", "station")

    query = (
        final_stream.writeStream.outputMode("complete")
        .format("console")
        .option("truncate", "false")
        .start()
    )

    try:
        query.awaitTermination()
    except Exception as e:
        print("Streaming query terminated or timed out:", e)


if __name__ == "__main__":
    main()
