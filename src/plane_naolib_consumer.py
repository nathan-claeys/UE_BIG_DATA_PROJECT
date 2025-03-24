from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, unix_timestamp, date_format, concat, lit, explode,
    regexp_replace, lpad, from_unixtime
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, ArrayType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

def create_spark_session():
    return SparkSession.builder.appName("PlaneBusWaitTime").getOrCreate()

def define_plane_schema():
    return StructType([
        StructField("icao24", StringType(), True),
        StructField("firstSeen", IntegerType(), True),
        StructField("estDepartureAirport", StringType(), True),
        StructField("lastSeen", IntegerType(), True),  # Utilisé pour l'heure d'arrivée
        StructField("estArrivalAirport", StringType(), True),
        StructField("callsign", StringType(), True),
        StructField("estDepartureAirportHorizDistance", IntegerType(), True),
        StructField("estDepartureAirportVertDistance", IntegerType(), True),
        StructField("estArrivalAirportHorizDistance", IntegerType(), True),
        StructField("estArrivalAirportVertDistance", IntegerType(), True),
        StructField("departureAirportCandidatesCount", IntegerType(), True),
        StructField("arrivalAirportCandidatesCount", IntegerType(), True)
    ])

def define_bus_schema():
    return StructType([
        StructField("bus", IntegerType(), True),
        StructField("heure", StringType(), True),
        StructField("passages", ArrayType(StringType()), True)
    ])

def read_plane_data(spark, plane_schema, begin, end):
    plane_raw = spark.read.format("kafka") \
        .option("kafka.bootstrap.servers", "kafka1:9092") \
        .option("subscribe", "plane_arrival") \
        .option("startingOffsets", "earliest") \
        .load()
    
    plane_df = plane_raw.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), plane_schema).alias("data")) \
        .select("data.*")
    
    # Conversion du timestamp UNIX en type Timestamp
    plane_df = plane_df.withColumn("arrival_time", from_unixtime(col("lastSeen")).cast(TimestampType()))
    plane_df = plane_df.filter(col("lastSeen").between(begin, end))
    return plane_df

def read_bus_data(spark, bus_schema):
    bus_raw = spark.read.format("kafka") \
        .option("kafka.bootstrap.servers", "kafka1:9092") \
        .option("subscribe", "bus_airport") \
        .option("startingOffsets", "earliest") \
        .load()
    
    bus_df = bus_raw.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), bus_schema).alias("data")) \
        .select("data.*")
    return bus_df

def process_bus_data(bus_df):
    bus_df = bus_df.withColumn("passage", explode(col("passages")))
    bus_df = bus_df.withColumn("bus_hour", regexp_replace(col("heure"), "h", "").cast("int"))
    bus_df = bus_df.withColumn("bus_minute", regexp_replace(col("passage"), "[^0-9]", "").cast("int"))
    
    # Construction du timestamp de départ du bus
    bus_df = bus_df.withColumn(
        "bus_time_str", 
        concat(lit("2025-03-17 "), col("bus_hour"), lit(":"), lpad(col("bus_minute"), 2, "0"), lit(":00"))
    )
    bus_df = bus_df.withColumn("bus_time", col("bus_time_str").cast(TimestampType()))
    return bus_df

def join_and_calculate(plane_df, bus_df):
    # Jointure sur la condition bus_time > arrival_time
    joined_df = plane_df.join(bus_df, bus_df.bus_time > plane_df.arrival_time, how="inner")
    
    # Calcul du temps d'attente en minutes
    joined_df = joined_df.withColumn(
        "wait_minutes", 
        ((unix_timestamp(col("bus_time")) - unix_timestamp(col("arrival_time"))) / 60).cast("int")
    )
    
    # Fenêtre partitionnée par l'heure d'arrivée pour sélectionner le bus minimal
    windowSpec = Window.partitionBy("arrival_time").orderBy("wait_minutes")
    ranked_df = joined_df.withColumn("rank", row_number().over(windowSpec)) \
                         .filter(col("rank") == 1)
    return ranked_df

def format_result(ranked_df):
    result_df = ranked_df.select(
        date_format(col("arrival_time"), "H'h'mm'm'").alias("heure d'arrivee"),
        concat(col("wait_minutes").cast("string"), lit("m")).alias("temps d'attente"),
        col("bus")
    )
    return result_df

def main():
    spark = create_spark_session()
    
    # Définir les schémas
    plane_schema = define_plane_schema()
    bus_schema = define_bus_schema()
    
    # Plage de timestamps UNIX pour les avions (correspondant par exemple à une journée)
    begin = 1742166015
    end = 1742252385
    
    # Lecture des données depuis Kafka
    plane_df = read_plane_data(spark, plane_schema, begin, end)
    bus_df = read_bus_data(spark, bus_schema)
    
    # Traitement des données de bus pour obtenir bus_time
    bus_df = process_bus_data(bus_df)
    
    # Jointure et calcul du temps d'attente minimal
    ranked_df = join_and_calculate(plane_df, bus_df)
    
    # Formatage du résultat final
    result_df = format_result(ranked_df)
    
    # Affichage du résultat
    result_df.show(truncate=False)
    
    # Conversion en Pandas DataFrame pour d'autres traitements ou visualisation
    result_pd = result_df.toPandas()
    print(result_pd)

if __name__ == "__main__":
    main()
