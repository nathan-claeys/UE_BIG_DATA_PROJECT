from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import from_json, col, current_timestamp, lit, struct, array, udf, collect_list, asc, window, when, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType, TimestampType
import matplotlib.pyplot as plt
import pandas as pd

from naolib_producer import get_stops_of_line

# Kafka configuration
kafka_config = {
    "bootstrap_servers": "kafka1:9092",  # Update with your Kafka broker
}

LIST_STOPS={
    "C6": ["HERM", "PLCL", "APRE", "TILL", "DERV", "LNAI", "PEON", "SLAU", "VARI", "PCRE", "PRCE", "DOME", "CMUS", "MDSI", "HARO", "DLME", "SNIC", "CRQU", "HVNA", "STPI", "FOCH", "BOND", "COCH", "BTEC", "LALL", "CLTT", "COUD", "BEMZ", "GDRE", "ERDI", "KERE", "PCVO", "KOUF", "RAZA", "BATI", "BJOI", "RSRA", "LNTT", "PTQQ", "EMBE", "SJPO", "CCHA", "MDPO", "CTRE"]
}
    
line = "C6"

stops_name = get_stops_of_line(line)

def main():
    conf = SparkConf() \
        .setAppName('SparkApp') \
        .setMaster('spark://spark:7077') \
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
        StructField("created_at", StringType(), True)
    ])

    # Read raw data from Kafka
    raw_stream = sql_context.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"]) \
        .option("subscribe", "bus_position") \
        .option("startingOffsets", "latest") \
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
            col("data.numLigne").alias("numLigne"),
            col("data.created_at").cast(TimestampType()).alias("created_at")
        ) \
        .filter( \
            col("tempsReel") == "true" \
        ).withColumn(
            "minutes",
            when(col("temps") == "proche", 0)  # Si "proche", retourne 0
            .otherwise(
                regexp_replace(col("temps"), "mn", "").cast("int")  # Sinon, enlève "mn" et cast en int
        )
    )

    # Ajouter watermark et fenêtre temporelle de 1 minute
    windowed_stream = parsed_stream \
        .withWatermark("created_at", "1 minute") \
        .groupBy(
            window("created_at", "1 minute"),
            col("sens")
        ) \
        .agg(collect_list(struct(col("stop"), col("minutes"))).alias("stops")) # Aggrégation des arrêts et minutes d'attente


    # Fonction pour détecter la position des bus sur une ligne ordonnée
    def detect_bus_positions(stops, stops_list):
        result = []
        for idx, stop in enumerate(stops_list):
            # Trouver l'arrêt actuel dans les données
            current = next((x for x in stops if x["stop"] == stop), None)
            # Trouver l'arrêt précédent (s'il y en a un)
            prev = next((x for x in stops if x["stop"] == stops_list[idx - 1]), None) if idx > 0 else None

            # Cas où le bus est À un arrêt précis
            if current and current["minutes"] == 0:
                result.append((stop, True))
            
            # Cas où le bus est ENTRE deux arrêts
            elif current and prev and current["minutes"] > 0 and prev["minutes"] > current["minutes"]:
                result.append((stops_list[idx - 1] + "|" + stop, False))
        return result

    schema_result = ArrayType(StructType([
        StructField("location", StringType(), False),
        StructField("status", BooleanType(), False)
    ]))

    detect_bus_positions_udf = udf(lambda stops, sens: detect_bus_positions(
        stops,
        LIST_STOPS[line] if sens == 1 else LIST_STOPS[line][::-1]
    ), schema_result)

    # Appliquer l'UDF sur l'agrégation
    bus_positions = windowed_stream.withColumn("positions", detect_bus_positions_udf(col("stops"), col("sens")))

    def plot_bus_positions(df, batch_id):
        pandas_df = df.toPandas()

        for sens in pandas_df["sens"].unique():
            df_sens = pandas_df[pandas_df["sens"] == sens]
            positions = df_sens.iloc[0]["positions"]  # Liste de (location, status)

            stops_list = LIST_STOPS[line] if sens == 1 else LIST_STOPS[line][::-1]

            plt.figure(figsize=(15, 2))
            plt.title(f"Position des bus - Ligne {line} vers {stops_name[stops_list[-1]]}")

            # Affichage des arrêts
            y_stops = [0] * len(stops_list)
            plt.scatter(range(len(stops_list)), y_stops, c='gray', s=100, label='Arrêts')

            # Ajout des bus
            for position in positions:
                loc = position["location"]
                isAtStop = position["status"]

                if not isAtStop:
                    loc_split = loc.split("|")
                    prev_stop, next_stop = loc_split[0], loc_split[1]
                    idx_prev = stops_list.index(prev_stop)
                    idx_next = stops_list.index(next_stop)
                    idx = (idx_prev + idx_next) / 2  # Position entre deux arrêts
                else:
                    idx = stops_list.index(loc)

                color = 'red' if isAtStop else 'orange'
                plt.scatter([idx], [0], c=color, s=200)

            plt.xticks(range(len(stops_list)), list(map(lambda stop : stops_name[stop], stops_list)), rotation=90)
            plt.yticks([])
            plt.legend()
            plt.grid(True)
            plt.tight_layout()
            plt.show()

    query_graph = bus_positions.writeStream \
        .foreachBatch(plot_bus_positions) \
        .outputMode("append") \
        .start()

    # query_console = bus_positions.select(
    #     col("sens"),
    #     col("window.start").alias("window_start"),
    #     col("window.end").alias("window_end"),
    #     col("positions")
    # ).writeStream \
    #     .outputMode("append") \
    #     .format("console") \
    #     .option("truncate", "false") \
    #     .start()

    query_graph.awaitTermination()
    #query_console.awaitTermination()