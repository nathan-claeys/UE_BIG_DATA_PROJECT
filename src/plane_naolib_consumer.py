from datetime import datetime
from kafka import KafkaConsumer
import json

# Kafka configuration
kafka_config = {
    "bootstrap_servers": "kafka1:9092",
}
topics = ["plane_arrival", "bus_airport"]

# Initialiser le consommateur Kafka
consumer = KafkaConsumer(
    *topics,
    bootstrap_servers=kafka_config["bootstrap_servers"],
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

plane_arrivals = []  # Liste des arrivées d'avion
bus_schedules = []  # Liste des départs de bus

def parse_unix_time(timestamp):
    """ Convertir un timestamp UNIX en objet datetime """
    return datetime.datetime.utcfromtimestamp(timestamp)

def compute_wait_times():
    """ Calculer le temps d'attente entre un avion et le premier bus suivant """
    for arrival_time in plane_arrivals:
        closest_bus = min(bus_schedules, key=lambda t: t if t > arrival_time else datetime.datetime.max, default=None)
        if closest_bus:
            wait_time = (closest_bus - arrival_time).seconds // 60  # Convertir en minutes
            print(f"✈️ → 🚌 Temps d'attente : {wait_time} min")



def main ():
    print("🔍 En attente des données...")

    for message in consumer:
        topic = message.topic
        data = message.value
        
        if topic == "plane_arrival":
            arrival_time = parse_unix_time(data["lastSeen"])  # On prend le moment où l'avion a été vu en dernier
            plane_arrivals.append(arrival_time)
            print(f"✈️ Avion arrivé à {arrival_time}")

        elif topic == "bus_airport":
            # Convertir les horaires de bus en datetime
            hour_str = data["heure"].replace("h", "")  # Exemple : "4h" → "4"
            for minute in data["passages"]:
                bus_time = datetime.datetime.utcnow().replace(hour=int(hour_str), minute=int(minute), second=0, microsecond=0)
                bus_schedules.append(bus_time)
                print(f"🚌 Bus prévu à {bus_time}")

        # Calculer le temps d'attente après chaque mise à jour
        compute_wait_times()