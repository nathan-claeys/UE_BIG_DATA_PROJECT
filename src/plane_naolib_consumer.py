from datetime import datetime
from kafka import KafkaConsumer
import json
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import threading

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

# Stockage des arrivées d'avions et des horaires de bus
plane_arrivals = []  # liste des datetime d'arrivée des avions
bus_schedules = []   # liste de dicts {"bus": <numéro>, "time": datetime}
wait_data = []       # liste de dicts avec le format souhaité
# Exemple attendu : [{"heure d'arrivee": "1h25m", "temps d'attente": "15m", "bus": 38}, ...]

def parse_unix_time(timestamp):
    """Convertir un timestamp UNIX en objet datetime"""
    return datetime.utcfromtimestamp(timestamp)

def compute_wait_times():
    """
    Pour chaque arrivée d'avion, trouve l'horaire de bus (parmi les horaires
    reçus) qui est immédiatement après et calcule le temps d'attente.
    """
    global wait_data
    wait_data = []
    for arrival_time in plane_arrivals:
        # Ne considérer que les bus dont l'heure est strictement postérieure à l'arrivée
        candidates = [bus for bus in bus_schedules if bus["time"] > arrival_time]
        if candidates:
            closest_bus = min(candidates, key=lambda b: (b["time"] - arrival_time).total_seconds())
            wait_seconds = (closest_bus["time"] - arrival_time).total_seconds()
            wait_minutes = int(wait_seconds // 60)
            # Formatage de l'heure d'arrivée (exemple "14h05m")
            arrival_str = f"{arrival_time.hour}h{arrival_time.minute:02d}m"
            wait_str = f"{wait_minutes}m"
            wait_data.append({
                "arrival_time": arrival_time,  # pour le tracé
                "heure d'arrivee": arrival_str,
                "temps d'attente": wait_str,
                "bus": closest_bus["bus"]
            })
            print(f"✈️ → 🚌 Temps d'attente : {wait_str} pour le bus {closest_bus['bus']} à {arrival_str}")

# Pour gérer une couleur différente pour chaque bus
bus_colors = {}
color_index = 0
colors_cycle = plt.rcParams['axes.prop_cycle'].by_key()['color']

def get_color(bus):
    global bus_colors, color_index, colors_cycle
    if bus not in bus_colors:
        bus_colors[bus] = colors_cycle[color_index % len(colors_cycle)]
        color_index += 1
    return bus_colors[bus]

# Initialisation du graphique
fig, ax = plt.subplots()

def update_plot(frame):
    """Mise à jour du graphique en affichage en temps réel."""
    ax.clear()
    ax.set_xlabel("Heure d'arrivée de l'avion")
    ax.set_ylabel("Temps d'attente (min)")
    ax.set_title("Temps d'attente entre l'arrivée d'un avion et le bus suivant")
    
    for item in wait_data:
        arrival_dt = item["arrival_time"]
        wait_minutes = int(item["temps d'attente"].replace("m", ""))
        bus_num = item["bus"]
        color = get_color(bus_num)
        # Affichage d'un point pour chaque résultat
        ax.scatter(arrival_dt, wait_minutes, color=color, label=f"Bus {bus_num}")
        # Annotation avec le temps d'attente
        ax.annotate(item["temps d'attente"], (arrival_dt, wait_minutes))
        
    # Pour éviter des doublons dans la légende
    handles, labels = ax.get_legend_handles_labels()
    by_label = dict(zip(labels, handles))
    ax.legend(by_label.values(), by_label.keys())
    fig.autofmt_xdate()

def kafka_listener():
    """Écoute Kafka et met à jour les listes d'événements."""
    print("🔍 En attente des données...")
    for message in consumer:
        topic = message.topic
        data = message.value
        
        if topic == "plane_arrival":
            # Utilisation de "lastSeen" pour la date d'arrivée de l'avion
            arrival_time = parse_unix_time(data["lastSeen"])
            plane_arrivals.append(arrival_time)
            print(f"✈️ Avion arrivé à {arrival_time}")
            
        elif topic == "bus_airport":
            # Récupération de l'heure de passage des bus
            # Exemple : "heure": "4h" et "passages": ["50d"]
            hour_str = data["heure"].replace("h", "")  # "4h" → "4"
            bus_num = data["bus"]
            for minute in data["passages"]:
                # Nettoyage de la chaîne (ex : "50d" → "50")
                minute_clean = int(minute.replace("a", "").replace("d", ""))
                # On se base sur l'heure courante mais en remplaçant l'heure et les minutes,
                # ce qui suppose que la date du bus est la même que celle du jour courant.
                bus_time = datetime.utcnow().replace(
                    hour=int(hour_str),
                    minute=minute_clean,
                    second=0,
                    microsecond=0
                )
                bus_schedules.append({"bus": bus_num, "time": bus_time})
                print(f"🚌 Bus {bus_num} prévu à {bus_time}")
                
        # Recalculer les temps d'attente après chaque nouveau message
        compute_wait_times()
        
    consumer.close()
    print("🛑 Fin du programme")

def main():
    """Fonction principale"""
    # Lancer l'écoute Kafka dans un thread dédié
    thread = threading.Thread(target=kafka_listener, daemon=True)
    thread.start()
    
    # Lancer l'animation pour la visualisation en temps réel
    ani = animation.FuncAnimation(fig, update_plot, interval=1000)
    plt.show()

if __name__ == "__main__":
    main()
