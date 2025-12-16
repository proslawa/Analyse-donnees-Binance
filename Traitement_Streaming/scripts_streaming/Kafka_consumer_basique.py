from kafka import KafkaConsumer
import json


# CONFIGURATION KAFKA
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "projet-bdcc"


# CRÉATION DU CONSUMER KAFKA
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="latest",   # "earliest" pour lire depuis le début
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

print(f" Lecture du topic Kafka : {KAFKA_TOPIC}")
print("Appuyez sur CTRL + C pour arrêter.\n")


# BOUCLE DE LECTURE DES MESSAGES
try:
    for message in consumer:
        print("Nouveau message reçu :")
        print(message.value)
        print("-" * 40)
except KeyboardInterrupt:
    print("Arrêt du consumer Kafka.")
