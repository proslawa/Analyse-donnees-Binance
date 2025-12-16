import json
import time
import logging
from websocket import create_connection, WebSocketConnectionClosedException
from kafka import KafkaProducer


# CONFIGURATION LOGGING
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)


# CONFIGURATION KAFKA
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC = 'projet-bdcc'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=500,
    acks='all'
)


# CONFIGURATION BINANCE
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/!miniTicker@arr"


# FONCTIONS UTILES DE CONFIGURATION
def send_to_kafka(message):
    try:
        producer.send(KAFKA_TOPIC, value=message).add_callback(
            lambda rec: logging.debug(f"Message envoyé: {message['symbol']}")
        ).add_errback(
            lambda e: logging.error(f"Erreur Kafka: {e}")
        )
    except Exception as e:
        logging.error(f"Erreur en envoyant à Kafka: {e}")

def process_tickers(tickers):
    for ticker in tickers:
        try:
            message = {
                'symbol': ticker.get('s', ''),
                'close_price': float(ticker.get('c', 0)),
                'open_price': float(ticker.get('o', 0)),
                'high_price': float(ticker.get('h', 0)),
                'low_price': float(ticker.get('l', 0)),
                'volume': float(ticker.get('v', 0)),
                'quote_volume': float(ticker.get('q', 0)),
                'timestamp': int(ticker.get('E', 0))
            }
            send_to_kafka(message)
        except Exception as e:
            logging.error(f"Erreur traitement ticker: {e}")


# FONCTION PRINCIPALE DE GESTION DE LOGS
def start_binance_stream():
    retry_delay = 1
    while True:
        ws = None
        try:
            logging.info("Connexion au WebSocket Binance...")
            ws = create_connection(BINANCE_WS_URL, timeout=10)
            logging.info("Connexion réussie !")
            retry_delay = 1  # reset après succès

            last_flush = time.time()

            while True:
                try:
                    result = ws.recv()
                    tickers = json.loads(result)
                    process_tickers(tickers)

                    # Flush Kafka toutes les secondes
                    if time.time() - last_flush > 1:
                        producer.flush()
                        last_flush = time.time()

                except WebSocketConnectionClosedException:
                    logging.warning("WebSocket fermé par Binance, reconnexion...")
                    break
                except json.JSONDecodeError:
                    logging.warning("Message JSON invalide, ignoré")
                    continue
                except Exception as e:
                    logging.error(f"Erreur inattendue: {e}")
                    break

        except Exception as e:
            logging.error(f"Impossible de se connecter à Binance : {e}")
            logging.info(f"Nouvelle tentative dans {retry_delay} secondes...")
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 60)  # Exponentiel jusqu'à 60s

        finally:
            if ws:
                try:
                    ws.close()
                except:
                    pass

# LANCEMENT LE SCRIPT

if __name__ == "__main__":
    try:
        start_binance_stream()
    finally:
        try:
            producer.flush()
            producer.close()
        except:
            pass
