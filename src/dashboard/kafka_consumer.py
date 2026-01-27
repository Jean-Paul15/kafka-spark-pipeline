# =============================================================================
# KAFKA CONSUMER - Lecture des données temps réel pour Streamlit
# Consomme les 3 topics et maintient un buffer de données récentes
# =============================================================================

import os
import json
import threading
from collections import deque
from datetime import datetime
import pandas as pd
from kafka import KafkaConsumer


class KafkaDataConsumer:
    """Consumer Kafka thread-safe pour alimentation Streamlit."""
    
    def __init__(self, max_buffer_size=100):
        """Initialise le consumer avec buffers circulaires."""
        self.kafka_broker = os.getenv("KAFKA_BROKER", "kafka:29092")
        self.max_buffer_size = max_buffer_size
        
        # Buffers circulaires pour chaque topic
        self._annonces_buffer = deque(maxlen=max_buffer_size)
        self._prix_buffer = deque(maxlen=max_buffer_size)
        self._transactions_buffer = deque(maxlen=max_buffer_size)
        
        # Locks pour thread-safety
        self._annonces_lock = threading.Lock()
        self._prix_lock = threading.Lock()
        self._transactions_lock = threading.Lock()
        
        # Demarrage des threads consumers
        self._start_consumers()
    
    def _start_consumers(self):
        """Démarre les threads de consommation Kafka."""
        topics = [
            ("annonces", self._annonces_buffer, self._annonces_lock),
            ("prix", self._prix_buffer, self._prix_lock),
            ("transactions", self._transactions_buffer, self._transactions_lock),
        ]
        
        for topic, buffer, lock in topics:
            thread = threading.Thread(
                target=self._consume_topic,
                args=(topic, buffer, lock),
                daemon=True,
            )
            thread.start()
    
    def _consume_topic(self, topic, buffer, lock):
        """Consomme un topic Kafka et remplit le buffer."""
        max_retries = 60
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                consumer = KafkaConsumer(
                    topic,
                    bootstrap_servers=self.kafka_broker,
                    auto_offset_reset="latest",
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                    consumer_timeout_ms=1000,
                )
                
                print(f"[KAFKA] Consumer connecte au topic: {topic}")
                
                while True:
                    for message in consumer:
                        data = message.value
                        data["_consumed_at"] = datetime.now().isoformat()
                        
                        with lock:
                            buffer.append(data)
                
            except Exception as e:
                retry_count += 1
                print(f"[KAFKA] Erreur topic {topic}: {e} (retry {retry_count})")
                import time
                time.sleep(2)
    
    def get_latest_annonces(self) -> pd.DataFrame:
        """Retourne les dernières annonces sous forme de DataFrame."""
        with self._annonces_lock:
            data = list(self._annonces_buffer)
        return pd.DataFrame(data) if data else pd.DataFrame()
    
    def get_latest_prix(self) -> pd.DataFrame:
        """Retourne les derniers prix sous forme de DataFrame."""
        with self._prix_lock:
            data = list(self._prix_buffer)
        return pd.DataFrame(data) if data else pd.DataFrame()
    
    def get_latest_transactions(self) -> pd.DataFrame:
        """Retourne les dernières transactions sous forme de DataFrame."""
        with self._transactions_lock:
            data = list(self._transactions_buffer)
        return pd.DataFrame(data) if data else pd.DataFrame()
