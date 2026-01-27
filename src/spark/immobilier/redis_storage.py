# =============================================================================
# REDIS STORAGE - Persistance des Resultats Spark dans Redis
# Couche de stockage temps reel pour l'architecture Big Data
# =============================================================================

import os
import json
import redis
from datetime import datetime


class RedisStorage:
    """Gestionnaire de stockage Redis pour les resultats Spark."""
    
    def __init__(self):
        """Initialise la connexion Redis."""
        self.host = os.getenv("REDIS_HOST", "redis")
        self.port = int(os.getenv("REDIS_PORT", 6379))
        self.client = None
        self._connect()
    
    def _connect(self):
        """Etablit la connexion Redis avec retry."""
        max_retries = 30
        for attempt in range(max_retries):
            try:
                self.client = redis.Redis(
                    host=self.host,
                    port=self.port,
                    decode_responses=True,
                )
                self.client.ping()
                print(f"[REDIS] Connexion etablie: {self.host}:{self.port}")
                return
            except Exception as e:
                print(f"[REDIS] Tentative {attempt + 1}/{max_retries}: {e}")
                import time
                time.sleep(2)
        raise Exception("Impossible de se connecter a Redis")
    
    def store_annonces_aggregation(self, data_dict):
        """Stocke les agregations d'annonces."""
        key = f"agg:annonces:{datetime.now().strftime('%H%M%S')}"
        self.client.hset(key, mapping=data_dict)
        self.client.expire(key, 300)  # TTL 5 minutes
        
        # Mise a jour des stats globales
        self._update_global_stats("annonces", data_dict)
    
    def store_prix_aggregation(self, data_dict):
        """Stocke les agregations de prix."""
        key = f"agg:prix:{datetime.now().strftime('%H%M%S')}"
        self.client.hset(key, mapping=data_dict)
        self.client.expire(key, 300)
        
        self._update_global_stats("prix", data_dict)
    
    def store_transactions_aggregation(self, data_dict):
        """Stocke les agregations de transactions."""
        key = f"agg:transactions:{datetime.now().strftime('%H%M%S')}"
        self.client.hset(key, mapping=data_dict)
        self.client.expire(key, 300)
        
        self._update_global_stats("transactions", data_dict)
    
    def _update_global_stats(self, category, data_dict):
        """Met a jour les statistiques globales."""
        stats_key = f"stats:{category}"
        
        # Incrementer le compteur
        self.client.hincrby(stats_key, "total_events", 1)
        self.client.hset(stats_key, "last_update", datetime.now().isoformat())
        
        # Stocker dans une liste pour historique
        list_key = f"history:{category}"
        self.client.lpush(list_key, json.dumps(data_dict))
        self.client.ltrim(list_key, 0, 99)  # Garder 100 derniers
    
    def store_city_stats(self, ville, stats_dict):
        """Stocke les statistiques par ville."""
        key = f"city:{ville}"
        self.client.hset(key, mapping=stats_dict)
        self.client.expire(key, 600)  # TTL 10 minutes
        
        # Ajouter la ville a l'ensemble des villes actives
        self.client.sadd("active_cities", ville)
    
    def get_active_cities(self):
        """Retourne la liste des villes actives."""
        return self.client.smembers("active_cities")
    
    def get_city_stats(self, ville):
        """Recupere les stats d'une ville."""
        return self.client.hgetall(f"city:{ville}")
    
    def get_global_stats(self, category):
        """Recupere les stats globales d'une categorie."""
        return self.client.hgetall(f"stats:{category}")
    
    def get_history(self, category, limit=50):
        """Recupere l'historique d'une categorie."""
        list_key = f"history:{category}"
        raw_data = self.client.lrange(list_key, 0, limit - 1)
        return [json.loads(item) for item in raw_data]


def create_redis_storage():
    """Factory pour creer une instance RedisStorage."""
    return RedisStorage()
