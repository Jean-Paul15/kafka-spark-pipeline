# =============================================================================
# REDIS CLIENT - Lecture des donnees stockees par Spark
# Recuperation des agregations et statistiques depuis Redis
# =============================================================================

import os
import json
import redis
from typing import Dict, List, Optional
import pandas as pd


class RedisClient:
    """Client Redis pour lecture des donnees traitees par Spark."""
    
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
                print(f"[REDIS] Client connecte: {self.host}:{self.port}")
                return
            except Exception as e:
                print(f"[REDIS] Tentative {attempt + 1}/{max_retries}")
                import time
                time.sleep(2)
        print("[REDIS] Connexion echouee, mode degrade active")
        self.client = None
    
    def is_connected(self) -> bool:
        """Verifie si Redis est connecte."""
        if not self.client:
            return False
        try:
            return self.client.ping()
        except Exception:
            return False
    
    def get_global_stats(self, category: str) -> Dict:
        """Recupere les statistiques globales d'une categorie."""
        if not self.client:
            return {}
        try:
            return self.client.hgetall(f"stats:{category}")
        except Exception:
            return {}
    
    def get_active_cities(self) -> List[str]:
        """Retourne la liste des villes actives."""
        if not self.client:
            return []
        try:
            return list(self.client.smembers("active_cities"))
        except Exception:
            return []
    
    def get_city_stats(self, ville: str) -> Dict:
        """Recupere les statistiques d'une ville."""
        if not self.client:
            return {}
        try:
            return self.client.hgetall(f"city:{ville}")
        except Exception:
            return {}
    
    def get_all_cities_stats(self) -> pd.DataFrame:
        """Recupere les stats de toutes les villes actives."""
        cities = self.get_active_cities()
        if not cities:
            return pd.DataFrame()
        
        data = []
        for city in cities:
            stats = self.get_city_stats(city)
            if stats:
                stats["ville"] = city
                data.append(stats)
        
        return pd.DataFrame(data) if data else pd.DataFrame()
    
    def get_history(self, category: str, limit: int = 50) -> List[Dict]:
        """Recupere l'historique d'une categorie."""
        if not self.client:
            return []
        try:
            list_key = f"history:{category}"
            raw_data = self.client.lrange(list_key, 0, limit - 1)
            return [json.loads(item) for item in raw_data]
        except Exception:
            return []
    
    def get_history_df(self, category: str, limit: int = 50) -> pd.DataFrame:
        """Recupere l'historique sous forme de DataFrame."""
        history = self.get_history(category, limit)
        return pd.DataFrame(history) if history else pd.DataFrame()
    
    def get_redis_info(self) -> Dict:
        """Recupere les informations Redis pour monitoring."""
        if not self.client:
            return {"status": "disconnected"}
        try:
            info = self.client.info("memory")
            return {
                "status": "connected",
                "used_memory": info.get("used_memory_human", "N/A"),
                "keys": self.client.dbsize(),
            }
        except Exception:
            return {"status": "error"}


def create_redis_client() -> RedisClient:
    """Factory pour creer une instance RedisClient."""
    return RedisClient()
