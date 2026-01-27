import os

# ─── SÉCURITÉ ─────────────────────────────────────────────
SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

# ─── BASE DE DONNÉES INTERNE SUPERSET ────────────────────
# SQLite stocké dans SUPERSET_HOME (volume Docker, bonnes permissions)
SUPERSET_HOME = os.environ.get("SUPERSET_HOME", "/app/superset_home")
SQLALCHEMY_DATABASE_URI = f"sqlite:///{SUPERSET_HOME}/superset.db"

# ─── CACHE ────────────────────────────────────────────────
CACHE_CONFIG = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
}

# ─── FEATURE FLAGS ────────────────────────────────────────
FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS":  True,
    "ENABLE_TEMPLATE_PROCESSING": True,
    "ALERT_REPORTS": False,
}

# ─── DIVERS ───────────────────────────────────────────────
ENABLE_PROXY_FIX = True
WTF_CSRF_ENABLED = False   # désactivé pour le script de configuration par API (environnement local uniquement)

# Connexion PostgreSQL à ajouter dans l'UI Superset (ou via scripts/setup_superset.py) :
# postgresql://<POSTGRES_USER>:<POSTGRES_PASSWORD>@postgres:5432/<POSTGRES_DB>