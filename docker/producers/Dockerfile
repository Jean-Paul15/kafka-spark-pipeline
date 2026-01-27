# =============================================================================
# DOCKERFILE - Producteurs de Donnees Immobilieres
# Simulation de sources temps reel multiples
# =============================================================================

FROM python:3.11-slim




WORKDIR /app

# Installation des dependances systeme
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copie et installation des dependances Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copie du code source
COPY . .

# Demarrage du producteur
CMD ["python", "-u", "immobilier_producer.py"]
