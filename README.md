<div align="center">
  <h1>kafka-spark-pipeline</h1>
  <p><b>Un pipeline de streaming qui fait circuler des événements e-commerce, taxi, météo et immobiliers de Kafka vers Spark, puis vers PostgreSQL, HDFS, Redis, Grafana, Superset et Streamlit, le tout démarré par Docker Compose.</b></p>
  <p>
    <img src="https://img.shields.io/badge/Apache%20Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white" alt="Apache Kafka"/> <img src="https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white" alt="Apache Spark"/> <img src="https://img.shields.io/badge/PostgreSQL-4169E1?style=for-the-badge&logo=postgresql&logoColor=white" alt="PostgreSQL"/> <img src="https://img.shields.io/badge/Redis-DC382D?style=for-the-badge&logo=redis&logoColor=white" alt="Redis"/> <img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white" alt="Docker"/> <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python"/>
  </p>
  <p>
    <img src="https://img.shields.io/badge/Licence-Tous%20droits%20r%C3%A9serv%C3%A9s-lightgrey?style=flat-square" alt="Licence : tous droits réservés"/>
    <img src="https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white" alt="Python"/>
  </p>
  <p>
    <a href="#démarrer-en-cinq-minutes">Démarrer</a> ·
    <a href="#comment-ça-marche">Architecture</a> ·
    <a href="#stack-technique">Stack</a> ·
    <a href="#structure-du-dépôt">Structure</a> ·
    <a href="#tests">Tests</a>
  </p>
</div>

## Sommaire

- [Ce que ça apporte](#ce-que-ça-apporte)
- [Le problème](#le-problème)
- [Démarrer en cinq minutes](#démarrer-en-cinq-minutes)
- [Comment ça marche](#comment-ça-marche)
- [Stack technique](#stack-technique)
- [Structure du dépôt](#structure-du-dépôt)
- [Tests](#tests)
- [Limites et suite](#limites-et-suite)
- [Licence et contact](#licence-et-contact)

## Ce que ça apporte

- **Une pile entière, par couches.** `docker compose up -d` lance le socle (Kafka, Kafka UI, cluster Spark). Trois profils ajoutent chacun un étage : `immobilier`, `analytics`, `superset`.
- **Deux modes de calcul, côte à côte.** Spark Structured Streaming agrège en fenêtres glissantes vers Redis. Un job Spark batch relit des Parquet sur HDFS et calcule neuf tables de KPI dans PostgreSQL.
- **Une chaîne testable sans broker.** Les générateurs d'événements se vérifient par cinq tests `pytest` qui n'ont besoin ni de Kafka ni de Docker.

## Le problème

Un site marchand enregistre une commande toutes les secondes. Le tableau de bord de la direction affiche pourtant le chiffre d'affaires d'hier. Entre les deux, il manque un maillon qui reçoit les événements, les nettoie, les agrège et les publie sans que personne ne lance de script à la main.

Ce dépôt construit ce maillon de bout en bout. Il reste local, reproductible et lisible. Les données sont simulées ou publiques : il n'y a pas de client réel derrière.

## Démarrer en cinq minutes

Prérequis : Docker avec Compose, Python 3.

```bash
cp .env.example .env        # puis remplacer les valeurs factices change_me_*
docker compose --profile analytics up -d --build
pip install -r src/producers/requirements.txt
python scripts/launch_all_producers.py
python -m pytest
```

Grafana répond ensuite sur `http://localhost:3101`, Kafka UI sur `http://localhost:8080`, le Spark master sur `http://localhost:8090`. Le job de streaming vers PostgreSQL démarre seul, après une attente de 45 secondes prévue dans l'entrypoint.

## Comment ça marche

```mermaid
flowchart LR
    subgraph Sources
        E[Producteur e-commerce<br/>Faker]
        T[Producteur taxi<br/>replay Parquet ou simulation]
        W[Producteur météo<br/>API Open-Meteo]
        I[Producteurs immobilier<br/>annonces, prix, transactions]
    end

    K[(Kafka<br/>6 topics, 3 partitions)]
    E --> K
    T --> K
    W --> K
    I --> K

    subgraph Analytics
        S1[Spark Streaming<br/>kafka_to_postgres.py]
        PG[(PostgreSQL<br/>tables events, vues, KPI)]
        H[(HDFS<br/>Parquet brut)]
        B[Spark batch<br/>batch_hdfs_to_postgres.py]
        G[Grafana]
        SS[Superset]
    end

    K --> S1 --> PG
    PG -- export_to_hdfs.py --> H
    H --> B --> PG
    PG --> G
    PG --> SS

    subgraph Immobilier
        S2[Spark Streaming<br/>fenêtres 30 s glissantes 10 s]
        R[(Redis<br/>TTL 5 et 10 min)]
        D[Streamlit]
    end

    K --> S2 --> R --> D
    K -. buffer récent .-> D
```

Trois choix structurent le dépôt.

**Deux écoutes pour un seul broker Kafka.** Le broker annonce `kafka:29092` aux conteneurs et `localhost:9092` à la machine hôte. Les jobs Spark restent dans le réseau Docker. Les producteurs, eux, se lancent depuis votre terminal sans configuration réseau.

**Des profils Compose plutôt qu'un fichier géant à tout lancer.** Le socle tient sur une machine modeste. HDFS, PostgreSQL et Grafana n'arrivent qu'avec `--profile analytics`. Redis et Streamlit n'arrivent qu'avec `--profile immobilier`. Chaque étage se démarre, se casse et se débogue seul.

**Redis pour le chaud, PostgreSQL et HDFS pour le durable.** Côté immobilier, les agrégats sont écrits avec une durée de vie (5 minutes pour les fenêtres, 10 minutes pour les villes) et un historique borné à 100 entrées. Le dashboard lit des chiffres frais sans accumuler. Côté analytics, les événements bruts sont ajoutés en base par micro-lots de 10 secondes, au plus 2000 offsets par lot. Les KPI sont ensuite recalculés en batch et écrasés, ce qui rend le calcul rejouable.

Les secrets ne vivent jamais dans le code : `docker-compose.yml` refuse de démarrer si `POSTGRES_PASSWORD`, `GRAFANA_ADMIN_PASSWORD` ou les variables Superset manquent dans `.env`.

## Stack technique

| Domaine | Technologies | Rôle dans le projet |
|---|---|---|
| Messagerie | <img src="https://img.shields.io/badge/Apache%20Kafka-231F20?style=flat-square&logo=apachekafka&logoColor=white" alt="Apache Kafka"/> <img src="https://img.shields.io/badge/Zookeeper-231F20?style=flat-square&logo=apache&logoColor=white" alt="Zookeeper"/> <img src="https://img.shields.io/badge/Kafka%20UI-231F20?style=flat-square&logo=apachekafka&logoColor=white" alt="Kafka UI"/> | Transport des événements, 6 topics de 3 partitions, Kafka UI pour les inspecter |
| Calcul | <img src="https://img.shields.io/badge/Apache%20Spark-E25A1C?style=flat-square&logo=apachespark&logoColor=white" alt="Apache Spark"/> <img src="https://img.shields.io/badge/PySpark-E25A1C?style=flat-square&logo=apachespark&logoColor=white" alt="PySpark"/> | Structured Streaming en fenêtres et job batch qui calcule les KPI |
| Stockage | <img src="https://img.shields.io/badge/PostgreSQL-4169E1?style=flat-square&logo=postgresql&logoColor=white" alt="PostgreSQL"/> <img src="https://img.shields.io/badge/Apache%20Hadoop-66CCFF?style=flat-square&logo=apachehadoop&logoColor=black" alt="Apache Hadoop"/> <img src="https://img.shields.io/badge/Redis-DC382D?style=flat-square&logo=redis&logoColor=white" alt="Redis"/> | PostgreSQL et HDFS pour le durable, Redis pour les agrégats récents avec durée de vie |
| Producteurs | <img src="https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white" alt="Python"/> <img src="https://img.shields.io/badge/Faker-3776AB?style=flat-square&logo=python&logoColor=white" alt="Faker"/> <img src="https://img.shields.io/badge/pandas-150458?style=flat-square&logo=pandas&logoColor=white" alt="pandas"/> <img src="https://img.shields.io/badge/Apache%20Arrow-3776AB?style=flat-square&logo=apache&logoColor=white" alt="Apache Arrow"/> | Générateurs d'événements e-commerce, taxi, météo et immobilier |
| Visualisation | <img src="https://img.shields.io/badge/Grafana-F46800?style=flat-square&logo=grafana&logoColor=white" alt="Grafana"/> <img src="https://img.shields.io/badge/Apache%20Superset-20A6C9?style=flat-square&logo=apache&logoColor=white" alt="Apache Superset"/> <img src="https://img.shields.io/badge/Streamlit-FF4B4B?style=flat-square&logo=streamlit&logoColor=white" alt="Streamlit"/> <img src="https://img.shields.io/badge/Plotly-3F4F75?style=flat-square&logo=plotly&logoColor=white" alt="Plotly"/> | Tableaux de bord sur PostgreSQL et Redis |
| Infrastructure | <img src="https://img.shields.io/badge/Docker%20Compose-2496ED?style=flat-square&logo=docker&logoColor=white" alt="Docker Compose"/> | Pile unique démarrée par profils : immobilier, analytics, superset |
| Tests | <img src="https://img.shields.io/badge/pytest-0A9EDC?style=flat-square&logo=pytest&logoColor=white" alt="pytest"/> | Tests des générateurs d'événements, sans broker ni Docker |

Versions : Confluent 7.5.0, Spark 3.5, PostgreSQL 15, Hadoop 3.2.1, Redis 7, Grafana 10.2, Superset 3.0.

## Structure du dépôt

```
docker-compose.yml     pile unique, profils immobilier / analytics / superset
docker/                Dockerfile (spark, producteurs, dashboard), entrypoint Spark, SQL d'initialisation
config/                Grafana (sources, dashboards), Hadoop, Spark, Superset
src/producers/         quatre producteurs Kafka (e-commerce, taxi, météo, immobilier)
src/spark/             jobs : kafka_to_postgres, export_to_hdfs, batch_hdfs_to_postgres
src/spark/immobilier/  streaming par fenêtres et stockage Redis
src/dashboard/         application Streamlit, client Redis, consommateur Kafka
scripts/               lancement des producteurs, init HDFS, dashboards Grafana et Superset, diagnostic
tests/                 tests des générateurs d'événements
```

## Tests

```bash
python -m pytest
```

Les générateurs d'événements se vérifient par cinq tests `pytest` qui n'ont besoin ni de Kafka ni de Docker. Les jobs Spark se vérifient en lançant la pile.

## Limites et suite

Le périmètre est celui d'un pipeline d'apprentissage et de démonstration, et il est choisi ainsi : un seul broker, un seul worker Spark, une réplication à 1, des données simulées ou publiques. Cela garde l'ensemble reproductible sur un poste, au prix d'aucune tolérance aux pannes.

Quelques points sont connus et assumés :

- Les événements arrivent dans HDFS par un export depuis PostgreSQL (`export_to_hdfs.py`), pas par une écriture directe du flux.
- Les checkpoints du streaming sont dans `/tmp`. Un conteneur recréé repart de zéro.
- Dans l'entrypoint Spark, le mode `immobilier` attend le cluster mais ne soumet pas encore le job. Le code du job est dans `src/spark/immobilier/streaming_immobilier.py` et reste à brancher.
- `scripts/check_pipeline.py` sonde Grafana sur le port 3000, alors que Compose l'expose sur 3101.
- Les tests couvrent les générateurs. Les jobs Spark se vérifient en lançant la pile.

La suite logique : brancher le job immobilier, écrire le flux vers HDFS directement, ajouter des tests d'intégration sur les transformations Spark.

## Licence et contact

Tous droits réservés. Le code est visible pour consultation. Toute réutilisation demande l'accord de l'auteur.

Contact : [Jean-Paul15](https://github.com/Jean-Paul15) sur GitHub.
