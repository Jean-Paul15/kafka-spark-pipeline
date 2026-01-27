# =============================================================================
# STREAMLIT DASHBOARD - Visualisation Temps Réel Immobilier Togo
# Page principale avec métriques et graphiques dynamiques
# Données en XOF (Franc CFA BCEAO)
# =============================================================================

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import time

from kafka_consumer import KafkaDataConsumer
from redis_client import create_redis_client
from components import (
    render_header,
    render_architecture,
    render_metrics_row,
    render_annonces_charts,
    render_prix_charts,
    render_transactions_charts,
    render_redis_status,
)

# Configuration de la page
st.set_page_config(
    page_title="Spark Streaming - Immobilier Togo",
    page_icon="TG",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Style CSS personnalise
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1E88E5;
        text-align: center;
        padding: 1rem;
        background: linear-gradient(90deg, #E3F2FD 0%, #BBDEFB 100%);
        border-radius: 10px;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: #FFFFFF;
        padding: 1rem;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        text-align: center;
    }
    .status-online { color: #4CAF50; font-weight: bold; }
    .status-offline { color: #F44336; font-weight: bold; }
</style>
""", unsafe_allow_html=True)


def main():
    """Point d'entree principal du dashboard."""
    # Sidebar - Navigation et configuration
    with st.sidebar:
        st.image("https://spark.apache.org/images/spark-logo-trademark.png", width=150)
        st.markdown("---")
        st.markdown("### Configuration")
        
        refresh_rate = st.slider(
            "Taux de rafraîchissement (sec)",
            min_value=1,
            max_value=10,
            value=3,
        )
        
        show_architecture = st.checkbox("Afficher l'architecture", value=True)
        show_raw_data = st.checkbox("Afficher données brutes", value=False)
        show_redis_data = st.checkbox("Afficher données Redis", value=True)
        
        st.markdown("---")
        st.markdown("### Statut des Services")
        
        # Affichage statut Redis
        redis_client = create_redis_client()
        render_redis_status(redis_client)
        
    # Header principal
    render_header()
    
    # Section Architecture
    if show_architecture:
        render_architecture()
    
    # Initialisation du consumer Kafka
    consumer = KafkaDataConsumer()
    
    # Placeholder pour le contenu dynamique
    placeholder = st.empty()
    
    # Boucle de rafraichissement
    while True:
        with placeholder.container():
            # Recuperation des donnees temps reel (Kafka)
            annonces_data = consumer.get_latest_annonces()
            prix_data = consumer.get_latest_prix()
            transactions_data = consumer.get_latest_transactions()
            
            # Recuperation des donnees stockees (Redis)
            if show_redis_data and redis_client.is_connected():
                redis_cities = redis_client.get_all_cities_stats()
                redis_annonces_history = redis_client.get_history_df("annonces", 20)
                redis_prix_history = redis_client.get_history_df("prix", 20)
                redis_transactions_history = redis_client.get_history_df("transactions", 20)
            
            # Metriques globales
            render_metrics_row(annonces_data, prix_data, transactions_data)
            
            st.markdown("---")
            
            # Onglets pour les différentes vues
            tab1, tab2, tab3, tab4 = st.tabs([
                "Annonces en Temps Réel",
                "Évolution des Prix",
                "Transactions",
                "Données Stockées (Redis)",
            ])
            
            with tab1:
                render_annonces_charts(annonces_data)
                if show_raw_data and not annonces_data.empty:
                    st.dataframe(annonces_data, use_container_width=True)
            
            with tab2:
                render_prix_charts(prix_data)
                if show_raw_data and not prix_data.empty:
                    st.dataframe(prix_data, use_container_width=True)
            
            with tab3:
                render_transactions_charts(transactions_data)
                if show_raw_data and not transactions_data.empty:
                    st.dataframe(transactions_data, use_container_width=True)
            
            with tab4:
                st.subheader("Données Persistées dans Redis")
                if redis_client.is_connected():
                    col1, col2 = st.columns(2)
                    with col1:
                        st.markdown("#### Statistiques par Ville")
                        if not redis_cities.empty:
                            st.dataframe(redis_cities, use_container_width=True)
                        else:
                            st.info("En attente de données...")
                    with col2:
                        st.markdown("#### Stats Globales")
                        for cat in ["annonces", "prix", "transactions"]:
                            stats = redis_client.get_global_stats(cat)
                            if stats:
                                st.json({cat: stats})
                else:
                    st.warning("Redis non connecte")
            
            # Footer avec timestamp
            st.markdown("---")
            col1, col2, col3 = st.columns(3)
            with col2:
                st.caption(f"Dernière mise à jour: {datetime.now().strftime('%H:%M:%S')}")
        
        time.sleep(refresh_rate)


if __name__ == "__main__":
    main()
