# =============================================================================
# COMPOSANTS STREAMLIT - Éléments visuels réutilisables
# Header, métriques, graphiques pour le dashboard immobilier Togo
# Prix en XOF (Franc CFA BCEAO)
# =============================================================================

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Import des graphiques supplementaires
from charts import render_prix_charts, render_transactions_charts

# Re-export pour usage dans app.py
__all__ = [
    "render_header",
    "render_architecture", 
    "render_metrics_row",
    "render_annonces_charts",
    "render_prix_charts",
    "render_transactions_charts",
    "render_redis_status",
]


def format_xof(value):
    """Formate un montant en XOF avec separateur de milliers."""
    if value is None or pd.isna(value):
        return "--"
    return f"{int(value):,} XOF".replace(",", " ")


def render_redis_status(redis_client):
    """Affiche le statut de Redis dans la sidebar."""
    info = redis_client.get_redis_info()
    
    if info.get("status") == "connected":
        st.success("Redis: Connecté")
        st.caption(f"Mémoire: {info.get('used_memory', 'N/A')}")
        st.caption(f"Clés: {info.get('keys', 0)}")
    else:
        st.error("Redis: Déconnecté")


def render_header():
    """Affiche le header principal du dashboard."""
    st.markdown("""
    <div class="main-header">
        SPARK STREAMING - IMMOBILIER TOGO TEMPS REEL
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <p style="text-align: center; font-size: 1.1rem; color: #666;">
        Démonstration Big Data : Traitement de flux temps réel avec Apache Spark et Kafka<br/>
        <strong>Marché immobilier togolais - Prix en XOF (Franc CFA)</strong>
    </p>
    """, unsafe_allow_html=True)


def render_architecture():
    """Affiche le schéma d'architecture du système."""
    with st.expander("Architecture du Système", expanded=False):
        st.markdown("""
        ```
        +------------------+     +------------------+     +------------------+
        |   PRODUCTEUR 1   |     |   PRODUCTEUR 2   |     |   PRODUCTEUR 3   |
        |    Annonces      |     |      Prix        |     |  Transactions    |
        | (Nouvelles offres)|    | (Indices marche) |     | (Ventes reelles) |
        |   Lome, Kara...  |     |   XOF / m2       |     |   Volume XOF     |
        +--------+---------+     +--------+---------+     +--------+---------+
                 |                        |                        |
                 v                        v                        v
        +-----------------------------------------------------------------------+
        |                         APACHE KAFKA                                   |
        |    Topic: annonces     Topic: prix        Topic: transactions         |
        |    [Partition 0-2]     [Partition 0-2]    [Partition 0-2]            |
        +--------------------------------+------------------------------------------+
                                         |
                                         v
        +-----------------------------------------------------------------------+
        |                      APACHE SPARK STREAMING                           |
        |  +-------------+  +-------------+  +-------------+                   |
        |  | Aggregation |  |  Jointures  |  |  Analytics  |                   |
        |  | par Ville   |  | Multi-flux  |  | Tendances   |                   |
        |  +-------------+  +-------------+  +-------------+                   |
        |                         |                                             |
        |         Window Operations (30s sliding, 10s interval)                |
        +--------------------------------+--------------------------------------+
                                         |
                                         v
        +-----------------------------------------------------------------------+
        |                      REDIS (Data Lake In-Memory)                      |
        |  +---------------+  +---------------+  +-------------------+         |
        |  | Stats Villes  |  |  Historique   |  | Agregations Live  |         |
        |  +---------------+  +---------------+  +-------------------+         |
        +--------------------------------+--------------------------------------+
                                         |
                                         v
        +-----------------------------------------------------------------------+
        |                         STREAMLIT DASHBOARD                           |
        |                   (Visualisation Temps Reel)                          |
        |  +---------------+  +---------------+  +---------------+             |
        |  | Metriques KPI |  |  Graphiques   |  | Donnees Redis |             |
        |  +---------------+  +---------------+  +---------------+             |
        +-----------------------------------------------------------------------+
        ```
        
        **Technologies utilisées:**
        - **Apache Kafka**: Message broker distribué pour ingestion temps réel
        - **Apache Spark Streaming**: Moteur de traitement distribué
        - **Redis**: Stockage in-memory pour résultats traités (Data Lake)
        - **Streamlit**: Framework Python pour dashboards interactifs
        - **Docker**: Containerisation et orchestration des services
        
        **Données Togolaises:**
        - Villes: Lome, Kara, Sokode, Kpalime, Atakpame, Dapaong, Tsevie, Notse...
        - Monnaie: XOF (Franc CFA BCEAO)
        - Prix moyens: 125 000 - 475 000 XOF/m2 selon la ville
        """)


def render_metrics_row(annonces_df, prix_df, transactions_df):
    """Affiche la ligne de métriques KPI (prix en XOF)."""
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        nb_annonces = len(annonces_df) if not annonces_df.empty else 0
        st.metric(
            label="Annonces Reçues",
            value=nb_annonces,
            delta=f"+{min(nb_annonces, 5)}" if nb_annonces > 0 else None,
        )
    
    with col2:
        if not annonces_df.empty and "prix_xof" in annonces_df.columns:
            prix_moyen = int(annonces_df["prix_xof"].mean())
            st.metric(
                label="Prix Moyen",
                value=format_xof(prix_moyen),
            )
        else:
            st.metric(label="Prix Moyen", value="--")
    
    with col3:
        nb_transactions = len(transactions_df) if not transactions_df.empty else 0
        st.metric(
            label="Transactions",
            value=nb_transactions,
            delta=f"+{min(nb_transactions, 3)}" if nb_transactions > 0 else None,
        )
    
    with col4:
        if not prix_df.empty and "variation_pct" in prix_df.columns:
            variation = round(prix_df["variation_pct"].mean(), 2)
            st.metric(
                label="Tendance Prix",
                value=f"{variation:+.2f}%",
                delta="Hausse" if variation > 0 else "Baisse",
                delta_color="normal" if variation > 0 else "inverse",
            )
        else:
            st.metric(label="Tendance Prix", value="--")


def render_annonces_charts(df):
    """Affiche les graphiques pour les annonces togolaises."""
    if df.empty:
        st.info("En attente de données d'annonces...")
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        if "ville" in df.columns:
            ville_counts = df["ville"].value_counts().reset_index()
            ville_counts.columns = ["Ville", "Nombre"]
            fig = px.bar(
                ville_counts,
                x="Ville",
                y="Nombre",
                title="Annonces par Ville Togolaise",
                color="Nombre",
                color_continuous_scale="Greens",
            )
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        if "type_bien" in df.columns:
            type_counts = df["type_bien"].value_counts().reset_index()
            type_counts.columns = ["Type", "Nombre"]
            fig = px.pie(
                type_counts,
                values="Nombre",
                names="Type",
                title="Répartition par Type de Bien",
                hole=0.4,
                color_discrete_sequence=px.colors.sequential.Greens_r,
            )
            st.plotly_chart(fig, use_container_width=True)
