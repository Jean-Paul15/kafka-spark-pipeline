# =============================================================================
# COMPOSANTS STREAMLIT - Graphiques Prix et Transactions
# Suite des éléments visuels pour le dashboard (Togo - XOF)
# =============================================================================

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


def format_xof_millions(value):
    """Formate un montant en millions de XOF."""
    return f"{value / 1_000_000:.1f} M XOF"


def render_prix_charts(df):
    """Affiche les graphiques pour l'évolution des prix togolais (XOF)."""
    if df.empty:
        st.info("En attente de données de prix...")
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        if "ville" in df.columns and "nouveau_prix_m2_xof" in df.columns:
            prix_ville = df.groupby("ville")["nouveau_prix_m2_xof"].mean().reset_index()
            prix_ville.columns = ["Ville", "Prix m2 XOF"]
            prix_ville = prix_ville.sort_values("Prix m2 XOF", ascending=True)
            
            fig = px.bar(
                prix_ville,
                y="Ville",
                x="Prix m2 XOF",
                orientation="h",
                title="Prix au m2 par Ville (XOF)",
                color="Prix m2 XOF",
                color_continuous_scale="RdYlGn_r",
            )
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        if "variation_pct" in df.columns and "ville" in df.columns:
            var_ville = df.groupby("ville")["variation_pct"].mean().reset_index()
            var_ville.columns = ["Ville", "Variation"]
            
            colors = ["#4CAF50" if v > 0 else "#F44336" for v in var_ville["Variation"]]
            
            fig = go.Figure(data=[
                go.Bar(
                    x=var_ville["Ville"],
                    y=var_ville["Variation"],
                    marker_color=colors,
                )
            ])
            fig.update_layout(
                title="Variation des Prix (%)",
                xaxis_title="Ville",
                yaxis_title="Variation (%)",
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Indicateur de tendance globale
    if "tendance" in df.columns:
        tendance_counts = df["tendance"].value_counts()
        hausse = tendance_counts.get("hausse", 0)
        baisse = tendance_counts.get("baisse", 0)
        
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            if hausse > baisse:
                st.success(f"Tendance globale: HAUSSE ({hausse} vs {baisse})")
            elif baisse > hausse:
                st.error(f"Tendance globale: BAISSE ({baisse} vs {hausse})")
            else:
                st.warning("Tendance globale: STABLE")


def render_transactions_charts(df):
    """Affiche les graphiques pour les transactions togolaises (XOF)."""
    if df.empty:
        st.info("En attente de données de transactions...")
        return
    
    col1, col2 = st.columns(2)
    
    with col1:
        if "ville" in df.columns and "prix_vente_xof" in df.columns:
            volume_ville = df.groupby("ville")["prix_vente_xof"].sum().reset_index()
            volume_ville.columns = ["Ville", "Volume"]
            volume_ville["Volume_M"] = volume_ville["Volume"] / 1_000_000
            
            fig = px.bar(
                volume_ville,
                x="Ville",
                y="Volume_M",
                title="Volume des Ventes par Ville (M XOF)",
                color="Volume_M",
                color_continuous_scale="Viridis",
            )
            fig.update_layout(showlegend=False, yaxis_title="Volume (M XOF)")
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        if "type_acheteur" in df.columns:
            acheteur_counts = df["type_acheteur"].value_counts().reset_index()
            acheteur_counts.columns = ["Type Acheteur", "Nombre"]
            
            fig = px.pie(
                acheteur_counts,
                values="Nombre",
                names="Type Acheteur",
                title="Profil des Acheteurs",
                color_discrete_sequence=px.colors.qualitative.Set2,
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Metriques additionnelles
    if not df.empty:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if "duree_vente_jours" in df.columns:
                duree_moy = int(df["duree_vente_jours"].mean())
                st.metric("Durée Moyenne de Vente", f"{duree_moy} jours")
        
        with col2:
            if "financement" in df.columns:
                mode_financement = df["financement"].mode()
                if len(mode_financement) > 0:
                    st.metric("Mode de Financement Principal", mode_financement.iloc[0])
        
        with col3:
            if "prix_vente_xof" in df.columns:
                volume_total = df["prix_vente_xof"].sum()
                st.metric("Volume Total", f"{volume_total/1_000_000:.2f} M XOF")
