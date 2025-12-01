# visualize_data.py
# Interface utilisateur simple avec Streamlit.
# Exécuter avec : streamlit run visualize_data.py

import streamlit as st
import psycopg2
import pandas as pd
import yaml
from datetime import datetime
from pathlib import Path

# --- Fonctions utilitaires ---
def load_config(path="config.yaml"):
    """Charge le fichier de configuration."""
    try:
        # Chemin ajusté pour l'exécution depuis la racine du projet ou depuis le dossier 'scripts'
        config_path = path if Path(path).exists() else Path(__file__).parent.parent / path
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except Exception as e:
        st.error(f"Erreur lors du chargement de la configuration: {e}")
        st.stop()

@st.cache_data(ttl=600) 
def fetch_consolidated_data(conn_params):
    """Récupère les données consolidées depuis la base de données."""
    try:
        conn = psycopg2.connect(**conn_params)
        query = """
        -- Récupération des données des 7 derniers jours pour la visualisation
        SELECT *
        FROM consolidated_measurements
        WHERE date >= NOW() - INTERVAL '7 days'
        ORDER BY date DESC;
        """
        df = pd.read_sql(query, conn)
        conn.close()
        
        df['date'] = pd.to_datetime(df['date'])
        return df

    except psycopg2.OperationalError as e:
        st.error(f"Erreur de connexion à la base de données. Vérifiez les paramètres: {e}")
        return pd.DataFrame()


# --- Application Streamlit ---
def main():
    st.set_page_config(layout="wide", page_title="Tableau de Bord Éolien IA")
    
    # NOTE: Si vous utilisez viewer_user, changez les paramètres de connexion ici
    cfg = load_config()

    st.title("📊 Tableau de Bord des Mesures Consolidées")
    st.caption(f"Données mises à jour au {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 1. Récupération des données
    # Utilisation du paramètre 'postgres' de la config (à changer pour 'viewer_user' si besoin)
    data_df = fetch_consolidated_data(cfg["postgres"]) 

    if data_df.empty:
        st.warning("Aucune donnée consolidée trouvée ou erreur de connexion à la base de données.")
        return

    # 2. Indicateurs Clés
    total_kwh = data_df['energie_kwh'].sum()
    avg_temp_c = data_df['temperature_k'].mean() - 273.15 # Conversion rapide K -> C
    avg_wind_ms = data_df['wind_ms'].mean()
    nb_rows = len(data_df)

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Mesures (7 derniers jours)", f"{nb_rows:,}")
    with col2:
        st.metric("Énergie Totale (kWh)", f"{total_kwh:,.0f}")
    with col3:
        st.metric("Temp. Moyenne (°C)", f"{avg_temp_c:.1f} °C")
    with col4:
        st.metric("Vent Moyen (m/s)", f"{avg_wind_ms:.2f} m/s")

    st.markdown("---")
    
    # 3. Graphique de Tendance (Vent et Température)
    st.subheader("Tendance du Vent et de la Température par Heure")
    
    # Agrégation horaire
    data_df['date_hour'] = data_df['date'].dt.floor('H')
    chart_data = data_df.groupby(['date_hour']).agg(
        Temp_C=('temperature_k', lambda x: x.mean() - 273.15),
        Vent_MS=('wind_ms', 'mean')
    ).reset_index()

    st.line_chart(chart_data, x='date_hour', y=['Vent_MS', 'Temp_C'])
    
    # 4. Affichage des données brutes
    st.subheader("Aperçu des 100 dernières Mesures")
    st.dataframe(data_df.head(100), use_container_width=True)


if __name__ == "__main__":
    main()