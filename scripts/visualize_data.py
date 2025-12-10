"""
visualize_data.py

Interface utilisateur Streamlit pour la visualisation des données consolidées.
Ce module interroge la table 'consolidated_measurements' pour afficher les indicateurs 
clés, les tendances historiques (énergie, vent, température) et l'état des turbines.

Pour lancer l'application : streamlit run visualize_data.py
"""

import streamlit as st
import psycopg2
import pandas as pd
import yaml
from datetime import datetime, timedelta
from pathlib import Path
import altair as alt

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
    """
    Récupère les données consolidées des 7 derniers jours depuis PostgreSQL pour la visualisation.
    """
    try:
        conn = psycopg2.connect(**conn_params)
        
        # Date de début pour les 7 derniers jours
        seven_days_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')

        query = f"""
        SELECT 
            turbine_id, 
            date, 
            -- Utilisation de COALESCE(temperature_k, NULL) pour s'assurer que les NULL restent NULL
            COALESCE(temperature_k, NULL) as temperature_k, 
            COALESCE(wind_ms, 0) as wind_ms, 
            COALESCE(vibration_mm_s, 0) as vibration_mm_s,
            COALESCE(consumption_kwh, 0) as consumption_kwh,
            COALESCE(energie_kwh, 0) as energie_kwh, 
            arret_planifie, 
            arret_non_planifie
        FROM consolidated_measurements
        WHERE date >= '{seven_days_ago}'
        ORDER BY date DESC;
        """
        
        data_df = pd.read_sql(query, conn)
        conn.close()
        
        if data_df.empty:
            return pd.DataFrame()
            
        data_df['date'] = pd.to_datetime(data_df['date'])
        
        return data_df

    except psycopg2.Error as e:
        st.error(f"Erreur lors de la connexion/requête PostgreSQL : {e}")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Erreur inattendue : {e}")
        return pd.DataFrame()


# --- Fonction Principale ---
def main():
    st.set_page_config(layout="wide", page_title="Tableau de Bord Éoliennes")
    st.title("📊 Tableau de Bord Éoliennes (7 Jours)")

    cfg = load_config()

    # 1. Extraction des données
    data_df = fetch_consolidated_data(cfg["postgres"]) 

    if data_df.empty:
        st.warning("Aucune donnée consolidée trouvée ou erreur de connexion à la base de données.")
        return

    # 2. Indicateurs Clés
    total_kwh = data_df['energie_kwh'].sum()
    avg_temp_k = data_df['temperature_k'].mean() 
    
    # Vérification : Si la moyenne est absurde (i.e. très proche de 0K), on l'ignore.
    if avg_temp_k is not None and avg_temp_k > 200: 
        avg_temp_c = avg_temp_k - 273.15
    else:
        avg_temp_c = "N/A" 
        
    avg_wind_ms = data_df['wind_ms'].mean()
    nb_rows = len(data_df)

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Mesures (7 derniers jours)", f"{nb_rows:,}")
    with col2:
        st.metric("Énergie Totale (kWh)", f"{total_kwh:,.0f}")
    with col3:
        st.metric("Temp. Moyenne (°C)", f"{avg_temp_c:.1f} °C" if isinstance(avg_temp_c, float) else avg_temp_c)
    with col4:
        st.metric("Vent Moyen (m/s)", f"{avg_wind_ms:.2f} m/s")

    st.markdown("---")
    
    # --- 3. Graphiques de Tendance (Vent et Température) ---
    st.subheader("Tendance du Vent et de la Température par Heure")
    
    # Agrégation horaire
    data_df['date_hour'] = data_df['date'].dt.floor('H')
    chart_data = data_df.groupby('date_hour').agg({
        'wind_ms': 'mean',
        'temperature_k': 'mean'
    }).reset_index()

    # Conversion de Kelvin vers Celsius pour la visualisation
    chart_data['temperature_c'] = chart_data['temperature_k'] - 273.15 
    
    # Création des colonnes pour aligner les deux graphiques
    col_wind, col_temp = st.columns(2)
    
    # ------------------------------------
    # Graphique du Vent (Colonne de gauche)
    # ------------------------------------
    with col_wind:
        
        # Définition du graphique
        chart_wind = alt.Chart(chart_data).mark_line(color='#1f77b4').encode(
            # MODIFICATION ICI : Format de date plus précis
            x=alt.X('date_hour', title='Heure (UTC)', axis=alt.Axis(format='%Y-%m-%d %H:%M')),
            y=alt.Y('wind_ms', title='Vitesse du Vent (m/s)'),
            tooltip=[
                alt.Tooltip('date_hour', title='Heure'), 
                alt.Tooltip('wind_ms', title='Vent (m/s)', format='.2f')
            ]
        ).properties(
            title='Vitesse du Vent par Heure'
        ).interactive() # Permet le zoom et le déplacement

        st.altair_chart(chart_wind, use_container_width=True)

    # ------------------------------------
    # Graphique de la Température (Colonne de droite)
    # ------------------------------------
    with col_temp:
        
        # Définition du graphique
        chart_temp = alt.Chart(chart_data).mark_line(color='#d62728').encode(
            # MODIFICATION ICI : Format de date plus précis
            x=alt.X('date_hour', title='Heure (UTC)', axis=alt.Axis(format='%Y-%m-%d %H:%M')),
            y=alt.Y('temperature_c', title='Température (°C)'),
            tooltip=[
                alt.Tooltip('date_hour', title='Heure'), 
                alt.Tooltip('temperature_c', title='Temp. (°C)', format='.2f')
            ]
        ).properties(
            title='Température Moyenne par Heure'
        ).interactive() # Permet le zoom et le déplacement

        st.altair_chart(chart_temp, use_container_width=True)

    # Fin de la section 3 (Graphiques)
    st.markdown("---")

    # 4. Graphique de Production (Énergie)
    st.subheader("Production d'Énergie Totale (kWh)")
    
    # Agrégation par jour pour l'énergie (plus pertinent que par heure)
    prod_data = data_df[data_df['energie_kwh'] > 0].groupby(data_df['date'].dt.date).agg({'energie_kwh': 'sum'}).reset_index()
    prod_data['date'] = pd.to_datetime(prod_data['date'])
    
    chart_prod = alt.Chart(prod_data).mark_bar().encode(
        x=alt.X('date', title='Jour', axis=alt.Axis(format='%Y-%m-%d')),
        y=alt.Y('energie_kwh', title='Énergie Totale (kWh)'),
        tooltip=['date', alt.Tooltip('energie_kwh', format=',.0f')]
    ).properties(
        title="Énergie Totale Produite par Jour"
    ).interactive()

    st.altair_chart(chart_prod, use_container_width=True)

    st.markdown("---")

    # 5. Statut des Turbines
    st.subheader("État des Turbines et Statistiques d'Arrêt")
    
    # Calcul des stats par turbine
    turbine_status = data_df.groupby('turbine_id').agg(
        Total_Energie_kWh=('energie_kwh', 'sum'),
        Jours_Arret_Planifie=('arret_planifie', 'sum'),
        Jours_Arret_Non_Planifie=('arret_non_planifie', 'sum'),
        Derniere_Mesure=('date', 'max')
    ).reset_index()

    # Déterminer le statut (Arbitraire: "Hors Ligne" si pas de mesure depuis > 1 jour)
    turbine_status['Statut'] = turbine_status.apply(
        lambda row: 'En Production' if (datetime.now().date() - row['Derniere_Mesure'].date()).days <= 1 else 'Hors Ligne',
        axis=1
    )
    
    # Mise en forme pour l'affichage
    turbine_status['Total_Energie_kWh'] = turbine_status['Total_Energie_kWh'].map('{:,.0f}'.format)
    turbine_status['Derniere_Mesure'] = turbine_status['Derniere_Mesure'].dt.strftime('%Y-%m-%d %H:%M')

    # Affichage
    col_status, col_table = st.columns([1, 2])

    with col_status:
        st.markdown("#### Vue d'ensemble")
        for _, row in turbine_status.iterrows():
            if row['Statut'] == 'En Production':
                st.success(f"**{row['turbine_id']}** : {row['Statut']}")
            else:
                st.error(f"**{row['turbine_id']}** : {row['Statut']}")

    with col_table:
        st.markdown("#### Statut sur 7 Jours (Mesures d'Arrêt)")
        st.dataframe(
            turbine_status[['turbine_id', 'Total_Energie_kWh', 'Jours_Arret_Planifie', 'Jours_Arret_Non_Planifie', 'Derniere_Mesure']],
            hide_index=True,
            column_config={
                "turbine_id": "Turbine ID",
                "Total_Energie_kWh": st.column_config.TextColumn("Énergie (kWh)"),
                "Jours_Arret_Planifie": st.column_config.NumberColumn("Arrêts Planifiés (Jours)", format="%d"),
                "Jours_Arret_Non_Planifie": st.column_config.NumberColumn("Arrêts Non Planifiés (Jours)", format="%d"),
                "Derniere_Mesure": st.column_config.DatetimeColumn("Dernière Mesure", format="YYYY-MM-DD HH:mm")
            }
        )
        
    st.markdown("---")
    st.caption(f"Dernière mise à jour du cache: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    main()