"""
extract_db.py

Module d'extraction des données de capteurs depuis la base de données PostgreSQL interne.
Il exécute une requête SQL pour récupérer les mesures brutes sur une période donnée (lookback window),
en utilisant un curseur basé sur un dictionnaire (RealDictCursor) pour faciliter la transformation.

CORRECTION : Intégration de la génération de données aléatoires pour simuler un flux continu.
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
import pandas as pd
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# --- FONCTION DE GÉNÉRATION DE DONNÉES SIMULÉES ---

def generate_and_insert_new_data(conn_params, turbines=['T001', 'T002']):
    """
    Simule l'arrivée de nouvelles mesures en insérant un jeu de données 
    aléatoires pour l'heure en cours dans la table raw_measurements.
    
    Utilise NOW() de la DB pour déterminer le timestamp à la minute.
    """
    
    current_ts = None
    try:
        with psycopg2.connect(**conn_params) as conn:
            with conn.cursor() as cur:
                # Déterminer le timestamp exact (à la minute la plus récente) via la DB
                get_ts_query = "SELECT (now() at time zone 'UTC') - (EXTRACT(SECOND FROM (now() at time zone 'UTC')) || ' second')::interval AS current_ts;"
                cur.execute(get_ts_query)
                current_ts = cur.fetchone()[0]
                
                insert_query = """
                INSERT INTO raw_measurements (turbine_id, ts_utc, wind_speed_mps,
                                              temperature_k, vibration_mm_s, consumption_kwh)
                SELECT * FROM fn_random_measurement(%s, %s) 
                ON CONFLICT (turbine_id, ts_utc) DO NOTHING;
                """
                
                # Insérer une nouvelle mesure pour chaque turbine à cet horodatage
                for turbine_id in turbines:
                    cur.execute(insert_query, (turbine_id, current_ts))
                
                conn.commit()
                logger.info(f"Génération et insertion réussies de {len(turbines)} nouvelles mesures pour le timestamp: {current_ts}.")
                
    except psycopg2.Error as e:
        logger.error(f"Erreur SQL lors de la génération de données : {e.pgcode} - {e.pgerror}")
    except Exception as e:
        logger.exception("Erreur inattendue lors de la génération de données simulées")


# --- FONCTION D'EXTRACTION (Appelle la génération en premier) ---

def fetch_last_24h(conn_params, lookback_minutes=1440):
    """
    Récupère les mesures brutes de capteurs sur la période de temps spécifiée.
    """
    
    # 1. SIMULATION : Génère et insère de nouvelles données
    logger.info("Étape 1/2 : Génération de nouvelles mesures (simulé)")
    generate_and_insert_new_data(conn_params) 
    
    # 2. EXTRACTION DES DONNÉES (y compris les nouvelles)
    query = """
    SELECT 
        turbine_id, 
        ts_utc AS date,
        wind_speed_mps AS wind_ms,
        temperature_k, 
        vibration_mm_s,
        consumption_kwh
    FROM raw_measurements
    -- Extrait les données de la période de lookback
    WHERE ts_utc >= (now() at time zone 'UTC') - interval %s
    """
    
    try:
        with psycopg2.connect(**conn_params) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, (f'{lookback_minutes} minutes',))
                rows = cur.fetchall()
                logger.info(f"Étape 2/2 : {len(rows)} mesures extraites de raw_measurements.")
                # Retourne les données sous forme de liste de dictionnaires
                return rows

    except psycopg2.Error as e:
        logger.error(f"Erreur SQL PostgreSQL : {e.pgcode} - {e.pgerror}")
        return []

    except Exception as e:
        logger.exception("Erreur inattendue lors de l'extraction DB")
        return []