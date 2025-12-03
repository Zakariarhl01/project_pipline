"""
extract_db.py

Module d'extraction des données de capteurs depuis la base de données PostgreSQL interne.
Il exécute une requête SQL pour récupérer les mesures brutes sur une période donnée (lookback window),
en utilisant un curseur basé sur un dictionnaire (RealDictCursor) pour faciliter la transformation.
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
import pandas as pd

logger = logging.getLogger(__name__)

def fetch_last_24h(conn_params, lookback_minutes=1440):
    """
    Récupère les mesures brutes de capteurs sur la période de temps spécifiée.

    La requête sélectionne les données de la table 'raw_measurements' et les renomme pour 
    correspondre au schéma de la table cible (ex: ts_utc -> date).

    Args:
        conn_params (dict): Paramètres de connexion PostgreSQL.
        lookback_minutes (int): Nombre de minutes à remonter dans le temps (par défaut 1440 min = 24h).

    Returns:
        list[dict]: Liste des enregistrements extraits ou une liste vide en cas d'erreur.
    """

    query = """
    SELECT 
        turbine_id, 
        ts_utc AS date, -- Renommage de la colonne pour correspondre au schéma cible (date)
        wind_speed_mps AS wind_ms, -- Renommage pour correspondre au schéma cible (wind_ms)
        temperature_k, 
        vibration_mm_s,
        consumption_kwh
    FROM raw_measurements
    WHERE ts_utc >= (now() at time zone 'UTC') - interval %s
    """
    

    try:
        with psycopg2.connect(**conn_params) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, (f'{lookback_minutes} minutes',))
                rows = cur.fetchall()
                logger.info(f"{len(rows)} mesures extraites depuis la base interne")
                return rows

    except psycopg2.Error as e:
        logger.error(f"Erreur SQL PostgreSQL : {e.pgcode} - {e.pgerror}")
        return []

    except Exception as e:
        logger.exception("Erreur inattendue lors de l'extraction DB")
        return []


def extract_sensors_from_db(conn_params):
    # Cette fonction n'est pas appelée dans le pipeline principal
    return []