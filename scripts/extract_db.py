"""
extract_db.py
Extraction des dernières mesures depuis la base PostgreSQL.
"""
import psycopg2
from psycopg2.extras import RealDictCursor
import logging
import pandas as pd

logger = logging.getLogger(__name__)

def fetch_last_24h(conn_params, lookback_minutes=1440):

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