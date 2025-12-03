import psycopg2
from psycopg2.extras import execute_values
import logging

logger = logging.getLogger(__name__)

def insert_measurements(conn_params, table, rows, batch_size=500):
    if not rows:
        logger.info("Aucune donnée à insérer.")
        return 0
    

    cols = list(rows[0].keys())
    values = [[row[col] for col in cols] for row in rows]

    col_names = ",".join(f'"{c}"' for c in cols)

    # Upsert selon turbine_id + date
    sql = f"""
    INSERT INTO {table} ({col_names}) VALUES %s
    ON CONFLICT (turbine_id, date) DO UPDATE SET
      energie_kwh = COALESCE(EXCLUDED.energie_kwh, consolidated_measurements.energie_kwh),
      temperature_k = COALESCE(EXCLUDED.temperature_k, consolidated_measurements.temperature_k),
      wind_ms = COALESCE(EXCLUDED.wind_ms, consolidated_measurements.wind_ms),
      vibration_mm_s = COALESCE(EXCLUDED.vibration_mm_s, consolidated_measurements.vibration_mm_s),
      consumption_kwh = COALESCE(EXCLUDED.consumption_kwh, consolidated_measurements.consumption_kwh),
      arret_planifie = COALESCE(EXCLUDED.arret_planifie, consolidated_measurements.arret_planifie),
      arret_non_planifie = COALESCE(EXCLUDED.arret_non_planifie, consolidated_measurements.arret_non_planifie),
      source = EXCLUDED.source -- La source DOIT refléter le dernier écrivain (Météo/Capteur/CSV)
    """

    inserted = 0
    
    try:
        with psycopg2.connect(**conn_params) as conn:
            with conn.cursor() as cur:
                for i in range(0, len(values), batch_size):
                    batch = values[i:i+batch_size]
                    execute_values(cur, sql, batch)
                    inserted += len(batch)

        logger.info(f"{inserted} lignes insérées dans {table}")
        return inserted
    
    except Exception:
        logger.exception("Erreur lors de l'insertion")
        raise
