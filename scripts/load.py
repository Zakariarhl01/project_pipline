import psycopg2
from psycopg2.extras import execute_values
import logging

logger = logging.getLogger(__name__)

def insert_measurements(conn_params, table, rows, batch_size=500):
    if not rows:
        logger.info("Aucune donnée à insérer.")
        return 0
    
    cols = list(rows[0].key())
    values = [[row[col] for col in cols] for row in rows]

    col_names = ",".join(f'"{c}"' for c in cols)
    sql = f'INSERT INTO {table} ({col_names}) VALUES %s'

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
