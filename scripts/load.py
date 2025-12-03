"""
load.py

Module de chargement (Load) des données dans PostgreSQL.
Il utilise la fonction execute_values de psycopg2 pour l'insertion par lots.

Le point clé est l'opération d'UPSERT (INSERT ON CONFLICT DO UPDATE) qui assure 
la fusion des données hétérogènes (CSV, Capteurs, Météo) sans écrasement par des valeurs NULL, 
grâce à la fonction COALESCE.
"""

import psycopg2
from psycopg2.extras import execute_values
import logging

logger = logging.getLogger(__name__)

def insert_measurements(conn_params, table, rows, batch_size=500):
    """
    Insère ou met à jour (UPSERT) les enregistrements dans la table cible 'consolidated_measurements'.

    Ce processus est idempotent, garantissant qu'un enregistrement n'existe qu'une seule fois
    pour une combinaison donnée de (turbine_id, date).

    1. Déduplication : Une **déduplication en mémoire** est effectuée sur la clé (turbine_id, date) 
       avant l'insertion afin de garantir la propreté du lot.
    2. Fusion : La clause `ON CONFLICT DO UPDATE SET` utilise la fonction **COALESCE** pour fusionner les données. Si une colonne de l'enregistrement entrant (`EXCLUDED`) est NULL, 
       la valeur existante dans la table (`consolidated_measurements.col`) est conservée, 
       évitant ainsi l'écrasement des informations partielles.
    3. Performance : L'insertion est réalisée par lots (`execute_values`) pour optimiser la 
       performance de la transaction.

    Args:
        conn_params (dict): Paramètres de connexion PostgreSQL.
        table (str): Nom de la table cible ('consolidated_measurements').
        rows (list[dict]): Liste des enregistrements à insérer/mettre à jour.
        batch_size (int): Taille maximale du lot pour l'insertion par execute_values (par défaut 500).

    Returns:
        int: Le nombre total de lignes insérées ou mises à jour.
    
    Raises:
        psycopg2.Error: En cas d'erreur de base de données.
    """
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
