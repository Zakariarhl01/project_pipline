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
import operator

logger = logging.getLogger(__name__)

def deduplicate_and_merge_records(rows):
    """
    Effectue une déduplication et une fusion des enregistrements en mémoire
    basées sur la clé unique (turbine_id, date).

    Puisque le pipeline charge les données dans l'ordre (DB, CSV, API), 
    une simple déduplication de type "Last-One-Wins" (le dernier vu pour la clé gagne)
    est suffisante pour éviter l'erreur de PostgreSQL.
    
    L'UPSERT final dans la DB avec COALESCE gère la fusion des champs (non-NULL).

    Args:
        rows (list[dict]): Liste des enregistrements transformés.

    Returns:
        list[dict]: Liste des enregistrements dédupliqués.
    """
    # Utiliser un dictionnaire pour stocker les enregistrements uniques
    unique_records = {} 
    
    for row in rows:
        # Clé unique pour le dictionnaire : (turbine_id, date)
        # operator.itemgetter est utilisé pour gérer les dates qui sont des objets datetime.
        key = (row.get('turbine_id'), row.get('date'))
        
        # Last-One-Wins : l'enregistrement le plus récent (en fonction de l'ordre 
        # d'insertion dans 'rows') pour la clé gagne.
        # Cela évite les doublons stricts dans le lot, ce qui est le but.
        unique_records[key] = row 
        
    # Retourner la liste des valeurs du dictionnaire.
    return list(unique_records.values())


def insert_measurements(conn_params, table, rows, batch_size=500):
    """
    Insère ou met à jour (UPSERT) les enregistrements dans la table cible 'consolidated_measurements'.
    """
    
    # 1. Déduplication : Étape cruciale pour éviter l'erreur "ON CONFLICT cannot affect row a second time"
    values_to_insert = deduplicate_and_merge_records(rows)
    
    if not values_to_insert:
        logger.warning("Aucun enregistrement après déduplication. Rien à insérer.")
        return 0
    
    # 2. Préparation des données pour execute_values (liste de tuples)
    
    # On récupère l'ordre des colonnes depuis l'un des enregistrements (ils doivent tous avoir les mêmes clés)
    # L'ordre doit correspondre à celui utilisé dans la requête SQL.
    cols = list(values_to_insert[0].keys()) 
    col_names = ",".join(f'"{c}"' for c in cols)

    # Conversion de la liste de dicts en liste de tuples ordonnés
    values = [tuple(row[c] for c in cols) for row in values_to_insert]
    
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
                # Exécution par lots
                for i in range(0, len(values), batch_size):
                    batch = values[i:i+batch_size]
                    
                    # execute_values retourne le nombre de lignes traitées (insérées/mises à jour)
                    execute_values(cur, sql, batch)
                    inserted += len(batch)
                
                # Le commit est automatique grâce au 'with psycopg2.connect'
                
        return inserted

    except psycopg2.Error as e:
        logger.error(f"Erreur lors de l'insertion")
        # Renvoyer l'erreur pour la gestion FATALE dans le main_pipeline
        raise e 

    except Exception as e:
        logger.exception("Erreur inattendue lors du chargement DB")
        # Renvoyer l'erreur pour la gestion FATALE dans le main_pipeline
        raise e