"""
main_pipeline.py

Orchestrateur principal du pipeline ETL (Extract, Transform, Load) pour les données EnergiTech.
Ce script coordonne toutes les étapes du flux de travail :

1. Configuration et initialisation du logging.
2. Extraction des données (CSV de production, PostgreSQL des capteurs, API Météo).
3. Transformation et application des règles de qualité (conversion d'unités, nettoyage).
4. Chargement des données dans la table cible 'consolidated_measurements' via l'opération UPSERT.
5. Génération et affichage d'un rapport de synthèse.

CORRECTION : Correction de l'appel à transform_sensor_rows et transform_production_rows 
             pour correspondre à la signature qui ne prend qu'un seul argument.
"""

# -----------------------
# IMPORTS
# -----------------------
import logging
import logging.handlers 
from pathlib import Path
from datetime import datetime
import sys 
import shutil

import yaml
import pandas as pd 

from extract_csv import find_latest_csv, read_production_csv
from extract_db import fetch_last_24h
from extract_api import fetch_weather_open_meteo
from transform import (
    transform_production_rows,
    transform_sensor_rows,
    transform_api_rows,
    enforce_schema, 
    quality_check 
)
from load import insert_measurements


# -----------------------
# CHARGER CONFIGURATION
# -----------------------
def load_config(path="config.yaml"):
    """
    Charge le fichier de configuration YAML à partir du chemin spécifié.
    """
    try:
        config_path = path if Path(path).exists() else Path(__file__).parent.parent / path
        with open(config_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    except Exception as e:
        # Erreur non-gérée, on la propage au bloc try/except de l'appelant
        raise Exception(f"Erreur lors du chargement de la configuration: {e}")


# -----------------------
# CONFIGURATION DU LOGGING
# -----------------------
def setup_logging(cfg):
    """
    Configure le système de logging pour écrire à la fois dans la console (stdout)
    et dans un seul fichier 'pipeline.log' (en mode append).
    """
    
    LOG_FILE_NAME = "pipeline.log"
    logs_dir = cfg["paths"]["logs_dir"]
    log_path = Path(logs_dir) / LOG_FILE_NAME
    
    # Assurer que le répertoire des logs existe
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    # 1. Configuration de base pour la console (stdout)
    # Configure les loggers existants pour stdout (niveau INFO)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        stream=sys.stdout
    )
    
    # 2. Gestionnaire de fichiers pour pipeline.log
    # Utilisation de FileHandler pour écrire en continu dans le même fichier
    # mode='a' est essentiel pour ajouter au fichier existant
    file_handler = logging.FileHandler(log_path, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    
    # Définir le format pour le fichier
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    
    # S'assurer que le gestionnaire de fichiers est ajouté au logger racine
    root_logger = logging.getLogger()
    
    # S'assurer qu'aucun autre gestionnaire de fichiers n'est en place (pour éviter les doublons)
    for handler in root_logger.handlers[:]:
        if isinstance(handler, (logging.FileHandler, logging.handlers.RotatingFileHandler, logging.handlers.TimedRotatingFileHandler)):
            root_logger.removeHandler(handler)
            
    root_logger.addHandler(file_handler)
    
    # Retourner le logger pour main_pipeline
    return logging.getLogger(__name__)


# ----------------------
# FONCTION PRINCIPALE RUN
# ----------------------
def run():
    # 1) Configuration et Initialisation
    try:
        cfg = load_config()
    except Exception as e:
        print(f"\nFATAL ERROR: Impossible de charger la configuration. Cause : {e}", file=sys.stderr)
        sys.exit(1)
        
    # Configuration du logging (Utilise la nouvelle fonction)
    logger = setup_logging(cfg) 
    
    logger.info("Début de l'exécution du pipeline ETL EnergiTech")

    start_time = datetime.now()
    summary = {
        'start_time': start_time.strftime('%Y-%m-%d %H:%M:%S'),
        'status': 'RUNNING',
        'sources_extraites': {},
        'lignes_chargees': 0
    }

    # Créer le répertoire temporaire si nécessaire (pour les fichiers intermédiaires)
    tmp_dir = Path(cfg["paths"]["tmp_dir"])
    tmp_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Dossier temporaire créé/vérifié: {tmp_dir}")
    
    # 2) Extraction
    # 2.1) Extraction DB (Capteurs)
    try:
        logger.info("--- ÉTAPE 2.1 : Extraction des données capteurs (DB) ---")
        sensor_rows = fetch_last_24h(cfg["postgres"], cfg["pipeline"]["lookback_minutes"])
        summary['sources_extraites']['db_sensor'] = len(sensor_rows)
        logger.info(f"{len(sensor_rows)} lignes brutes extraites de la DB.")
    except Exception as e:
        logger.error(f"Échec de l'extraction DB : {e}")
        sensor_rows = []

    # 2.2) Extraction CSV (Production)
    try:
        logger.info("--- ÉTAPE 2.2 : Extraction des données production (CSV) ---")
        csv_path = find_latest_csv(cfg["paths"]["csv_input_dir"], pattern_prefix='production_')
        production_df = read_production_csv(csv_path)
        production_rows = production_df.to_dict('records') # Convertir en liste de dict
        summary['sources_extraites']['csv_production'] = len(production_rows)
        logger.info(f"{len(production_rows)} lignes brutes extraites du CSV.")
    except Exception as e:
        logger.warning(f"Échec de l'extraction CSV (Production): {e}. Poursuite sans données CSV.")
        production_rows = []

    # 2.3) Extraction API (Météo)
    try:
        logger.info("--- ÉTAPE 2.3 : Extraction des données météo (API) ---")
        weather_data = fetch_weather_open_meteo(cfg['api']['weather']['base_url'], cfg['api']['weather']['params'])
        # Le nombre de lignes extraites sera calculé lors de la transformation
        logger.info(f"Données météo récupérées.")
    except Exception as e:
        logger.warning(f"Échec de l'extraction API (Météo): {e}. Poursuite sans données Météo.")
        weather_data = {}

    # 3) Transformation
    logger.info("--- ÉTAPE 3 : Transformation des données brutes ---")
    
    # 3.1) Transformation
    # CORRECTION ICI : RETRAIT DE L'ARGUMENT 'db_sensor'
    transformed_sensor = transform_sensor_rows(sensor_rows)
    # CORRECTION ICI : RETRAIT DE L'ARGUMENT 'csv_production'
    transformed_production = transform_production_rows(production_rows)
    
    # Liste des turbines actives (pour dupliquer les données météo)
    active_turbines = set(r['turbine_id'] for r in transformed_sensor + transformed_production)
    if not active_turbines:
        logger.warning("Aucune turbine active trouvée dans les données capteurs/production pour l'application météo.")
    
    # L'API a besoin de la liste des turbines pour dupliquer les enregistrements météo
    transformed_api = transform_api_rows(weather_data, list(active_turbines), 'api_weather')
    
    summary['lignes_transformees'] = len(transformed_sensor) + len(transformed_production) + len(transformed_api)
    logger.info(f"{summary['lignes_transformees']} lignes transformées avant consolidation.")

    # 3.2) Consolidation et Vérification
    consolidated_records = transformed_sensor + transformed_production + transformed_api
    
    # Enforce schema pour garantir l'ordre et la complétude
    consolidated_records = enforce_schema(consolidated_records)

    # Application des règles de qualité
    cleaned_records, anomalies_count = quality_check(consolidated_records)
    summary['anomalies_corrigees'] = anomalies_count
    logger.info(f"{anomalies_count} anomalies corrigées par les règles de qualité.")
    logger.info(f"Total de {len(cleaned_records)} enregistrements prêts à être chargés après nettoyage.")

    
    # 4) Chargement
    logger.info("--- ÉTAPE 4 : Chargement des données (UPSERT) ---")
    inserted_count = 0
    
    try:
        inserted_count = insert_measurements(
            cfg["postgres"],
            "consolidated_measurements",
            cleaned_records,
            batch_size=cfg.get("pipeline", {}).get("batch_size", 500)
        )
        summary['status'] = 'COMPLETED_SUCCESS'
        summary['lignes_chargees'] = inserted_count
        logger.info(f"Chargement terminé. {inserted_count} enregistrements insérés/mis à jour.")

    except Exception as e:
        logger.error(f"Erreur FATALE lors du chargement dans la DB : {e}")
        summary['status'] = 'COMPLETED_FAILURE'
        summary['erreur_chargement'] = str(e)
        
    finally:
    # 5) Rapport et Nettoyage
        
        # Mise à jour du temps de fin
        summary['end_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        duration = datetime.now() - start_time
        summary['duration_seconds'] = round(duration.total_seconds(), 2)

        print("\n=== Pipeline terminé ===")
        print(f"Rapport d'exécution:\n{yaml.dump(summary, indent=2)}")
        
        # Log du rapport final
        logger.info(f"Rapport d'exécution final : {summary}")
        
        # 6) Nettoyage des fichiers temporaires (Sécurité)
        try:
            # Suppression récursive et sécurisée du répertoire temporaire
            shutil.rmtree(cfg["paths"]["tmp_dir"])
            logger.info(f"Nettoyage sécurisé du dossier temporaire: {cfg['paths']['tmp_dir']}")
        except OSError as e:
            # Utilisation de warning, car c'est un problème non fatal pour le chargement
            logger.warning(f"Impossible de supprimer le dossier temporaire {cfg['paths']['tmp_dir']}: {e}")

# ----------------------
# POINT D’ENTRÉE
# ----------------------
if __name__ == "__main__":
    # Démarre la pipeline et attrape les erreurs fatales (si la config n'est même pas trouvable)
    try:
        print("=== Lancement du pipeline complet ===")
        run()
    except Exception as e:
        # Affichage direct dans le terminal en cas d'échec critique (ex: config non trouvée)
        print(f"\nFATAL ERROR: Le pipeline a échoué. Cause : {e}", file=sys.stderr)
        sys.exit(1)