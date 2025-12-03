"""
main_pipeline.py

Orchestrateur principal du pipeline ETL (Extract, Transform, Load) pour les données EnergiTech.
Ce script coordonne toutes les étapes du flux de travail :

1. Configuration et initialisation du logging.
2. Extraction des données (CSV de production, PostgreSQL des capteurs, API Météo).
3. Transformation et application des règles de qualité (conversion d'unités, nettoyage).
4. Chargement des données dans la table cible 'consolidated_measurements' via l'opération UPSERT.
5. Génération et affichage d'un rapport de synthèse.
"""

# -----------------------
# IMPORTS
# -----------------------
import logging
import logging.handlers # Pour la gestion de la rotation des fichiers de log
from pathlib import Path
from datetime import datetime
import sys # Pour les logs dans la console
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
    quality_check 
)
from load import insert_measurements


# -----------------------
# CHARGER CONFIGURATION
# -----------------------
def load_config(path="config.yaml"):
    """
    Charge le fichier de configuration YAML à partir du chemin spécifié.

    Args:
        path (str): Chemin d'accès au fichier config.yaml.

    Returns:
        dict: Le contenu du fichier de configuration.

    Raises:
        FileNotFoundError: Si le fichier de configuration est introuvable.
        ValueError: Si le fichier YAML est vide ou mal formé.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
            if not cfg:
                raise ValueError("Fichier YAML vide")
            return cfg
    except FileNotFoundError:
        raise FileNotFoundError(f"Fichier de configuration {path} introuvable")
    except yaml.YAMLError as e:
        raise ValueError(f"Erreur lors de la lecture du YAML : {e}")


# -----------------------
# CONFIGURATION LOGGING (CORRIGÉE : Console + Fichier)
# -----------------------
def setup_logging(cfg):
    """Configure le logging pour écrire dans un fichier (avec rotation) et la console."""
    log_dir = Path(cfg["paths"]["logs_dir"])
    log_dir.mkdir(parents=True, exist_ok=True) # S'assure que le dossier 'logs' existe
    log_file = log_dir / "pipeline.log"
    
    logger = logging.getLogger() # Récupère le root logger (affecte tous les modules)
    logger.setLevel(logging.INFO)
    
    # Formatteur
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # Gestionnaire 1: Fichier (avec rotation)
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=10*1024*1024, # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    
    # Gestionnaire 2: Console (terminal)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    
    # Ajout des handlers (pour s'assurer qu'ils ne sont pas ajoutés plusieurs fois)
    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
    return logging.getLogger("pipeline")


# -----------------------
# CRÉATION DES DOSSIERS
# -----------------------
def ensure_dirs(cfg):
    """Crée les dossiers temporaires, de logs et de données s'ils n'existent pas."""
    logger = logging.getLogger("pipeline")
    paths = cfg.get("paths")

    for key, directory in paths.items():
        if key.endswith("dir"):
            Path(directory).mkdir(parents=True, exist_ok=True)
            logger.info(f"Dossier créé ou déjà existant : {directory}")


# -----------------------
# PIPELINE COMPLET
# -----------------------
def run():
    """
    Exécute le pipeline ETL complet de A à Z.

    Coordonne les appels successifs aux modules d'extraction, de transformation 
    et de chargement, gérant le flux de données entre chaque étape.
    """
    
    # 0) Charger config
    cfg = load_config()
    
    # 1) Configurer le logging et créer les dossiers
    logger = setup_logging(cfg) 
    
    # NOTE : L'affichage initial de la bannière est conservé en print pour l'esthétique du terminal
    print("=== Lancement du pipeline complet ===")
    logger.info("Démarrage du pipeline ETL.") 

    # S'assurer que les répertoires existent (doit se faire APRES la config logging)
    ensure_dirs(cfg) 
    
    active_turbines = set()

    # 2) Extraction CSV (Production)
    logger.info("-> Extraction du CSV (Production)")
    prod_rows = pd.DataFrame()
    prod_clean = []
    try:
        csv_path = find_latest_csv(cfg["paths"]["csv_input_dir"])
        logger.info(f"Fichier CSV trouvé : {csv_path}")
        prod_rows = read_production_csv(csv_path)
        prod_clean = transform_production_rows(prod_rows.to_dict(orient="records"))
        for p in prod_clean:
            if p.get("turbine_id"): active_turbines.add(p["turbine_id"])
    except FileNotFoundError as e:
        logger.warning(f"Fichier CSV non trouvé, extraction ignorée. {e}")
    except Exception:
        logger.exception("Erreur inattendue lors de l'extraction CSV.")


    # 3) Extraction DB (Capteurs)
    logger.info("-> Extraction des capteurs depuis DB")
    sensor_rows = fetch_last_24h(cfg["postgres"], cfg["pipeline"].get("lookback_minutes", 1440))
    sensor_clean = transform_sensor_rows(sensor_rows) if sensor_rows else []
    for s in sensor_clean:
        if s.get("turbine_id"): active_turbines.add(s["turbine_id"])
    
    # Utilisation des turbines trouvées, ou par défaut si rien n'est trouvé.
    active_turbines_list = list(active_turbines) if active_turbines else ["T001", "T002"]
    logger.info(f"Turbines actives détectées : {', '.join(active_turbines_list)}")


    # 4) Extraction API météo
    logger.info("-> Extraction météo depuis l'API")
    weather_clean = []
    try:
        weather_json = fetch_weather_open_meteo(
            cfg["api"]["weather"]["base_url"],
            cfg["api"]["weather"]["params"]
        )
        weather_clean = transform_api_rows(weather_json, active_turbines_list) 
    except Exception:
        logger.exception("Erreur lors de l'extraction météo. Les données météo seront manquantes.")


    # 5) Transformation et Qualité
    logger.info("-> Transformation et contrôle qualité des données")

    # Combinaison de toutes les sources
    combined = prod_clean + sensor_clean + weather_clean
    logger.info(f"Total lignes brutes combinées : {len(combined)}")

    # CORRECTION DE L'ERREUR : nb_anomalies reçoit directement l'entier
    if combined:
        cleaned_data, nb_anomalies = quality_check(combined) 
    else:
        cleaned_data, nb_anomalies = [], 0
        
    logger.info(f"Contrôle qualité terminé. Anomalies détectées/nettoyées : {nb_anomalies}")

    # 6) Chargement en PostgreSQL
    logger.info("-> Chargement en base PostgreSQL")
    inserted = insert_measurements(cfg["postgres"], "consolidated_measurements", cleaned_data)

    # 7) Résumé
    summary = {
        "timestamp": datetime.now().isoformat(),
        "turbines_actives": len(active_turbines_list),
        "csv_rows_lues": len(prod_rows) if prod_rows is not None else 0,
        "sensor_rows_lus": len(sensor_rows),
        "weather_rows_creees": len(weather_clean),
        "inserted_rows": inserted,
        "anomalies_corrigees": nb_anomalies
    }

    # Affichage du rapport final dans la console
    print("\n--- Pipeline terminé ---")
    print(f"Rapport d'exécution:\n{yaml.dump(summary, indent=2)}")
    
    # Log du rapport final
    logger.info(f"Rapport d'exécution final : {summary}")

# Affichage du rapport final dans la console
    print("\n--- Pipeline terminé ---")
    print(f"Rapport d'exécution:\n{yaml.dump(summary, indent=2)}")
    
    # Log du rapport final
    logger.info(f"Rapport d'exécution final : {summary}")
    
    # 8) Nettoyage des fichiers temporaires (Sécurité)
    try:
        # Suppression récursive et sécurisée du répertoire temporaire
        shutil.rmtree(cfg["paths"]["tmp_dir"])
        logger.info(f"Nettoyage sécurisé du dossier temporaire: {cfg['paths']['tmp_dir']}")
    except OSError as e:
        # Utilisation de warning, car c'est un problème non fatal pour le chargement
        logger.warning(f"Impossible de supprimer le dossier temporaire {cfg['paths']['tmp_dir']}: {e}")
# -----------------------
# POINT D’ENTRÉE
# -----------------------
if __name__ == "__main__":
    # Démarre la pipeline et attrape les erreurs fatales avant la configuration du logger
    try:
        run()
    except Exception as e:
        # Affichage direct dans le terminal en cas d'échec critique (ex: config non trouvée)
        print(f"\nFATAL ERROR: Le pipeline a échoué. Cause : {e}", file=sys.stderr)
        # Tente de logger l'exception au cas où le logger aurait été initialisé
        logging.exception("Erreur FATALE (hors pipeline) survenue.")