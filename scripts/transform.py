"""
transform.py

Module de transformation des données. Il centralise :
1. Les conversions d'unités (ex: Celsius vers Kelvin, km/h vers m/s).
2. Le parsing et la standardisation des formats de date/heure.
3. La normalisation des structures de données pour chaque source (CSV, DB, API).
4. Le contrôle qualité et le nettoyage des anomalies (outliers).
"""
import logging
from datetime import datetime
import pytz
import math
import pandas as pd
from dateutil.parser import parse 

logger = logging.getLogger(__name__)
PARIS = pytz.timezone("Europe/Paris")

# Liste de toutes les colonnes requises dans la table cible (pour garantir l'ordre et la présence)
FULL_SCHEMA_KEYS = [
    'turbine_id', 'date', 
    'temperature_k', 'wind_ms', 'vibration_mm_s', 'consumption_kwh', 
    'energie_kwh', 'arret_planifie', 'arret_non_planifie', 
    'source'
]

# --- CONVERSIONS ---
def celsius_to_kelvin(c):
    if c is None: return None
    try:
        return round(float(c) + 273.15, 2)
    except (TypeError, ValueError):
        return None

def kmh_to_ms(k):
    if k is None: return None
    try:
        return round(float(k) / 3.6, 2)
    except (TypeError, ValueError):
        return None

def safe_bool_int(val):
    """Convertit de manière sûre une valeur (float/str/nan) en booléen."""
    try:
        if pd.isna(val) or val == "":
            return False
        # On convertit d'abord en float pour gérer le cas "1.0" ou 1.0, puis en int
        # Si la valeur est '0' ou 0.0, cela donnera False, ce qui est correct pour un booléen.
        return bool(int(float(val)))
    except Exception:
        return False

# --- DATE PARSING ---
def parse_date(date_obj):
    if not date_obj:
        return None
    
    # Si c'est déjà un datetime (cas DB)
    if isinstance(date_obj, datetime):
        # S'assurer qu'il est bien en UTC (sans TimeZone) pour la DB
        if date_obj.tzinfo is not None:
            return date_obj.astimezone(pytz.utc).replace(tzinfo=None)
        return date_obj
    
    # Si c'est une chaîne de caractères (cas CSV et API)
    try:
        # Utilisation de dateutil.parser.parse pour gérer les formats variés (CSV, API)
        dt_aware = parse(str(date_obj))
        
        # Si la date n'a pas de timezone, on suppose que c'est l'heure locale (Paris) pour le CSV
        if dt_aware.tzinfo is None:
            dt_aware = PARIS.localize(dt_aware)
            
        # Conversion en UTC et suppression de la timezone pour la DB (TIMESTAMP WITHOUT TIME ZONE)
        return dt_aware.astimezone(pytz.utc).replace(tzinfo=None)
    except Exception as e:
        logger.error(f"Erreur de parsing de date pour '{date_obj}': {e}")
        return None


# --- TRANSFORMATION PAR SOURCE ---

def transform_sensor_rows(rows):
    """
    Transforme les lignes brutes de capteurs (DB).
    Les colonnes sont déjà partiellement renommées dans la requête SQL (ts_utc -> date, wind_speed_mps -> wind_ms).
    """
    # CORRECTION PRÉCÉDENTE : 'rows' est une liste, pas un DataFrame
    if not rows:
        logger.warning("Aucune ligne de capteur à transformer.")
        return []

    # Les données de la DB sont déjà propres et les dates sont des objets datetime non-tz.
    transformed = []
    for r in rows:
        transformed.append({
            'turbine_id': r.get('turbine_id'),
            'date': r.get('date'),
            'temperature_k': r.get('temperature_k'),
            'wind_ms': r.get('wind_ms'),
            'vibration_mm_s': r.get('vibration_mm_s'),
            'consumption_kwh': r.get('consumption_kwh'),
            # Les champs du CSV/API sont NULL ici pour l'UPSERT
            'energie_kwh': None,
            'arret_planifie': None,
            'arret_non_planifie': None,
            'source': 'db_sensor'
        })
    logger.info(f"{len(transformed)} lignes de capteurs transformées.")
    return transformed

def transform_production_rows(rows):
    """
    Transforme les lignes brutes de production (CSV).
    Effectue le nettoyage, la conversion de date, et la standardisation des booléens.
    """
    # CORRECTION APPLIQUÉE : On vérifie si la liste est vide, pas un attribut .empty de DataFrame.
    if not rows: 
        logger.warning("Aucune ligne de production à transformer.")
        return []

    transformed = []
    for r in rows:
        # Renommage et conversion de type
        tid = str(r.get('turbin_id')).upper() # turbin_id -> turbine_id (avec correction typo et standardisation)
        
        transformed.append({
            'turbine_id': tid,
            'date': parse_date(r.get('date')), # Le CSV a 'date', mais le type est string
            # Les champs du capteur/API sont NULL ici pour l'UPSERT
            'temperature_k': None,
            'wind_ms': None,
            'vibration_mm_s': None,
            'consumption_kwh': None,
            
            # Champs spécifiques au CSV
            # Assurer que la valeur est numérique (le CSV a des valeurs manquantes/vides)
            'energie_kwh': float(r.get('energie_kWh')) if r.get('energie_kWh') is not None and str(r.get('energie_kWh')).strip() != '' else None,
            'arret_planifie': safe_bool_int(r.get('arret_planifie')),
            'arret_non_planifie': safe_bool_int(r.get('arret_non_planifie')),
            'source': 'csv_production'
        })
    logger.info(f"{len(transformed)} lignes de production transformées.")
    return transformed


def transform_api_rows(data, turbine_ids, source_name):
    """
    Transforme les données API, les convertit en unités standardisées (Kelvin, m/s)
    et les duplique pour chaque turbine active.
    """
    if not data or 'hourly' not in data or not turbine_ids:
        if not turbine_ids:
            logger.warning("Pas de turbines actives pour appliquer les données météo.")
        return []

    hourly = data['hourly']
    transformed_rows = []

    # Le 'time' de l'API est en format ISO 8601 (ex: "2025-12-10T12:00")
    
    # On itère sur les timestamps disponibles
    for i in range(len(hourly['time'])):
        date_str = hourly['time'][i]
        
        # 1. Préparation des valeurs météorologiques
        # Les données sont en °C et km/h, on les convertit en K et m/s
        temp_k = celsius_to_kelvin(hourly['temperature_2m'][i])
        wind_ms = kmh_to_ms(hourly['windspeed_10m'][i])

        # 2. Duplication des données pour chaque turbine
        for tid in turbine_ids:
            transformed_rows.append({
                'turbine_id': tid,
                'date': parse_date(date_str),
                'temperature_k': temp_k,
                'wind_ms': wind_ms,
                # Les autres champs sont NULL ici pour l'UPSERT
                'vibration_mm_s': None,
                'consumption_kwh': None,
                'energie_kwh': None,
                'arret_planifie': None, 
                'arret_non_planifie': None,
                'source': source_name
            })
    logger.info(f"Météo dupliquée pour {len(turbine_ids)} turbines : {len(transformed_rows)} lignes.")
    return transformed_rows

# --- SCHÉMA ET QUALITÉ ---

def enforce_schema(records):
    """
    Garantit que tous les enregistrements ont la même structure de clés (FULL_SCHEMA_KEYS)
    et que les clés manquantes sont initialisées à None.
    """
    standardized = []
    for r in records:
        new_row = {k: r.get(k, None) for k in FULL_SCHEMA_KEYS}
        standardized.append(new_row)
    return standardized


def quality_check(records):
    """
    Applique des règles de qualité sur l'ensemble des enregistrements consolidés.
    """
    anomalies = 0
    
    for r in records:
        # Filtre Vent (Max ~42 m/s pour 150 km/h)
        wind_ms = r.get("wind_ms")
        if wind_ms is not None:
            if wind_ms < 0 or wind_ms > 42: 
                r["wind_ms"] = None 
                anomalies += 1
        
        # Filtre Température (Kelvin réaliste entre 200 et 330)
        temp_k = r.get("temperature_k")
        if temp_k is not None:
            if temp_k < 200 or temp_k > 330:
                r["temperature_k"] = None
                anomalies += 1

        # Filtre Vibration (Max 25 mm/s)
        vibration = r.get("vibration_mm_s")
        if vibration is not None:
            if vibration < 0 or vibration > 25:
                r["vibration_mm_s"] = None
                anomalies += 1
                
        # Filtre Énergie et Consommation (ne doit pas être négative)
        energie = r.get("energie_kwh")
        if energie is not None and energie < 0:
            r["energie_kwh"] = None
            anomalies += 1
            
        consumption = r.get("consumption_kwh")
        if consumption is not None and consumption < 0:
            r["consumption_kwh"] = None
            anomalies += 1
                
    return records, anomalies