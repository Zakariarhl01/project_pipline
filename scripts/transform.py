"""
transform.py — Transformations CSV, DB, API et qualité
"""
import logging
from datetime import datetime
import pytz
import math
import pandas as pd # Nécessaire pour vérifier pd.isna

logger = logging.getLogger(__name__)
PARIS = pytz.timezone("Europe/Paris")

# --- CONVERSIONS ---
def celsius_to_kelvin(c):
    if c is None: return None
    return round(c + 273.15, 2)

def kmh_to_ms(k):
    if k is None: return None
    return round(k / 3.6, 2)

def safe_bool_int(val):
    """Convertit de manière sûre une valeur (float/str/nan) en booléen."""
    try:
        if pd.isna(val) or val == "":
            return False
        # On convertit d'abord en float pour gérer le cas "1.0" ou 1.0, puis en int
        return bool(int(float(val)))
    except Exception:
        return False

# --- DATE PARSING ---
def parse_date(date_obj):
    if not date_obj:
        return None
    
    # Si c'est déjà un datetime (cas DB)
    if isinstance(date_obj, datetime):
        dt = date_obj
    else:
        # Cas CSV/API (string)
        dt = None
        formats = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d", "%Y-%m-%dT%H:%M"]
        for f in formats:
            try:
                dt = datetime.strptime(str(date_obj), f)
                break
            except ValueError:
                continue
        
        # Essai ISO format direct
        if not dt:
            try:
                dt = datetime.fromisoformat(str(date_obj))
            except:
                return None

    # Ajout Timezone si manquant
    if dt.tzinfo is None:
        dt = PARIS.localize(dt)
    
    return dt.isoformat()

# --- TRANSFORMATIONS ---

def transform_production_rows(rows):
    cleaned = []
    for r in rows:
        rec = {
            "date": parse_date(r.get("date")),
            "turbine_id": r.get("turbin_id") or r.get("turbine_id"), 
            "energie_kwh": float(r["energie_kWh"]) if r.get("energie_kWh") not in (None, "") else None,
            # Utilisation de la fonction safe_bool_int corrigée
            "arret_planifie": safe_bool_int(r.get("arret_planifie")),
            "arret_non_planifie": safe_bool_int(r.get("arret_non_planifie")),
            "temperature_k": None,
            "wind_ms": None,
            "source": "production_csv"
        }
        cleaned.append(rec)
    return cleaned

def transform_sensor_rows(rows):
    cleaned = []
    for r in rows:
        # On essaie de récupérer la date depuis 'date' ou 'timestamp' selon ce que la DB renvoie
        raw_date = r.get("date") or r.get("timestamp")
        
        rec = {
            "date": parse_date(raw_date),
            "turbine_id": r.get("turbine_id"),
            "temperature_k": None, 
            "wind_ms": float(r.get("value")) if r.get("value") else None, # Hypothèse: capteur vent
            "energie_kwh": None,
            "arret_planifie": False,
            "arret_non_planifie": False,
            "source": "sensor_db"
        }
        cleaned.append(rec)
    return cleaned

def transform_api_rows(api_data, active_turbines):
    if not api_data or "hourly" not in api_data:
        return []
    
    hourly = api_data["hourly"]
    times = hourly.get("time", [])
    temps = hourly.get("temperature_2m", []) 
    winds = hourly.get("wind_speed_10m", []) 

    cleaned = []
    
    if not active_turbines:
        logger.warning("Aucune turbine active fournie pour associer la météo.")
        return []

    for i, t in enumerate(times):
        iso_date = parse_date(t)
        t_kelvin = celsius_to_kelvin(temps[i]) if i < len(temps) else None
        w_ms = kmh_to_ms(winds[i]) if i < len(winds) else None

        for turbine in active_turbines:
            rec = {
                "date": iso_date,
                "turbine_id": turbine,
                "temperature_k": t_kelvin,
                "wind_ms": w_ms,
                "energie_kwh": None,
                "arret_planifie": False,
                "arret_non_planifie": False,
                "source": "api_weather"
            }
            cleaned.append(rec)

    logger.info(f"Météo dupliquée pour {len(active_turbines)} turbines : {len(cleaned)} lignes.")
    return cleaned

# --- QUALITÉ ---
def quality_check(records):
    valid_records = []
    anomalies = 0
    
    for r in records:
        is_valid = True
        
        # Filtre Vent (Max 150 km/h = ~42 m/s)
        if r["wind_ms"] is not None:
            if r["wind_ms"] < 0 or r["wind_ms"] > 50: 
                r["wind_ms"] = None 
                anomalies += 1
        
        # Filtre Température (Kelvin réaliste entre 200 et 330)
        if r["temperature_k"] is not None:
            if r["temperature_k"] < 200 or r["temperature_k"] > 330:
                r["temperature_k"] = None
                anomalies += 1

        if is_valid:
            valid_records.append(r)
            
    logger.info(f"Contrôle qualité terminé. Anomalies détectées/nettoyées : {anomalies}")
    return valid_records, anomalies