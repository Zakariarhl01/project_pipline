"""
extract_api.py

Module d'extraction des données météorologiques historiques via l'API Open-Meteo.
Il gère la connexion HTTP, les requêtes et la gestion des erreurs de réseau.
"""

import requests
import logging
from datetime import datetime
from tabulate import tabulate 

logger = logging.getLogger(__name__)

def fetch_weather_open_meteo(base_url, params, timeout=30):
    """
    Exécute une requête GET vers l'API Open-Meteo pour obtenir les données météo.

    Args:
        base_url (str): URL de base de l'API.
        params (dict): Paramètres de la requête (latitude, longitude, hourly, etc.).
        timeout (int): Délai d'attente maximum de la requête en secondes.

    Returns:
        dict: Le corps de la réponse JSON de l'API.

    Raises:
        requests.RequestException: Si une erreur de connexion ou de statut HTTP survient.
    """
    try:
        r = requests.get(base_url, params=params, timeout=timeout)
        r.raise_for_status()
        logger.info(f"Météo récupérée — status {r.status_code}")
        return r.json()
    except requests.RequestException as e:
        logger.exception("Erreur lors de la récupération météo")
        raise


if __name__ == "__main__":
    BASE_URL = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": 48.8566, 
        "longitude": 2.3522,
        "hourly": "temperature_2m,relative_humidity_2m,pressure_msl,wind_speed_10m",
        "timezone": "Europe/Paris"
    }

    try:
        data = fetch_weather_open_meteo(BASE_URL, params)

        if "hourly" in data:
            times = data["hourly"]["time"]
            temps = data["hourly"]["temperature_2m"]
            winds = data["hourly"]["wind_speed_10m"]
            hums = data["hourly"]["relative_humidity_2m"]
            press = data["hourly"]["pressure_msl"]

            # Préparer le tableau
            table = []
            for t, temp, wind, hum, p in zip(times, temps, winds, hums, press):
                table.append([t, temp, wind, hum, p])

            headers = ["Heure", "Temp (°C)", "Vent (m/s)", "Humidité (%)", "Pression (hPa)"]
            print("\n--- Données horaires ---")
            print(tabulate(table, headers=headers, tablefmt="grid"))

            # Heure la plus proche de l'actuelle
            current_time = datetime.now().replace(minute=0, second=0, microsecond=0)
            closest_idx = min(
                range(len(times)),
                key=lambda i: abs(datetime.fromisoformat(times[i]) - current_time)
            )

            print("\n--- Données météo les plus proches de l'heure actuelle ---")
            print(tabulate(
                [[temps[closest_idx], winds[closest_idx], hums[closest_idx], press[closest_idx]]],
                headers=["Temp (°C)", "Vent (m/s)", "Humidité (%)", "Pression (hPa)"],
                tablefmt="grid"
            ))

    except Exception as e:
        print("Erreur :", e)
