#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Génère un fichier CSV (séparateur ';') de production journalière par turbine pour un mois donné.
Entrées : année (AAAA) et mois (MM).
Sortie  : production_YYYY_MM.csv dans le dossier courant.
Exemple pour lancer le script : python nom-fichier.py 2025 10

Spécifications colonnes :
date;turbin_id;energie_kWh;arret_planifie;arret_non_planifie

- turbin_id ∈ {T001, T002}
- energie_kWh : valeur réaliste (aléatoire) de production journalière
- arret_planifie, arret_non_planifie : 0/1 (mutuellement exclusifs)
- Certaines lignes peuvent contenir des valeurs manquantes (champs vides)
"""

import argparse
import csv
import calendar
from datetime import date
from pathlib import Path
import random
from typing import Tuple

# Capacités nominales (MW) par turbine — valeurs plausibles modernes
RATED_MW = {
    "T001": 3.2,  # 3.2 MW
    "T002": 2.8,  # 2.8 MW
}

# Moyennes de facteur de charge par mois (hémisphère Nord, ordre 1..12)
# Valeurs indicatives : plus venté en hiver/automne, plus faible en été
MONTHLY_CF_MEAN = {
    1: 0.42, 2: 0.40, 3: 0.38, 4: 0.35,
    5: 0.30, 6: 0.28, 7: 0.25, 8: 0.27,
    9: 0.32, 10: 0.36, 11: 0.40, 12: 0.43,
}

def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def daily_capacity_factor(month: int) -> float:
    """
    Tire un facteur de charge journalier autour d'une moyenne mensuelle,
    avec de la variabilité quotidienne.
    """
    mean = MONTHLY_CF_MEAN.get(month, 0.33)
    # Écart-type raisonnable pour la variabilité journalière du vent
    cf = random.gauss(mu=mean, sigma=0.10)
    return clamp(cf, 0.0, 1.0)

def simulate_outages() -> Tuple[int, int]:
    """
    Simule les arrêts planifiés / non planifiés (mutuellement exclusifs).
    Probabilités journalières plausibles (à ajuster selon contexte réel) :
    - Arrêt planifié : ~2%/jour
    - Arrêt non planifié : ~1%/jour
    """
    planned = 1 if random.random() < 0.02 else 0
    if planned == 1:
        return 1, 0
    unplanned = 1 if random.random() < 0.01 else 0
    return 0, unplanned

def maybe_make_missing(row: dict) -> dict:
    """
    Introduit occasionnellement des valeurs manquantes.
    - 5% des lignes : energie_kWh manquante
    - 2% des lignes : un des deux drapeaux manquant (ou les deux 1% chacun)
    """
    r = random.random()
    if r < 0.05:
        row["energie_kWh"] = ""  # valeur manquante
    # Flags manquants de façon rare et indépendante
    if random.random() < 0.02:
        row["arret_planifie"] = ""
    if random.random() < 0.02:
        row["arret_non_planifie"] = ""
    return row

def compute_daily_energy_kwh(turbine: str, y: int, m: int, planned: int, unplanned: int) -> int:
    """
    Calcule l'énergie journalière (kWh) en fonction du facteur de charge et des arrêts.
    Si arrêt (planifié ou non), on force à ~0 kWh (zéro technique).
    """
    if planned == 1 or unplanned == 1:
        return 0

    cf = daily_capacity_factor(m)
    rated_mw = RATED_MW[turbine]
    energy = cf * rated_mw * 24.0 * 1000.0  # MW * 24h -> MWh ; *1000 -> kWh
    # Ajout d'une petite variabilité instrumentale (±3%)
    jitter = random.uniform(0.97, 1.03)
    energy *= jitter
    return int(round(energy))

def main():
    parser = argparse.ArgumentParser(description="Génère un CSV de production journalière par turbine.")
    parser.add_argument("annee", type=int, help="Année au format AAAA (ex: 2025)")
    parser.add_argument("mois", type=int, help="Mois au format MM (1-12)")
    parser.add_argument("--seed", type=int, default=None, help="Graine aléatoire (optionnelle) pour reproductibilité")
    args = parser.parse_args()

    y = args.annee
    m = args.mois
    if args.seed is not None:
        random.seed(args.seed)

    # Contrôles simples
    if not (1 <= m <= 12):
        raise SystemExit("Le mois doit être dans [1..12].")
    if y < 1:
        raise SystemExit("L'année doit être un entier positif (AAAA).")

    # Prépare la sortie
    out_name = f"production_{y:04d}_{m:02d}.csv"
    out_path = Path(out_name)

    # Génération des dates du mois
    nb_days = calendar.monthrange(y, m)[1]
    days = [date(y, m, d) for d in range(1, nb_days + 1)]
    turbines = ["T001", "T002"]

    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=';', lineterminator="\n")
        # En-tête
        writer.writerow(["date", "turbin_id", "energie_kWh", "arret_planifie", "arret_non_planifie"])

        for d in days:
            for t in turbines:
                planned, unplanned = simulate_outages()
                energy = compute_daily_energy_kwh(t, y, m, planned, unplanned)

                row = {
                    "date": d.strftime("%Y-%m-%d"),
                    "turbin_id": t,
                    "energie_kWh": str(energy),
                    "arret_planifie": str(planned),
                    "arret_non_planifie": str(unplanned),
                }

                # Introduire des valeurs manquantes sur certaines lignes
                row = maybe_make_missing(row)

                writer.writerow([
                    row["date"],
                    row["turbin_id"],
                    row["energie_kWh"],
                    row["arret_planifie"],
                    row["arret_non_planifie"],
                ])

    print(f"Fichier généré : {out_path.resolve()}")

if __name__ == "__main__":
    main()
