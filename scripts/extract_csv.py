"""
extract_csv.py

Module dédié à l'extraction des données de production à partir des fichiers CSV locaux.
Il identifie le fichier le plus récent dans le répertoire d'entrée et le charge en mémoire 
sous forme de DataFrame Pandas.
"""

import os
import logging
from pathlib import Path
import pandas as pd

logger = logging.getLogger(__name__)

def find_latest_csv(input_dir, pattern_prefix='production_'):
    
    """
    Recherche le fichier CSV le plus récent dans le répertoire spécifié, basé sur la date de modification.

    Args:
        input_dir (str): Chemin du répertoire contenant les fichiers CSV.
        pattern_prefix (str): Préfixe des fichiers à rechercher (ex: 'production_').

    Returns:
        str: Le chemin d'accès complet au fichier CSV le plus récent.
        
    Raises:
        FileNotFoundError: Si aucun fichier correspondant n'est trouvé.
    """
    p = Path(input_dir)

    candidates = sorted(p.glob(f"{pattern_prefix}*.csv"), key=os.path.getmtime, reverse=True)
    if not candidates:
        return FileNotFoundError(f"Aucun fichier CSV trouvé dans {input_dir} avec prefix {pattern_prefix}")
    logger.info(f"Fichier CSV sélectionée : {candidates[0]}")
    return str(candidates[0])

def read_production_csv(path, delemiter=';'):
    """
    Lit le fichier CSV spécifié dans un DataFrame Pandas.

    Args:
        path (str): Chemin d'accès au fichier CSV.
        delemiter (str): Séparateur de colonnes (par défaut ';').

    Returns:
        pd.DataFrame: Le DataFrame contenant les données de production.
    """
    df = pd.read_csv(path, delimiter=delemiter)
    logger.info(f"{len(df)} lignes lues depuis {path}")
    return df

if __name__ == "__main__":
    input_dir = "../data"
    latest_csv = find_latest_csv(input_dir)
    df = read_production_csv(latest_csv)
    print(df.head())