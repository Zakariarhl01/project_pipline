import os
import logging
from pathlib import Path
import pandas as pd

logger = logging.getLogger(__name__)

def find_latest_csv(input_dir, pattern_prefix='production_'):
    p = Path(input_dir)

    candidates = sorted(p.glob(f"{pattern_prefix}*.csv"), key=os.path.getmtime, reverse=True)
    if not candidates:
        return FileNotFoundError(f"Aucun fichier CSV trouvé dans {input_dir} avec prefix {pattern_prefix}")
    logger.info(f"Fichier CSV sélectionée : {candidates[0]}")
    return str(candidates[0])

def read_production_csv(path, delemiter=';'):
    df = pd.read_csv(path, delimiter=delemiter)
    logger.info(f"{len(df)} lignes lues depuis {path}")
    return df

if __name__ == "__main__":
    input_dir = "../data"
    latest_csv = find_latest_csv(input_dir)
    df = read_production_csv(latest_csv)
    print(df.head())