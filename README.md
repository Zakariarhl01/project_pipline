PROJET PIPELINE ETL POUR L'ÉNERGIE ÉOLIENNE (EnergiTech)
======================================================

Ce projet implémente un pipeline d'intégration de données (ETL - Extract, Transform, Load) en Python pour consolider les mesures de production, de capteurs et de météo dans une base de données PostgreSQL. Le pipeline est conçu pour traiter des sources de données hétérogènes et assurer la qualité ainsi que l'intégrité des informations.

OBJECTIFS DU PROJET
-------------------

L'objectif principal est de créer une SOURCE UNIQUE DE VÉRITÉ (table `consolidated_measurements`) en fusionnant les données provenant de trois sources distinctes :

1.  **Production CSV** : Données de performance (énergie, arrêts).
2.  **Base de Données Interne (PostgreSQL)** : Données de capteurs (vibration, température, vent).
3.  **API Externe (Open-Meteo)** : Données météorologiques historiques.

Le pipeline garantit que les données partielles (celles avec des valeurs `NULL`) ne viennent pas écraser les données complètes existantes lors de la fusion.

======================================================

ARCHITECTURE ET STRUCTURE DU PROJET
-----------------------------------

Le projet est organisé autour des étapes d'un pipeline ETL :

* `main_pipeline.py` : Orchestrateur (fichier principal qui gère la séquence ETL et le logging).
* `config.yaml` : Configuration (paramètres de connexion DB, API, chemins d'accès).
* `extract_*.py` : Extraction (scripts pour la lecture des données brutes : CSV, DB, API).
* `transform.py` : Transformation (logique de nettoyage, conversion d'unités, standardisation du schéma).
* `load.py` : Chargement (logique d'insertion/mise à jour *UPSERT* dans PostgreSQL).
* `requirements.txt` : Liste des dépendances Python.
* `visualize_data.py` : Application Streamlit de visualisation.
* `create_*.sql` : Scripts SQL pour la création des tables.

======================================================

5. POINTS CLÉS TECHNIQUES (POUR LA SOUTENANCE)
-----------------------------------------------

### A. La Fusion (UPSERT) et la Robustesse

Le point le plus critique est l'opération de fusion dans `load.py`, réalisée via l'opération **UPSERT** (`INSERT ON CONFLICT`) avec la fonction **COALESCE** et une étape de **déduplication en mémoire**.

* **Déduplication en mémoire (Nouveauté)** : Le lot d'insertion est dédoublé sur la clé `(turbine_id, date)` avant d'être envoyé à la base de données. Ceci résout l'erreur `CardinalityViolation` en empêchant les conflits de clés *au sein du même lot*.
* **Logique SQL (Idempotence) :**
    ```sql
    ON CONFLICT (turbine_id, date) DO UPDATE SET
      energie_kwh = COALESCE(EXCLUDED.energie_kwh, consolidated_measurements.energie_kwh),
      source = EXCLUDED.source
    ```
* **Rôle de COALESCE :** Elle empêche l'écrasement des données existantes par des valeurs `NULL` provenant des enregistrements partiels (ex: la ligne Météo a `NULL` pour `energie_kwh`). `COALESCE(Nouvelle_Valeur, Ancienne_Valeur)` conserve toujours la première valeur non-NULL.
* **Rôle de EXCLUDED.source :** Ce champ est toujours écrasé pour assurer la traçabilité en indiquant la dernière source qui a enrichi la ligne.

### B. Contrôle Qualité (`transform.py`)

La phase de transformation assure la qualité des données :

* **Standardisation :** Conversion des unités (Celsius -> Kelvin, km/h -> m/s) pour l'uniformité.
* **Nettoyage :** Mise en forme des dates (gestion des fuseaux horaires vers UTC) et conversion des booléens.
* **Filtrage :** Mise à `NULL` des valeurs aberrantes (`outliers`) comme le vent négatif ou les températures irréalistes (règles configurées pour Vent max 42 m/s, Température entre 200K et 330K) pour les rendre exploitables.

### C. Gestion des Erreurs et Performance

* **Logging :** Utilisation du module `logging` de Python pour tracer tous les événements (INFO, WARNING, ERROR) dans la console et dans des fichiers de log, permettant un débuggage facile.
* **Performance :** L'insertion est réalisée par lots (`psycopg2.extras.execute_values`) pour optimiser la vitesse de la transaction vers PostgreSQL.

### D. Simulation de Flux

* **`extract_db.py`** contient une étape de simulation (`generate_and_insert_new_data`) qui génère des données de capteurs en temps réel (à la minute la plus proche) avant l'extraction. Ceci simule l'arrivée continue de nouvelles mesures de capteurs, rendant le pipeline dynamique et pertinent pour l'ingestion de données en continu.

======================================================

INSTALLATION ET EXÉCUTION
---------------------------

### 1. Prérequis

* Python 3.9+
* Une instance PostgreSQL fonctionnelle.

### 2. Installation des dépendances

Installez les bibliothèques Python nécessaires en utilisant le fichier `requirements.txt` :

```bash
pip install -r requirements.txt