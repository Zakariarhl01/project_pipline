PROJET PIPELINE ETL POUR L'ÉNERGIE ÉOLIENNE (EnergiTech)
======================================================

Ce projet implémente un pipeline d'intégration de données (ETL - Extract, Transform, Load) en Python pour consolider les mesures de production, de capteurs et de météo dans une base de données PostgreSQL. Le pipeline est conçu pour traiter des sources de données hétérogènes et assurer la qualité ainsi que l'intégrité des informations.

OBJECTIFS DU PROJET
-------------------

L'objectif principal est de créer une SOURCE UNIQUE DE VÉRITÉ (table consolidated_measurements) en fusionnant les données provenant de trois sources distinctes :

1. Production CSV : Données de performance (énergie, arrêts).
2. Base de Données Interne (PostgreSQL) : Données de capteurs (vibration, température, vent).
3. API Externe (Open-Meteo) : Données météorologiques historiques.

Le pipeline garantit que les données partielles (celles avec des valeurs NULL) ne viennent pas écraser les données complètes existantes lors de la fusion.

======================================================

ARCHITECTURE ET STRUCTURE DU PROJET
-----------------------------------

Le projet est organisé autour des étapes d'un pipeline ETL :

- main_pipeline.py : Orchestrateur (fichier principal qui gère la séquence ETL et le logging).
- config.yaml : Configuration (paramètres de connexion DB, API, chemins d'accès).
- extract_*.py : Extraction (scripts pour la lecture des données brutes : CSV, DB, API).
- transform.py : Transformation (gestion des unités, des dates et du contrôle qualité).
- load.py : Chargement (connexion à la DB cible et opération de fusion UPSERT).
- visualize_data.py : Visualisation (interface utilisateur Streamlit pour les données consolidées).
- logs/ : Dossier des journaux d'exécution.
- data/ : Dossier des fichiers CSV d'entrée.

======================================================

1. PRÉREQUIS ET INSTALLATION
----------------------------

### Prérequis Logiciels
- Python 3.8+
- PostgreSQL (avec la base de données configurée)

### Installation des Dépendances
Installez les bibliothèques Python nécessaires :
pip install -r requirements.txt

### Configuration de la Base de Données
1. Créez la base de données PostgreSQL (ex: donnee_meteo).
2. Exécutez les scripts SQL fournis par EnergiTech pour créer le schéma.
3. Mettez à jour le fichier config.yaml avec vos identifiants PostgreSQL.

======================================================

2. CONFIGURATION DU PIPELINE
----------------------------

Le fichier config.yaml est l'interface de configuration :

Exemple de contenu clé :

postgres:
  host: localhost
  dbname: donnee_meteo
  user: postgres
  password: VOTRE_MOT_DE_PASSE

pipeline:
  lookback_minutes: 1440 # 24 heures

======================================================

3. EXÉCUTION DU PIPELINE
------------------------

Le pipeline s'exécute via le script principal :

python main_pipeline.py

En cas de succès, un rapport récapitulatif (lignes insérées, anomalies corrigées) est affiché et consigné dans logs/.

======================================================

4. VISUALISATION DES DONNÉES
----------------------------

Une interface Streamlit est fournie pour consulter les données :

streamlit run visualize_data.py

Une application web s'ouvrira, permettant de voir les tendances et indicateurs clés.

======================================================

5. POINTS CLÉS TECHNIQUES (POUR LA SOUTENANCE)
-----------------------------------------------

### A. La Fusion (UPSERT) et la Robustesse

Le point le plus critique est l'opération de fusion dans load.py, réalisée via l'opération UPSERT (INSERT ON CONFLICT) avec la fonction COALESCE.

* **Logique SQL :**
    ON CONFLICT (turbine_id, date) DO UPDATE SET
      energie_kwh = COALESCE(EXCLUDED.energie_kwh, energie_kwh),
      source = EXCLUDED.source

* **Rôle de COALESCE :** Elle permet d'empêcher l'écrasement des données existantes par des valeurs NULL provenant des enregistrements partiels (ex: la ligne Météo a NULL pour energie_kwh). COALESCE(Nouvelle_Valeur, Ancienne_Valeur) conserve toujours la première valeur non-NULL.

* **Rôle de EXCLUDED.source :** Ce champ est toujours écrasé pour assurer la traçabilité en indiquant la dernière source qui a enrichi la ligne.

### B. Contrôle Qualité (transform.py)

La phase de transformation assure la qualité des données :

* **Standardisation :** Conversion des unités (Celsius -> Kelvin, km/h -> m/s) pour l'uniformité.
* **Nettoyage :** Mise en forme des dates et conversion des booléens.
* **Filtrage :** Mise à NULL des valeurs aberrantes (ex: vent négatif ou températures irréalistes) pour les rendre exploitables.

### C. Gestion des Erreurs

L'utilisation du module logging de Python permet de tracer l'ensemble des événements, des erreurs, et des statistiques d'exécution, assurant la supervision de la chaîne de données.