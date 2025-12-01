-- create_target_table.sql
-- Structure de stockage pour les données consolidées, basée sur la clé (turbine_id, date).

CREATE TABLE IF NOT EXISTS consolidated_measurements (
    -- Clé primaire
    turbine_id VARCHAR(5) NOT NULL,
    -- L'horodatage consolidé, clé de l'upsert
    date TIMESTAMP WITHOUT TIME ZONE NOT NULL, 
    
    -- Mesures capteurs (peuvent être NULL si seulement données CSV/Météo)
    temperature_k NUMERIC,
    wind_ms NUMERIC,
    vibration_mm_s NUMERIC,
    consumption_kwh NUMERIC,
    
    -- Mesures production (peuvent être NULL si seulement données capteurs)
    energie_kwh NUMERIC,
    arret_planifie BOOLEAN DEFAULT FALSE,
    arret_non_planifie BOOLEAN DEFAULT FALSE,
    
    -- Métadonnée
    source VARCHAR(50) NOT NULL, -- Ex: 'db_sensor', 'csv_production', 'api_weather'
    extraction_ts TIMESTAMP WITH TIME ZONE DEFAULT NOW(), -- Quand la ligne a été insérée/mise à jour
    
    -- Clé primaire composite, essentielle pour le UPSSERT (ON CONFLICT) de load.py
    PRIMARY KEY (turbine_id, date)
);

COMMENT ON TABLE consolidated_measurements IS 'Données consolidées et nettoyées prêtes pour l''analyse IA.';