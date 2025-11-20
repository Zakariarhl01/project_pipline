/* --------------------------------------------------------------
    Master table : turbine
   -------------------------------------------------------------- */
CREATE TABLE turbine (
    turbine_id VARCHAR(10) PRIMARY KEY,
    model      VARCHAR(30) NOT NULL,
    power_mw   NUMERIC(5,2) NOT NULL,
    commissioning_date DATE NOT NULL
);

/* --------------------------------------------------------------
   Raw measurements 
   -------------------------------------------------------------- */
CREATE TABLE raw_measurements (
    meas_id          BIGSERIAL PRIMARY KEY,
    turbine_id       VARCHAR(10) NOT NULL,
    ts_utc           TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    wind_speed_mps   NUMERIC(5,2) NULL,
    temperature_k    NUMERIC(5,2) NULL,
    vibration_mm_s   NUMERIC(6,2) NULL,
  consumption_kwh  NUMERIC(8,2) NULL,  

    CONSTRAINT uq_turbine_timestamp UNIQUE (turbine_id, ts_utc),
    CONSTRAINT fk_rm_turbine
        FOREIGN KEY (turbine_id)
        REFERENCES turbine (turbine_id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT
);

CREATE INDEX idx_raw_measurements_turbine_ts
    ON raw_measurements (turbine_id, ts_utc);

/* --------------------------------------------------------------
   Sensor inventory 
   -------------------------------------------------------------- */
CREATE TABLE sensor_inventory (
    sensor_id        VARCHAR(20) PRIMARY KEY,
    turbine_id       VARCHAR(10) NOT NULL,
    sensor_type      TEXT CHECK (sensor_type IN ('WIND_SPEED','TEMPERATURE','VIBRATION','ENERGY_CONSUMPTION')),
    installation_dt  DATE NOT NULL,
    location_desc    VARCHAR(100) NOT NULL,

    CONSTRAINT fk_sensor_turbine
        FOREIGN KEY (turbine_id)
        REFERENCES turbine (turbine_id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT
);

/* --------------------------------------------------------------
    view – temperature in Celsius
   -------------------------------------------------------------- */
CREATE OR REPLACE VIEW vw_measurements_celsius AS
SELECT
    meas_id,
    turbine_id,
    ts_utc,
    wind_speed_mps,
    temperature_k - 273.15 AS temperature_c,
    vibration_mm_s
FROM raw_measurements;

