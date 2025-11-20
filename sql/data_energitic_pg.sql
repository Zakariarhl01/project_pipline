/* Insert the two turbines used in the exercise */
INSERT INTO turbine (turbine_id, model, power_mw, commissioning_date)
VALUES
    ('T001', 'EnergiTech‑E100', 3.5, '2020-03-15'),
    ('T002', 'EnergiTech‑E200', 4.0, '2021-06-01');

/* --------------------------------------------------------------
   Insert static sensor catalogue
   -------------------------------------------------------------- */
INSERT INTO sensor_inventory (sensor_id, turbine_id, sensor_type, installation_dt, location_desc)
VALUES
    ('S_T001_WIND','T001','WIND_SPEED','2020-03-15','Nacelle'),
    ('S_T001_TEMP','T001','TEMPERATURE','2020-03-15','Nacelle'),
    ('S_T001_VIB' ,'T001','VIBRATION' ,'2020-03-15','Base'),
    ('S_T002_WIND','T002','WIND_SPEED','2021-06-01','Nacelle'),
    ('S_T002_TEMP','T002','TEMPERATURE','2021-06-01','Nacelle'),
    ('S_T002_VIB' ,'T002','VIBRATION' ,'2021-06-01','Base'),
  ('S_T001_ENERGY','T001','ENERGY_CONSUMPTION','2020-03-15','Nacelle'),
  ('S_T002_ENERGY','T002','ENERGY_CONSUMPTION','2021-06-01','Nacelle');

/* --------------------------------------------------------------
    Function that returns a random measurement row
   -------------------------------------------------------------- */
CREATE OR REPLACE FUNCTION fn_random_measurement(
    p_turbine VARCHAR,
    p_ts      TIMESTAMP
)
RETURNS TABLE (
    turbine_id       VARCHAR,
    ts_utc           TIMESTAMP,
    wind_speed_mps   NUMERIC,
    temperature_k    NUMERIC,
    vibration_mm_s   NUMERIC,
   consumption_kwh  NUMERIC
)
LANGUAGE sql
AS $$
    SELECT
        p_turbine                         AS turbine_id,
        p_ts                              AS ts_utc,
        round( (3 + random() * 12)::numeric, 2)   AS wind_speed_mps,   -- 3‑15 m/s
        round( (260 + random() * 40)::numeric, 2) AS temperature_k,    -- 260‑300 K -> paramétrage des valeurs libre
        round( (random() * 12)::numeric, 2)       AS vibration_mm_s,    -- 0‑12 mm/s -> paramétrage des valeurs libre
	round( (800 + random() * 400)::numeric, 2) AS consumption_kwh   -- 800‑1200 kWh/m -> paramétrage des valeurs libre
$$;

/* --------------------------------------------------------------
   Bulk‑insert using generate_series (minute granularity)
   -------------------------------------------------------------- */
DO
$$
DECLARE
    start_ts TIMESTAMP := '2025-10-01 00:00:00'; -- 0‑12 mm/s -> paramétrage date libre
    end_ts   TIMESTAMP := '2025-12-31 23:59:00'; -- 0‑12 mm/s -> paramétrage date libre
    cur_ts   TIMESTAMP;
BEGIN
    FOR cur_ts IN
        SELECT generate_series(start_ts, end_ts, interval '1 minute')
    LOOP
        INSERT INTO raw_measurements (turbine_id, ts_utc, wind_speed_mps,
                                      temperature_k, vibration_mm_s, consumption_kwh)
        SELECT * FROM fn_random_measurement('T001', cur_ts);

        INSERT INTO raw_measurements (turbine_id, ts_utc, wind_speed_mps,
                                      temperature_k, vibration_mm_s, consumption_kwh)
        SELECT * FROM fn_random_measurement('T002', cur_ts);
    END LOOP;
END
$$ LANGUAGE plpgsql;

/* --------------------------------------------------------------
   Quick sanity check (optional)
   -------------------------------------------------------------- */
SELECT
    turbine_id,
    COUNT(*)                AS nb_rows,
    MIN(ts_utc)             AS first_timestamp,
    MAX(ts_utc)             AS last_timestamp
FROM raw_measurements
GROUP BY turbine_id;