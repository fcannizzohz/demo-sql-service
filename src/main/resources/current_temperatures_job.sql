CREATE JOB current_temperatures AS SINK INTO current_temperatures
SELECT
    temperature_updates.city_id * 100000 AS __key,
    temperature_updates.city_id as city_id,
    temperature_updates.temperature as temperature,
    temperature_updates.ts as ts
FROM
    temperature_updates;