CREATE JOB streamed_temperatures_to_map AS
INSERT INTO
  temperatures_streamed_map
SELECT
    city_id * 1000000 AS __key,
    city_id,
    temperature,
    ts
FROM
    temperature_updates;