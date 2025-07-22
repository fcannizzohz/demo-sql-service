CREATE
OR REPLACE VIEW temperature_enriched AS
SELECT tu.__key,
       tu.city_id,
       tu.temperature,
       tu.ts,
       c.country
FROM temperature_updates_ordered AS tu
         JOIN cities AS c
              ON tu.city_id = c.city_id;