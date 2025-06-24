# SQL Demo

This README walks through mapping, querying and CDC of **cities** and **temperatures** data using Hazelcast SQL.

## Steps

### Load cluster

Run docker compose to create the cluster
```shell
docker compose up
```

### Start Management Center

Go to`localhost:8080`, load the cluster and navigate to "SQL Browser"


### Create `cities` mapping

   ```sql
   CREATE MAPPING cities (
     __key     INT,
     city_id   INT,
     country   VARCHAR,
     name      VARCHAR,
     population INT
   ) TYPE IMap
   OPTIONS (
     'keyFormat'   = 'int',
     'valueFormat' = 'json-flat'
   );
   ```

Note that the `cities` table is empty: `SELECT * FROM cities;`

### Insert data into `cities`

   ```sql
INSERT INTO cities (__key, city_id, country, name, population)
VALUES
(1, 1001, 'United Kingdom',  'London',        8961989),
(2, 1002, 'United Kingdom',  'Manchester',     553230),
(3, 1003, 'United States',   'New York',      8336817),
(4, 1004, 'United States',   'Los Angeles',   3979576),
(5, 1005, 'Turkey',          'Ankara',        5445026),
(6, 1006, 'Turkey',          'Istanbul',     15519267),
(7, 1007, 'Brazil',          'Sao Paulo',    12325232),
(8, 1008, 'Brazil',          'Rio de Janeiro', 6747815);
   ```

Note that the `cities` table is not empty: `SELECT * FROM cities;`

### Insert and read data in map: `InsertIntoCitiesMap`

Execute, from IDE, the main method in `InsertIntoCitiesMap`; this inserts two cities showing how to input data 
either in JSON (as if data was coming from remote data sources like a REST service or a kafka topic) or using POJOs (when data
is inserted as part of the application directly in the map)

### Create mapping from file
Show file `temperatures.csv` locally, we're going to demo etherogeneous data sources that can be manipulated in a federated mode.

   ```sql
   CREATE MAPPING temperatures (city_id INT, temperature INT) TYPE File OPTIONS (
   -- absolute path inside the container
   'path' = '/home/hazelcast/data',
   -- CSV format with header
   'format' = 'csv',
   -- only this file
   'glob' = 'temperatures.csv',
   -- required to prevent dups and share the load
   'sharedFileSystem' = 'true'
   );
   ```
Show content matching the file content
   ```sql
   SELECT distinct(city_id), temperature
   FROM temperatures where temperature>15
   order by temperature;
   ```


### Show join query to federate file data and map data 
   ```sql
   SELECT
      cities.country AS country,
      avg(temperatures.temperature) AS temp
   FROM
      temperatures
   RIGHT JOIN cities ON cities.city_id = temperatures.city_id
   GROUP BY
     cities.country;
   ```

Note that Italy's avg temp is null because temperatures don't exist for city_id 2001 and 2002

Insert two values for those IDs in the csv file and rerun the join query to see the data picked up directly from file.

### Streaming 

Start producer of temperature data `TemperatureProducer`. This generates data in the form of 
`{"city_id":1003,"temperature":19, "ts", "2025-06-12T12:09:42""}` in a topic called `temperature_updates`

Create mapping to the topic:
```sql
CREATE MAPPING temperature_updates (__key INT, city_id INT, temperature INT) 
TYPE Kafka OPTIONS (
  'keyFormat' = 'int',
  'valueFormat' = 'json-flat',
  'bootstrap.servers' = 'kafka1:19092'
);
```

Run streaming query:
```sql
SELECT
  *
FROM
  temperature_updates
where
  temperature > 15
```

#### Handling late events

This allows you to specify a maximum event lag. Any event that arrives later than the maximum event lag is dropped.
See: https://docs.hazelcast.com/hazelcast/5.5/sql/querying-streams#late-events

```sql
CREATE
OR REPLACE VIEW temperature_updates_ordered AS
SELECT
  *
FROM
  TABLE (
    IMPOSE_ORDER (
      TABLE temperature_updates, -- (1)
      DESCRIPTOR (ts), -- (2)
      INTERVAL '5' SECOND -- (3)
    )
  );
```

#### Querying aggregations

With the event lag handled, the following query shows average temperature per city_id, over 3 seconds

```sql
SELECT
  window_start,
  window_end,
  city_id,
  AVG(temperature) AS avg_temperature
FROM
  TABLE (
    TUMBLE (
      TABLE temperature_updates_ordered, -- use the ordered view
      DESCRIPTOR (ts), -- same timestamp column
      INTERVAL '3' SECOND -- window size
    )
  )
GROUP BY
  window_start,
  window_end,
  city_id;
```

#### Federated join between table and streaming data

Create an enriched, watermarked view that brings in the country for each event:

```sql
CREATE OR REPLACE VIEW temperature_enriched AS
SELECT
  tu.__key,
  tu.city_id,
  tu.temperature,
  tu.ts,
  c.country
FROM temperature_updates_ordered AS tu
JOIN cities AS c
  ON tu.city_id = c.city_id;
```

Run 15s tumbling‐window aggregation over that view:

```sql
SELECT
    window_start,
    window_end,
    country,
    AVG(temperature) AS avg_temperature
FROM TABLE(
        TUMBLE(
            TABLE temperature_enriched,    -- your view, already watermarked & joined
            DESCRIPTOR(ts),                 -- use the event timestamp column
                INTERVAL '15' SECOND            -- fixed 15 s tumbling windows
        )
     )
GROUP BY
    window_start,
    window_end,
    country;                         -- group by exactly the window bounds + country
```



