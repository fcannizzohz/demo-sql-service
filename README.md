# SQL Demo

This README walks through mapping and querying of **cities** and **temperatures** data using Hazelcast SQL.

## Steps

### Load cluster

Run docker compose to create the cluster
```shell
docker compose up
```

### Setup seed data and mappings

You can create data and mappings by running main methods of the nested utility classes of 
`com.hazelcast.fcannizzohz.SetupSeedData`:
- `SetupSeedData$SetupCitiesMapping#main()`.
- `SetupSeedData$SetupCitiesData#main()`.

Or via Management Centre as following.

In both cases, via Management centre you can verify that the data is available by running `SELECT * FROM cities;`

#### Start Management Center

Go to`http://localhost:8080`, load the cluster and navigate to "SQL Browser"

#### Create `cities` mapping

View [Cities Mapping SQL](src/main/resources/cities_mapping.sql)

Note that the `cities` table is empty: `SELECT * FROM cities;`

#### Insert data into `cities`

View [City Data SQL](src/main/resources/cities_data.sql)

Note that the `cities` table is not empty: `SELECT * FROM cities;`

### Insert and read data in map: `InsertIntoCitiesMap`

Execute, from IDE, the main method in `InsertIntoCitiesMap`; this inserts two cities showing how to input data 
either in JSON (as if data was coming from remote data sources like a REST service or a kafka topic) or using POJOs (when data
is inserted as part of the application directly in the map)

### Create mapping from file
The file [temperatures.csv](./src/main/resources/temperatures.csv) contains temperatures of some cities in our database.
This file can be mapped so that its data is loaded directly in Hazelcast, showing how to make data available in the cluster from
heterogeneous data sources in a federated mode.

Create the mapping to the temperatures.csv file running this SQL either via MC or running `SetupSeedData$SetupTemperaturesMapping#main()`: [Temperatures mapping sql](./src/main/resources/temperatures_mapping.sql)

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

Note that for some countries (like Italy, Morocco, and others), the average temperature is null because temperatures don't exist for city_id for some `city_id`s.

Every time new temperatures are inserted on cities that don't have it, by extending the CSV file the view is updated with the correct average values.

### Streaming 

The class `TemperatureProducer` produces random temperatures for a list of city IDs. Run it via its `main()`.

This generates data in the form of
`{"city_id":1003,"temperature":19, "ts", "2025-06-12T12:09:42""}` in a topic called `temperature_updates`

Create mapping to the topic by running the [Temperature Updates Mapping](./src/main/resources/temperature_updates_mapping.sql) SQL in Management Centre or by running `SetupSeedData$SetupTemperatureUpdatesMapping#main()`:
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
See https://docs.hazelcast.com/hazelcast/5.5/sql/querying-streams#late-events for further details.

Create a new view with temperature updates that collects data in the past 5s and orders it. 

Run the SQL from [Temperature Updates Ordered View](./src/main/resources/temperature_updates_ordered_view.sql) or execute 
`SetupSeedData$SetupTemperatureUpdatesOrderedView#main()`

#### Querying aggregations

With the event lag handled, the following query shows average temperature per `city_id`, over 3 seconds

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

Create an enriched, watermarked view that brings in the country for each event. To create the view
execute the SQL in [Temperature Enriched View SQL](./src/main/resources/temperatures_enriched_view.sql) or run
`SetupSeedData$SetupTemperaturesEnrichedView#main()`.

Run 8s tumbling‐window aggregation over that view:

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
                INTERVAL '8' SECOND            -- fixed 8 s tumbling windows
        )
     )
GROUP BY
    window_start,
    window_end,
    country;                         -- group by exactly the window bounds + country
```

### Automatically run seed data and temperature producer

The container `"temperatures_producer"`, automatically started in the compose file, generates all the mappings and data.

This container is simply a wrapper for `com.hazelcast.fcannizzohz.TemperatureProducerCmd#main()` that connects to `kafka1:9092`,
loads the available city IDs from the cluster (`hazelcast1:5701`) and then starts producing random temperatures for illustration purposes.

Run it via `docker compose --profile producer up`.

Build the code that runs in the container via `docker compose build temperatures_producer`.

### Visualize data via SQL query

This project uses [Apache Superset](https://superset.apache.org/) to illustrate the integration with a modern Business Intelligence application, directly using SQL as the integration language. 

#### Login to Superset

Hazelcast doesn't come with a SQLAlchemy integration with Superset so a basic version is available in `src/main/python` and installed automatically in Superset docker container used in this exercise.

The very first time Superset is started it requires a new admin user. Execute the following steps to create one.

 1. Get a shell in the running Superset container  `docker compose exec superset bash`
 2. Use the Fabric CLI to create (or overwrite) an admin user: `superset fab create-admin` (you can accept or change the defaults)
 3. (Re)initialize any missing metadata: `superset db upgrade`
 4. Exit and restart Superset: `docker-compose restart superset`

Superset is spawned as part of the docker compose. To login use `admin:admin` (or whatever other values you have chosen when creating the admin user)

#### Connect to Hazelcast

To connect to hazelcast, 

1. go to `Settings\Database Connection` 
2. Click on `+ DATABASE`
3. Select `Other` in `Supported Databases` 
4. In the `Basic` tab, set `Display Name` as Hazelcast the following connection string `hazelcast+python://hazelcast1:5701`
5. Hit `Test connection to validate.
6. In the Advanced tab make sure you have the following selected in `SQL Lab`: `Allow DDL and DML`, `Allow this database to be explored`.

Hit finish to save the settings.

