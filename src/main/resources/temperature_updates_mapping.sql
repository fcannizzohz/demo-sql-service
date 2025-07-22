CREATE
OR REPLACE MAPPING temperature_updates (__key INT, city_id INT, temperature INT, ts Timestamp)
TYPE Kafka OPTIONS (
  'keyFormat' = 'int',
  'valueFormat' = 'json-flat',
  'bootstrap.servers' = 'kafka1:19092'
);