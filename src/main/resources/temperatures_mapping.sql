CREATE
OR REPLACE MAPPING temperatures (city_id INT, temperature INT) TYPE File OPTIONS (
-- absolute path inside the container
'path' = '/home/hazelcast/data',
-- CSV format with header
'format' = 'csv',
-- only this file
'glob' = 'temperatures.csv',
-- required to prevent dups and share the load
'sharedFileSystem' = 'true'
);