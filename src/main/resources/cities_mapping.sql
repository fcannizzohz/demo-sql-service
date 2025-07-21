CREATE OR REPLACE MAPPING cities (
     __key     INT,
     city_id   INT,
     country   VARCHAR,
     name      VARCHAR,
     population INT,
     latitude DOUBLE,
     longitude DOUBLE
   ) TYPE IMap
   OPTIONS (
     'keyFormat'   = 'int',
     'valueFormat' = 'json-flat'
   );