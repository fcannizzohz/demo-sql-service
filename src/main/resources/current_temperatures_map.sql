CREATE OR REPLACE MAPPING current_temperatures (
    __key INT,
    city_id INT,
    temperature INT,
    ts TIMESTAMP
)
TYPE IMap
OPTIONS (
    'keyFormat' = 'int',
    'valueFormat' = 'compact',
    'valueCompactTypeName' = 'Temperature'
);