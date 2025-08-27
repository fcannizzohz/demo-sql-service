# Very basic SQLAlchemy wrapper

1. cd src/main/python
2. python -mvenv .venv
3. pip install -r requirements.txt
4. pip install -e .
5. python ./test_temperature_mapping.py

It will get installed in the docker container of Superset in the docker compose (check the `build` config under the `superset` container config)

# REST Exposure

The SQLAlchemy connector is also exposed via REST to allow querying via an HTTP endpoint, this will allow integration with tools like PowerBI

To run the API:

`uvicorn hazelcast_sqlalchemy.restq:app --host 0.0.0.0 --port 8000`

## Test

```bash
curl -X POST http://localhost:8000/query \
-H "Content-Type: application/json" \
-d '{"sql":"SELECT * FROM information_schema.tables WHERE table_schema=:s", "params":{"s":"public"}}'
```

Response:

```json
{
  "columns": ["table_name"],
  "rows": [
    {
      "table_name": "my_mapping"
    },
    {
      "table_name": "another_mapping"
    }
  ]
}
```
