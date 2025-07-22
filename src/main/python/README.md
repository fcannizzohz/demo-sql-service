# Very basic SQLAlchemy wrapper

1. python -mvenv .venv
2. cd src/main/python
3. pip install -r requirements.txt
4. pip install -e .
5. python ./test_temperature_mapping.py

It will get installed in the docker container of Superset in the docker compose (check the `build` config under the `superset` container config)

