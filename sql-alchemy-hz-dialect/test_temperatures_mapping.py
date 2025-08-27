#!/usr/bin/env python3

import time
import tempfile
from pathlib import Path

import hazelcast
from testcontainers.core.container import DockerContainer
from sqlalchemy import create_engine, text

from hazelcast_sqlalchemy import SQL_COLUMNS, SQL_VIEWS, SQL_TABLES, SQL_HAS_TABLE, SQL_SCHEMA, DEFAULT_SCHEMA_NAME

# 1️⃣ Prepare the CSV under a temp directory
data_root = Path(tempfile.mkdtemp())
data_root.mkdir(parents=True, exist_ok=True)
csv_dir = data_root / "data"
csv_dir.mkdir()
(csv_dir / "temperatures.csv").write_text(
    "city_id,temperature\n"
    "1,20\n"
    "2,25\n"
    "3,15\n"
)

print("Start Hazelcast 5.5 (full distro) and mount our CSV dir")
hz = (
    DockerContainer("hazelcast/hazelcast:5.5")
    .with_volume_mapping(str(csv_dir), "/home/hazelcast/data", mode="ro")
    .with_exposed_ports(5701)
)
hz.start()
host = hz.get_container_host_ip()
port = hz.get_exposed_port(5701)
address = f"{host}:{port}"

print("Connect with the Hazelcast Python client and create the File mapping")
client = hazelcast.HazelcastClient(cluster_members=[address])
ddl = """
CREATE OR REPLACE MAPPING temperatures (
    city_id     INT,
    temperature INT
)
TYPE File
OPTIONS (
    'path'             = '/home/hazelcast/data',
    'format'           = 'csv',
    'glob'             = 'temperatures.csv',
    'sharedFileSystem' = 'true'
);
"""
client.sql.execute(ddl).result()

print("Verify with the Python client directly")
print("  Direct client SELECT")
rows = client.sql.execute("SELECT * FROM temperatures").result()
print([tuple(r) for r in rows])  # -> [(1,20),(2,25),(3,15)]

print("Now test via SQLAlchemy + your hazelcast_sqlalchemy dialect")
engine = create_engine(f"hazelcast+python://{host}:{port}?timeout=10")

print("  SQLAlchemy SELECT")
with engine.connect() as conn:
    result = conn.execute(text("SELECT * FROM temperatures"))
    data = result.all()
    print(data)

print("  SQLAlchemy SELECT schema")
with engine.connect() as conn:
    result = conn.execute(text(SQL_SCHEMA))
    data = result.all()
    print(data)

print("  SQLAlchemy SELECT tables")
with engine.connect() as conn:
    result = conn.execute(text(SQL_TABLES), {"schema": DEFAULT_SCHEMA_NAME})
    data = result.all()
    print(data)

print("  SQLAlchemy SELECT views")
with engine.connect() as conn:
    result = conn.execute(text(SQL_VIEWS), {"schema": DEFAULT_SCHEMA_NAME})
    data = result.all()
    print(data)

print("  SQLAlchemy SELECT columns")
with engine.connect() as conn:
    result = conn.execute(text(SQL_COLUMNS), {"schema": DEFAULT_SCHEMA_NAME, "table": "temperatures"})
    data = result.all()
    print(data)

print("  SQLAlchemy SELECT has_table")
with engine.connect() as conn:
    result = conn.execute(text(f"{SQL_HAS_TABLE}"), {"schema": DEFAULT_SCHEMA_NAME, "table": "temperatures"})
    data = result.all()
    print(data)


print("Cleanup")
client.shutdown()
hz.stop()
