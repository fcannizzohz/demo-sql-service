#!/usr/bin/env python3
import time
import tempfile
from pathlib import Path

import hazelcast
from testcontainers.core.container import DockerContainer
from sqlalchemy import create_engine, text

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

# 2️⃣ Start Hazelcast 5.5 (full distro) and mount our CSV dir
hz = (
    DockerContainer("hazelcast/hazelcast:5.5")
    .with_volume_mapping(str(csv_dir), "/home/hazelcast/data", mode="ro")
    .with_exposed_ports(5701)
)
hz.start()
host = hz.get_container_host_ip()
port = hz.get_exposed_port(5701)
address = f"{host}:{port}"

# Give Hazelcast a moment to finish startup
time.sleep(5)

# 3️⃣ Connect with the Hazelcast Python client and create the File mapping
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

# 4️⃣ Verify with the Python client directly
print("⏺ Direct client SELECT")
rows = client.sql.execute("SELECT * FROM temperatures").result()
print([tuple(r) for r in rows])  # -> [(1,20),(2,25),(3,15)]

# 5️⃣ Now test via SQLAlchemy + your hazelcast_sqlalchemy dialect
engine = create_engine(f"hazelcast+python://{host}:{port}?timeout=10")

print("⏺ SQLAlchemy SELECT")
with engine.connect() as conn:
    result = conn.execute(text("SELECT * FROM temperatures"))
    data = result.all()
    print(data)  # should match the list of tuples above

print("⏺ SQLAlchemy SELECT schema_name")
with engine.connect() as conn:
    result = conn.execute(text("SELECT schema_name FROM information_schema"))
    data = result.all()
    print(data)  # should match the list of tuples above

print("⏺ SQLAlchemy SELECT mapping_name")
with engine.connect() as conn:
    result = conn.execute(text("SELECT mapping_name FROM information_schema.mappings"))
    data = result.all()
    print(data)  # should match the list of tuples above

# 6️⃣ Cleanup
client.shutdown()
hz.stop()
