import time
import hazelcast
from sqlalchemy import create_engine, text

client = hazelcast.HazelcastClient(cluster_members=[f"127.0.0.1:5701"])
# Give Hazelcast a moment to finish startup
result = client.sql.execute("SELECT * FROM temperature_enriched").result()
for row in result:
    city_id = row["city_id"]
    temperature = row["temperature"]
    print(f"city_id: {city_id}, temperature: {temperature}")

client.shutdown()