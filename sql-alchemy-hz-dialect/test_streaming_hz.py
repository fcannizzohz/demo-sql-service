import hazelcast

print("⏺ SQLAlchemy streaming SELECT via HZ client")
print("Start the cluster and producer with 'docker compose --profile producer up'")
client = hazelcast.HazelcastClient(cluster_members=[f"127.0.0.1:5701"])
# Give Hazelcast a moment to finish startup
result = client.sql.execute("SELECT * FROM temperature_enriched").result()
stop_count = 5
for row in result:
    city_id = row["city_id"]
    temperature = row["temperature"]
    print(f"{stop_count}) city_id: {city_id}, temperature: {temperature}")
    stop_count -= 1
    if stop_count == 0: break

client.shutdown()