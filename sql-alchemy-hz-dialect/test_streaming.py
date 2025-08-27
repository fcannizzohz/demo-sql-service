from sqlalchemy import create_engine, text

# 5️⃣ Now test via SQLAlchemy + your hazelcast_sqlalchemy dialect
engine = create_engine(f"hazelcast+python://127.0.0.1:5701?timeout=10")

print("⏺ SQLAlchemy streaming SELECT via driver")
print("Start the cluster and producer with 'docker compose --profile producer up'")
with engine.connect() as conn:
    # 1) Enable streaming on the connection
    stream = conn.execution_options(stream_results=True) \
        .execute(text("SELECT * FROM temperature_enriched"))

    # 2) Iterate as rows arrive
    stop_count = 5
    for row in stream:
        print(f"{stop_count}) {row}")        # each `row` is a tuple
        stop_count -= 1
        if stop_count == 0: break

    # never reached - block with CTRL-C
    stream.close()