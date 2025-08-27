#!/usr/bin/env python3
"""
Probe Hazelcast SQL REST API (restq.py):
- /health
- /schemas
- /tables (base tables)
- /tables?include_views=true (views)
- /columns for each table
- /query (SELECT … LIMIT)
- /stream (NDJSON)
- /cursor lifecycle (create → next → close)

Assumptions:
- Cluster started with docker compose --profile producer up
- Base URL is http://localhost:8000 (override via HZ_API_URL env).
- Default schema is 'public' (override via HZ_SCHEMA env).
- Mapping 'temperatures' exists in that schema (override via HZ_TABLE env).

Exits non-zero on any failure.
"""

import os
import sys
import json
from typing import Any, Dict, List
import requests

BASE_URL = os.getenv("HZ_API_URL", "http://localhost:8000")
SCHEMA = os.getenv("HZ_SCHEMA", "public")
TABLE  = os.getenv("HZ_TABLE", "temperatures")

def _ok(resp: requests.Response) -> Dict[str, Any] | List[Any]:
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        print(f"HTTP {resp.status_code} for {resp.request.method} {resp.request.url}\n{resp.text}", file=sys.stderr)
        raise
    if resp.headers.get("content-type","").startswith("application/json") or resp.text.strip().startswith("{") or resp.text.strip().startswith("["):
        return resp.json()
    return {"raw": resp.text}

def ping_health():
    print("→ /health")
    j = _ok(requests.get(f"{BASE_URL}/health", timeout=10))
    print("  health:", j)

def list_schemas() -> List[str]:
    print("→ /schemas")
    j = _ok(requests.get(f"{BASE_URL}/schemas", timeout=20))
    print("  schemas:", j)
    return j

def list_tables(schema: str, include_views: bool=False) -> List[str]:
    print(f"→ /tables?schema={schema}&include_views={str(include_views).lower()}")
    j = _ok(requests.get(f"{BASE_URL}/tables", params={"schema": schema, "include_views": str(include_views).lower()}, timeout=30))
    print("  names:", j)
    return j

def list_columns(schema: str, table: str) -> List[Dict[str, Any]]:
    print(f"→ /columns?schema={schema}&table={table}")
    j = _ok(requests.get(f"{BASE_URL}/columns", params={"schema": schema, "table": table}, timeout=30))
    print("  columns:", json.dumps(j, indent=2))
    return j

def sample_query(schema: str, table: str, limit: int = 5):
    print("→ /query (sample rows)")
    payload = {
        "sql": f'SELECT * FROM "{schema}"."{table}"',
        "limit": limit,
        "timeout_sec": 20,
        "cursor_buffer_size": 500
    }
    j = _ok(requests.post(f"{BASE_URL}/query", json=payload, timeout=60))
    print("  columns:", j.get("columns"))
    print("  rows:", json.dumps(j.get("rows", []), indent=2))
    return j

def sample_stream(schema: str, table: str, max_chunks: int = 2):
    print("→ /stream (NDJSON, first chunks)")
    payload = {
        "sql": f'SELECT * FROM "{schema}"."{table}"',
        "cursor_buffer_size": 500,
        "timeout_sec": 20
    }
    with requests.post(f"{BASE_URL}/stream", json=payload, stream=True, timeout=120) as r:
        r.raise_for_status()
        chunk_count = 0
        for line in r.iter_lines(decode_unicode=True):
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                print("  (non-JSON line skipped)")
                continue
            if "columns" in obj:
                print("  header:", obj["columns"])
            if "rows" in obj:
                print(f"  chunk[{chunk_count}] rows:", len(obj["rows"]))
                chunk_count += 1
                if chunk_count >= max_chunks:
                    break

def cursor_lifecycle(schema: str, table: str, fetch_rows: int = 5):
    print("→ /cursor (create)")
    payload = {
        "sql": f'SELECT * FROM "{schema}"."{table}" ORDER BY 1',
        "params": [],
        "timeout_sec": 20,
        "cursor_buffer_size": 200
    }
    j = _ok(requests.post(f"{BASE_URL}/cursor", json=payload, timeout=30))
    cid = j["cursor_id"]
    print("  cursor_id:", cid)
    print("  columns:", j["columns"])
    print("→ /cursor/{id}/next".format(id=cid))
    j2 = _ok(requests.get(f"{BASE_URL}/cursor/{cid}/next", params={"rows": fetch_rows, "include_columns": "true"}, timeout=60))
    print("  batch rows:", len(j2["rows"]), "exhausted:", j2["exhausted"])
    # we can fetch again to show TTL bump
    j3 = _ok(requests.get(f"{BASE_URL}/cursor/{cid}/next", params={"rows": fetch_rows}, timeout=60))
    print("  next batch rows:", len(j3["rows"]), "exhausted:", j3["exhausted"])
    print("→ /cursor/{id} (close)".format(id=cid))
    j4 = _ok(requests.delete(f"{BASE_URL}/cursor/{cid}", timeout=20))
    print("  closed:", j4["closed"])

def main():
    print(f"== Probing API at {BASE_URL} ==")
    ping_health()

    schemas = list_schemas()
    if SCHEMA not in schemas:
        print(f"WARNING: schema '{SCHEMA}' not returned by /schemas; proceeding anyway…")

    tables = list_tables(SCHEMA, include_views=False)
    print(f"Checking columns for all base tables in schema '{SCHEMA}'")
    for t in tables:
        list_columns(SCHEMA, t)

    views = list_tables(SCHEMA, include_views=True)
    view_only = [v for v in views if v not in tables]
    if view_only:
        print(f"Schema '{SCHEMA}' views:", view_only)

    # Ensure target mapping exists (soft check)
    if TABLE not in tables:
        print(f"WARNING: mapping '{TABLE}' not found among base tables; sample queries may fail.", file=sys.stderr)

    # Quick sample queries against temperatures
    try:
        sample_query(SCHEMA, TABLE, limit=5)
    except requests.HTTPError:
        print("Sample /query failed (perhaps mapping empty or missing?)", file=sys.stderr)

    # Stream a couple of chunks (if present)
    try:
        sample_stream(SCHEMA, TABLE, max_chunks=2)
    except requests.HTTPError:
        print("Sample /stream failed.", file=sys.stderr)

    # Cursor lifecycle
    try:
        cursor_lifecycle(SCHEMA, TABLE, fetch_rows=5)
    except requests.HTTPError:
        print("Cursor lifecycle test failed.", file=sys.stderr)

    print("== Done ==")

if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError:
        sys.exit(2)
    except Exception as e:
        print(f"FATAL: {e}", file=sys.stderr)
        sys.exit(1)
