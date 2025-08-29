# restq.py
import os
import json
import time
import uuid
import logging
import asyncio
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta
from collections import defaultdict
from time import monotonic
from threading import Lock

from fastapi import FastAPI, HTTPException, Request, Response, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field, constr, validator

from sqlalchemy import create_engine, text, event
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import inspect

from .dialect import SQL_VIEWS, SQL_SCHEMA, SQL_TABLES, SQL_COLUMNS

# --------------------------------------------------------------------
# Configuration (env-driven)
# --------------------------------------------------------------------
HZ_HOST = os.getenv("HZ_HOST", "localhost")
HZ_PORT = int(os.getenv("HZ_PORT", "5701"))
DB_URL = os.getenv("DB_URL", f"hazelcast+python://{HZ_HOST}:{HZ_PORT}")

HZ_CLUSTER_NAME = os.getenv("HZ_CLUSTER_NAME")  # e.g. "dev"
HZ_CLUSTER_MEMBERS = os.getenv("HZ_CLUSTER_MEMBERS")  # e.g. "hz1:5701,hz2:5701"

CONNECT_ARGS: Dict[str, Any] = {}
if HZ_CLUSTER_NAME:
    CONNECT_ARGS["cluster_name"] = HZ_CLUSTER_NAME
if HZ_CLUSTER_MEMBERS:
    CONNECT_ARGS["cluster_members"] = [
        s.strip() for s in HZ_CLUSTER_MEMBERS.split(",") if s.strip()
    ]

POOL_SIZE = int(os.getenv("POOL_SIZE", "5"))
MAX_OVERFLOW = int(os.getenv("MAX_OVERFLOW", "10"))
POOL_TIMEOUT = int(os.getenv("POOL_TIMEOUT", "30"))
POOL_RECYCLE = int(os.getenv("POOL_RECYCLE", "1800"))  # seconds

# Security / behaviour toggles
ALLOW_DML = os.getenv("ALLOW_DML", "false").lower() == "true"
ALLOW_DDL = os.getenv("ALLOW_DDL", "false").lower() == "true"

# CORS
ALLOWED_ORIGINS = [o.strip() for o in os.getenv("CORS_ORIGINS", "*").split(",")]

# Rate limiting
RATE_LIMIT_RPS = float(os.getenv("RATE_LIMIT_RPS", "5"))
RATE_LIMIT_BURST = int(os.getenv("RATE_LIMIT_BURST", "10"))

# Query safety
MAX_LIMIT = int(os.getenv("MAX_LIMIT", "10000"))
DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "1000"))
DEFAULT_TIMEOUT = float(os.getenv("DEFAULT_TIMEOUT", "30"))
DEFAULT_CURSOR_BUFFER = int(os.getenv("DEFAULT_CURSOR_BUFFER", "200"))

# Streaming defaults
STREAM_CHUNK_ROWS = int(os.getenv("STREAM_CHUNK_ROWS", "1000"))
STREAM_FLUSH_EVERY = int(os.getenv("STREAM_FLUSH_EVERY", "1"))

# Cursor store settings
CURSOR_TTL_SECONDS = int(os.getenv("CURSOR_TTL_SECONDS", "900"))       # 15 min
CURSOR_CLEAN_INTERVAL = int(os.getenv("CURSOR_CLEAN_INTERVAL", "60"))  # seconds
CURSOR_BATCH_ROWS = int(os.getenv("CURSOR_BATCH_ROWS", "1000"))

# --------------------------------------------------------------------
# Logging
# --------------------------------------------------------------------
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
log = logging.getLogger("hazelcast-rest")

# --------------------------------------------------------------------
# SQLAlchemy Engine + event hook
# --------------------------------------------------------------------
engine: Engine = create_engine(
    DB_URL,
    connect_args=CONNECT_ARGS,
    pool_size=POOL_SIZE,
    max_overflow=MAX_OVERFLOW,
    pool_timeout=POOL_TIMEOUT,
    pool_recycle=POOL_RECYCLE,
    pool_pre_ping=False,
    future=True,
)

@event.listens_for(Engine, "before_cursor_execute")
def _inject_hz_options(conn, cursor, statement, parameters, context, executemany):
    """
    Bridge SQLAlchemy execution_options -> DB-API cursor fields.
    Your dbapi.Cursor reads:
      - cursor._timeout (seconds)
      - cursor.arraysize (also used as Hazelcast cursor_buffer_size)
    """
    opts = context.execution_options or {}
    if "hz_timeout" in opts:
        try:
            cursor._timeout = float(opts["hz_timeout"])
        except Exception:
            pass
    if "hz_cursor_buffer_size" in opts:
        try:
            cursor.arraysize = int(opts["hz_cursor_buffer_size"])
        except Exception:
            pass

# --------------------------------------------------------------------
# FastAPI App + middleware
# --------------------------------------------------------------------
app = FastAPI(title="Hazelcast SQL REST API", version="1.0.0")

app.add_middleware(GZipMiddleware, minimum_size=1024)
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# --------------------------------------------------------------------
# Simple per-IP token bucket rate limiter
# --------------------------------------------------------------------
class TokenBucket:
    __slots__ = ("rate", "burst", "tokens", "timestamp")
    def __init__(self, rate: float, burst: int):
        self.rate = max(rate, 0.001)
        self.burst = max(burst, 1)
        self.tokens = float(burst)
        self.timestamp = monotonic()
    def consume(self, n: float = 1.0) -> bool:
        now = monotonic()
        elapsed = now - self.timestamp
        self.timestamp = now
        self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
        if self.tokens >= n:
            self.tokens -= n
            return True
        return False

_buckets: Dict[str, TokenBucket] = defaultdict(lambda: TokenBucket(RATE_LIMIT_RPS, RATE_LIMIT_BURST))

async def rate_limit(request: Request):
    ip = request.client.host if request.client else "unknown"
    bucket = _buckets[ip]
    if not bucket.consume():
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

# --------------------------------------------------------------------
# Models
# --------------------------------------------------------------------
SQL_MAX_LEN = 200_000  # guardrail

class QueryRequest(BaseModel):
    sql: constr(strip_whitespace=True, min_length=1, max_length=SQL_MAX_LEN)
    params: Optional[Dict[str, Any]] = Field(default_factory=dict)
    limit: Optional[int] = Field(None, ge=1)
    offset: Optional[int] = Field(None, ge=0)
    timeout_sec: Optional[float] = Field(None, gt=0)
    cursor_buffer_size: Optional[int] = Field(None, ge=1, le=100000)
    @validator("params")
    def _params_not_none(cls, v):
        return v or {}

class ExecResponse(BaseModel):
    columns: List[str]
    rows: List[Dict[str, Any]]
    rowcount: Optional[int] = None
    elapsed_ms: int

class ErrorResponse(BaseModel):
    error: str
    detail: Optional[str] = None
    request_id: Optional[str] = None

class StreamRequest(BaseModel):
    sql: constr(strip_whitespace=True, min_length=1, max_length=SQL_MAX_LEN)
    params: Optional[Dict[str, Any]] = Field(default_factory=dict)
    timeout_sec: Optional[float] = Field(None, gt=0)
    cursor_buffer_size: Optional[int] = Field(None, ge=1, le=100000)
    limit: Optional[int] = Field(None, ge=1)
    offset: Optional[int] = Field(None, ge=0)
    @validator("params")
    def _sparams_not_none(cls, v):
        return v or {}

class CursorCreateRequest(BaseModel):
    sql: constr(strip_whitespace=True, min_length=1, max_length=SQL_MAX_LEN)
    # Use positional parameters for qmark style
    params: Optional[List[Any]] = Field(default_factory=list)
    timeout_sec: Optional[float] = Field(None, gt=0)
    cursor_buffer_size: Optional[int] = Field(None, ge=1, le=100000)

class CursorCreateResponse(BaseModel):
    cursor_id: str
    columns: List[str]
    expires_at: str  # ISO8601

class CursorNextResponse(BaseModel):
    cursor_id: str
    rows: List[List[Any]]
    columns: Optional[List[str]] = None
    exhausted: bool
    expires_at: str

class CursorCloseResponse(BaseModel):
    cursor_id: str
    closed: bool

# --------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------
READ_ONLY_PREFIXES = ("SELECT", "WITH", "SHOW", "EXPLAIN", "DESCRIBE")

def _classify_sql(sql: str) -> str:
    return sql.lstrip().split(None, 1)[0].upper() if sql.strip() else ""

def _enforce_read_only(sql: str):
    head = _classify_sql(sql)
    if head in READ_ONLY_PREFIXES:
        return
    # Everything else is forbidden, no env toggles
    raise HTTPException(status_code=403, detail=f"Statement '{head}' is not permitted (read-only API)")

def _wrap_with_limit_offset(sql: str, limit: Optional[int], offset: Optional[int]) -> Tuple[str, Dict[str, Any]]:
    if limit is None and offset is None:
        return sql, {}
    head = _classify_sql(sql)
    if head not in ("SELECT", "WITH", "EXPLAIN", "SHOW", "DESCRIBE"):
        return sql, {}
    eff_limit = min(limit if limit is not None else DEFAULT_LIMIT, MAX_LIMIT)
    eff_offset = offset or 0
    wrapped = f"SELECT * FROM ( {sql} ) t LIMIT :_lim OFFSET :_off"
    return wrapped, {"_lim": eff_limit, "_off": eff_offset}

def _request_id() -> str:
    return uuid.uuid4().hex[:16]

# --------------------------------------------------------------------
# Middleware for request ID & structured errors
# --------------------------------------------------------------------
@app.middleware("http")
async def add_request_id_and_log(request: Request, call_next):
    rid = request.headers.get("X-Request-ID", _request_id())
    start = time.time()
    try:
        response: Response = await call_next(request)
    except HTTPException as he:
        payload = ErrorResponse(error=he.detail, request_id=rid).dict()
        return JSONResponse(status_code=he.status_code, content=payload)
    except Exception as e:
        log.exception("Unhandled error")
        payload = ErrorResponse(error="Internal Server Error", detail=str(e), request_id=rid).dict()
        return JSONResponse(status_code=500, content=payload)
    finally:
        dur_ms = int((time.time() - start) * 1000)
        log.info(f"{request.method} {request.url.path} {dur_ms}ms rid={rid}")
    response.headers["X-Request-ID"] = rid
    return response

# --------------------------------------------------------------------
# Cursor store (stateful)
# --------------------------------------------------------------------
class _CursorState:
    __slots__ = ("dbc", "cur", "cols", "deadline", "timeout", "buffer", "created", "last_access")
    def __init__(self, dbc, cur, cols, ttl_sec: int, timeout: float, buffer_size: int):
        self.dbc = dbc
        self.cur = cur
        self.cols = cols
        self.deadline = datetime.utcnow() + timedelta(seconds=ttl_sec)
        self.timeout = timeout
        self.buffer = buffer_size
        self.created = datetime.utcnow()
        self.last_access = self.created
    def touch(self, ttl_sec: int):
        self.last_access = datetime.utcnow()
        self.deadline = datetime.utcnow() + timedelta(seconds=ttl_sec)

_cursors: Dict[str, _CursorState] = {}
_cursors_lock = Lock()

def _close_cursor_state(state: _CursorState):
    try:
        if state.cur:
            try:
                state.cur.close()
            except Exception:
                pass
        if state.dbc:
            try:
                state.dbc.close()
            except Exception:
                pass
    finally:
        state.cur = None
        state.dbc = None

async def _sweep_cursors_task():
    while True:
        await asyncio.sleep(CURSOR_CLEAN_INTERVAL)
        now = datetime.utcnow()
        to_close: List[Tuple[str, _CursorState]] = []
        with _cursors_lock:
            for cid, st in list(_cursors.items()):
                if st.deadline <= now:
                    to_close.append((cid, st))
                    _cursors.pop(cid, None)
        for _, st in to_close:
            _close_cursor_state(st)

@app.on_event("startup")
async def _startup_tasks():
    asyncio.create_task(_sweep_cursors_task())

# --------------------------------------------------------------------
# Endpoints
# --------------------------------------------------------------------
@app.get("/health")
async def health():
    return {"status": "ok"}

@app.get("/schemas", dependencies=[Depends(rate_limit)])
def list_schemas():
    insp = inspect(engine)
    # This invokes HazelcastDialect.get_schema_names()
    return insp.get_schema_names()

@app.get("/tables", dependencies=[Depends(rate_limit)])
def list_tables(schema: str = "public", include_views: bool = False):
    insp = inspect(engine)
    tables = insp.get_table_names(schema=schema)
    if include_views:
        views = insp.get_view_names(schema=schema)
        # preserve order; de-dup if a name appears in both sets
        seen = set()
        out = []
        for name in tables + views:
            if name not in seen:
                seen.add(name)
                out.append(name)
        return out
    return tables

def _sa_type_to_api(type_obj) -> Tuple[str, Optional[int]]:
    """
    Convert a SQLAlchemy type object (from dialect.get_columns) to:
      - data_type: UPPER token like 'INTEGER', 'VARCHAR', 'TIMESTAMP', etc.
      - length: optional length for VARCHAR/CHAR if available, else None
    """
    # __visit_name__ is stable across SA types (Integer -> 'integer', String -> 'string', etc.)
    name = getattr(type_obj, "__visit_name__", None)
    if not name:
        return (type(type_obj).__name__.upper(), None)
    token = name.upper()
    # Normalize common Hazelcast-friendly tokens
    if token in ("STRING", "VARCHAR", "CHAR"):
        length = getattr(type_obj, "length", None)
        return ("VARCHAR", length)
    if token == "FLOAT":
        # In SA, Float covers DOUBLE/FLOAT/REAL; dialect mapped DOUBLE->Float already
        return ("FLOAT", None)
    if token == "NUMERIC":
        return ("NUMERIC", None)
    if token == "BOOLEAN":
        return ("BOOLEAN", None)
    if token == "INTEGER":
        return ("INTEGER", None)
    if token == "BIGINTEGER":
        return ("BIGINT", None)
    if token == "SMALLINTEGER":
        return ("SMALLINT", None)
    if token == "TIMESTAMP":
        return ("TIMESTAMP", None)
    if token == "DATE":
        return ("DATE", None)
    if token == "TIME":
        return ("TIME", None)
    # Fallback: uppercased visit name
    return (token, None)

@app.get("/columns", dependencies=[Depends(rate_limit)])
def list_columns(schema: str, table: str):
    insp = inspect(engine)
    cols = insp.get_columns(table_name=table, schema=schema)
    out = []
    for c in cols:
        # Reuse the dialect’s type object but serialize it to something JSON-friendly
        sa_type = c["type"]
        dtype = getattr(sa_type, "__visit_name__", str(sa_type)).upper()
        length = getattr(sa_type, "length", None)

        out.append({
            "name": c["name"],
            "data_type": dtype,
            "nullable": c["nullable"],
            "length": length,
            "default": c["default"],
        })

    return out

@app.post("/query", response_model=ExecResponse, dependencies=[Depends(rate_limit)])
def run_query(req: QueryRequest):
    _enforce_read_only(req.sql)
    wrapped_sql, paging_params = _wrap_with_limit_offset(req.sql, req.limit, req.offset)

    params = dict(req.params or {})
    if "_lim" in params or "_off" in params:
        raise HTTPException(status_code=400, detail="Parameter names '_lim' and '_off' are reserved")
    params.update(paging_params)

    exec_opts = {
        "hz_timeout": req.timeout_sec if req.timeout_sec is not None else DEFAULT_TIMEOUT,
        "hz_cursor_buffer_size": req.cursor_buffer_size if req.cursor_buffer_size is not None else DEFAULT_CURSOR_BUFFER,
    }

    started = time.time()
    try:
        with engine.connect() as conn:
            conn = conn.execution_options(**exec_opts)
            result = conn.execute(text(wrapped_sql), params)

            head = _classify_sql(req.sql)
            if head in ("INSERT", "UPDATE", "DELETE", "MERGE"):
                rc = result.rowcount
                return ExecResponse(columns=[], rows=[], rowcount=rc, elapsed_ms=int((time.time() - started) * 1000))

            cols = list(result.keys())
            rows = []
            batch = result.fetchmany(size=int(exec_opts["hz_cursor_buffer_size"]) or DEFAULT_CURSOR_BUFFER)
            while batch:
                rows.extend(dict(zip(cols, r)) for r in batch)
                batch = result.fetchmany(size=int(exec_opts["hz_cursor_buffer_size"]) or DEFAULT_CURSOR_BUFFER)

            elapsed = int((time.time() - started) * 1000)
            return ExecResponse(columns=cols, rows=rows, rowcount=None, elapsed_ms=elapsed)

    except SQLAlchemyError as e:
        raise HTTPException(status_code=400, detail=str(e))

# -------------------- Stateless NDJSON Streaming --------------------
@app.post("/stream", dependencies=[Depends(rate_limit)])
def stream_rows(req: StreamRequest):
    _enforce_read_only(req.sql)
    wrapped_sql, paging_params = _wrap_with_limit_offset(req.sql, req.limit, req.offset)

    params = dict(req.params or {})
    if "_lim" in params or "_off" in params:
        raise HTTPException(status_code=400, detail="Parameter names '_lim' and '_off' are reserved")
    params.update(paging_params)

    exec_opts = {
        "hz_timeout": req.timeout_sec if req.timeout_sec is not None else DEFAULT_TIMEOUT,
        "hz_cursor_buffer_size": req.cursor_buffer_size if req.cursor_buffer_size is not None else DEFAULT_CURSOR_BUFFER,
    }

    def row_generator():
        with engine.connect() as conn:
            conn = conn.execution_options(**exec_opts)
            result = conn.execute(text(wrapped_sql), params)

            head = _classify_sql(req.sql)
            if head in ("INSERT", "UPDATE", "DELETE", "MERGE"):
                payload = {"columns": [], "rows": [], "rowcount": result.rowcount}
                yield json.dumps(payload) + "\n"
                return

            cols = list(result.keys())
            # header line (optional for clients)
            yield json.dumps({"columns": cols, "rows": []}) + "\n"

            chunk: List[Dict[str, Any]] = []
            count = 0
            for row in result:
                chunk.append(dict(zip(cols, row)))
                if len(chunk) >= STREAM_CHUNK_ROWS:
                    yield json.dumps({"rows": chunk}) + "\n"
                    chunk.clear()
                    count += 1
                    if count % STREAM_FLUSH_EVERY == 0:
                        pass
            if chunk:
                yield json.dumps({"rows": chunk}) + "\n"

    return StreamingResponse(row_generator(), media_type="application/x-ndjson")

# -------------------- Stateful Cursor API --------------------------
@app.post("/cursor", response_model=CursorCreateResponse, dependencies=[Depends(rate_limit)])
def cursor_create(req: CursorCreateRequest):
    _enforce_read_only(req.sql)

    # raw DB-API connection so the cursor persists across requests
    dbc = engine.raw_connection()
    cur = dbc.cursor()

    cur._timeout = req.timeout_sec if req.timeout_sec is not None else DEFAULT_TIMEOUT
    cur.arraysize = req.cursor_buffer_size if req.cursor_buffer_size is not None else DEFAULT_CURSOR_BUFFER

    try:
        params_tuple = tuple(req.params or [])
        cur.execute(req.sql, params_tuple)
        log.debug("cursor_create: executed OK, desc cols=%s", [c[0] for c in (cur.description or [])])

        cols = [c[0] for c in (cur.description or [])] if cur.description else []
        if not cur.description:
            # no rows/DDL/etc. For a read-only API, we should treat this as error.
            raise HTTPException(status_code=400, detail="Statement did not produce a result set")
    except Exception as e:
        try:
            cur.close()
        except Exception:
            pass
        try:
            dbc.close()
        except Exception:
            pass
        raise HTTPException(status_code=400, detail=str(e))

    cursor_id = uuid.uuid4().hex
    state = _CursorState(
        dbc=dbc,
        cur=cur,
        cols=cols,
        ttl_sec=CURSOR_TTL_SECONDS,
        timeout=cur._timeout,
        buffer_size=cur.arraysize,
    )
    with _cursors_lock:
        _cursors[cursor_id] = state

    return CursorCreateResponse(
        cursor_id=cursor_id,
        columns=cols,
        expires_at=(datetime.utcnow() + timedelta(seconds=CURSOR_TTL_SECONDS)).isoformat() + "Z",
    )

@app.get("/cursor/{cursor_id}/next", response_model=CursorNextResponse, dependencies=[Depends(rate_limit)])
def cursor_next(cursor_id: str, rows: Optional[int] = None, include_columns: bool = False):
    batch_size = rows or CURSOR_BATCH_ROWS
    with _cursors_lock:
        state = _cursors.get(cursor_id)
    if not state:
        raise HTTPException(status_code=404, detail="Cursor not found or expired")
    # NEW: sanity – executed?
    if getattr(state.cur, "description", None) is None:
        # translate to client-friendly status instead of surfacing DB-API Error
        _close_cursor_state(state)
        with _cursors_lock:
            _cursors.pop(cursor_id, None)
        raise HTTPException(status_code=409, detail="Cursor not ready: execute() did not complete")

    try:
        state.cur.arraysize = batch_size
        data = state.cur.fetchmany(batch_size)
        exhausted = len(data) == 0
        if exhausted:
            _close_cursor_state(state)
            with _cursors_lock:
                _cursors.pop(cursor_id, None)
            return CursorNextResponse(
                cursor_id=cursor_id,
                rows=[],
                columns=state.cols if include_columns else None,
                exhausted=True,
                expires_at=datetime.utcnow().isoformat() + "Z",
            )
        else:
            state.touch(CURSOR_TTL_SECONDS)
            with _cursors_lock:
                _cursors[cursor_id] = state
            return CursorNextResponse(
                cursor_id=cursor_id,
                rows=[list(r) for r in data],
                columns=state.cols if include_columns else None,
                exhausted=False,
                expires_at=state.deadline.isoformat() + "Z",
            )
    except Exception as e:
        _close_cursor_state(state)
        with _cursors_lock:
            _cursors.pop(cursor_id, None)
        raise HTTPException(status_code=400, detail=str(e))

@app.delete("/cursor/{cursor_id}", response_model=CursorCloseResponse, dependencies=[Depends(rate_limit)])
def cursor_close(cursor_id: str):
    with _cursors_lock:
        state = _cursors.pop(cursor_id, None)
    if not state:
        return CursorCloseResponse(cursor_id=cursor_id, closed=False)
    _close_cursor_state(state)
    return CursorCloseResponse(cursor_id=cursor_id, closed=True)
