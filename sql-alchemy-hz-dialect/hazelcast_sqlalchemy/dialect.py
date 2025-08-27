import re

from sqlalchemy.engine import default
from sqlalchemy import types as sqltypes
from sqlalchemy import text

SQL_SCHEMA = """
             SELECT DISTINCT table_schema FROM information_schema.tables
             UNION
             SELECT table_schema FROM information_schema.views
             """
SQL_TABLES = """
             SELECT table_name FROM information_schema.tables 
             WHERE table_schema = :schema AND table_type = 'BASE TABLE'
             """
SQL_VIEWS = """
            SELECT table_name FROM information_schema.views 
            WHERE table_schema = :schema
            """
SQL_COLUMNS = """
              SELECT column_name, column_external_name, ordinal_position, is_nullable, data_type
              FROM information_schema.columns
              WHERE table_schema = :schema AND table_name = :table
              ORDER BY ordinal_position
              """
SQL_HAS_TABLE = """
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = :schema AND table_name = :table
                """

DEFAULT_SCHEMA_NAME = "public"

_VARCHAR_LEN_RE = re.compile(r"^VARCHAR\s*\(\s*(\d+)\s*\)$", re.IGNORECASE)

class HazelcastDialect(default.DefaultDialect):
    name = "hazelcast"
    driver = "python"
    paramstyle = "qmark"

    # Capabilities
    supports_statement_cache = True
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = True
    supports_native_boolean = True
    supports_native_decimal = True

    ischema_names = {
        "BOOLEAN": sqltypes.Boolean,
        "TINYINT": sqltypes.SmallInteger,
        "SMALLINT": sqltypes.SmallInteger,
        "INTEGER": sqltypes.Integer,
        "INT": sqltypes.Integer,
        "BIGINT": sqltypes.BigInteger,
        "REAL": sqltypes.Float,
        "FLOAT": sqltypes.Float,
        "DOUBLE": sqltypes.Float,
        "DECIMAL": sqltypes.Numeric,
        "NUMERIC": sqltypes.Numeric,
        "VARCHAR": sqltypes.String,
        "CHAR": sqltypes.CHAR,
        "TEXT": sqltypes.Text,
        "DATE": sqltypes.Date,
        "TIME": sqltypes.Time,
        "TIMESTAMP": sqltypes.TIMESTAMP,
    }

    @classmethod
    def import_dbapi(cls):
        import hazelcast_sqlalchemy.dbapi as dbapi_module
        return dbapi_module

    @classmethod
    def dbapi(cls):
        return cls.import_dbapi()

    def create_connect_args(self, url):
        host = url.host
        port = url.port
        q = url.query
        connect_kwargs = {k: (v[-1] if isinstance(v, (list, tuple)) else v) for k, v in q.items()}
        if "timeout" in connect_kwargs:
            connect_kwargs["timeout"] = float(connect_kwargs["timeout"])
        if "cluster_members" in connect_kwargs:
            cm = connect_kwargs["cluster_members"]
            if isinstance(cm, str):
                connect_kwargs["cluster_members"] = [s.strip() for s in cm.split(",") if s.strip()]
        return [(host, port), connect_kwargs]

    def do_connect(self, *cargs, **cparams):
        dbapi = self.import_dbapi()
        if cargs:
            host, port = cargs
            return dbapi.connect(host=host, port=port, **cparams)
        return dbapi.connect(**cparams)

    def do_execute(self, cursor, statement, parameters, context=None):
        if parameters is None:
            cursor.execute(statement)
        else:
            cursor.execute(statement, parameters)

    def do_executemany(self, cursor, statement, parameters, context=None):
        cursor.executemany(statement, parameters or [])

    @classmethod
    def visit_datatype(cls, type_):
        """
        Map Hazelcast SQL type names to SQLAlchemy types.
        """
        t = type_.upper()
        if t in ("INT", "INTEGER"):
            return sqltypes.Integer()
        if t in ("VARCHAR", "CHAR", "TEXT"):
            return sqltypes.String()
        if t == "DOUBLE":
            return sqltypes.Float()
        if t in ("DECIMAL", "NUMERIC"):
            return sqltypes.Numeric()
        return None

    def get_schema_names(self, connection, **kwargs):
        res = connection.execute(text(f"{SQL_SCHEMA}"))
        return [r[0] for r in res.fetchall()]

    def get_table_names(self, connection, schema=None, **kwargs):
        sch = schema or DEFAULT_SCHEMA_NAME
        res = connection.execute(text(f"{SQL_TABLES}"), {"schema": sch})
        return [r[0] for r in res.fetchall()]

    def get_view_names(self, connection, schema=None, **kwargs):
        sch = schema or DEFAULT_SCHEMA_NAME
        res = connection.execute(text(f"{SQL_VIEWS}"), {"schema": sch})
        return [r[0] for r in res.fetchall()]

    def get_indexes(self, connection, table_name, schema=None, **kw):
        # Hazelcast SQL doesn’t expose relational indexes in a way that SQLAlchemy expects.
        # Return an empty list so callers (e.g., Superset) don’t crash.
        return []

    def get_columns(self, connection, table_name, schema=None, **kwargs):
        sch = schema or DEFAULT_SCHEMA_NAME
        res = connection.execute(text(SQL_COLUMNS), {"schema": sch, "table": table_name})

        cols = []
        for row in res:
            m = row._mapping  # stable, name-based access
            name = m["column_name"]
            dtype = (m.get("data_type") or "").strip().upper()
            is_nullable = m.get("is_nullable")

            # normalise nullable across bool/YES/NO
            if isinstance(is_nullable, bool):
                nullable = is_nullable
            else:
                nullable = str(is_nullable).strip().upper() in ("Y", "YES", "TRUE", "T", "1")

            # Optional: try to extract VARCHAR(n) length if server encodes it in data_type
            varchar_len = None
            if dtype.startswith("VARCHAR"):
                mlen = _VARCHAR_LEN_RE.match(dtype)
                if mlen:
                    try:
                        varchar_len = int(mlen.group(1))
                    except ValueError:
                        varchar_len = None
                # normalise dtype to bare token for mapping below
                dtype = "VARCHAR"

            # Map to SQLAlchemy types
            if dtype in ("INT", "INTEGER"):
                coltype = sqltypes.Integer()
            elif dtype == "BIGINT":
                coltype = sqltypes.BigInteger()
            elif dtype in ("TINYINT", "SMALLINT"):
                coltype = sqltypes.SmallInteger()
            elif dtype in ("DOUBLE", "FLOAT", "REAL"):
                coltype = sqltypes.Float()
            elif dtype in ("DECIMAL", "NUMERIC"):
                coltype = sqltypes.Numeric()
            elif dtype in ("BOOLEAN", "BOOL"):
                coltype = sqltypes.Boolean()
            elif dtype == "DATE":
                coltype = sqltypes.Date()
            elif dtype == "TIME":
                coltype = sqltypes.Time()
            elif dtype == "TIMESTAMP":
                coltype = sqltypes.TIMESTAMP()
            elif dtype == "VARCHAR":
                coltype = sqltypes.String(length=varchar_len) if varchar_len else sqltypes.String()
            else:
                # Fallback: treat as string if unknown
                coltype = sqltypes.String()

            cols.append({
                "name": name,
                "type": coltype,
                "nullable": nullable,
                "default": None,
            })
        return cols

    def has_table(self, connection, table_name, schema=None, **kw):
        sch = schema or DEFAULT_SCHEMA_NAME
        row = connection.execute(text(f"{SQL_HAS_TABLE}"), {"schema": sch, "table": table_name}).fetchone()
        return row is not None

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        return {"constrained_columns": [], "name": None}

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        return []

    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        return []

    def get_table_comment(self, connection, table_name, schema=None, **kw):
        # Superset may call this to show descriptions; Hazelcast doesn’t store them.
        return {"text": None}

    def get_view_definition(self, connection, view_name, schema=None, **kw):
        # If Hazelcast exposes definition in the future, you can SELECT it here.
        return None

    def get_check_constraints(self, connection, table_name, schema=None, **kw):
        # Hazelcast SQL doesn’t manage relational CHECK constraints.
        return []

    def get_sequence_names(self, connection, schema=None, **kw):
        # No sequences in Hazelcast.
        return []

    def get_temp_table_names(self, connection, schema=None, **kw):
        # No temporary tables in Hazelcast SQL.
        return []

    def get_table_options(self, connection, table_name, schema=None, **kw):
        sch = schema or self.default_schema_name
        row = connection.exec_driver_sql(
            "SELECT table_type, is_insertable_into, is_typed "
            "FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ?",
            (sch, table_name)
        ).fetchone()
        if not row:
            return {}
        m = row._mapping
        def _to_bool(v):
            if isinstance(v, bool): return v
            return str(v).strip().upper() in ("Y","YES","TRUE","T","1")
        return {
            "hazelcast_table_type": m.get("table_type"),
            "is_insertable_into": _to_bool(m.get("is_insertable_into")),
            "is_typed": _to_bool(m.get("is_typed")),
        }

    # Isolation level (Hazelcast SQL is effectively autocommit)
    def get_isolation_level(self, dbapi_connection):
        return "AUTOCOMMIT"

    def set_isolation_level(self, dbapi_connection, level):
        # No-op; keeping signature for SQLAlchemy.
        pass

# Register this dialect for URLs of the form hazelcast+python://
from sqlalchemy.dialects import registry
registry.register(
    "hazelcast.python",
    "hazelcast_sqlalchemy.dialect",
    "HazelcastDialect"
)
