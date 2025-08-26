# hazelcast_sqlalchemy/dialect.py
"""
SQLAlchemy dialect for Hazelcast Python client.
"""

from sqlalchemy.engine import default
from sqlalchemy import types as sqltypes
from sqlalchemy import text

class HazelcastDialect(default.DefaultDialect):
    name = "hazelcast"
    driver = "python"
    paramstyle = "qmark"

    supports_statement_cache = True

    supports_sane_rowcount = True
    supports_native_boolean = True
    supports_sane_multi_rowcount = True
    default_schema_name = "public"

    ischema_names = {
        "BOOLEAN": sqltypes.Boolean,
        "TINYINT": sqltypes.SmallInteger,  # or custom
        "SMALLINT": sqltypes.SmallInteger,
        "INTEGER": sqltypes.Integer,
        "INT": sqltypes.Integer,
        "BIGINT": sqltypes.BigInteger,
        "REAL": sqltypes.Float,      # or sqltypes.REAL
        "FLOAT": sqltypes.Float,
        "DOUBLE": sqltypes.Float,    # SA Float maps to DOUBLE PRECISION by default
        "DECIMAL": sqltypes.Numeric,
        "NUMERIC": sqltypes.Numeric,
        "VARCHAR": sqltypes.String,
        "CHAR": sqltypes.CHAR,
        "TEXT": sqltypes.Text,
        "DATE": sqltypes.Date,
        "TIME": sqltypes.Time,
        "TIMESTAMP": sqltypes.TIMESTAMP,
        # Add more Hazelcast types here as needed (OBJECT, JSON?, UUID?)
    }

    @classmethod
    def import_dbapi(cls):
        """
        Return the DBAPI module that implements .connect(), Cursor, etc.
        """
        import hazelcast_sqlalchemy.dbapi as dbapi_module
        return dbapi_module

    @classmethod
    def dbapi(cls):
        # alias for backward compatibility
        return cls.import_dbapi()

    def create_connect_args(self, url):
        host = url.host
        port = url.port
        # url.query is a immutabledict mapping str -> str | list[str]
        q = url.query
        connect_kwargs = {k: (v[-1] if isinstance(v, (list, tuple)) else v) for k, v in q.items()}

        if "timeout" in connect_kwargs:
            connect_kwargs["timeout"] = float(connect_kwargs["timeout"])

        # Optional: allow "cluster_members" as comma-separated list
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
        # Or support DSN-less forms as needed.
        return dbapi.connect(**cparams)

    def do_execute(self, cursor, statement, parameters, context=None):
        """
        Execute a SQL statement using our DBAPI Cursor.
        """
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
        """
        Return list of available schemas in Hazelcast.
        """
        return ["hazelcast", "public"]

    def get_table_names(self, connection, schema=None, **kwargs):
        """
        Return list of mapping names (tables) in a given schema.
        """
        try:
            sch = schema or self.default_schema_name
            result = connection.execute(text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = :schema "
            ), {"schema": sch})
            return [row[0] for row in result.fetchall()]
        except Exception as e:
            print(e)
            raise Exception(f"Unable to get table names for schema {schema}: {e}")

    def has_table(self, connection, table_name, schema=None, **kw):
        sch = schema or self.default_schema_name
        res = connection.exec_driver_sql(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ?",
            (sch, table_name)
        ).fetchone()
        return res is not None

    def get_view_names(self, connection, schema=None, **kwargs):
        """
        Return list of mapping names (tables) in a given schema.
        """
        try:
            sch = schema or self.default_schema_name
            result = connection.execute(text(
                "SELECT mapping_name FROM information_schema.views "
                "WHERE table_schema = :schema"
            ), {"schema": sch})
            return [row[0] for row in result.fetchall()]
        except Exception as e:
            raise Exception(f"Unable to get table names for schema {schema}: {e}")

    def get_columns(self, connection, table_name, schema=None, **kwargs):
        """
        Return column metadata list for a given table.
        Each column is a dict with keys: name, type, nullable.
        """
        sch = schema or self.default_schema_name
        query = text("""
                     SELECT column_name, data_type, is_nullable, character_maximum_length
                     FROM information_schema.columns
                     WHERE table_schema = :schema AND table_name = :table
                     ORDER BY ordinal_position
                     """)
        rows = connection.execute(query, {"schema": sch, "table": table_name})

        cols = []
        for name, dtype, nullable, char_len in rows.fetchall():
            t = (dtype or "").upper()
            if t in ("INT", "INTEGER"):
                coltype = sqltypes.Integer()
            elif t in ("BIGINT",):
                coltype = sqltypes.BigInteger()
            elif t in ("TINYINT", "SMALLINT"):
                coltype = sqltypes.SmallInteger()
            elif t in ("DOUBLE", "FLOAT", "REAL"):
                coltype = sqltypes.Float()
            elif t in ("DECIMAL", "NUMERIC"):
                coltype = sqltypes.Numeric()
            elif t in ("BOOLEAN", "BOOL"):
                coltype = sqltypes.Boolean()
            elif t in ("DATE",):
                coltype = sqltypes.Date()
            elif t in ("TIME",):
                coltype = sqltypes.Time()
            elif t in ("TIMESTAMP",):
                coltype = sqltypes.TIMESTAMP()
            else:
                coltype = sqltypes.String(length=char_len) if char_len else sqltypes.String()

            cols.append({
                "name": name,
                "type": coltype,
                "nullable": (str(nullable).upper() == "YES"),
                "default": None,
            })
        return cols

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        return {"constrained_columns": [], "name": None}

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        return []

    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        return []

# Register this dialect for URLs of the form hazelcast+python://
from sqlalchemy.dialects import registry
registry.register(
    "hazelcast.python",
    "hazelcast_sqlalchemy.dialect",
    "HazelcastDialect"
)
