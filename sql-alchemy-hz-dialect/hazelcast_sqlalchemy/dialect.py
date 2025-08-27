from sqlalchemy.engine import default
from sqlalchemy import types as sqltypes
from sqlalchemy import text

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

    default_schema_name = "public"

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
        res = connection.execute(text("SELECT schema_name FROM information_schema.schemata"))
        return [r[0] for r in res.fetchall()]

    def get_table_names(self, connection, schema=None, **kwargs):
        sch = schema or self.default_schema_name
        res = connection.execute(text(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = :schema AND table_type = 'BASE TABLE'"
        ), {"schema": sch})
        return [r[0] for r in res.fetchall()]

    def get_view_names(self, connection, schema=None, **kwargs):
        sch = schema or self.default_schema_name
        res = connection.execute(text(
            "SELECT table_name FROM information_schema.views "
            "WHERE table_schema = :schema"
        ), {"schema": sch})
        return [r[0] for r in res.fetchall()]

    def get_columns(self, connection, table_name, schema=None, **kwargs):
        sch = schema or self.default_schema_name
        rows = connection.execute(text("""
                                       SELECT column_name, data_type, is_nullable, character_maximum_length
                                       FROM information_schema.columns
                                       WHERE table_schema = :schema AND table_name = :table
                                       ORDER BY ordinal_position
                                       """), {"schema": sch, "table": table_name})

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
            elif t == "DATE":
                coltype = sqltypes.Date()
            elif t == "TIME":
                coltype = sqltypes.Time()
            elif t == "TIMESTAMP":
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

    def has_table(self, connection, table_name, schema=None, **kw):
        sch = schema or self.default_schema_name
        row = connection.execute(text(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = :schema AND table_name = :table"
        ), {"schema": sch, "table": table_name}).fetchone()
        return row is not None

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
