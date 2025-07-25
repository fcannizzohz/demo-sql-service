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
        """
        Parse a URL like hazelcast+python://host:port?opt1=val1 into
        positional args (host, port) and a kwargs dict of the extras.
        """
        host = url.host
        port = url.port
        connect_kwargs = dict(url.query)
        # pop and convert timeout if present
        if "timeout" in connect_kwargs:
            connect_kwargs["timeout"] = float(connect_kwargs.pop("timeout"))
        return [(host, port), connect_kwargs]

    def do_connect(self, host, port, **kwargs):
        """
        Establish a connection via our DBAPI wrapper.
        """
        dbapi = self.import_dbapi()
        return dbapi.connect(host=host, port=port, **kwargs)

    def do_execute(self, cursor, statement, parameters, context=None):
        """
        Execute a SQL statement using our DBAPI Cursor.
        """
        cursor.execute(statement, parameters or {})

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
        query = text(
            "SELECT column_name, data_type, is_nullable "
            "FROM information_schema.columns "
            "WHERE table_schema = :schema AND table_name = :table"
        )
        rows = connection.execute(query, {"schema": sch, "table": table_name})
        cols = []
        for name, dtype, nullable in rows.fetchall():
            t = dtype.upper()
            if t in ("INT", "INTEGER"):
                coltype = sqltypes.Integer()
            elif t in ("DOUBLE", "FLOAT", "REAL"):
                coltype = sqltypes.Float()
            elif t in ("DECIMAL", "NUMERIC"):
                coltype = sqltypes.Numeric()
            else:
                coltype = sqltypes.String()
            cols.append({
                "name": name,
                "type": coltype,
                "nullable": (nullable.upper() == "YES")
            })
        return cols

# Register this dialect for URLs of the form hazelcast+python://
from sqlalchemy.dialects import registry
registry.register(
    "hazelcast.python",
    "hazelcast_sqlalchemy.dialect",
    "HazelcastDialect"
)
