# PEP-249 paramstyle + Error
paramstyle = "pyformat"
class Error(Exception): pass

import hazelcast

class Cursor:
    def __init__(self, client):
        self._client = client
        self._rows = []
        self.description = None
        self.rowcount = -1

    def execute(self, statement, parameters=None):
        # Must be numeric seconds, not timedelta
        timeout_secs = getattr(self, "_timeout", 30)
        try:
            result = self._client.sql.execute(
                statement,
                # parameters or {},
                timeout=timeout_secs
            ).result()
        except Exception as e:
            raise Error from e

        self.description = [
            (col.name, None, None, None, None, None, None)
            for col in result.get_row_metadata().columns
        ]
        self._rows = [tuple(row) for row in result]
        self.rowcount = len(self._rows)

    def fetchone(self):
        return self._rows.pop(0) if self._rows else None

    def fetchall(self):
        rows, self._rows = self._rows, []
        return rows

    def close(self):
        pass

class Connection:
    def __init__(self, client):
        self._client = client

    def cursor(self):
        return Cursor(self._client)

    def commit(self): pass
    def rollback(self): pass
    def close(self):
        self._client.shutdown()

def connect(host, port, timeout=None, **kwargs):
    # allow passing timeout via connect_args

    client = hazelcast.HazelcastClient(
        cluster_members=[f"{host}:{port}"],
        **{k: v for k, v in kwargs.items() if k != "timeout"}
    )
    return Connection(client)
