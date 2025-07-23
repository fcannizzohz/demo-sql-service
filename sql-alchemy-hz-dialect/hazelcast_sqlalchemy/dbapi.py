import atexit

_shared_client = None

# PEP-249 paramstyle + Error
paramstyle = "pyformat"
class Error(Exception): pass

import hazelcast

class Cursor:
    def __init__(self, client):
        self._client   = client
        self._result   = None
        self._iterator = None
        self.description = None
        self.rowcount = -1
        # default batch size for fetchmany()
        self.arraysize = 1

    def execute(self, statement, parameters=None):
        timeout_secs = getattr(self, "_timeout", 30)
        ## ignoring parameters for now
        try:
            self._result = self._client.sql.execute(
                sql=statement,
                timeout=timeout_secs
            ).result()
        except Exception as e:
            raise Error(f"{e}") from e

        meta = self._result.get_row_metadata()
        self.description = [
            (col.name, None, None, None, None, None, None)
            for col in meta.columns
        ]

        # the blocking iterator that fetches pages on demand
        self._iterator = iter(self._result)
        self.rowcount = -1
        # reset arraysize if you like
        self.arraysize = 1

    def fetchone(self):
        try:
            row = next(self._iterator)
        except StopIteration:
            return None
        return tuple(row)

    def fetchmany(self, size=None):
        n = size or getattr(self, "arraysize", 1)
        rows = []
        for _ in range(n):
            row = self.fetchone()
            if row is None:
                break
            rows.append(row)
        return rows

    def fetchall(self):
        return list(self)

    def __iter__(self):
        return self

    def __next__(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def close(self):
        # optional: drop references
        self._result = None
        self._iterator = None

class Connection:
    def __init__(self, client):
        self._client = client

    def cursor(self):
        return Cursor(self._client)

    def commit(self): pass
    def rollback(self): pass
    def close(self):
        pass

def connect(host, port, timeout=None, **kwargs):
    # allow passing timeout via connect_args

    global _shared_client
    if _shared_client is None:
        _shared_client = hazelcast.HazelcastClient(
            cluster_members=[f"{host}:{port}"],
            **{k: v for k, v in kwargs.items() if k != "timeout"}
        )
    return Connection(_shared_client)

@atexit.register
def _shutdown_client():
    if _shared_client:
        _shared_client.shutdown()