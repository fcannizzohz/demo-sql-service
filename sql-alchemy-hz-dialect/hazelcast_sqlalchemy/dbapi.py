import atexit
import hazelcast
from typing import Optional, Tuple, Sequence, List

_shared_client = None

# PEP-249
paramstyle = "qmark"
class Error(Exception): pass

_DEFAULT_TIMEOUT = 30

class Cursor:
    def __init__(self, client):
        self._client    = client
        self._result    = None
        self._iterator  = None
        self.description = None
        self.rowcount   = -1
        self.arraysize  = 100  # default page size

    def execute(self, operation: str, parameters: Optional[Tuple] = None) -> None:
        """
        Bind any positional parameters (e.g. LIMIT ? OFFSET ?) and kick off execution,
        but do NOT fetch all rows yet.
        """
        timeout_secs = getattr(self, "_timeout", _DEFAULT_TIMEOUT)
        params = parameters or ()

        # HazSQL client wants varargs for parameters:
        # e.g. .execute(sql, limit, offset, timeout=..., cursor_buffer_size=...)
        try:
            self._result = (
                self._client.sql
                .execute(
                    operation,
                    *params,
                    timeout=timeout_secs,
                    cursor_buffer_size=self.arraysize
                )
                .result()
            )
        except Exception as e:
            raise Error(f"Hazelcast execute failed: {e}") from e

        # Build cursor.description from metadata
        meta = self._result.get_row_metadata()
        self.description = [
            (col.name, None, None, None, None, None, None)
            for col in meta.columns
        ]

        # Create the blocking iterator that fetches pages under the hood
        self._iterator = iter(self._result)
        self.rowcount = -1

    def executemany(self, operation: str, seq_of_params: Sequence[Tuple]) -> None:
        """
        Run the same statement multiple times. Superset won't use this,
        but PEP-249 expects it.
        """
        for params in seq_of_params:
            self.execute(operation, params)

    def fetchone(self) -> Optional[Tuple]:
        """
        Return one row (tuple) or None when exhausted.
        """
        if self._iterator is None:
            raise Error("fetchone() called before execute()")

        try:
            row = next(self._iterator)
        except StopIteration:
            return None

        return tuple(row)

    def fetchmany(self, size: Optional[int] = None) -> List[Tuple]:
        """
        Return up to `size` rows (default: self.arraysize).
        Superset’s backend calls this repeatedly to page results.
        """
        n = size or self.arraysize
        rows = []
        for _ in range(n):
            r = self.fetchone()
            if r is None:
                break
            rows.append(r)
        return rows

    def fetchall(self) -> List[Tuple]:
        """
        Drain the remaining iterator.  Used if someone calls .all().
        """
        return list(self)

    def __iter__(self):
        if self._iterator is None:
            raise Error("__iter__ called before execute()")
        return self

    def __next__(self):
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row

    def close(self) -> None:
        """
        Cursor close does not shut down the client.  Just free refs.
        """
        self._result   = None
        self._iterator = None

class Connection:
    def __init__(self, client):
        self._client = client

    def cursor(self):
        return Cursor(self._client)

    def commit(self): pass
    def rollback(self): pass
    def close(self): pass  # keep the client alive

def connect(
        host: str,
        port: int,
        timeout: Optional[float] = None,
        **kwargs
) -> Connection:
    """
    Return a PEP-249 Connection wrapping a shared HazelcastClient.
    """
    global _shared_client
    if _shared_client is None:
        _shared_client = hazelcast.HazelcastClient(
            cluster_members=[f"{host}:{port}"],
            **{k:v for k,v in kwargs.items() if k != "timeout"}
        )
    return Connection(_shared_client)

@atexit.register
def _shutdown_client():
    if _shared_client:
        _shared_client.shutdown()
