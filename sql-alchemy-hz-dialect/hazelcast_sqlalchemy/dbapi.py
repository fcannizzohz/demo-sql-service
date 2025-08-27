import hazelcast
from typing import Optional, Tuple, Sequence, List, Union

# PEP-249
paramstyle = "qmark"

class Warning(Exception): pass
class Error(Exception): pass
class InterfaceError(Error): pass
class DatabaseError(Error): pass
class DataError(DatabaseError): pass
class OperationalError(DatabaseError): pass
class IntegrityError(DatabaseError): pass
class InternalError(DatabaseError): pass
class ProgrammingError(DatabaseError): pass
class NotSupportedError(DatabaseError): pass

_DEFAULT_TIMEOUT = 30

def _normalize_members(
        host: Optional[str],
        port: Optional[int],
        cluster_members: Optional[Union[str, Sequence[str]]],
) -> List[str]:
    """
    Produce a clean list of member endpoints like ['host:5701', ...].
    - If cluster_members is provided (str or list), use it.
    - Else, fall back to host+port if both present.
    - Raise if neither is provided.
    """
    members: List[str] = []

    if cluster_members:
        if isinstance(cluster_members, str):
            # allow comma-separated "h1:5701,h2:5701"
            parts = [p.strip() for p in cluster_members.split(",") if p.strip()]
            members.extend(parts)
        else:
            members.extend([str(m).strip() for m in cluster_members if str(m).strip()])

    if not members:
        if host and port:
            members = [f"{host}:{port}"]
        else:
            raise InterfaceError(
                "connect() requires either cluster_members or both host and port"
            )

    return members

class Cursor:
    def __init__(self, client, timeout: Optional[float] = None):
        self._client = client
        self._result = None
        self._iterator = None
        self.description = None
        self.rowcount = -1
        self.arraysize = 100
        self.lastrowid = None
        self._timeout = timeout or _DEFAULT_TIMEOUT

    def execute(self, operation: str, parameters: Optional[Tuple] = None) -> None:
        if self._result is not None:
            self.close()

        params = parameters or ()
        if isinstance(params, dict):
            raise ProgrammingError("qmark paramstyle requires positional parameters (tuple/list)")

        try:
            self._result = self._client.sql.execute(
                operation, *params,
                timeout=self._timeout,
                cursor_buffer_size=self.arraysize
            ).result()
        except Exception as e:
            raise DatabaseError(f"Hazelcast execute failed: {e}") from e

        meta = self._result.get_row_metadata()
        self.description = None if meta is None else [
            (col.name, None, None, None, None, None, None) for col in meta.columns
        ]

        uc = self._result.update_count()
        self.rowcount = -1 if uc is None else uc

        # Iterator after we know if it's a row-producing statement
        self._iterator = iter(self._result) if self.description is not None else None

    def executemany(self, operation: str, seq_of_params: Sequence[Tuple]) -> None:
        total = 0
        last_desc = None
        for params in seq_of_params:
            self.execute(operation, params)
            if self.rowcount != -1:
                total += self.rowcount
            last_desc = self.description
            self.close()
        self.description = last_desc
        self.rowcount = total if total else -1

    def fetchone(self):
        if self._iterator is None:
            if self._result is None:
                raise Error("fetchone() called before execute()")
            # DML/DDL: nothing to fetch
            return None
        try:
            row = next(self._iterator)
            return tuple(row)
        except StopIteration:
            # Free server-side resources but KEEP description for metadata consumers
            try:
                if self._result is not None:
                    self._result.close()
            finally:
                self._result = None
                self._iterator = None
            return None

    def fetchmany(self, size: Optional[int] = None) -> List[Tuple]:
        n = size or self.arraysize
        rows = []
        for _ in range(n):
            r = self.fetchone()
            if r is None:
                break
            rows.append(r)
        return rows

    def fetchall(self) -> List[Tuple]:
        rows = []
        while True:
            r = self.fetchone()
            if r is None:
                break
            rows.append(r)
        return rows

    def __iter__(self):
        if self._iterator is None:
            raise Error("__iter__ called before execute() or statement does not return rows")
        return self

    def __next__(self):
        r = self.fetchone()
        if r is None:
            raise StopIteration
        return r

    def setinputsizes(self, sizes): return None
    def setoutputsize(self, size, column=None): return None

    def close(self) -> None:
        try:
            if self._result is not None:
                try:
                    self._result.close()
                except Exception:
                    pass
        finally:
            self._result = None
            self._iterator = None
            # self.description = None
            self.rowcount = -1

    def __enter__(self): return self
    def __exit__(self, exc_type, exc, tb): self.close()

class Connection:
    def __init__(self, client, default_timeout: Optional[float] = None):
        self._client = client
        self._default_timeout = default_timeout

    def cursor(self):
        return Cursor(self._client, timeout=self._default_timeout)

    def commit(self):  # autocommit semantics
        pass

    def rollback(self):
        pass

    def close(self):
        if self._client is not None:
            try:
                self._client.shutdown()
            finally:
                self._client = None

    def __enter__(self): return self
    def __exit__(self, exc_type, exc, tb): self.close()

def connect(
        host: Optional[str] = None,
        port: Optional[int] = None,
        timeout: Optional[float] = None,
        cluster_members: Optional[Union[str, Sequence[str]]] = None,
        **kwargs
) -> Connection:
    """
    Create a HazelcastClient and wrap it in a DB-API Connection.

    Parameters
    ----------
    host, port : optional
        Single member address (used if cluster_members not provided).
    cluster_members : str | list[str], optional
        Comma-separated string or list of 'host:port' entries; takes precedence
        over host/port when provided.
    timeout : float, optional
        Default SQL execution timeout (seconds) for cursors from this connection.
        (Not the Hazelcast client connection timeout.)
    **kwargs :
        Passed through to hazelcast.HazelcastClient(...), e.g.:
        cluster_name, smart_routing, ssl_* options, connection_timeout, etc.
    """
    members = _normalize_members(host, port, cluster_members)

    # Ensure we don't accidentally forward our SQL timeout into the client kwargs
    client_kwargs = dict(kwargs)  # shallow copy
    # Common pitfall: users might supply 'timeout' expecting client connect timeout.
    # We reserve 'timeout' here for SQL execution. If they need client connect timeouts,
    # they should pass the appropriate hazelcast client options (e.g. connection_timeout).
    if "timeout" in client_kwargs:
        client_kwargs.pop("timeout", None)

    client = hazelcast.HazelcastClient(
        cluster_members=members,
        **client_kwargs
    )
    return Connection(client, default_timeout=timeout)