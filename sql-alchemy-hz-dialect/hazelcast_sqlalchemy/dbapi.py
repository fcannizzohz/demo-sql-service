import hazelcast
from typing import Optional, Tuple, Sequence, List

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

    def commit(self): pass
    def rollback(self): pass

    def close(self):
        if self._client is not None:
            try:
                self._client.shutdown()
            finally:
                self._client = None

    def __enter__(self): return self
    def __exit__(self, exc_type, exc, tb): self.close()

def connect(host: str, port: int, timeout: Optional[float] = None, **kwargs) -> Connection:
    client = hazelcast.HazelcastClient(
        cluster_members=[f"{host}:{port}"],
        **{k: v for k, v in kwargs.items() if k != "timeout"}
    )
    return Connection(client, default_timeout=timeout)
