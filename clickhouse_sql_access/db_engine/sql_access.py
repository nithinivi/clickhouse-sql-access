from typing import Any

from clickhouse_driver import Client


class CHAccess:
    connection_: Client
    connection_string_: str

    def __init__(self, connection_string: str, settings=None):
        self.connection_string_ = connection_string

    def open(self, connection_string: str) -> bool:
        """ Open the Connection. Once the use is over ,
            close should be called.
            can be called with context manager with statement
        """
        self.connection_string_ = connection_string
        if self.connection_ is not None:
            self.connection_.disconnect_connection()
            self.connection_ = None

        self.connection_ = Client.from_url(self.connection_string_)
        return True

    def close(self):
        if self.connection_ is not None:
            self.connection_.disconnect_connection()
            self.connection_ = None

        return True

    def __enter__(self):
        self.open(self.connection_string_)
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def execute(self, sql_string: str, params: Any = None):
        """Execute a SELECT statement"""
        if self.connection_ is None:
            return None
        return self.connection_.execute(sql_string, params)

    def execute_and_return_as_stream(self, sql_string: str, settings: Any):
        if settings is None:
            return None
        if self.connection_ is None:
            return None
        return self.connection_.execute_iter(sql_string, settings=settings)

    def execute_non_query(self, sql_string: str, params: Any = None) -> Optional[bool]:
        """Execute a DELETE , INSERT statement"""

        if self.connection_ is None:
            return None

        result = self.connection_.execute(sql_string, params)
        if isinstance(result, (int, list)):
            return True
        return False
