"""ArangoDB integration for pandas."""

from pandas_arangodb.read import iter_aql, read_aql
from pandas_arangodb.write import WriteError, WriteResult, write_collection

__all__ = [
    "WriteError",
    "WriteResult",
    "iter_aql",
    "read_aql",
    "write_collection",
]
