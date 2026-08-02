# pandas-arangodb

A connector between ArangoDB and pandas DataFrames.

## Reading AQL results

`read_aql` executes a query and eagerly materializes its cursor as one
DataFrame when `chunksize` is omitted:

```python
from pandas_arangodb import read_aql

frame = read_aql(
    database,
    "FOR document IN users FILTER document.active RETURN document",
    columns=["_key", "name", "active"],
    index="_key",
)
```

Missing attributes become NA values. Nested objects and arrays remain values
in object columns, and ArangoDB system attributes remain strings. An empty AQL
result has no recoverable schema: it produces a DataFrame with the names passed
through `columns`, or no columns when `columns` is omitted.

For large results, pass `chunksize` or call `iter_aql` directly:

```python
from pandas_arangodb import iter_aql

chunks = iter_aql(
    database,
    "FOR document IN users RETURN document",
    chunksize=10_000,
)
try:
    for chunk in chunks:
        process(chunk)
finally:
    chunks.close()
```

Chunked reads use streaming AQL cursors and default the driver's server-side
`batch_size` to `chunksize`. Set `query_options={"batch_size": ...}` to tune
the server batch separately. Peak client memory is proportional to the larger
of `chunksize` and `batch_size`, because the driver deserializes a complete
server batch. Close the iterator when stopping early so its server cursor is
released immediately. An empty chunked result yields one empty DataFrame.

## Compatibility

pandas-arangodb supports Python 3.11 through 3.14, pandas 2.2 or newer, and
python-arango 8.0 or newer. CI tests the minimum dependency versions on Python
3.11 and the latest compatible dependency versions on Python 3.14.

## Development

Install the package and development tools:

```console
python -m pip install -e ".[dev]"
```

Start a local ArangoDB instance and run the test suite:

```console
docker compose up -d --wait
python -m pytest
docker compose down
```

Without a local server, integration tests are skipped. CI sets
`PANDAS_ARANGODB_REQUIRE_SERVER=1` so an unavailable server fails the suite.

Run the static checks with:

```console
python -m ruff check .
python -m mypy
```

Install the pre-commit hooks with:

```console
python -m pre_commit install
```

Pre-commit runs the pinned Ruff and mypy versions in managed environments. To
run the hooks against the entire repository:

```console
python -m pre_commit run --all-files
```
