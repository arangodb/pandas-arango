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

## Writing collections

`write_collection` writes DataFrame rows with explicit bulk batching:

```python
from pandas_arangodb import write_collection

result = write_collection(
    frame,
    database,
    "users",
    key_column="customer_id",
    batch_size=1_000,
)
```

The key column is renamed to `_key`. Key values are explicitly converted to
strings and validated before any documents are written. If the default `_key`
column is absent, ArangoDB generates keys. Duplicate keys and other
per-document failures are available through `result.errors`; each error carries
the original DataFrame row position and index label. `result.written_count`
reports successful rows. Request-level errors, such as a missing collection,
are raised by the driver.

Set `create_collection=True` to create a missing collection. DataFrame indexes
are excluded by default. To include one, opt in explicitly:

```python
result = write_collection(
    frame,
    database,
    "users",
    include_index=True,
    index_label="_key",
)
```

A RangeIndex is not used as `_key` through an implicit index name. Setting
`index_label="_key"` is an explicit opt-in.

Write conversion is configured per call:

```python
result = write_collection(
    frame,
    database,
    "measurements",
    null_policy="omit",
    datetime_format="unix_ms",
    converters={"price": str, "uuid": str},
)
```

`null_policy="null"` converts `None`, `NaN`, `pandas.NA`, and `NaT` column
values to JSON null. `null_policy="omit"` leaves the corresponding document
attribute out. Missing values inside nested lists or objects become JSON null;
the omit policy applies to DataFrame columns.

Timezone-aware timestamps use ISO 8601 strings by default. Pass
`datetime_format="unix_ms"` for Unix milliseconds or a callable for a custom
JSON-safe representation. Timezone-naive timestamps are rejected; the
connector never assumes UTC. Per-column converters run on non-missing values
before built-in datetime conversion and JSON validation. Values such as
`Decimal` and `UUID` therefore require a converter. Unsupported values produce
an error naming the source column, dtype, and row position.

Nullable pandas integers and booleans remain Python integer and boolean values
instead of being coerced through floats. Some JSON/VelocyPack client stacks
cannot preserve integer precision beyond 2^53, so applications using larger
integers should choose an explicit string converter.

Four write modes are available:

| Mode | Existing `_key` | Absent `_key` in collection | Attributes |
| --- | --- | --- | --- |
| `insert` | Per-row conflict error | Insert | All supplied attributes |
| `update` | Update | Per-row not-found error | Supplied attributes only |
| `replace` | Replace | Per-row not-found error | Complete replacement |
| `upsert` | Update | Insert | Supplied attributes only |

`update`, `replace`, and `upsert` require an explicit document key column.
Insert may omit `_key`, in which case ArangoDB generates one. Update and upsert
merge nested objects. Replace removes old user attributes that are absent from
the DataFrame row.

For update and the update branch of upsert, `null_policy="omit"` leaves an
existing attribute unchanged because the attribute is not sent. With
`null_policy="null"`, the attribute is sent as null: `keep_none=True` stores
the null, while `keep_none=False` removes the attribute. For a newly inserted
document, null remains null. Replace stores supplied nulls normally because
`keep_none` applies only to update operations.

Write modes never drop, recreate, or truncate a collection. Collection
creation remains an independent `create_collection=True` opt-in, preserving
indexes, graph definitions, and collection configuration on existing
collections.

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
