# pandas-arangodb

`pandas-arangodb` is a synchronous connector for moving data between ArangoDB
documents and pandas DataFrames. It supports AQL and collection reads, chunked
results, and batched insert, update, replace, and upsert operations.

## Requirements

- Python 3.11 or newer
- pandas 2.2 or newer
- python-arango 8.0 or newer
- A running ArangoDB server

Install it with:

```console
python -m pip install pandas-arangodb
```

## Quickstart

Connect with `python-arango`, read documents into a DataFrame, use pandas, and
write the result to another collection:

```python
from arango import ArangoClient
from pandas_arangodb import read_collection, write_collection

client = ArangoClient(hosts="http://127.0.0.1:8529")
database = client.db("my_database", username="root", password="passwd")

users = read_collection(
    database,
    "users",
    columns=["_key", "name", "active"],
)
active_users = users.loc[users["active"]]

result = write_collection(
    active_users,
    database,
    "active_users",
    mode="upsert",
    create_collection=True,
)
print(result.written_count)
```

Use `read_aql` for custom queries and pass `chunksize` for large results.

### Advanced example

Converters let you store Python values that are not JSON-compatible by
default. This example preserves decimal prices as strings, converts UUIDs to
document keys, omits missing fields, and reads matching documents in chunks:

```python
from decimal import Decimal
from uuid import uuid4

import pandas as pd
from pandas_arangodb import read_aql, write_collection

measurements = pd.DataFrame(
    [
        {
            "measurement_id": uuid4(),
            "price": Decimal("19.95"),
            "captured_at": pd.Timestamp.now(tz="UTC"),
            "comment": pd.NA,
        }
    ]
)

write_collection(
    measurements,
    database,
    "measurements",
    key_column="measurement_id",
    create_collection=True,
    null_policy="omit",
    converters={"measurement_id": str, "price": str},
)

chunks = read_aql(
    database,
    """
    FOR measurement IN measurements
        FILTER TO_NUMBER(measurement.price) >= @minimum_price
        RETURN measurement
    """,
    bind_vars={"minimum_price": 10},
    chunksize=10_000,
)
for chunk in chunks:
    print(chunk[["_key", "price", "captured_at"]])
```

## Constraints

- Nested objects and arrays remain values in DataFrame cells by default.
- AQL projection is preferred; client-side flattening is opt-in.
- Writes accept JSON-compatible values. Other values require converters.
- Timezone-naive timestamps are rejected instead of assuming a timezone.

## More information

- [Example notebook](examples/example.ipynb)
- [Documentation](docs/index.rst)
- [Contributing and development](CONTRIBUTING.md)
