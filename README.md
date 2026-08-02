# pandas-arangodb

A connector between ArangoDB and pandas DataFrames.

The package is currently a scaffold. The public read and write APIs will be
added incrementally.

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
