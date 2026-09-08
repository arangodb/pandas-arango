# Contributing

## Development setup

Create and activate a Python 3.11 or newer environment, then install the
package and development dependencies:

```console
python -m pip install -e ".[dev,docs]"
```

Install the Git hooks if desired:

```console
python -m pre_commit install
```

## ArangoDB

The repository Compose configuration starts the ArangoDB version used by CI:

```console
docker compose up -d --wait
```

Local tests and documentation use these defaults:

- URL: `http://127.0.0.1:8529`
- Username: `root`
- Password: `passwd`

Override them with `PANDAS_ARANGODB_TEST_URL`,
`PANDAS_ARANGODB_TEST_USERNAME`, and `PANDAS_ARANGODB_TEST_PASSWORD`.

Stop the service when finished:

```console
docker compose down
```

## Tests and checks

Run the test suite:

```console
python -m pytest
```

Tests are skipped when ArangoDB is unavailable. Require a live server with:

```console
PANDAS_ARANGODB_REQUIRE_SERVER=1 python -m pytest
```

Run the static checks:

```console
python -m ruff check .
python -m mypy
python -m pre_commit run --all-files
```

## Documentation

Build the HTML documentation:

```console
make -C docs html
```

The result is written to `docs/_build/html/index.html`.

With ArangoDB running, execute all documentation examples and treat warnings
as errors:

```console
make -C docs SPHINXOPTS="-W --keep-going" doctest
```

Documentation tests recreate a database named `pandas_arangodb_docs`.

Remove generated documentation with:

```console
make -C docs clean
```

CI runs Ruff, mypy, pytest, and Sphinx doctests against the supported minimum
and latest dependency sets.
