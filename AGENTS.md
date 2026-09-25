# Agent guide

## Project and structure

`pandas-arango` is a synchronous connector between ArangoDB documents and
pandas DataFrames, built on `python-arango`. The Python import is `pandas_arango`.

- `README.md`: installation, quickstart, and supported workflows.
- `CONTRIBUTING.md`: development setup, database setup, and checks.
- `pandas_arango/`: library code; `read.py` handles reads and chunking,
  `write.py` handles conversion and bulk writes, and `__init__.py` exports
  the public API. `py.typed` marks the package as typed.
- `tests/`: unit and integration tests; `conftest.py` manages test databases.
- `docs/`: Sphinx documentation and executable examples.
- `examples/example.ipynb`: an end-to-end package dependency graph example.
- `pyproject.toml`: package version, dependencies, and tool configuration.
- `.github/workflows/`: CI, documentation checks, and PyPI publishing.
- `compose.yaml`: local ArangoDB service; `.readthedocs.yaml`: hosted docs build.

When available, use the local `context/` directory (referenced as `@context`)
for design notes and dependency sources. Start with `context/pandas-arangodb.md`;
consult `context/python-arango/` and `context/pandas/` as needed. These are
reference materials, not files to modify with library changes.

## Maintenance

- Read the relevant implementation and tests before editing. Keep changes
  focused and preserve unrelated work.
- Keep public API behavior, type annotations, documentation, and examples
  consistent. Add regression tests for bug fixes and tests for new behavior.
- Preserve bounded-memory reads and batched writes. Check cursor cleanup,
  missing values, document keys, and partial write failures when relevant.
- Use `pandas-arango` for the distribution and `pandas_arango` for imports.
  Keep the version in `pyproject.toml`; release tags must match it.
- Follow the Python compatibility and dependency bounds in `pyproject.toml`.
  Run relevant tests, Ruff, and mypy; build docs when their content or API
  references change. Report any failed or skipped checks.
- Do not commit credentials, build output, or local database data.

## Environment and testing

Use an activated development environment with a supported Python version
for all commands below.

Install dependencies and run unit tests without a database:

```bash
python -m pip install -e ".[dev,docs]"
python -m pytest -m "not integration"
```

Start ArangoDB with `arangodock.sh` if available, otherwise use
`docker compose up -d --wait`. Run the complete suite with a required server:

```bash
PANDAS_ARANGO_REQUIRE_SERVER=1 python -m pytest
python -m ruff check .
python -m mypy
python -m sphinx -W --keep-going -b html docs docs/_build/html
python -m sphinx -W --keep-going -b doctest docs docs/_build/doctest
```

The default database connection is `http://127.0.0.1:8529`, user `root`, password
`passwd`. See `CONTRIBUTING.md` for `PANDAS_ARANGO_TEST_*` overrides.
Without `PANDAS_ARANGO_REQUIRE_SERVER=1`, integration tests skip when the
server is unavailable. Use a disposable local server: tests create and delete
databases, and documentation doctests recreate `pandas_arango_docs`.
