"""Shared fixtures for integration tests."""

from __future__ import annotations

import os
from collections.abc import Iterator
from uuid import uuid4

import pytest
from arango import ArangoClient
from arango.database import StandardDatabase
from arango.exceptions import ServerConnectionError


@pytest.fixture
def arango_database() -> Iterator[StandardDatabase]:
    """Provision a throwaway database on the configured local server."""
    url = os.getenv("PANDAS_ARANGODB_TEST_URL", "http://127.0.0.1:8529")
    username = os.getenv("PANDAS_ARANGODB_TEST_USERNAME", "root")
    password = os.getenv("PANDAS_ARANGODB_TEST_PASSWORD", "passwd")
    require_server = os.getenv("PANDAS_ARANGODB_REQUIRE_SERVER") == "1"
    database_name = f"pandas_arangodb_test_{uuid4().hex}"

    client = ArangoClient(hosts=url)
    system_database = client.db(
        "_system",
        username=username,
        password=password,
    )

    try:
        system_database.version()
    except (ConnectionError, ServerConnectionError) as error:
        client.close()
        if require_server:
            pytest.fail(f"ArangoDB is required but unavailable at {url}: {error}")
        pytest.skip(f"ArangoDB is unavailable at {url}")

    system_database.create_database(database_name)
    database = client.db(
        database_name,
        username=username,
        password=password,
    )

    try:
        yield database
    finally:
        system_database.delete_database(database_name, ignore_missing=True)
        client.close()
