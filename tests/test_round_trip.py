"""Live-server smoke tests for the package scaffold."""

from __future__ import annotations

import pytest
from arango.database import StandardDatabase

import pandas_arango


@pytest.mark.integration
def test_trivial_round_trip(arango_database: StandardDatabase) -> None:
    """Write documents and materialize the AQL result as a DataFrame."""
    assert pandas_arango.__doc__

    collection = arango_database.create_collection("round_trip")
    collection.insert_many(
        [
            {"_key": "one", "value": 1},
            {"_key": "two", "value": 2},
        ]
    )

    frame = pandas_arango.read_aql(
        arango_database,
        "FOR document IN round_trip SORT document._key "
        "RETURN {key: document._key, value: document.value}",
    )

    assert frame.to_dict(orient="records") == [
        {"key": "one", "value": 1},
        {"key": "two", "value": 2},
    ]
