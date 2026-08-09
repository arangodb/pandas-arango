"""Tests for synchronous collection writes."""

from __future__ import annotations

from typing import cast
from unittest.mock import Mock, call

import pandas as pd
import pytest
from arango.database import StandardDatabase
from arango.exceptions import DocumentInsertError

from pandas_arangodb import write_collection


def test_write_collection_batches_insert_requests() -> None:
    """Split rows into bounded bulk calls and aggregate successful inserts."""
    database = Mock()
    collection = database.collection.return_value
    collection.insert_many.side_effect = [
        [{"_key": "0"}, {"_key": "1"}],
        [{"_key": "2"}],
    ]
    frame = pd.DataFrame({"value": [10, 20, 30]})

    result = write_collection(
        frame,
        cast(StandardDatabase, database),
        "items",
        batch_size=2,
    )

    assert result.inserted_count == 3
    assert result.attempted_count == 3
    assert result.error_count == 0
    assert result.ok
    assert collection.insert_many.call_args_list == [
        call(
            [{"value": 10}, {"value": 20}],
            overwrite_mode="conflict",
        ),
        call(
            [{"value": 30}],
            overwrite_mode="conflict",
        ),
    ]


def test_write_collection_empty_frame_uses_no_bulk_request() -> None:
    """Return an empty successful result without sending an empty bulk body."""
    database = Mock()

    result = write_collection(
        pd.DataFrame(columns=["_key", "value"]),
        cast(StandardDatabase, database),
        "items",
    )

    assert result.inserted_count == 0
    assert result.attempted_count == 0
    assert result.errors == ()
    database.collection.return_value.insert_many.assert_not_called()


def test_write_collection_maps_a_named_key_column() -> None:
    """Rename an explicit key source and validate keys before insertion."""
    database = Mock()
    collection = database.collection.return_value
    collection.insert_many.return_value = [{"_key": "101"}, {"_key": "user-2"}]
    frame = pd.DataFrame(
        {"customer_id": [101, "user-2"], "name": ["Alice", "Bob"]}
    )

    result = write_collection(
        frame,
        cast(StandardDatabase, database),
        "users",
        key_column="customer_id",
    )

    assert result.inserted_count == 2
    collection.insert_many.assert_called_once_with(
        [
            {"name": "Alice", "_key": "101"},
            {"name": "Bob", "_key": "user-2"},
        ],
        overwrite_mode="conflict",
    )


def test_write_collection_requires_explicit_range_index_key() -> None:
    """Prevent an implicitly named RangeIndex from becoming document identity."""
    frame = pd.DataFrame({"value": [1]})
    frame.index.name = "_key"

    with pytest.raises(ValueError, match="RangeIndex cannot be written as _key"):
        write_collection(
            frame,
            Mock(),
            "items",
            include_index=True,
        )


def test_write_collection_accepts_explicit_range_index_key() -> None:
    """Allow a RangeIndex key only when the caller labels it explicitly."""
    database = Mock()
    collection = database.collection.return_value
    collection.insert_many.return_value = [{"_key": "0"}, {"_key": "1"}]

    result = write_collection(
        pd.DataFrame({"value": [10, 20]}),
        cast(StandardDatabase, database),
        "items",
        include_index=True,
        index_label="_key",
    )

    assert result.inserted_count == 2
    collection.insert_many.assert_called_once_with(
        [{"value": 10, "_key": "0"}, {"value": 20, "_key": "1"}],
        overwrite_mode="conflict",
    )


@pytest.mark.parametrize("key", [None, "", "contains space", "path/segment"])
def test_write_collection_rejects_invalid_keys_before_writing(key: object) -> None:
    """Reject missing or illegal document keys before the first bulk request."""
    database = Mock()

    with pytest.raises(ValueError, match="key column '_key'"):
        write_collection(
            pd.DataFrame({"_key": ["valid", key]}),
            cast(StandardDatabase, database),
            "items",
            batch_size=1,
        )

    database.collection.assert_not_called()


def test_write_collection_rejects_oversized_keys_before_writing() -> None:
    """Enforce ArangoDB's 254-byte document-key limit client-side."""
    database = Mock()

    with pytest.raises(ValueError, match="invalid ArangoDB key"):
        write_collection(
            pd.DataFrame({"_key": ["x" * 255]}),
            cast(StandardDatabase, database),
            "items",
        )

    database.collection.assert_not_called()


@pytest.mark.integration
def test_write_collection_reports_duplicate_key_per_row(
    arango_database: StandardDatabase,
) -> None:
    """Keep successful rows while mapping a duplicate-key error to its row."""
    collection = arango_database.create_collection("write_partial_failure")
    collection.insert({"_key": "duplicate", "value": 0})
    frame = pd.DataFrame(
        {
            "_key": ["one", "duplicate", "two"],
            "value": [1, 2, 3],
        },
        index=["first", "conflict", "last"],
    )

    result = write_collection(frame, arango_database, collection.name)

    assert result.inserted_count == 2
    assert result.attempted_count == 3
    assert result.error_count == 1
    assert not result.ok
    assert result.errors[0].row_position == 1
    assert result.errors[0].row_index == "conflict"
    assert isinstance(result.errors[0].error, DocumentInsertError)
    assert result.errors[0].error.error_code == 1210
    assert sorted(collection.keys()) == ["duplicate", "one", "two"]


@pytest.mark.integration
def test_write_collection_missing_collection_raises(
    arango_database: StandardDatabase,
) -> None:
    """Surface a request-level driver error when the collection is absent."""
    with pytest.raises(DocumentInsertError):
        write_collection(
            pd.DataFrame({"_key": ["one"], "value": [1]}),
            arango_database,
            "missing_collection",
        )


@pytest.mark.integration
def test_write_collection_can_create_missing_collection(
    arango_database: StandardDatabase,
) -> None:
    """Create the target only when create_collection is explicitly enabled."""
    result = write_collection(
        pd.DataFrame({"_key": ["one"], "value": [1]}),
        arango_database,
        "created_collection",
        create_collection=True,
    )

    assert result.inserted_count == 1
    assert arango_database.has_collection("created_collection")
