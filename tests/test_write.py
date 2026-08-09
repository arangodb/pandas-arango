"""Tests for synchronous collection writes."""

from __future__ import annotations

from decimal import Decimal
from typing import Literal, cast
from unittest.mock import Mock, call
from uuid import UUID

import pandas as pd
import pytest
from arango.database import StandardDatabase
from arango.exceptions import DocumentInsertError

from pandas_arangodb import write_collection

WriteMode = Literal["insert", "update", "replace", "upsert"]


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


@pytest.mark.parametrize(
    ("mode", "method", "options"),
    [
        (
            "update",
            "update_many",
            {"check_rev": False, "merge": True, "keep_none": False},
        ),
        ("replace", "replace_many", {"check_rev": False}),
        (
            "upsert",
            "insert_many",
            {"overwrite_mode": "update", "keep_none": False, "merge": True},
        ),
    ],
)
def test_write_collection_dispatches_non_insert_modes(
    mode: str,
    method: str,
    options: dict[str, object],
) -> None:
    """Map each non-insert mode to its native bulk driver operation."""
    database = Mock()
    collection = database.collection.return_value
    bulk_method = getattr(collection, method)
    bulk_method.return_value = [{"_key": "one"}]

    result = write_collection(
        pd.DataFrame({"_key": ["one"], "value": [1]}),
        cast(StandardDatabase, database),
        "items",
        mode=cast(WriteMode, mode),
        keep_none=False,
    )

    assert result.written_count == 1
    bulk_method.assert_called_once_with(
        [{"value": 1, "_key": "one"}],
        **options,
    )


@pytest.mark.parametrize("mode", ["update", "replace", "upsert"])
def test_write_collection_requires_keys_for_non_insert_modes(mode: str) -> None:
    """Reject writes that cannot identify their target document reliably."""
    database = Mock()

    with pytest.raises(ValueError, match="requires a document key column"):
        write_collection(
            pd.DataFrame({"value": [1]}),
            cast(StandardDatabase, database),
            "items",
            mode=cast(WriteMode, mode),
        )

    database.collection.assert_not_called()


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


@pytest.mark.parametrize(
    ("null_policy", "expected"),
    [
        (
            "null",
            {
                "_key": "one",
                "none": None,
                "nan": None,
                "na": None,
                "nat": None,
            },
        ),
        ("omit", {"_key": "one"}),
    ],
)
def test_write_collection_applies_null_policy(
    null_policy: str,
    expected: dict[str, object],
) -> None:
    """Convert every pandas missing scalar to null or omit its attribute."""
    database = Mock()
    collection = database.collection.return_value
    collection.insert_many.return_value = [{"_key": "one"}]
    frame = pd.DataFrame(
        {
            "_key": ["one"],
            "none": pd.Series([None], dtype="object"),
            "nan": pd.Series([float("nan")], dtype="object"),
            "na": pd.Series([pd.NA], dtype="object"),
            "nat": pd.Series([pd.NaT], dtype="object"),
        }
    )

    write_collection(
        frame,
        cast(StandardDatabase, database),
        "items",
        null_policy=cast(Literal["null", "omit"], null_policy),
    )

    collection.insert_many.assert_called_once_with(
        [expected],
        overwrite_mode="conflict",
    )


@pytest.mark.parametrize(
    ("datetime_format", "expected"),
    [
        ("iso", "2026-07-15T10:30:00.000000+00:00"),
        ("unix_ms", 1_784_111_400_000),
    ],
)
def test_write_collection_converts_timezone_aware_timestamps(
    datetime_format: str,
    expected: str | int,
) -> None:
    """Encode aware timestamps using either documented built-in format."""
    database = Mock()
    collection = database.collection.return_value
    collection.insert_many.return_value = [{"_key": "one"}]
    frame = pd.DataFrame(
        {
            "_key": ["one"],
            "created_at": [pd.Timestamp("2026-07-15T10:30:00+00:00")],
        }
    )

    write_collection(
        frame,
        cast(StandardDatabase, database),
        "items",
        datetime_format=cast(Literal["iso", "unix_ms"], datetime_format),
    )

    collection.insert_many.assert_called_once_with(
        [{"created_at": expected, "_key": "one"}],
        overwrite_mode="conflict",
    )


def test_write_collection_uses_callable_datetime_format() -> None:
    """Allow callers to define the serialized representation of datetimes."""
    database = Mock()
    collection = database.collection.return_value
    collection.insert_many.return_value = [{"_key": "one"}]

    write_collection(
        pd.DataFrame(
            {
                "_key": ["one"],
                "created_at": [pd.Timestamp("2026-07-15T10:30:00+00:00")],
            }
        ),
        cast(StandardDatabase, database),
        "items",
        datetime_format=lambda value: value.strftime("%Y%m%d"),
    )

    collection.insert_many.assert_called_once_with(
        [{"created_at": "20260715", "_key": "one"}],
        overwrite_mode="conflict",
    )


def test_write_collection_rejects_timezone_naive_timestamps() -> None:
    """Reject naive timestamps instead of silently assuming a timezone."""
    database = Mock()

    with pytest.raises(TypeError, match="timezone-naive"):
        write_collection(
            pd.DataFrame(
                {
                    "_key": ["one"],
                    "created_at": [pd.Timestamp("2026-07-15T10:30:00")],
                }
            ),
            cast(StandardDatabase, database),
            "items",
        )

    database.collection.return_value.insert_many.assert_not_called()


def test_write_collection_preserves_nullable_scalars() -> None:
    """Keep nullable integers and booleans as JSON scalars without floats."""
    database = Mock()
    collection = database.collection.return_value
    collection.insert_many.return_value = [{"_key": "one"}, {"_key": "two"}]
    frame = pd.DataFrame(
        {
            "_key": ["one", "two"],
            "count": pd.Series([7, None], dtype="Int64"),
            "active": pd.Series([True, None], dtype="boolean"),
        }
    )

    write_collection(frame, cast(StandardDatabase, database), "items")

    documents = collection.insert_many.call_args.args[0]
    assert documents == [
        {"count": 7, "active": True, "_key": "one"},
        {"count": None, "active": None, "_key": "two"},
    ]
    assert type(documents[0]["count"]) is int
    assert type(documents[0]["active"]) is bool


@pytest.mark.parametrize("value", [Decimal("1.25"), UUID(int=1)])
def test_write_collection_rejects_non_json_values(value: object) -> None:
    """Identify the source column and dtype for unsupported Python values."""
    database = Mock()

    with pytest.raises(TypeError, match=r"column 'value' \(dtype object\)"):
        write_collection(
            pd.DataFrame({"_key": ["one"], "value": [value]}),
            cast(StandardDatabase, database),
            "items",
        )

    database.collection.return_value.insert_many.assert_not_called()


def test_write_collection_applies_column_converters() -> None:
    """Use per-column hooks to make Decimal and UUID values JSON-safe."""
    database = Mock()
    collection = database.collection.return_value
    collection.insert_many.return_value = [{"_key": "one"}]
    identifier = UUID(int=1)

    write_collection(
        pd.DataFrame(
            {
                "_key": ["one"],
                "price": [Decimal("1.25")],
                "identifier": [identifier],
            }
        ),
        cast(StandardDatabase, database),
        "items",
        converters={"price": str, "identifier": str},
    )

    collection.insert_many.assert_called_once_with(
        [
            {
                "price": "1.25",
                "identifier": str(identifier),
                "_key": "one",
            }
        ],
        overwrite_mode="conflict",
    )


@pytest.mark.integration
def test_write_collection_round_trips_converted_values(
    arango_database: StandardDatabase,
) -> None:
    """Confirm converted scalars pass driver serialization and reach ArangoDB."""
    collection = arango_database.create_collection("write_converted_values")
    frame = pd.DataFrame(
        {
            "_key": ["one"],
            "count": pd.Series([7], dtype="Int64"),
            "active": pd.Series([True], dtype="boolean"),
            "missing": pd.Series([pd.NA], dtype="object"),
            "created_at": [pd.Timestamp("2026-07-15T10:30:00+00:00")],
            "price": [Decimal("1.25")],
        }
    )

    result = write_collection(
        frame,
        arango_database,
        collection.name,
        converters={"price": str},
    )

    assert result.inserted_count == 1
    document = collection.get("one")
    assert document is not None
    assert document["count"] == 7
    assert document["active"] is True
    assert document["missing"] is None
    assert document["created_at"] == "2026-07-15T10:30:00.000000+00:00"
    assert document["price"] == "1.25"


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
@pytest.mark.parametrize(
    ("mode", "key_exists", "succeeds", "keeps_old_attribute"),
    [
        ("insert", True, False, True),
        ("insert", False, True, False),
        ("update", True, True, True),
        ("update", False, False, False),
        ("replace", True, True, False),
        ("replace", False, False, False),
        ("upsert", True, True, True),
        ("upsert", False, True, False),
    ],
)
def test_write_collection_mode_matrix(
    arango_database: StandardDatabase,
    mode: str,
    key_exists: bool,
    succeeds: bool,
    keeps_old_attribute: bool,
) -> None:
    """Define each mode for keys that exist or are absent in the collection."""
    collection = arango_database.create_collection("write_mode_matrix")
    if key_exists:
        collection.insert({"_key": "one", "value": 0, "old": True})

    result = write_collection(
        pd.DataFrame({"_key": ["one"], "value": [1]}),
        arango_database,
        collection.name,
        mode=cast(WriteMode, mode),
    )

    assert result.written_count == int(succeeds)
    assert result.error_count == int(not succeeds)
    document = collection.get("one")
    if succeeds:
        assert document is not None
        assert document["value"] == 1
        assert ("old" in document) is keeps_old_attribute
    elif key_exists:
        assert document is not None
        assert document["value"] == 0
        assert document["old"] is True
        assert result.errors[0].error.error_code == 1210
    else:
        assert document is None
        assert result.errors[0].error.error_code == 1202


@pytest.mark.integration
@pytest.mark.parametrize(
    ("null_policy", "keep_none", "expected"),
    [
        ("omit", True, "original"),
        ("null", True, None),
        ("null", False, "absent"),
    ],
)
def test_write_collection_update_null_semantics(
    arango_database: StandardDatabase,
    null_policy: str,
    keep_none: bool,
    expected: object,
) -> None:
    """Distinguish an omitted update attribute from a kept or removed null."""
    collection = arango_database.create_collection("write_update_null")
    collection.insert({"_key": "one", "optional": "original", "stable": 1})

    result = write_collection(
        pd.DataFrame(
            {
                "_key": ["one"],
                "optional": pd.Series([None], dtype="object"),
            }
        ),
        arango_database,
        collection.name,
        mode="update",
        null_policy=cast(Literal["null", "omit"], null_policy),
        keep_none=keep_none,
    )

    assert result.ok
    document = collection.get("one")
    assert document is not None
    assert document["stable"] == 1
    if expected == "absent":
        assert "optional" not in document
    else:
        assert document["optional"] == expected


@pytest.mark.integration
@pytest.mark.parametrize("key_exists", [True, False])
def test_write_collection_upsert_keep_none_applies_only_to_updates(
    arango_database: StandardDatabase,
    key_exists: bool,
) -> None:
    """Apply keep_none to an upsert update but not to a new insertion."""
    collection = arango_database.create_collection("write_upsert_null")
    if key_exists:
        collection.insert({"_key": "one", "optional": "original"})

    result = write_collection(
        pd.DataFrame(
            {
                "_key": ["one"],
                "optional": pd.Series([None], dtype="object"),
            }
        ),
        arango_database,
        collection.name,
        mode="upsert",
        keep_none=False,
    )

    assert result.ok
    document = collection.get("one")
    assert document is not None
    if key_exists:
        assert "optional" not in document
    else:
        assert document["optional"] is None


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
