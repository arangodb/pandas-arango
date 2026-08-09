"""Tests for synchronous AQL reads."""

from __future__ import annotations

import gc
import tracemalloc
from collections.abc import Iterable, Iterator
from typing import Any, cast
from unittest.mock import Mock

import pandas as pd
import pytest
from arango.database import StandardDatabase

from pandas_arangodb import iter_aql, read_aql, read_collection


class _TrackingCursor:
    def __init__(
        self,
        records: Iterable[dict[str, Any]],
        *,
        fail_after: int | None = None,
    ) -> None:
        self._records: Iterator[dict[str, Any]] = iter(records)
        self._fail_after = fail_after
        self.rows_read = 0
        self.close_calls: list[bool] = []

    def __iter__(self) -> _TrackingCursor:
        return self

    def __next__(self) -> dict[str, Any]:
        if self._fail_after == self.rows_read:
            raise RuntimeError("cursor fetch failed")
        record = next(self._records)
        self.rows_read += 1
        return record

    def close(self, ignore_missing: bool = False) -> None:
        self.close_calls.append(ignore_missing)


def test_read_aql_forwards_execution_options() -> None:
    """Forward bind variables and query options to the driver unchanged."""
    database = Mock()
    database.aql.execute.return_value = iter([{"value": 42}])

    frame = read_aql(
        cast(StandardDatabase, database),
        "RETURN @value",
        {"value": 42},
        query_options={"batch_size": 10, "cache": False},
    )

    database.aql.execute.assert_called_once_with(
        "RETURN @value",
        bind_vars={"value": 42},
        batch_size=10,
        cache=False,
    )
    assert frame.to_dict(orient="records") == [{"value": 42}]


def test_read_collection_binds_columns_filter_and_limit() -> None:
    """Compile collection options without interpolating user data into AQL."""
    database = Mock()
    database.aql.execute.return_value = iter([{"_key": "one", "name": "Alice"}])
    filter_value = "Alice' REMOVE document IN users //"

    frame = read_collection(
        cast(StandardDatabase, database),
        "users",
        columns=["_key", "name"],
        filter={"name": filter_value},
        limit=5,
        query_options={"cache": False},
    )

    query = (
        "FOR document IN @@collection\n"
        "    FILTER document[@filter_field_0] == @filter_value_0\n"
        "    LIMIT @limit\n"
        "    RETURN KEEP(document, @columns)"
    )
    database.aql.execute.assert_called_once_with(
        query,
        bind_vars={
            "@collection": "users",
            "filter_field_0": "name",
            "filter_value_0": filter_value,
            "limit": 5,
            "columns": ["_key", "name"],
        },
        cache=False,
    )
    assert filter_value not in query
    assert frame.to_dict(orient="records") == [{"_key": "one", "name": "Alice"}]


def test_read_collection_compiles_projection_and_aql_filter() -> None:
    """Expose trusted AQL expressions while binding their data and output names."""
    database = Mock()
    database.aql.execute.return_value = iter(
        [{"user_key": "one", "city": "Berlin"}]
    )

    frame = read_collection(
        cast(StandardDatabase, database),
        "users",
        projection={
            "user_key": "document._key",
            "city": "document.address.city",
        },
        aql_filter="document.age >= @limit",
        bind_vars={"limit": 18},
        limit=2,
    )

    query = (
        "FOR document IN @@collection\n"
        "    FILTER (document.age >= @limit)\n"
        "    LIMIT @limit_1\n"
        "    RETURN ZIP(@projection_names, "
        "[document._key, document.address.city])"
    )
    database.aql.execute.assert_called_once_with(
        query,
        bind_vars={
            "limit": 18,
            "@collection": "users",
            "limit_1": 2,
            "projection_names": ["user_key", "city"],
        },
    )
    assert frame.columns.tolist() == ["user_key", "city"]
    assert frame.iloc[0].to_dict() == {"user_key": "one", "city": "Berlin"}


def test_read_collection_reuses_chunked_reading() -> None:
    """Return the existing streaming generator when a chunk size is supplied."""
    database = Mock()
    cursor = _TrackingCursor({"value": value} for value in range(3))
    database.aql.execute.return_value = cursor

    frames = list(
        read_collection(
            cast(StandardDatabase, database),
            "numbers",
            columns=["value"],
            chunksize=2,
        )
    )

    assert [frame["value"].tolist() for frame in frames] == [[0, 1], [2]]
    database.aql.execute.assert_called_once_with(
        "FOR document IN @@collection\n"
        "    RETURN KEEP(document, @columns)",
        bind_vars={"@collection": "numbers", "columns": ["value"]},
        batch_size=2,
        stream=True,
    )
    assert cursor.close_calls == [True]


def test_read_collection_rejects_conflicting_selection_options() -> None:
    """Reject ambiguous calls that provide both selection mechanisms."""
    with pytest.raises(ValueError, match="mutually exclusive"):
        read_collection(
            Mock(),
            "users",
            columns=["name"],
            projection={"name": "document.name"},
        )


@pytest.mark.parametrize("limit", [-1, True, 1.5])
def test_read_collection_rejects_invalid_limit(limit: Any) -> None:
    """Reject limits that cannot be used as a non-negative AQL row count."""
    with pytest.raises((TypeError, ValueError), match="limit"):
        read_collection(Mock(), "users", limit=limit)


def test_read_aql_flattens_nested_objects_with_custom_separator() -> None:
    """Flatten object paths and allow callers to choose their column separator."""
    database = Mock()
    database.aql.execute.return_value = iter(
        [{"_key": "one", "profile": {"name": "Alice", "age": 30}}]
    )

    frame = read_aql(
        cast(StandardDatabase, database),
        "RETURN @document",
        flatten=True,
        flatten_separator="__",
        columns=["_key", "profile__name", "profile__age"],
        index="_key",
    )

    assert frame.index.tolist() == ["one"]
    assert frame.columns.tolist() == ["profile__name", "profile__age"]
    assert frame.iloc[0].to_dict() == {"profile__name": "Alice", "profile__age": 30}


def test_read_aql_does_not_escape_flatten_separator_collisions() -> None:
    """Expose pandas' ambiguous collision behavior instead of inventing escaping."""
    database = Mock()
    database.aql.execute.return_value = iter(
        [{"profile.name": "literal", "profile": {"name": "nested"}}]
    )

    frame = read_aql(
        cast(StandardDatabase, database),
        "RETURN @document",
        flatten=True,
    )

    assert frame.columns.tolist() == ["profile.name"]
    assert frame.loc[0, "profile.name"] == "nested"


def test_read_aql_flattens_inconsistent_nesting_into_union_columns() -> None:
    """Create union columns with missing values for mixed object shapes."""
    database = Mock()
    database.aql.execute.return_value = iter(
        [
            {"profile": {"name": "Alice"}},
            {"profile": "unknown"},
            {"profile": {"age": 30}},
        ]
    )

    frame = read_aql(
        cast(StandardDatabase, database),
        "FOR document IN @documents RETURN document",
        flatten=True,
    )

    assert frame.columns.tolist() == ["profile.name", "profile", "profile.age"]
    assert frame.loc[0, "profile.name"] == "Alice"
    assert frame.loc[1, "profile"] == "unknown"
    assert frame.loc[2, "profile.age"] == 30
    assert pd.isna(frame.loc[0, "profile"])
    assert pd.isna(frame.loc[1, "profile.name"])


def test_read_aql_flatten_keeps_arrays_of_objects_intact() -> None:
    """Keep object arrays in one cell instead of creating additional rows."""
    database = Mock()
    items = [{"sku": "one"}, {"sku": "two"}]
    database.aql.execute.return_value = iter([{"order": {"items": items}}])

    frame = read_aql(
        cast(StandardDatabase, database),
        "RETURN @document",
        flatten=True,
    )

    assert frame.columns.tolist() == ["order.items"]
    assert frame.loc[0, "order.items"] == items


def test_iter_aql_applies_flattening_to_each_chunk() -> None:
    """Use the same flattening behavior for every lazily produced frame."""
    database = Mock()
    cursor = _TrackingCursor(
        {"value": value, "nested": {"even": value % 2 == 0}}
        for value in range(3)
    )
    database.aql.execute.return_value = cursor

    frames = list(
        iter_aql(
            cast(StandardDatabase, database),
            "FOR value IN 0..2 RETURN {value, nested: {even: value % 2 == 0}}",
            chunksize=2,
            flatten=True,
        )
    )

    assert [frame.columns.tolist() for frame in frames] == [
        ["value", "nested.even"],
        ["value", "nested.even"],
    ]
    assert [frame["nested.even"].tolist() for frame in frames] == [
        [True, False],
        [True],
    ]
    assert cursor.close_calls == [True]


def test_read_aql_chunks_default_to_streaming_server_batches() -> None:
    """Use a streaming cursor and preserve an explicit server batch size."""
    database = Mock()
    cursor = _TrackingCursor({"value": value} for value in range(5))
    database.aql.execute.return_value = cursor

    frames = list(
        read_aql(
            cast(StandardDatabase, database),
            "FOR value IN 0..4 RETURN {value}",
            query_options={"batch_size": 3, "stream": False, "ttl": 60},
            chunksize=2,
        )
    )

    database.aql.execute.assert_called_once_with(
        "FOR value IN 0..4 RETURN {value}",
        bind_vars=None,
        batch_size=3,
        stream=True,
        ttl=60,
    )
    assert [len(frame) for frame in frames] == [2, 2, 1]
    assert cursor.close_calls == [True]


def test_iter_aql_closes_cursor_when_iteration_stops_early() -> None:
    """Release server resources when the caller closes a partial read."""
    database = Mock()
    cursor = _TrackingCursor({"value": value} for value in range(10))
    database.aql.execute.return_value = cursor
    frames = iter_aql(
        cast(StandardDatabase, database),
        "FOR value IN 0..9 RETURN {value}",
        chunksize=2,
    )

    first = next(frames)
    frames.close()

    assert first["value"].tolist() == [0, 1]
    assert cursor.rows_read == 2
    assert cursor.close_calls == [True]
    database.aql.execute.assert_called_once_with(
        "FOR value IN 0..9 RETURN {value}",
        bind_vars=None,
        batch_size=2,
        stream=True,
    )


def test_iter_aql_closes_cursor_after_iteration_failure() -> None:
    """Close the server cursor without hiding a later batch-fetch failure."""
    database = Mock()
    cursor = _TrackingCursor(
        ({"value": value} for value in range(10)),
        fail_after=4,
    )
    database.aql.execute.return_value = cursor
    frames = iter_aql(
        cast(StandardDatabase, database),
        "FOR value IN 0..9 RETURN {value}",
        chunksize=2,
    )

    assert next(frames)["value"].tolist() == [0, 1]
    assert next(frames)["value"].tolist() == [2, 3]
    with pytest.raises(RuntimeError, match="cursor fetch failed"):
        next(frames)

    assert cursor.close_calls == [True]


@pytest.mark.parametrize("chunksize", [0, -1])
def test_iter_aql_rejects_non_positive_chunksize(chunksize: int) -> None:
    """Reject chunk sizes that cannot make forward progress."""
    with pytest.raises(ValueError, match="greater than zero"):
        iter_aql(Mock(), "RETURN 1", chunksize=chunksize)


def test_iter_aql_peak_memory_is_independent_of_result_size() -> None:
    """Coarsely verify that lazy reads do not materialize the full result."""

    def measure_peak(row_count: int) -> int:
        database = Mock()
        payload = "x" * 1_024
        cursor = _TrackingCursor(
            {"value": value, "payload": payload} for value in range(row_count)
        )
        database.aql.execute.return_value = cursor
        frames = iter_aql(
            cast(StandardDatabase, database),
            "FOR value IN 1..@count RETURN {value, payload: @payload}",
            {"count": row_count, "payload": payload},
            chunksize=128,
        )

        gc.collect()
        tracemalloc.start()
        try:
            for frame in frames:
                assert len(frame) <= 128
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        assert cursor.close_calls == [True]
        return peak

    small_peak = measure_peak(2_048)
    large_peak = measure_peak(32_768)

    assert large_peak < small_peak * 3


@pytest.mark.integration
def test_read_aql_empty_result(arango_database: StandardDatabase) -> None:
    """Preserve requested columns when an AQL query returns no records."""
    without_columns = read_aql(arango_database, "FOR value IN [] RETURN value")
    with_columns = read_aql(
        arango_database,
        "FOR value IN [] RETURN value",
        columns=["_key", "value"],
    )

    assert without_columns.empty
    assert without_columns.columns.empty
    assert with_columns.empty
    assert with_columns.columns.tolist() == ["_key", "value"]


@pytest.mark.integration
def test_iter_aql_yields_one_empty_frame(
    arango_database: StandardDatabase,
) -> None:
    """Yield one schema-preserving frame for an empty chunked result."""
    frames = list(
        iter_aql(
            arango_database,
            "FOR value IN [] RETURN value",
            columns=["_key", "value"],
            chunksize=2,
        )
    )

    assert len(frames) == 1
    assert frames[0].empty
    assert frames[0].columns.tolist() == ["_key", "value"]


@pytest.mark.integration
def test_iter_aql_handles_chunk_boundaries(
    arango_database: StandardDatabase,
) -> None:
    """Yield a short final chunk and one chunk for a smaller result."""
    uneven = list(
        iter_aql(
            arango_database,
            "FOR value IN 1..5 RETURN {value}",
            chunksize=2,
        )
    )
    smaller = list(
        iter_aql(
            arango_database,
            "FOR value IN 1..2 RETURN {value}",
            chunksize=5,
        )
    )

    assert [frame["value"].tolist() for frame in uneven] == [[1, 2], [3, 4], [5]]
    assert [frame["value"].tolist() for frame in smaller] == [[1, 2]]


@pytest.mark.integration
def test_read_aql_heterogeneous_attributes(
    arango_database: StandardDatabase,
) -> None:
    """Represent missing document attributes as pandas missing values."""
    frame = read_aql(
        arango_database,
        "FOR document IN [{name: 'first', count: 1}, "
        "                  {name: 'second', active: true}] "
        "RETURN document",
    )

    assert frame.columns.tolist() == ["name", "count", "active"]
    assert frame["name"].tolist() == ["first", "second"]
    assert frame.loc[0, "count"] == 1
    assert pd.isna(frame.loc[1, "count"])
    assert pd.isna(frame.loc[0, "active"])
    assert bool(frame.loc[1, "active"])


@pytest.mark.integration
def test_read_aql_preserves_nested_values(
    arango_database: StandardDatabase,
) -> None:
    """Keep nested objects and arrays intact in object columns."""
    frame = read_aql(
        arango_database,
        "RETURN {name: 'Alice', address: {city: 'Berlin'}, "
        "        roles: ['admin', 'reviewer']}",
    )

    assert frame.loc[0, "address"] == {"city": "Berlin"}
    assert frame.loc[0, "roles"] == ["admin", "reviewer"]
    assert frame["address"].dtype == object
    assert frame["roles"].dtype == object


@pytest.mark.integration
def test_read_aql_preserves_system_attributes_as_strings(
    arango_database: StandardDatabase,
) -> None:
    """Keep ArangoDB document and edge identifiers as strings."""
    frame = read_aql(
        arango_database,
        "RETURN {_key: 'edge', _id: 'edges/edge', "
        "        _from: 'vertices/source', _to: 'vertices/target'}",
    )

    expected = {
        "_key": "edge",
        "_id": "edges/edge",
        "_from": "vertices/source",
        "_to": "vertices/target",
    }
    assert frame.iloc[0].to_dict() == expected
    assert all(isinstance(value, str) for value in frame.iloc[0])


@pytest.mark.integration
def test_read_aql_applies_columns_and_index(
    arango_database: StandardDatabase,
) -> None:
    """Apply explicit column ordering and index-field selection."""
    frame = read_aql(
        arango_database,
        "FOR document IN [{_key: 'one', value: 1, ignored: true}, "
        "                  {_key: 'two', value: 2, ignored: false}] "
        "RETURN document",
        columns=["_key", "value", "missing"],
        index="_key",
    )

    assert frame.index.tolist() == ["one", "two"]
    assert frame.index.name == "_key"
    assert frame.columns.tolist() == ["value", "missing"]
    assert frame["value"].tolist() == [1, 2]
    assert frame["missing"].isna().all()


@pytest.mark.integration
def test_read_collection_applies_projection_filter_and_limit(
    arango_database: StandardDatabase,
) -> None:
    """Run generated projection, filter, and limit AQL against ArangoDB."""
    collection = arango_database.create_collection("users")
    collection.insert_many(
        [
            {
                "_key": "alice",
                "active": True,
                "score": 10,
                "address": {"city": "Berlin"},
            },
            {
                "_key": "bob",
                "active": False,
                "score": 20,
                "address": {"city": "Paris"},
            },
            {
                "_key": "carol",
                "active": True,
                "score": 30,
                "address": {"city": "Rome"},
            },
        ]
    )

    frame = read_collection(
        arango_database,
        "users",
        filter={"active": True},
        projection={
            "user_key": "document._key",
            "city": "document.address.city",
        },
        aql_filter="document.score >= @minimum_score",
        bind_vars={"minimum_score": 10},
        limit=1,
    )

    assert len(frame) == 1
    assert frame.columns.tolist() == ["user_key", "city"]
    assert frame.iloc[0].to_dict() in [
        {"user_key": "alice", "city": "Berlin"},
        {"user_key": "carol", "city": "Rome"},
    ]
