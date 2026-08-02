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

from pandas_arangodb import iter_aql, read_aql


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
