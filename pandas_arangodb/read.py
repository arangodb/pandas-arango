"""Synchronous DataFrame read operations."""

from __future__ import annotations

from collections.abc import Generator, Mapping, Sequence
from itertools import islice
from typing import Any, cast, overload

import pandas as pd
from arango.cursor import Cursor
from arango.database import StandardDatabase


@overload
def read_aql(
    db: StandardDatabase,
    query: str,
    bind_vars: Mapping[str, Any] | None = None,
    *,
    query_options: Mapping[str, Any] | None = None,
    columns: Sequence[str] | None = None,
    flatten: bool = False,
    flatten_separator: str = ".",
    index: str | Sequence[str] | None = None,
    chunksize: None = None,
) -> pd.DataFrame: ...


@overload
def read_aql(
    db: StandardDatabase,
    query: str,
    bind_vars: Mapping[str, Any] | None = None,
    *,
    query_options: Mapping[str, Any] | None = None,
    columns: Sequence[str] | None = None,
    flatten: bool = False,
    flatten_separator: str = ".",
    index: str | Sequence[str] | None = None,
    chunksize: int,
) -> Generator[pd.DataFrame, None, None]: ...


def read_aql(
    db: StandardDatabase,
    query: str,
    bind_vars: Mapping[str, Any] | None = None,
    *,
    query_options: Mapping[str, Any] | None = None,
    columns: Sequence[str] | None = None,
    flatten: bool = False,
    flatten_separator: str = ".",
    index: str | Sequence[str] | None = None,
    chunksize: int | None = None,
) -> pd.DataFrame | Generator[pd.DataFrame, None, None]:
    """Execute an AQL query and return its results as DataFrames.

    Without ``chunksize``, the entire result is eagerly materialized as one
    DataFrame. With a positive ``chunksize``, return the same lazy generator
    as :func:`iter_aql`.

    ``columns`` selects and orders record fields. Requested fields that are
    absent from every record become all-NA columns. ``index`` names one or
    more record fields to use as the DataFrame index; those fields must also
    be present in ``columns`` when both arguments are supplied.

    Nested objects and arrays remain Python objects by default. Set
    ``flatten=True`` to flatten nested objects with :func:`pandas.json_normalize`
    and join path components with ``flatten_separator``. Arrays remain intact.
    Separator collisions are not escaped or reported. Prefer projecting the
    desired tabular shape in AQL when possible.

    ArangoDB system attributes such as ``_key``, ``_id``, ``_from``, and
    ``_to`` remain strings.
    """
    if chunksize is not None:
        return iter_aql(
            db,
            query,
            bind_vars,
            query_options=query_options,
            columns=columns,
            flatten=flatten,
            flatten_separator=flatten_separator,
            index=index,
            chunksize=chunksize,
        )

    options = dict(query_options) if query_options is not None else {}
    variables = dict(bind_vars) if bind_vars is not None else None
    cursor = cast(
        Cursor,
        db.aql.execute(query, bind_vars=variables, **options),
    )
    records = list(cursor)
    return _records_to_frame(
        records,
        columns=columns,
        flatten=flatten,
        flatten_separator=flatten_separator,
        index=index,
    )


def iter_aql(
    db: StandardDatabase,
    query: str,
    bind_vars: Mapping[str, Any] | None = None,
    *,
    chunksize: int,
    query_options: Mapping[str, Any] | None = None,
    columns: Sequence[str] | None = None,
    flatten: bool = False,
    flatten_separator: str = ".",
    index: str | Sequence[str] | None = None,
) -> Generator[pd.DataFrame, None, None]:
    """Execute an AQL query and lazily yield bounded-size DataFrames.

    Chunked reads always enable server-side streaming. The driver
    ``batch_size`` defaults to ``chunksize`` but can be set independently in
    ``query_options``. Peak client memory is therefore proportional to the
    larger of the two sizes.

    ``flatten`` and ``flatten_separator`` have the same behavior as in
    :func:`read_aql`, applied independently to each chunk.

    Close the returned generator when stopping early to release the
    server-side cursor immediately. Exhaustion and iteration errors also
    close it. An empty query result yields one empty DataFrame.
    """
    if isinstance(chunksize, bool) or not isinstance(chunksize, int):
        raise TypeError("chunksize must be an integer")
    if chunksize <= 0:
        raise ValueError("chunksize must be greater than zero")

    options = dict(query_options) if query_options is not None else {}
    options.setdefault("batch_size", chunksize)
    options["stream"] = True
    variables = dict(bind_vars) if bind_vars is not None else None
    return _iter_aql(
        db,
        query,
        variables,
        options=options,
        columns=columns,
        flatten=flatten,
        flatten_separator=flatten_separator,
        index=index,
        chunksize=chunksize,
    )


def _iter_aql(
    db: StandardDatabase,
    query: str,
    bind_vars: dict[str, Any] | None,
    *,
    options: dict[str, Any],
    columns: Sequence[str] | None,
    flatten: bool,
    flatten_separator: str,
    index: str | Sequence[str] | None,
    chunksize: int,
) -> Generator[pd.DataFrame, None, None]:
    cursor = cast(
        Cursor,
        db.aql.execute(query, bind_vars=bind_vars, **options),
    )
    has_read_data = False
    try:
        while True:
            records = list(islice(cursor, chunksize))
            if not records:
                if not has_read_data:
                    yield _records_to_frame(
                        records,
                        columns=columns,
                        flatten=flatten,
                        flatten_separator=flatten_separator,
                        index=index,
                    )
                return

            has_read_data = True
            yield _records_to_frame(
                records,
                columns=columns,
                flatten=flatten,
                flatten_separator=flatten_separator,
                index=index,
            )
    finally:
        cursor.close(ignore_missing=True)


def _records_to_frame(
    records: list[Any],
    *,
    columns: Sequence[str] | None,
    flatten: bool,
    flatten_separator: str,
    index: str | Sequence[str] | None,
) -> pd.DataFrame:
    if not flatten:
        return pd.DataFrame.from_records(records, columns=columns, index=index)

    frame = pd.json_normalize(records, sep=flatten_separator)
    if columns is not None:
        frame = frame.reindex(columns=columns)
    if index is not None:
        index_columns = [index] if isinstance(index, str) else list(index)
        frame = frame.set_index(index_columns)
    return frame
