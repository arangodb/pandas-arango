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

    Args:
        db (StandardDatabase): Database used to execute the query.
        query (str): AQL query to execute.
        bind_vars (Mapping[str, Any] | None): Values bound to AQL parameters.
        query_options (Mapping[str, Any] | None): Additional keyword arguments
            passed to ``db.aql.execute``.
        columns (Sequence[str] | None): Columns to select and their order.
        flatten (bool): Whether to flatten nested objects into columns.
        flatten_separator (str): Separator used between flattened path parts.
        index (str | Sequence[str] | None): Column or columns to use as the
            DataFrame index.
        chunksize (int | None): Number of records per lazily returned DataFrame.
            If omitted, all records are returned in one DataFrame.

    Returns:
        DataFrame | Generator[DataFrame, None, None]: One eagerly materialized
        DataFrame, or a lazy generator when ``chunksize`` is set.

    Raises:
        TypeError: If ``chunksize`` is not an integer.
        ValueError: If ``chunksize`` is not positive.
        ArangoError: If query execution fails.
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


@overload
def read_collection(
    db: StandardDatabase,
    collection: str,
    *,
    columns: Sequence[str] | None = None,
    filter: Mapping[str, Any] | None = None,
    limit: int | None = None,
    projection: Mapping[str, str] | None = None,
    aql_filter: str | None = None,
    bind_vars: Mapping[str, Any] | None = None,
    query_options: Mapping[str, Any] | None = None,
    chunksize: None = None,
) -> pd.DataFrame: ...


@overload
def read_collection(
    db: StandardDatabase,
    collection: str,
    *,
    columns: Sequence[str] | None = None,
    filter: Mapping[str, Any] | None = None,
    limit: int | None = None,
    projection: Mapping[str, str] | None = None,
    aql_filter: str | None = None,
    bind_vars: Mapping[str, Any] | None = None,
    query_options: Mapping[str, Any] | None = None,
    chunksize: int,
) -> Generator[pd.DataFrame, None, None]: ...


def read_collection(
    db: StandardDatabase,
    collection: str,
    *,
    columns: Sequence[str] | None = None,
    filter: Mapping[str, Any] | None = None,
    limit: int | None = None,
    projection: Mapping[str, str] | None = None,
    aql_filter: str | None = None,
    bind_vars: Mapping[str, Any] | None = None,
    query_options: Mapping[str, Any] | None = None,
    chunksize: int | None = None,
) -> pd.DataFrame | Generator[pd.DataFrame, None, None]:
    """Read documents from a collection through generated AQL.

    ``filter`` supports equality checks on top-level attributes only. Use
    ``aql_filter`` and ``bind_vars`` for other conditions. ``projection`` maps
    output column names to raw AQL expressions; callers are responsible for
    providing trusted expressions. ``columns`` and ``projection`` are mutually
    exclusive.

    Collection names, column names, equality-filter names and values, limits,
    and projection output names are sent as bind variables. Passing
    ``chunksize`` delegates to :func:`read_aql` and returns its lazy generator.

    Args:
        db (StandardDatabase): Database containing the collection.
        collection (str): Name of the collection to read.
        columns (Sequence[str] | None): Document attributes to select and their
            order.
        filter (Mapping[str, Any] | None): Equality filters for top-level
            document attributes.
        limit (int | None): Maximum number of documents to return.
        projection (Mapping[str, str] | None): Mapping of output column names to
            trusted AQL expressions.
        aql_filter (str | None): Additional trusted AQL filter expression.
        bind_vars (Mapping[str, Any] | None): Values bound to parameters used by
            ``aql_filter`` or ``projection``.
        query_options (Mapping[str, Any] | None): Additional keyword arguments
            passed to ``db.aql.execute``.
        chunksize (int | None): Number of records per lazily returned DataFrame.
            If omitted, all records are returned in one DataFrame.

    Returns:
        DataFrame | Generator[DataFrame, None, None]: One eagerly materialized
        DataFrame, or a lazy generator when ``chunksize`` is set.

    Raises:
        TypeError: If an argument has an invalid type.
        ValueError: If an argument value or combination is invalid.
        ArangoError: If query execution fails.
    """
    if not isinstance(collection, str):
        raise TypeError("collection must be a string")
    if not collection:
        raise ValueError("collection must not be empty")
    if columns is not None and projection is not None:
        raise ValueError("columns and projection are mutually exclusive")
    if isinstance(limit, bool) or (limit is not None and not isinstance(limit, int)):
        raise TypeError("limit must be an integer")
    if limit is not None and limit < 0:
        raise ValueError("limit must be greater than or equal to zero")

    selected_columns = _validate_columns(columns)
    projected_columns = _validate_projection(projection)
    filters = _validate_filter(filter)
    variables = dict(bind_vars) if bind_vars is not None else {}

    collection_name = _add_bind_var(
        variables,
        "collection",
        collection,
        collection=True,
    )
    query_lines = [f"FOR document IN @@{collection_name}"]

    for position, (attribute, value) in enumerate(filters):
        field_name = _add_bind_var(
            variables,
            f"filter_field_{position}",
            attribute,
        )
        value_name = _add_bind_var(
            variables,
            f"filter_value_{position}",
            value,
        )
        query_lines.append(
            f"    FILTER document[@{field_name}] == @{value_name}"
        )

    if aql_filter is not None:
        if not isinstance(aql_filter, str):
            raise TypeError("aql_filter must be a string")
        if not aql_filter.strip():
            raise ValueError("aql_filter must not be empty")
        query_lines.append(f"    FILTER ({aql_filter})")

    if limit is not None:
        limit_name = _add_bind_var(variables, "limit", limit)
        query_lines.append(f"    LIMIT @{limit_name}")

    result_columns: list[str] | None
    if projected_columns is not None:
        result_columns = list(projected_columns)
        projection_name = _add_bind_var(
            variables,
            "projection_names",
            result_columns,
        )
        expressions = ", ".join(projected_columns.values())
        query_lines.append(f"    RETURN ZIP(@{projection_name}, [{expressions}])")
    elif selected_columns is not None:
        result_columns = selected_columns
        columns_name = _add_bind_var(variables, "columns", result_columns)
        query_lines.append(f"    RETURN KEEP(document, @{columns_name})")
    else:
        result_columns = None
        query_lines.append("    RETURN document")

    return read_aql(
        db,
        "\n".join(query_lines),
        variables,
        query_options=query_options,
        columns=result_columns,
        chunksize=chunksize,
    )


def _validate_columns(columns: Sequence[str] | None) -> list[str] | None:
    if columns is None:
        return None
    if isinstance(columns, str):
        raise TypeError("columns must be a sequence of strings")

    result = list(columns)
    if any(not isinstance(column, str) for column in result):
        raise TypeError("columns must contain only strings")
    return result


def _validate_projection(
    projection: Mapping[str, str] | None,
) -> dict[str, str] | None:
    if projection is None:
        return None

    result = dict(projection)
    if any(not isinstance(name, str) for name in result):
        raise TypeError("projection names must be strings")
    if any(not isinstance(expression, str) for expression in result.values()):
        raise TypeError("projection expressions must be strings")
    if any(not expression.strip() for expression in result.values()):
        raise ValueError("projection expressions must not be empty")
    return result


def _validate_filter(filter: Mapping[str, Any] | None) -> list[tuple[str, Any]]:
    if filter is None:
        return []

    result = list(filter.items())
    if any(not isinstance(attribute, str) for attribute, _ in result):
        raise TypeError("filter attribute names must be strings")
    return result


def _add_bind_var(
    variables: dict[str, Any],
    preferred_name: str,
    value: Any,
    *,
    collection: bool = False,
) -> str:
    name = preferred_name
    suffix = 1
    key = f"@{name}" if collection else name
    while key in variables:
        name = f"{preferred_name}_{suffix}"
        suffix += 1
        key = f"@{name}" if collection else name
    variables[key] = value
    return name


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

    Args:
        db (StandardDatabase): Database used to execute the query.
        query (str): AQL query to execute.
        bind_vars (Mapping[str, Any] | None): Values bound to AQL parameters.
        chunksize (int): Number of records per yielded DataFrame.
        query_options (Mapping[str, Any] | None): Additional keyword arguments
            passed to ``db.aql.execute``.
        columns (Sequence[str] | None): Columns to select and their order.
        flatten (bool): Whether to flatten nested objects into columns.
        flatten_separator (str): Separator used between flattened path parts.
        index (str | Sequence[str] | None): Column or columns to use as the
            DataFrame index.

    Returns:
        Generator[DataFrame, None, None]: Lazy generator of DataFrames.

    Raises:
        TypeError: If ``chunksize`` is not an integer.
        ValueError: If ``chunksize`` is not positive.
        ArangoError: If query execution or cursor iteration fails.
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
