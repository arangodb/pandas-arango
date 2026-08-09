"""Synchronous DataFrame write operations."""

from __future__ import annotations

import math
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, cast

import pandas as pd
from arango.collection import StandardCollection
from arango.database import StandardDatabase
from arango.exceptions import ArangoServerError

_KEY_PATTERN = re.compile(r"[A-Za-z0-9_\-:.@()+,=;$!*'%]+")
_MAX_KEY_BYTES = 254

DatetimeFormat = Literal["iso", "unix_ms"] | Callable[[datetime], Any]
WriteMode = Literal["insert", "update", "replace", "upsert"]


@dataclass(frozen=True)
class WriteError:
    """A document error associated with its source DataFrame row."""

    row_position: int
    row_index: Any
    error: ArangoServerError


@dataclass(frozen=True)
class WriteResult:
    """Aggregate result of a collection write."""

    inserted_count: int
    errors: tuple[WriteError, ...] = ()

    @property
    def written_count(self) -> int:
        """Return the number of documents successfully written."""
        return self.inserted_count

    @property
    def error_count(self) -> int:
        """Return the number of documents that failed."""
        return len(self.errors)

    @property
    def attempted_count(self) -> int:
        """Return the total number of attempted documents."""
        return self.written_count + self.error_count

    @property
    def ok(self) -> bool:
        """Return whether every document was written."""
        return not self.errors


def write_collection(
    frame: pd.DataFrame,
    db: StandardDatabase,
    collection: str,
    *,
    mode: WriteMode = "insert",
    key_column: str | None = "_key",
    include_index: bool = False,
    index_label: str | None = None,
    batch_size: int = 1_000,
    create_collection: bool = False,
    null_policy: Literal["null", "omit"] = "null",
    keep_none: bool = True,
    datetime_format: DatetimeFormat = "iso",
    converters: Mapping[str, Callable[[Any], Any]] | None = None,
) -> WriteResult:
    """Write DataFrame rows into an ArangoDB collection in batches.

    ``insert`` creates new documents and reports key conflicts. ``update``
    modifies existing documents, ``replace`` replaces their complete user
    attribute set, and ``upsert`` updates existing keys or inserts missing
    keys. Modes other than ``insert`` require an explicit key source.

    ``key_column`` identifies a column to rename to ``_key``. If the default
    ``_key`` column is absent, ArangoDB generates document keys. Pass
    ``key_column=None`` to disable key mapping explicitly.

    ``include_index`` adds the DataFrame index to each document under
    ``index_label``, the index name, or ``"index"``. To use an index as the
    document key, label it ``_key`` or select its label with ``key_column``.
    A default RangeIndex is never used as ``_key`` unless ``index_label`` is
    explicitly set to ``_key``.

    Request-level driver errors, such as a missing collection, are raised.
    Per-document write errors are returned in :class:`WriteResult`.

    Missing column values become JSON null with ``null_policy="null"`` or
    are omitted with ``null_policy="omit"``. Timezone-aware timestamps are
    encoded as ISO 8601 strings or Unix milliseconds; timezone-naive values
    are rejected. Column converters run before the built-in conversion.

    For ``update`` and the update branch of ``upsert``, omitted attributes
    remain unchanged. Sent nulls are stored when ``keep_none=True`` and remove
    existing attributes when ``keep_none=False``. ``keep_none`` does not
    affect insert or replace operations.
    """
    if mode not in ("insert", "update", "replace", "upsert"):
        raise ValueError("mode must be 'insert', 'update', 'replace', or 'upsert'")
    if isinstance(batch_size, bool) or not isinstance(batch_size, int):
        raise TypeError("batch_size must be an integer")
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")
    if index_label is not None and not include_index:
        raise ValueError("index_label requires include_index=True")
    if include_index and isinstance(frame.index, pd.MultiIndex):
        raise ValueError("include_index does not support a MultiIndex")
    if null_policy not in ("null", "omit"):
        raise ValueError("null_policy must be 'null' or 'omit'")
    if not isinstance(keep_none, bool):
        raise TypeError("keep_none must be a boolean")
    if not callable(datetime_format) and datetime_format not in ("iso", "unix_ms"):
        raise ValueError("datetime_format must be 'iso', 'unix_ms', or callable")

    resolved_index_label = _resolve_index_label(frame, include_index, index_label)
    if resolved_index_label is not None and resolved_index_label in frame.columns:
        raise ValueError(
            f"index label {resolved_index_label!r} conflicts with a DataFrame column"
        )

    converter_map = dict(converters or {})
    column_dtypes = {column: str(frame[column].dtype) for column in frame.columns}
    if resolved_index_label is not None:
        column_dtypes[resolved_index_label] = str(frame.index.dtype)
    _validate_converters(converter_map, column_dtypes)

    key_values = _prepare_key_values(
        frame,
        key_column=key_column,
        index_label=resolved_index_label,
        converters=converter_map,
        column_dtypes=column_dtypes,
    )
    if mode != "insert" and key_values is None:
        raise ValueError(f"mode {mode!r} requires a document key column")

    target = _get_collection(db, collection, create_collection=create_collection)
    inserted_count = 0
    errors: list[WriteError] = []

    for start in range(0, len(frame), batch_size):
        stop = min(start + batch_size, len(frame))
        batch = frame.iloc[start:stop]
        raw_documents = batch.to_dict(orient="records")
        index_values = batch.index.tolist()
        documents: list[dict[str, Any]] = []

        for offset, raw_document in enumerate(raw_documents):
            position = start + offset
            if resolved_index_label is not None:
                raw_document[resolved_index_label] = index_values[offset]
            if key_values is not None and key_column is not None:
                raw_document.pop(key_column, None)

            document = _convert_document(
                raw_document,
                row_position=position,
                null_policy=null_policy,
                datetime_format=datetime_format,
                converters=converter_map,
                column_dtypes=column_dtypes,
            )
            if key_values is not None:
                document["_key"] = key_values[position]
            documents.append(document)

        batch_result = _write_batch(
            target,
            documents,
            mode=mode,
            keep_none=keep_none,
        )
        if len(batch_result) != len(documents):
            raise RuntimeError(
                "python-arango returned an unexpected number of bulk results"
            )

        for offset, item in enumerate(batch_result):
            if isinstance(item, ArangoServerError):
                position = start + offset
                errors.append(
                    WriteError(
                        row_position=position,
                        row_index=frame.index[position],
                        error=item,
                    )
                )
            else:
                inserted_count += 1

    return WriteResult(inserted_count=inserted_count, errors=tuple(errors))


def _write_batch(
    collection: StandardCollection,
    documents: list[dict[str, Any]],
    *,
    mode: WriteMode,
    keep_none: bool,
) -> list[dict[str, Any] | ArangoServerError]:
    if mode == "insert":
        result = collection.insert_many(documents, overwrite_mode="conflict")
    elif mode == "update":
        result = collection.update_many(
            documents,
            check_rev=False,
            merge=True,
            keep_none=keep_none,
        )
    elif mode == "replace":
        result = collection.replace_many(documents, check_rev=False)
    else:
        result = collection.insert_many(
            documents,
            overwrite_mode="update",
            keep_none=keep_none,
            merge=True,
        )
    return cast(list[dict[str, Any] | ArangoServerError], result)


def _resolve_index_label(
    frame: pd.DataFrame,
    include_index: bool,
    index_label: str | None,
) -> str | None:
    if not include_index:
        return None

    resolved = index_label
    if resolved is None:
        resolved = frame.index.name if isinstance(frame.index.name, str) else "index"
    if (
        resolved == "_key"
        and isinstance(frame.index, pd.RangeIndex)
        and index_label is None
    ):
        raise ValueError(
            "a RangeIndex cannot be written as _key implicitly; "
            "pass index_label='_key' to opt in"
        )
    return resolved


def _prepare_key_values(
    frame: pd.DataFrame,
    *,
    key_column: str | None,
    index_label: str | None,
    converters: Mapping[str, Callable[[Any], Any]],
    column_dtypes: Mapping[Any, str],
) -> list[str] | None:
    if key_column is None:
        if "_key" in frame.columns:
            raise ValueError("key_column=None cannot be used with an _key column")
        return None

    if key_column == index_label:
        values = frame.index.tolist()
    elif key_column in frame.columns:
        values = frame[key_column].tolist()
    elif key_column == "_key":
        return None
    else:
        raise ValueError(f"key column {key_column!r} does not exist")

    if key_column != "_key" and "_key" in frame.columns:
        raise ValueError(
            f"cannot map key column {key_column!r}: DataFrame already has '_key'"
        )

    converter = converters.get(key_column)
    normalized: list[str] = []
    for position, value in enumerate(values):
        if converter is not None and not _is_missing(value):
            value = _apply_converter(
                value,
                converter=converter,
                column=key_column,
                dtype=column_dtypes[key_column],
                row_position=position,
            )
        normalized.append(
            _normalize_key(value, row_position=position, key_column=key_column)
        )
    return normalized


def _validate_converters(
    converters: Mapping[str, Callable[[Any], Any]],
    column_dtypes: Mapping[Any, str],
) -> None:
    for column, converter in converters.items():
        if column not in column_dtypes:
            raise ValueError(f"converter column {column!r} does not exist")
        if not callable(converter):
            raise TypeError(f"converter for column {column!r} must be callable")


def _convert_document(
    document: Mapping[Any, Any],
    *,
    row_position: int,
    null_policy: Literal["null", "omit"],
    datetime_format: DatetimeFormat,
    converters: Mapping[str, Callable[[Any], Any]],
    column_dtypes: Mapping[Any, str],
) -> dict[str, Any]:
    converted: dict[str, Any] = {}
    for column, value in document.items():
        if not isinstance(column, str):
            raise TypeError(
                f"DataFrame column {column!r} is not a valid JSON object key"
            )

        dtype = column_dtypes[column]
        if _is_missing(value):
            if null_policy == "null":
                converted[column] = None
            continue

        converter = converters.get(column)
        if converter is not None:
            value = _apply_converter(
                value,
                converter=converter,
                column=column,
                dtype=dtype,
                row_position=row_position,
            )
            if _is_missing(value):
                if null_policy == "null":
                    converted[column] = None
                continue

        try:
            converted[column] = _convert_json_value(
                value,
                datetime_format=datetime_format,
            )
        except (TypeError, ValueError) as error:
            raise TypeError(
                f"column {column!r} (dtype {dtype}) has an invalid value "
                f"at row {row_position}: {error}"
            ) from error
    return converted


def _apply_converter(
    value: Any,
    *,
    converter: Callable[[Any], Any],
    column: str,
    dtype: str,
    row_position: int,
) -> Any:
    try:
        return converter(value)
    except Exception as error:
        raise ValueError(
            f"converter for column {column!r} (dtype {dtype}) failed "
            f"at row {row_position}: {error}"
        ) from error


def _convert_json_value(
    value: Any,
    *,
    datetime_format: DatetimeFormat,
    allow_datetime: bool = True,
) -> Any:
    if _is_missing(value):
        return None
    if isinstance(value, datetime):
        if not allow_datetime:
            raise TypeError("datetime converter returned another datetime value")
        converted = _convert_datetime(value, datetime_format=datetime_format)
        return _convert_json_value(
            converted,
            datetime_format=datetime_format,
            allow_datetime=False,
        )
    if isinstance(value, str | bool | int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("non-finite floats are not valid JSON numbers")
        return value
    if isinstance(value, list | tuple):
        return [
            _convert_json_value(item, datetime_format=datetime_format)
            for item in value
        ]
    if isinstance(value, dict):
        converted_dict: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(f"nested object key {key!r} is not a string")
            converted_dict[key] = _convert_json_value(
                item,
                datetime_format=datetime_format,
            )
        return converted_dict
    raise TypeError(f"{type(value).__name__} is not JSON-serializable")


def _convert_datetime(value: datetime, *, datetime_format: DatetimeFormat) -> Any:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timezone-naive timestamps are not supported")
    if callable(datetime_format):
        return datetime_format(value)
    if datetime_format == "iso":
        return value.isoformat(timespec="microseconds")
    return pd.Timestamp(value).value // 1_000_000


def _is_missing(value: Any) -> bool:
    if value is None or value is pd.NA or value is pd.NaT:
        return True
    if not pd.api.types.is_scalar(value):
        return False
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _normalize_key(value: Any, *, row_position: int, key_column: str) -> str:
    if not pd.api.types.is_scalar(value):
        raise ValueError(
            f"key column {key_column!r} has a non-scalar value "
            f"at row {row_position}"
        )
    missing = pd.isna(value)
    try:
        is_missing = bool(missing)
    except ValueError:
        is_missing = False
    if is_missing:
        raise ValueError(
            f"key column {key_column!r} has a missing value at row {row_position}"
        )

    key = value if isinstance(value, str) else str(value)
    key_size = len(key.encode("utf-8"))
    invalid_characters = _KEY_PATTERN.fullmatch(key) is None
    if key_size == 0 or key_size > _MAX_KEY_BYTES or invalid_characters:
        raise ValueError(
            f"key column {key_column!r} has invalid ArangoDB key {key!r} "
            f"at row {row_position}"
        )
    return key


def _get_collection(
    db: StandardDatabase,
    name: str,
    *,
    create_collection: bool,
) -> StandardCollection:
    if create_collection and not db.has_collection(name):
        return cast(StandardCollection, db.create_collection(name))
    return db.collection(name)
