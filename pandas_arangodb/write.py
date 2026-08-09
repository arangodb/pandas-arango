"""Synchronous DataFrame write operations."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Literal, cast

import pandas as pd
from arango.collection import StandardCollection
from arango.database import StandardDatabase
from arango.exceptions import ArangoServerError

_KEY_PATTERN = re.compile(r"[A-Za-z0-9_\-:.@()+,=;$!*'%]+")
_MAX_KEY_BYTES = 254


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
    def error_count(self) -> int:
        """Return the number of documents that failed."""
        return len(self.errors)

    @property
    def attempted_count(self) -> int:
        """Return the total number of attempted documents."""
        return self.inserted_count + self.error_count

    @property
    def ok(self) -> bool:
        """Return whether every document was inserted."""
        return not self.errors


def write_collection(
    frame: pd.DataFrame,
    db: StandardDatabase,
    collection: str,
    *,
    mode: Literal["insert"] = "insert",
    key_column: str | None = "_key",
    include_index: bool = False,
    index_label: str | None = None,
    batch_size: int = 1_000,
    create_collection: bool = False,
) -> WriteResult:
    """Insert DataFrame rows into an ArangoDB collection in batches.

    ``key_column`` identifies a column to rename to ``_key``. If the default
    ``_key`` column is absent, ArangoDB generates document keys. Pass
    ``key_column=None`` to disable key mapping explicitly.

    ``include_index`` adds the DataFrame index to each document under
    ``index_label``, the index name, or ``"index"``. To use an index as the
    document key, label it ``_key`` or select its label with ``key_column``.
    A default RangeIndex is never used as ``_key`` unless ``index_label`` is
    explicitly set to ``_key``.

    Request-level driver errors, such as a missing collection, are raised.
    Per-document insertion errors are returned in :class:`WriteResult`.
    """
    if mode != "insert":
        raise ValueError("mode must be 'insert'")
    if isinstance(batch_size, bool) or not isinstance(batch_size, int):
        raise TypeError("batch_size must be an integer")
    if batch_size <= 0:
        raise ValueError("batch_size must be greater than zero")
    if index_label is not None and not include_index:
        raise ValueError("index_label requires include_index=True")
    if include_index and isinstance(frame.index, pd.MultiIndex):
        raise ValueError("include_index does not support a MultiIndex")

    resolved_index_label = _resolve_index_label(frame, include_index, index_label)
    if resolved_index_label is not None and resolved_index_label in frame.columns:
        raise ValueError(
            f"index label {resolved_index_label!r} conflicts with a DataFrame column"
        )

    key_values = _prepare_key_values(
        frame,
        key_column=key_column,
        index_label=resolved_index_label,
    )

    target = _get_collection(db, collection, create_collection=create_collection)
    inserted_count = 0
    errors: list[WriteError] = []

    for start in range(0, len(frame), batch_size):
        stop = min(start + batch_size, len(frame))
        batch = frame.iloc[start:stop]
        documents = batch.to_dict(orient="records")
        index_values = batch.index.tolist()

        for offset, document in enumerate(documents):
            position = start + offset
            if resolved_index_label is not None:
                document[resolved_index_label] = index_values[offset]
            if key_values is not None and key_column is not None:
                if key_column != "_key":
                    document.pop(key_column, None)
                document["_key"] = key_values[position]

        batch_result = cast(
            list[dict[str, Any] | ArangoServerError],
            target.insert_many(
                documents,
                overwrite_mode="conflict",
            ),
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

    return [
        _normalize_key(value, row_position=position, key_column=key_column)
        for position, value in enumerate(values)
    ]


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
