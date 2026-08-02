"""Synchronous DataFrame read operations."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, cast

import pandas as pd  # type: ignore[import-untyped]
from arango.cursor import Cursor  # type: ignore[import-not-found]
from arango.database import StandardDatabase  # type: ignore[import-not-found]


def read_aql(
    db: StandardDatabase,
    query: str,
    bind_vars: Mapping[str, Any] | None = None,
    *,
    query_options: Mapping[str, Any] | None = None,
    columns: Sequence[str] | None = None,
    index: str | Sequence[str] | None = None,
) -> pd.DataFrame:
    """Execute an AQL query and materialize its results as a DataFrame.

    ``columns`` selects and orders record fields. Requested fields that are
    absent from every record become all-NA columns. ``index`` names one or
    more record fields to use as the DataFrame index; those fields must also
    be present in ``columns`` when both arguments are supplied.

    Nested objects and arrays remain Python objects. ArangoDB system
    attributes such as ``_key``, ``_id``, ``_from``, and ``_to`` remain
    strings.
    """
    options = dict(query_options) if query_options is not None else {}
    variables = dict(bind_vars) if bind_vars is not None else None
    cursor = cast(
        Cursor,
        db.aql.execute(query, bind_vars=variables, **options),
    )
    records = list(cursor)
    return pd.DataFrame.from_records(records, columns=columns, index=index)
