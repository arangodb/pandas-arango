Document-to-table behavior
==========================

ArangoDB collections can contain documents with different shapes. A DataFrame
is rectangular, so ``pandas-arangodb`` applies these rules when reading:

.. list-table::
   :header-rows: 1

   * - ArangoDB value
     - DataFrame result
   * - Document attribute
     - Column
   * - Missing document attribute
     - Missing pandas value in that row
   * - ``null``
     - Missing pandas value
   * - Nested object
     - Python ``dict`` unless flattening is enabled
   * - Array
     - Python ``list`` in one cell
   * - ``_key``, ``_id``, ``_from``
     - String
   * - Empty result
     - Requested ``columns``, or no columns

For example, attributes that are absent from some documents become missing
values in those rows:

.. doctest::

   >>> from pandas_arangodb import read_aql
   >>> frame = read_aql(
   ...     database,
   ...     "FOR item IN [{name: 'Ada', score: 10}, {name: 'Grace'}] RETURN item",
   ... )
   >>> frame["name"].tolist()
   ['Ada', 'Grace']
   >>> bool(frame["score"].isna().iloc[1])
   True

If ``columns`` is supplied, it both selects columns and fixes their order.
Requested fields that do not occur are still present as all-missing columns.

.. doctest::

   >>> ordered = read_aql(
   ...     database,
   ...     "RETURN {name: 'Ada'}",
   ...     columns=["missing", "name"],
   ... )
   >>> ordered.columns.tolist()
   ['missing', 'name']
   >>> bool(ordered["missing"].isna().iloc[0])
   True

Scope
-----

The package is a small DataFrame connector. It does not provide a SQLAlchemy
dialect, a ``pandas.read_sql`` integration, or a graph-to-table object model.
Use AQL to choose how vertices, edges, paths, and nested documents should be
represented as rows and columns.
