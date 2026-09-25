Reading data
============

Use :func:`~pandas_arango.read_collection` for common collection reads and
:func:`~pandas_arango.read_aql` when the query needs sorting, joins,
traversals, or other AQL features.

.. testsetup::

   users = database.create_collection("users")
   users.insert_many(
       [
           {
               "_key": "ada",
               "name": "Ada",
               "active": True,
               "profile": {"city": "London"},
           },
           {
               "_key": "grace",
               "name": "Grace",
               "active": True,
               "profile": {"city": "New York"},
           },
           {
               "_key": "inactive",
               "name": "Linus",
               "active": False,
               "profile": {"city": "Helsinki"},
           },
       ]
   )

Collection reads
----------------

``filter`` performs equality checks on top-level document attributes.
``columns`` selects the returned fields and their order.

.. doctest::

   >>> from pandas_arango import read_collection
   >>> active = read_collection(
   ...     database,
   ...     "users",
   ...     columns=["_key", "name"],
   ...     filter={"active": True},
   ... )
   >>> sorted(active["name"].tolist())
   ['Ada', 'Grace']

Use ``projection`` for computed or nested values. Projection expressions and
``aql_filter`` are raw AQL, so only pass trusted strings.

.. doctest::

   >>> cities = read_collection(
   ...     database,
   ...     "users",
   ...     projection={
   ...         "name": "document.name",
   ...         "city": "document.profile.city",
   ...     },
   ...     aql_filter="document.active == @active",
   ...     bind_vars={"active": True},
   ... )
   >>> sorted(cities["city"].tolist())
   ['London', 'New York']

AQL reads
---------

Bind variables keep data separate from AQL source code.

.. doctest::

   >>> from pandas_arango import read_aql
   >>> selected = read_aql(
   ...     database,
   ...     """
   ...     FOR user IN users
   ...         FILTER user.profile.city == @city
   ...         RETURN {key: user._key, name: user.name}
   ...     """,
   ...     {"city": "London"},
   ... )
   >>> selected.to_dict(orient="records")
   [{'key': 'ada', 'name': 'Ada'}]

Chunked reads
-------------

Pass ``chunksize`` to receive a lazy generator instead of one eagerly
materialized DataFrame. Close the generator if iteration stops early.

.. doctest::

   >>> chunks = read_aql(
   ...     database,
   ...     "FOR user IN users SORT user._key RETURN user",
   ...     chunksize=2,
   ... )
   >>> [len(chunk) for chunk in chunks]
   [2, 1]

Client-side flattening
----------------------

Nested objects remain object-column values by default. ``flatten=True``
turns nested object paths into columns. Arrays are not expanded into rows.

.. doctest::

   >>> flattened = read_aql(
   ...     database,
   ...     "FOR user IN users SORT user._key RETURN user",
   ...     flatten=True,
   ... )
   >>> flattened.loc[0, "profile.city"]
   'London'

Prefer AQL projection when possible: it sends less data and defines the table
shape explicitly. Flattening is useful when the query cannot be changed.
