Writing data
============

:func:`~pandas_arango.write_collection` converts each DataFrame row to one
ArangoDB document and sends documents in batches.

Insert
------

By default, the ``_key`` column supplies document keys. If it is absent,
ArangoDB generates keys.

.. doctest::

   >>> import pandas as pd
   >>> from pandas_arango import write_collection
   >>> frame = pd.DataFrame(
   ...     [
   ...         {"_key": "one", "name": "Ada", "city": "London"},
   ...         {"_key": "two", "name": "Grace", "city": "New York"},
   ...     ]
   ... )
   >>> result = write_collection(
   ...     frame,
   ...     database,
   ...     "users",
   ...     create_collection=True,
   ... )
   >>> result.written_count, result.error_count
   (2, 0)

Use ``key_column`` when a differently named DataFrame column should become
``_key``. The DataFrame index is excluded unless ``include_index=True``.

Write modes
-----------

Four modes control what happens for a document key:

=========== =========================== ==========================
Mode        Existing key                Missing key
=========== =========================== ==========================
``insert``  Per-document conflict       Insert
``update``  Update supplied attributes  Per-document not-found
``replace`` Replace all user attributes Per-document not-found
``upsert``  Update supplied attributes  Insert
=========== =========================== ==========================

``update``, ``replace``, and ``upsert`` require an explicit key source.

.. doctest::

   >>> update = pd.DataFrame([{"_key": "one", "name": "Augusta"}])
   >>> write_collection(update, database, "users", mode="update").ok
   True
   >>> database.collection("users").get("one")["city"]
   'London'
   >>> replacement = pd.DataFrame([{"_key": "one", "name": "Ada"}])
   >>> write_collection(replacement, database, "users", mode="replace").ok
   True
   >>> "city" in database.collection("users").get("one")
   False
   >>> upsert = pd.DataFrame(
   ...     [{"_key": "one", "name": "Ada"}, {"_key": "three", "name": "Linus"}]
   ... )
   >>> write_collection(upsert, database, "users", mode="upsert").written_count
   2

Missing values and conversions
------------------------------

``null_policy="null"`` writes missing column values as JSON null.
``null_policy="omit"`` leaves those attributes out of the document. For
updates, ``keep_none=False`` removes attributes that are explicitly sent as
null.

Timezone-aware timestamps become ISO 8601 strings by default. Unsupported
values such as :class:`decimal.Decimal` and :class:`uuid.UUID` need a
per-column converter that returns a JSON-safe value.

.. doctest::

   >>> from decimal import Decimal
   >>> from uuid import UUID
   >>> products = pd.DataFrame(
   ...     [
   ...         {
   ...             "_key": "keyboard",
   ...             "price": Decimal("19.95"),
   ...             "vendor_id": UUID("12345678-1234-5678-1234-567812345678"),
   ...         }
   ...     ]
   ... )
   >>> result = write_collection(
   ...     products,
   ...     database,
   ...     "products",
   ...     create_collection=True,
   ...     converters={"price": str, "vendor_id": str},
   ... )
   >>> result.ok
   True
   >>> stored = database.collection("products").get("keyboard")
   >>> stored["price"], stored["vendor_id"]
   ('19.95', '12345678-1234-5678-1234-567812345678')
