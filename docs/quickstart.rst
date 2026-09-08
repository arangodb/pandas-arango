Quickstart
==========

Connect with ``python-arango``. The resulting database object is passed to
the pandas helpers.

.. code-block:: python

   from arango import ArangoClient

   client = ArangoClient(hosts="http://127.0.0.1:8529")
   database = client.db("my_database", username="root", password="passwd")

Write a DataFrame and read it back with AQL:

.. doctest::

   >>> import pandas as pd
   >>> from pandas_arangodb import read_aql, write_collection
   >>> source = pd.DataFrame(
   ...     [
   ...         {"_key": "ada", "name": "Ada", "score": 10},
   ...         {"_key": "grace", "name": "Grace", "score": 20},
   ...     ]
   ... )
   >>> result = write_collection(
   ...     source,
   ...     database,
   ...     "users",
   ...     create_collection=True,
   ... )
   >>> result.ok, result.written_count
   (True, 2)
   >>> loaded = read_aql(
   ...     database,
   ...     "FOR user IN users SORT user._key RETURN user",
   ...     columns=["_key", "name", "score"],
   ... )
   >>> loaded.to_dict(orient="records")
   [{'_key': 'ada', 'name': 'Ada', 'score': 10}, {'_key': 'grace', 'name': 'Grace', 'score': 20}]

``write_collection`` reports per-document failures in ``result.errors``.
Request-level failures, such as an unavailable database, are raised by
``python-arango``.
