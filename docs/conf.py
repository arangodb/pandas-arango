"""Sphinx configuration for pandas-arangodb."""

from __future__ import annotations

import sys
from importlib.metadata import version as package_version
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

project = "pandas-arangodb"
copyright = "2026, Alexandru Petenchea"
author = "Alexandru Petenchea"
release = package_version("pandas-arangodb")

extensions = [
    "sphinx_rtd_theme",
    "sphinx.ext.autodoc",
    "sphinx.ext.doctest",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
]

exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]
html_theme = "sphinx_rtd_theme"
master_doc = "index"

autodoc_member_order = "bysource"
autodoc_typehints = "none"
napoleon_google_docstring = True
napoleon_numpy_docstring = False

doctest_global_setup = """
import os
from arango import ArangoClient

client = ArangoClient(
    hosts=os.environ.get("PANDAS_ARANGODB_TEST_URL", "http://127.0.0.1:8529")
)
password = os.environ.get("PANDAS_ARANGODB_TEST_PASSWORD", "passwd")
system_database = client.db("_system", username="root", password=password)
database_name = "pandas_arangodb_docs"
if system_database.has_database(database_name):
    system_database.delete_database(database_name)
system_database.create_database(database_name)
database = client.db(database_name, username="root", password=password)
"""

doctest_global_cleanup = """
if system_database.has_database(database_name):
    system_database.delete_database(database_name)
"""
