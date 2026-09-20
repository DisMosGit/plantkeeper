"""HTTP schemas of the write API.

Each module maps the application layer's views onto the wire format. The two are
kept apart on purpose: this is the contract clients depend on, and the views are
free to change without breaking it.
"""

from __future__ import annotations
