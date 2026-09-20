"""Documentation artefacts: the tests that keep generated files honest.

The diagrams in ``docs/diagrams/`` and the OpenAPI/AsyncAPI documents in ``docs/``
are derived from the code, and each test here regenerates its artefact in memory
and compares it with what the repository has. A change to an event, a consumer or a
saga step that is not followed by ``make contracts`` fails here rather than being
discovered by a reader of a stale diagram.
"""
