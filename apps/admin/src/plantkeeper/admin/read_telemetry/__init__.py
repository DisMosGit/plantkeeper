"""Read models of the ``read_telemetry`` schema.

They are Django's, not SQLAlchemy's, for the same reason the ``read_analytics``
models are: the read side is a Django application, and a second table definition
for the same table would only be a thing to keep in sync. The API's read-only
SQLAlchemy mappings live in
``plantkeeper.infrastructure.persistence.models.read_telemetry`` and a parity test
fails when the two disagree.
"""
