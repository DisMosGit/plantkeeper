"""External systems the write side talks to through an anti-corruption layer.

The layer is deliberately thin and dependency-free apart from ``httpx``: a
project-owned circuit breaker, and the Trefle client with its Pydantic wire
models and the mapping into domain values. Nothing here knows about Kafka,
SQLAlchemy or Dishka; the DI providers compose it.
"""
