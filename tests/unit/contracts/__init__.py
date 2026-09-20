"""The generated integration contracts: OpenAPI and AsyncAPI.

Each test builds its document from the same code ``make contracts`` runs and checks
that it says what the application and the consumer registries say. The documents
themselves are gitignored build products, so what is under test is the generator —
in particular the channel naming, which FastStream derives from a handler name when
a subscription has no title and would silently overwrite a sibling with.
"""
