"""Server-sent event framing.

The wire format is small enough to be a function and too easy to get subtly wrong
to be inline in a route: an event is optional ``id`` and ``event`` lines, then one
``data`` line per line of the payload, then a blank line; a comment is a line
starting with a colon; ``retry`` tells a reconnecting client how long to wait.

Keeping it here means the router composes frames and a unit test can pin the
format — including the blank-line terminator that a stream that "sends nothing"
usually forgot to write.
"""

from __future__ import annotations

RETRY_MILLISECONDS = 3000
"""How long a client should wait before reconnecting, in the SSE ``retry`` field.

The server is not aware of a disconnect the instant it happens, so a client that
reconnects immediately would race the old stream's teardown; three seconds is the
browsers' own default, stated explicitly so it does not depend on one.
"""


def sse_event(*, data: str, event: str | None = None, event_id: str | None = None) -> str:
    """Render one event frame.

    ``data`` is split into one ``data:`` line per line: a payload containing a
    newline would otherwise terminate the frame early and be read as a second,
    malformed event.
    """
    lines: list[str] = []
    if event_id is not None:
        lines.append(f"id: {event_id}")
    if event is not None:
        lines.append(f"event: {event}")
    lines.extend(f"data: {line}" for line in data.splitlines() or [""])
    return "\n".join(lines) + "\n\n"


def sse_comment(text: str) -> str:
    """Render one comment frame: what a keep-alive and a status note are."""
    return f": {text}\n\n"


def sse_retry(milliseconds: int) -> str:
    """Render the reconnection hint field."""
    return f"retry: {milliseconds}\n\n"
