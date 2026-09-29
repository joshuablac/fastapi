"""
Regression tests for the client-disconnect teardown order of streaming
endpoints (JSONL, raw `StreamingResponse`, SSE), sync and async generators.

Two bugs are covered:

- The endpoint generator is not closed as part of request teardown, so on a
  client disconnect observed *between* items (the generator parked at its own
  `yield`, not inside one of its own `await`s) its `finally` only runs later,
  from GC or an async-generator finalizer, *after* request-scoped
  `Depends(..., yield)` dependencies already tore down (e.g. closed a DB
  session the generator's cleanup then uses). See
  docs/en/docs/tutorial/dependencies/dependencies-with-yield.md and
  advanced/custom-response.md ("Dependencies with yield and
  StreamingResponse").
- SSE: an ordinary disconnect under backpressure (or always on trio) raises
  `ExceptionGroup([BrokenResourceError])` out of the app because the
  producer/keepalive task group's streams are closed before the group itself
  is cancelled. https://github.com/fastapi/fastapi/discussions/15725
"""

from collections.abc import AsyncIterable, Iterable
from typing import Annotated, Any

import anyio
import pytest
from fastapi import Depends, FastAPI
from fastapi.responses import EventSourceResponse, StreamingResponse
from starlette.types import Message, Scope

pytestmark = [
    pytest.mark.anyio,
    pytest.mark.filterwarnings("ignore::pytest.PytestUnraisableExceptionWarning"),
]


@pytest.fixture(params=["asyncio", "trio"])
def anyio_backend(request: pytest.FixtureRequest) -> str:
    return request.param


events: list[str] = []


class Session:
    closed = False


async def get_session() -> Any:
    s = Session()
    try:
        yield s
    finally:
        s.closed = True
        events.append("dep exit")


SessionDep = Annotated[Session, Depends(get_session)]

app = FastAPI()


def _cleanup(session: Session) -> None:
    events.append("gen cleanup after dep exit" if session.closed else "gen cleanup")


@app.get("/jsonl")
async def stream_jsonl(session: SessionDep) -> AsyncIterable[int]:
    try:
        i = 0
        while True:
            yield i
            i += 1
            await anyio.sleep(0)
    finally:
        _cleanup(session)


@app.get("/jsonl-sync")
def stream_jsonl_sync(session: SessionDep) -> Iterable[int]:
    try:
        i = 0
        while True:
            yield i
            i += 1
    finally:
        _cleanup(session)


@app.get("/raw", response_class=StreamingResponse)
async def stream_raw(session: SessionDep) -> AsyncIterable[str]:
    try:
        i = 0
        while True:
            yield f"{i}\n"
            i += 1
            await anyio.sleep(0)
    finally:
        _cleanup(session)


@app.get("/raw-sync", response_class=StreamingResponse)
def stream_raw_sync(session: SessionDep) -> Iterable[str]:
    try:
        i = 0
        while True:
            yield f"{i}\n"
            i += 1
    finally:
        _cleanup(session)


@app.get("/sse", response_class=EventSourceResponse)
async def stream_sse(session: SessionDep) -> AsyncIterable[int]:
    try:
        i = 0
        while True:
            yield i
            i += 1
            await anyio.sleep(0)
    finally:
        _cleanup(session)


@app.get("/sse-sync", response_class=EventSourceResponse)
def stream_sse_sync(session: SessionDep) -> Iterable[int]:
    try:
        i = 0
        while True:
            yield i
            i += 1
    finally:
        _cleanup(session)


async def _call_with_http_disconnect(
    path: str, *, after: int = 3, backpressure: bool = False
) -> Exception | None:
    """Simulate ASGI spec < 2.4: the client disconnect arrives as an
    `http.disconnect` message on `receive()`, same as uvicorn's advertised
    spec_version 2.3."""
    sent = 0
    disconnected = anyio.Event()

    async def receive() -> Message:
        await disconnected.wait()
        return {"type": "http.disconnect"}

    async def send(message: Message) -> None:
        nonlocal sent
        if backpressure:
            await anyio.sleep(0.001)
        if message["type"] == "http.response.body" and message.get("body"):
            sent += 1
            if sent >= after:
                disconnected.set()

    scope: Scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.0"},
        "http_version": "1.1",
        "method": "GET",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "root_path": "",
        "headers": [],
        "server": ("test", 80),
        "client": ("test", 1),
        "scheme": "http",
    }
    caught: Exception | None = None
    try:
        with anyio.fail_after(5):
            await app(scope, receive, send)
    except Exception as exc:  # noqa: BLE001 - recorded for the assertions below
        caught = exc
    return caught


async def _call_with_send_raising(path: str, *, after: int = 3) -> Exception | None:
    """Simulate ASGI spec >= 2.4: the server signals a disconnect by raising
    out of `send()` (Starlette wraps `OSError` as `ClientDisconnect`) instead
    of ever delivering an `http.disconnect` message."""
    sent = 0

    async def receive() -> Message:
        await anyio.sleep(float("inf"))
        return {"type": "http.disconnect"}  # pragma: no cover

    async def send(message: Message) -> None:
        nonlocal sent
        if message["type"] == "http.response.body" and message.get("body"):
            sent += 1
            if sent >= after:
                raise OSError("simulated write to a closed connection")

    scope: Scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.4"},
        "http_version": "1.1",
        "method": "GET",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "root_path": "",
        "headers": [],
        "server": ("test", 80),
        "client": ("test", 1),
        "scheme": "http",
    }
    caught: Exception | None = None
    try:
        with anyio.fail_after(5):
            await app(scope, receive, send)
    except Exception as exc:  # noqa: BLE001 - recorded for the assertions below
        caught = exc
    return caught


@pytest.mark.parametrize(
    "path", ["/jsonl", "/jsonl-sync", "/raw", "/raw-sync", "/sse", "/sse-sync"]
)
async def test_generator_closed_before_dependency_exit_http_disconnect(
    path: str,
) -> None:
    events.clear()
    await _call_with_http_disconnect(path)
    # The endpoint generator must be closed as part of request teardown,
    # before the exit code of dependencies with yield (the generator's own
    # cleanup may still need their resources, e.g. a DB session).
    assert events == ["gen cleanup", "dep exit"]


@pytest.mark.parametrize(
    "path", ["/jsonl", "/jsonl-sync", "/raw", "/raw-sync", "/sse", "/sse-sync"]
)
async def test_generator_closed_before_dependency_exit_send_raises(
    path: str,
) -> None:
    events.clear()
    await _call_with_send_raising(path)
    assert events == ["gen cleanup", "dep exit"]


@pytest.mark.parametrize(
    "backpressure", [False, True], ids=["no_backpressure", "backpressure"]
)
async def test_sse_disconnect_returns_cleanly(backpressure: bool) -> None:
    events.clear()
    # A normal client disconnect must not raise an ExceptionGroup out of the
    # app (it would be logged as "Exception in ASGI application") nor reach
    # dependencies with yield as one.
    caught = await _call_with_http_disconnect("/sse", backpressure=backpressure)
    assert caught is None, f"disconnect raised: {caught!r}"
    assert "dep exit" in events
