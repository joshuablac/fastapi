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

import functools
from collections.abc import AsyncIterable, Iterable
from typing import Annotated, Any

import anyio
import pytest
from fastapi import Depends, FastAPI
from fastapi.responses import EventSourceResponse, StreamingResponse
from fastapi.testclient import TestClient
from starlette.types import Message, Scope

pytestmark = [
    pytest.mark.anyio,
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


async def _cleanup_checkpoint(session: Session) -> None:
    # A generator `finally` that itself *awaits a checkpoint* must complete
    # - including everything after that await - before the dependency's
    # teardown. Checking `session.closed` only *after* the await (not
    # before) is what proves the await itself was not cut short by a
    # cancelled scope.
    await anyio.sleep(0)
    events.append(
        "gen cleanup post-checkpoint after dep exit"
        if session.closed
        else "gen cleanup post-checkpoint"
    )


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


@app.get("/sse-slow-producer", response_class=EventSourceResponse)
async def sse_slow_producer(session: SessionDep) -> AsyncIterable[int]:
    # Unlike `/sse` above, this generator is parked *inside its own
    # `await`* (not at a bare `yield`) at the moment the disconnect lands,
    # for as long as `_producer` is genuinely driving it - i.e. `gen`'s
    # frame is entered on `_producer`'s task, not idle. Closing `gen`
    # before the producer task group has been cancelled+joined would then
    # race a live task still inside `gen`, which is exactly what
    # guarantees "no longer under active iteration" is meant to prevent.
    try:
        i = 0
        while True:
            yield i
            i += 1
            if i >= 3:
                await anyio.sleep(10)
    finally:
        _cleanup(session)


# The endpoints below have *no* internal checkpoint in their loop (unlike
# the ones above), so they are always parked at their own bare `yield` with
# nothing in flight when a disconnect happens - the case this fix targets.
# Their `finally` instead awaits a checkpoint itself
# (`_cleanup_checkpoint`), which is the specific required behavior: the
# checkpoint *inside* the `finally`, not just the `finally` starting, must
# complete before the dependency's teardown.
@app.get("/jsonl-checkpoint-finally")
async def jsonl_checkpoint_finally(session: SessionDep) -> AsyncIterable[int]:
    try:
        i = 0
        while True:
            yield i
            i += 1
    finally:
        await _cleanup_checkpoint(session)


@app.get("/raw-checkpoint-finally", response_class=StreamingResponse)
async def raw_checkpoint_finally(session: SessionDep) -> AsyncIterable[str]:
    try:
        i = 0
        while True:
            yield f"{i}\n"
            i += 1
    finally:
        await _cleanup_checkpoint(session)


@app.get("/sse-checkpoint-finally", response_class=EventSourceResponse)
async def sse_checkpoint_finally(session: SessionDep) -> AsyncIterable[int]:
    try:
        i = 0
        while True:
            yield i
            i += 1
    finally:
        await _cleanup_checkpoint(session)


@app.get("/jsonl-finally-raises")
async def jsonl_finally_raises(session: SessionDep) -> AsyncIterable[int]:
    # A generator whose own `finally` raises: previously this error was
    # only ever observable as an unraisable/unretrieved-task warning at GC
    # time, detached from the request. With the generator closed via the
    # request's own exit stack, it now surfaces as a real exception from
    # that unwind, visible to (and here, caught by) the caller.
    try:
        i = 0
        while True:
            yield i
            i += 1
            await anyio.sleep(0)
    finally:
        _cleanup(session)
        raise RuntimeError("cleanup itself failed")


# Regression endpoints: `dependant.call(...)` is not guaranteed to return an
# actual generator. `is_sse_stream` is based on the response class, not on
# the endpoint being a generator, and a `functools.wraps` decorator can make
# `_is_async_gen_callable`/`_is_gen_callable` true (they use
# `inspect.unwrap`) while the object actually returned has no
# `close`/`aclose`. Closing the generator must not assume the method exists.
@app.get("/list-sse", response_class=EventSourceResponse)
def list_sse() -> list[int]:
    return [1, 2, 3]


class _SyncIterator:
    """A plain iterator - has `__iter__`/`__next__`, but no `close`."""

    def __init__(self, source: Iterable[int]) -> None:
        self._it = iter(source)

    def __iter__(self) -> "_SyncIterator":
        return self

    def __next__(self) -> int:
        return next(self._it)


class _AsyncIterator:
    """A plain async iterator - has `__aiter__`/`__anext__`, but no `aclose`."""

    def __init__(self, source: AsyncIterable[str]) -> None:
        self._ait = source.__aiter__()

    def __aiter__(self) -> "_AsyncIterator":
        return self

    async def __anext__(self) -> str:
        return await self._ait.__anext__()


def _wraps_sync_generator_as_plain_iterator(
    f: Any,
) -> Any:
    @functools.wraps(f)
    def wrapper(*args: Any, **kwargs: Any) -> _SyncIterator:
        return _SyncIterator(f(*args, **kwargs))

    return wrapper


def _wraps_async_generator_as_plain_iterator(
    f: Any,
) -> Any:
    @functools.wraps(f)
    def wrapper(*args: Any, **kwargs: Any) -> _AsyncIterator:
        return _AsyncIterator(f(*args, **kwargs))

    return wrapper


@app.get("/jsonl-decorated-plain-iterator")
@_wraps_sync_generator_as_plain_iterator
def jsonl_decorated_plain_iterator() -> Iterable[int]:
    yield from range(3)


@app.get("/raw-decorated-plain-iterator", response_class=StreamingResponse)
@_wraps_async_generator_as_plain_iterator
async def raw_decorated_plain_iterator() -> AsyncIterable[str]:
    for i in range(3):
        yield f"{i}"


client = TestClient(app)


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

    async def receive() -> Message:  # pragma: no cover
        # For spec_version >= 2.4, Starlette's `StreamingResponse.__call__`
        # awaits `stream_response(send)` directly and never starts a
        # `listen_for_disconnect` task, so this must never actually be
        # called - assert instead of hanging forever if that's ever not
        # true. Excluded from the coverage requirement: by design, nothing
        # in this file calls it.
        raise AssertionError(
            "receive() should not be called on the ASGI spec >= 2.4 "
            "send()-raises disconnect path"
        )

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


@pytest.mark.parametrize(
    "path", ["/jsonl-checkpoint-finally", "/raw-checkpoint-finally"]
)
async def test_generator_finally_checkpoint_completes_before_dependency_exit_http_disconnect(
    path: str,
) -> None:
    # A generator `finally` that itself awaits a checkpoint (not just
    # starts) must complete *before* the dependency's teardown - i.e. the
    # close must not be cancelled/cut off partway through. `session.closed`
    # is only checked *after* the await inside `_cleanup_checkpoint`, so
    # "gen cleanup post-checkpoint" (without "after dep exit") proves the
    # await itself ran to completion
    # first.
    events.clear()
    await _call_with_http_disconnect(path)
    assert events == ["gen cleanup post-checkpoint", "dep exit"]


@pytest.mark.parametrize(
    "path", ["/jsonl-checkpoint-finally", "/raw-checkpoint-finally"]
)
async def test_generator_finally_checkpoint_completes_before_dependency_exit_send_raises(
    path: str,
) -> None:
    events.clear()
    await _call_with_send_raising(path)
    assert events == ["gen cleanup post-checkpoint", "dep exit"]


async def test_sse_generator_finally_checkpoint_completes_before_dependency_exit() -> (
    None
):
    # SSE specifically: the producer task group's `_producer` drives the
    # user generator directly (`sse_aiter` *is* `gen` for async
    # generators). This endpoint has no internal checkpoint in its loop, so
    # `gen` is idle at its own bare `yield` (not inside one of its own
    # `await`s) when the disconnect lands - only our `aclose()` call ever
    # resumes it - and the checkpoint inside `finally` must complete before
    # the dependency's exit. (A generator parked in its own `await` instead
    # is a different case, covered by
    # `test_sse_generator_closed_only_after_producer_group_is_joined`
    # below, where cancellation reaches it directly and its `finally`'s own
    # checkpoint is itself cancelled - also correct, just not what this
    # test exercises.)
    events.clear()
    await _call_with_http_disconnect("/sse-checkpoint-finally")
    assert events == ["gen cleanup post-checkpoint", "dep exit"]


async def test_generator_finally_exception_propagates_and_dependency_still_exits() -> (
    None
):
    events.clear()
    caught = await _call_with_http_disconnect("/jsonl-finally-raises")
    assert isinstance(caught, RuntimeError)
    assert str(caught) == "cleanup itself failed"
    # The generator's own cleanup work still happened (before it raised),
    # and the dependency's exit still ran afterward - one misbehaving
    # generator does not stop the rest of the exit stack from unwinding.
    assert events == ["gen cleanup", "dep exit"]


def test_sse_response_class_without_a_generator_endpoint() -> None:
    # `is_sse_stream` is based on `response_class`, not on the endpoint
    # being a generator - a plain function returning a list must still
    # work, not crash while trying to close something that was never a
    # generator.
    response = client.get("/list-sse")
    assert response.status_code == 200
    assert response.text == "data: 1\n\ndata: 2\n\ndata: 3\n\n"


def test_jsonl_decorated_endpoint_returning_a_plain_iterator() -> None:
    # `_is_gen_callable` sees through `functools.wraps` (`inspect.unwrap`),
    # so a decorator can make FastAPI believe the endpoint is a generator
    # callable while the object actually returned has no `close`.
    response = client.get("/jsonl-decorated-plain-iterator")
    assert response.status_code == 200
    assert response.text == "0\n1\n2\n"


def test_raw_decorated_endpoint_returning_a_plain_async_iterator() -> None:
    response = client.get("/raw-decorated-plain-iterator")
    assert response.status_code == 200
    assert response.text == "012"


async def test_sse_generator_closed_only_after_producer_group_is_joined() -> None:
    # Every other SSE test disconnects while `gen` is idle at its own bare
    # `yield`, so it can't tell the difference between "close `gen` before
    # the producer task group is cancelled" and "close it after" - `gen`
    # isn't under active iteration either way. Here `gen` is genuinely
    # parked inside its own `await anyio.sleep(10)`, entered on the
    # producer's task, when the disconnect lands. If `gen.aclose()` were
    # pushed *after* `_sse_producer_cm` (closing before the group is
    # cancelled+joined instead of after), this races a still-running task
    # against `gen` and fails - by hanging (if the producer group is never
    # cancelled) or by an escaped `ExceptionGroup` (if the generator is
    # closed before the group is joined).
    events.clear()
    caught = await _call_with_http_disconnect("/sse-slow-producer", after=3)
    assert caught is None, f"disconnect raised: {caught!r}"
    assert events == ["gen cleanup", "dep exit"]
