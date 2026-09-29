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
    # The required behavior (task brief): a generator `finally` that itself
    # *awaits a checkpoint* must complete - including everything after that
    # await - before the dependency's teardown. Checking `session.closed`
    # only *after* the await (not before) is what proves the await itself
    # was not cut short by a cancelled scope.
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
    # The literal required behavior: a generator `finally` that itself
    # awaits a checkpoint (not just starts) must complete *before* the
    # dependency's teardown - i.e. the close must not be cancelled/cut off
    # partway through. `session.closed` is only checked *after* the await
    # inside `_cleanup_checkpoint`, so "gen cleanup post-checkpoint"
    # (without "after dep exit") proves the await itself ran to completion
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
    # generators), so there's a theoretical race between
    # `tg.cancel_scope.cancel()` landing while `_producer` is genuinely
    # inside `gen.__anext__()` (in which case cancellation reaches the
    # generator directly, ahead of and independently of our `aclose()`
    # callback) versus `gen` being parked at its own bare `yield` (the
    # case this fix targets, where only our `aclose()` call ever resumes
    # it). This endpoint has no internal checkpoint in its loop, so it is
    # always in the latter state when idle. Either way the checkpoint
    # inside `finally` must complete before the dependency's exit.
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
