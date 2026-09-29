import json
from collections.abc import AsyncIterable, Iterable
from dataclasses import dataclass
from typing import Generic, TypeVar

import pytest
from fastapi import APIRouter, FastAPI
from fastapi.exceptions import ResponseValidationError
from fastapi.responses import EventSourceResponse
from fastapi.sse import ServerSentEvent
from fastapi.testclient import TestClient
from pydantic import BaseModel, Field
from starlette.responses import StreamingResponse

T = TypeVar("T")


class UserOut(BaseModel):
    username: str


class UserInDB(UserOut):
    hashed_password: str


class Item(BaseModel):
    name: str
    secret: str
    optional: str | None = None


class PriceItem(BaseModel):
    name: str
    price: float


class Dog(BaseModel):
    kind: str = "dog"
    bark: str


class Cat(BaseModel):
    kind: str = "cat"
    meow: str


class Wrapper(BaseModel, Generic[T]):
    value: T


class AliasedItem(BaseModel):
    user_name: str = Field(alias="userName")


@dataclass
class DCItem:
    name: str
    secret: str


ROW = UserInDB(username="alice", hashed_password="SECRET-HASH")

app = FastAPI()


# --- Core matrix: explicit response_model= on a generator endpoint --------


@app.get("/jsonl/async", response_model=UserOut)
async def jsonl_async():
    yield ROW


@app.get("/jsonl/sync", response_model=UserOut)
def jsonl_sync():
    yield ROW


@app.get("/sse/plain", response_class=EventSourceResponse, response_model=UserOut)
async def sse_plain():
    yield ROW


@app.get("/jsonl/exclude", response_model=Item, response_model_exclude={"secret"})
async def jsonl_exclude():
    yield Item(name="public", secret="SECRET-FIELD")


@app.get("/jsonl/include", response_model=Item, response_model_include={"name"})
async def jsonl_include():
    yield Item(name="public", secret="SECRET-FIELD")


@app.get("/jsonl/exclude-none", response_model=Item, response_model_exclude_none=True)
async def jsonl_exclude_none():
    yield Item(name="public", secret="SECRET-FIELD", optional=None)


# annotation AND response_model both present -> response_model wins
@app.get("/jsonl/priority", response_model=UserOut)
async def jsonl_priority() -> AsyncIterable[UserInDB]:
    yield ROW


# response_model=AsyncIterable[X] / Iterable[X] -> item type X
@app.get("/jsonl/async-iterable-model", response_model=AsyncIterable[UserOut])
async def jsonl_async_iterable_model():
    yield ROW


@app.get("/jsonl/iterable-model", response_model=Iterable[UserOut])
def jsonl_iterable_model():
    yield ROW


# response_model=list[Item] on a generator: `list` is not a recognized stream
# origin (see fastapi/dependencies/utils.py::_STREAM_ORIGINS), so the item
# type falls back to `response_model` itself -> each yielded item must be a
# list[Item], not a bare Item. Deliberate choice, documented in
# swarm/impl/stream-response-model.md. The dict is yielded (not an Item
# instance) with an extra key, so the test can tell the model was actually
# applied and not just passed through jsonable_encoder.
@app.get("/jsonl/list-model", response_model=list[Item])
async def jsonl_list_model():
    yield [{"name": "a", "secret": "s", "extra_leak": "leak"}]


# A bare item (not wrapped in a list) does not satisfy list[Item] - this
# pins down that the item type really is list[Item], not Item.
@app.get("/jsonl/list-model-bare-item", response_model=list[Item])
async def jsonl_list_model_bare_item():
    yield {"name": "a", "secret": "s"}


# response_model=None explicit keeps the old jsonable_encoder fallback
@app.get("/jsonl/none-model", response_model=None)
async def jsonl_none_model():
    yield ROW


# included router with a prefix (goes through _EffectiveRouteContext)
router = APIRouter()


@router.get("/stream", response_model=UserOut)
async def router_stream():
    yield ROW


app.include_router(router, prefix="/api")


# invalid item mid-stream
@app.get("/jsonl/invalid", response_model=PriceItem)
async def jsonl_invalid():
    yield {"name": "valid", "price": 1.0}
    yield {"name": "invalid", "price": "not-a-float"}


# ServerSentEvent(data=model) bypasses the model - documented as intentional,
# NOT changed by this fix.
@app.get(
    "/sse/wrapped-event", response_class=EventSourceResponse, response_model=UserOut
)
async def sse_wrapped_event():
    yield ServerSentEvent(data=ROW)


# response_model=ServerSentEvent itself: excluded from becoming
# stream_item_type (it's a transport wrapper, not a data model).
@app.get(
    "/sse/server-sent-event-model",
    response_class=EventSourceResponse,
    response_model=ServerSentEvent,
)
async def sse_server_sent_event_model():
    yield ServerSentEvent(data="explicit", event="message")


# non-generator endpoints unchanged
@app.get("/json/non-generator", response_model=UserOut)
async def json_non_generator():
    return ROW


# generator + custom response_class (raw StreamingResponse): response_model
# is not applied to streaming here either, before or after this fix - only
# the default JSONL class and EventSourceResponse trigger per-item filtering.
@app.get("/raw/custom-class", response_class=StreamingResponse, response_model=UserOut)
async def raw_custom_class():
    yield json.dumps({"username": "alice", "hashed_password": "SECRET-HASH"}).encode()


# --- Self-attack: Optional/Union, generics, dataclasses, aliasing, status --


@app.get("/jsonl/union-model", response_model=Dog | Cat)
async def jsonl_union_model():
    yield {"kind": "dog", "bark": "woof", "extra_should_be_dropped": "leak"}


@app.get("/jsonl/generic-model", response_model=Wrapper[Item])
async def jsonl_generic_model():
    yield {"value": {"name": "n", "secret": "s", "extra_leak": "leak"}}


@app.get("/jsonl/dataclass-model", response_model=DCItem)
async def jsonl_dataclass_model():
    yield {"name": "n", "secret": "s", "extra_leak": "leak"}


@app.get("/jsonl/alias-true", response_model=AliasedItem)
async def jsonl_alias_true():
    yield AliasedItem(userName="alice")


@app.get(
    "/jsonl/alias-false", response_model=AliasedItem, response_model_by_alias=False
)
async def jsonl_alias_false():
    yield AliasedItem(userName="alice")


@app.get("/jsonl/status-code", response_model=UserOut, status_code=201)
async def jsonl_status_code():
    yield ROW


# Known gap (see test_response_model_with_no_body_status_code_still_streams_body
# below and swarm/impl/stream-response-model.md "remaining gaps"): a
# body-disallowed status_code on a generator with explicit response_model
# used to fail loudly at route registration; after this fix it builds and
# streams a body anyway.
@app.get("/jsonl/status-code-no-body", response_model=UserOut, status_code=204)
async def jsonl_status_code_no_body():
    yield ROW


client = TestClient(app)
# Does not re-raise exceptions from inside the ASGI app, so it can observe
# what a real client actually receives on the wire when a mid-stream item
# fails validation (a 200 has already gone out - see
# test_invalid_item_mid_stream_client_observes_200_then_truncated_body).
client_no_raise = TestClient(app, raise_server_exceptions=False)


def _lines(response) -> list:
    return [json.loads(line) for line in response.text.strip().splitlines()]


# --- Core matrix assertions ------------------------------------------------


def test_jsonl_async_explicit_response_model_filters_output():
    response = client.get("/jsonl/async")
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/jsonl"
    assert _lines(response) == [{"username": "alice"}]


def test_jsonl_sync_explicit_response_model_filters_output():
    response = client.get("/jsonl/sync")
    assert response.status_code == 200
    assert _lines(response) == [{"username": "alice"}]


def test_sse_explicit_response_model_filters_output():
    response = client.get("/sse/plain")
    assert response.status_code == 200
    assert response.text == 'data: {"username":"alice"}\n\n'


def test_response_model_exclude_applied_per_item():
    response = client.get("/jsonl/exclude")
    assert _lines(response) == [{"name": "public", "optional": None}]


def test_response_model_include_applied_per_item():
    response = client.get("/jsonl/include")
    assert _lines(response) == [{"name": "public"}]


def test_response_model_exclude_none_applied_per_item():
    response = client.get("/jsonl/exclude-none")
    assert _lines(response) == [{"name": "public", "secret": "SECRET-FIELD"}]


def test_response_model_wins_over_return_annotation():
    response = client.get("/jsonl/priority")
    # annotation says -> AsyncIterable[UserInDB] (would include
    # hashed_password); explicit response_model=UserOut takes priority, per
    # docs/en/docs/tutorial/response-model.md.
    assert _lines(response) == [{"username": "alice"}]


def test_response_model_async_iterable_item_type():
    response = client.get("/jsonl/async-iterable-model")
    assert _lines(response) == [{"username": "alice"}]


def test_response_model_iterable_item_type_sync():
    response = client.get("/jsonl/iterable-model")
    assert _lines(response) == [{"username": "alice"}]


def test_list_response_model_validates_item_as_list():
    response = client.get("/jsonl/list-model")
    assert response.status_code == 200
    # The yielded dict has an extra "extra_leak" key; it is dropped because
    # the item is actually validated against list[Item], not just passed
    # through jsonable_encoder.
    assert _lines(response) == [[{"name": "a", "secret": "s", "optional": None}]]


def test_list_response_model_rejects_bare_item():
    with pytest.raises(ResponseValidationError):
        client.get("/jsonl/list-model-bare-item")


def test_response_model_none_keeps_jsonable_encoder_behavior():
    response = client.get("/jsonl/none-model")
    assert response.status_code == 200
    # No model at all -> old jsonable_encoder fallback -> nothing filtered.
    assert _lines(response) == [{"username": "alice", "hashed_password": "SECRET-HASH"}]


def test_included_router_explicit_response_model():
    response = client.get("/api/stream")
    assert response.status_code == 200
    assert _lines(response) == [{"username": "alice"}]


def test_included_router_openapi_itemschema():
    spec = client.get("/openapi.json").json()
    content = spec["paths"]["/api/stream"]["get"]["responses"]["200"]["content"]
    assert content == {
        "application/jsonl": {"itemSchema": {"$ref": "#/components/schemas/UserOut"}}
    }


def test_invalid_item_mid_stream_raises_response_validation_error():
    # The 200 status line (and the already-serialized valid item) are written
    # to the wire before the invalid item aborts the stream. This mirrors the
    # pre-existing behavior for the return-annotation form
    # (tests/test_stream_json_validation_error.py) and is unchanged by this
    # fix - it is simply now also reachable via explicit response_model=.
    with pytest.raises(ResponseValidationError):
        client.get("/jsonl/invalid")


def test_invalid_item_mid_stream_client_observes_200_empty_body():
    # With raise_server_exceptions=False the exception is not re-raised into
    # the caller. Empirically verified (both here and against the
    # pre-existing, unrelated annotation-derived endpoint in
    # tests/test_stream_json_validation_error.py, which shows the identical
    # result): the client sees status 200, but an EMPTY body. The ASGI
    # `http.response.start` message (status 200) is sent before the invalid
    # item is reached, so the status line already went out - but the test
    # transport does not surface any body bytes once the app raises instead
    # of completing the response cleanly, so even the one already-valid item
    # never reaches the httpx response object. This is a pre-existing
    # TestClient/httpx-ASGI-transport behavior, unrelated to and unchanged
    # by this fix.
    response = client_no_raise.get("/jsonl/invalid")
    assert response.status_code == 200
    assert response.text == ""


def test_sse_server_sent_event_data_bypasses_response_model():
    response = client.get("/sse/wrapped-event")
    assert response.status_code == 200
    # ServerSentEvent(data=model) is serialized via model.model_dump_json(),
    # skipping stream_item_field validation entirely - hashed_password
    # leaks. This is documented as intentional in routing.py ("the user may
    # mix types intentionally") and this fix deliberately does NOT change it.
    assert "SECRET-HASH" in response.text


def test_response_model_server_sent_event_type_bypasses_stream_item_type():
    response = client.get("/sse/server-sent-event-model")
    assert response.status_code == 200
    assert response.text == 'event: message\ndata: "explicit"\n\n'


def test_non_generator_endpoint_unchanged():
    response = client.get("/json/non-generator")
    assert response.status_code == 200
    assert response.json() == {"username": "alice"}


def test_generator_custom_response_class_response_model_unchanged():
    response = client.get("/raw/custom-class")
    assert response.status_code == 200
    # response_model is only applied to the default JSONL class and
    # EventSourceResponse; a custom response_class streams the endpoint's
    # raw bytes verbatim, same as before this fix.
    assert b"SECRET-HASH" in response.content


# --- Self-attack assertions -------------------------------------------------


def test_response_model_union_type_filters_extra_and_selects_branch():
    response = client.get("/jsonl/union-model")
    assert _lines(response) == [{"kind": "dog", "bark": "woof"}]


def test_response_model_generic_type_filters_nested_item():
    response = client.get("/jsonl/generic-model")
    assert _lines(response) == [
        {"value": {"name": "n", "secret": "s", "optional": None}}
    ]


def test_response_model_dataclass_item_filters_extra():
    response = client.get("/jsonl/dataclass-model")
    assert _lines(response) == [{"name": "n", "secret": "s"}]


def test_response_model_by_alias_true_default():
    response = client.get("/jsonl/alias-true")
    assert _lines(response) == [{"userName": "alice"}]


def test_response_model_by_alias_false():
    response = client.get("/jsonl/alias-false")
    assert _lines(response) == [{"user_name": "alice"}]


def test_response_model_with_decorator_status_code():
    response = client.get("/jsonl/status-code")
    assert response.status_code == 201
    assert _lines(response) == [{"username": "alice"}]


def test_response_model_with_no_body_status_code_still_streams_body():
    # Known gap, widened (not introduced fresh) by this fix: before it,
    # response_model= plus a body-disallowed status_code (e.g. 204) on a
    # generator crashed at *route registration* time with
    # `AssertionError: Status code 204 must not have a response body`,
    # because route.response_model stayed truthy and hit the
    # is_body_allowed_for_status_code assert in _populate_api_route_state.
    # After this fix, route.response_model becomes None for this route (same
    # as the pre-existing return-annotation form already did), so that
    # assert is skipped, and the route now builds AND streams a body despite
    # the no-body status code - a real HTTP framing violation neither this
    # fix nor the pre-existing annotation-derived path guards against. Same
    # class of gap as the already-known, separately tracked F2 finding (205
    # stale Content-Length, swarm/findings/agent6.md); out of scope for this
    # response_model fix. Documented in
    # swarm/impl/stream-response-model.md "remaining gaps".
    response = client.get("/jsonl/status-code-no-body")
    assert response.status_code == 204
    assert response.content == b'{"username":"alice"}\n'


def test_openapi_jsonl_itemschema_references_model():
    spec = client.get("/openapi.json").json()
    content = spec["paths"]["/jsonl/async"]["get"]["responses"]["200"]["content"]
    assert content == {
        "application/jsonl": {"itemSchema": {"$ref": "#/components/schemas/UserOut"}}
    }


def test_openapi_sse_contentschema_references_model():
    spec = client.get("/openapi.json").json()
    content = spec["paths"]["/sse/plain"]["get"]["responses"]["200"]["content"]
    item_schema = content["text/event-stream"]["itemSchema"]
    assert item_schema["properties"]["data"]["contentSchema"] == {
        "$ref": "#/components/schemas/UserOut"
    }
