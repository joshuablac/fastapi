from typing import Annotated

import pytest
from fastapi import Body, Depends, FastAPI, Query
from fastapi.exceptions import FastAPIDeprecationWarning
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.testclient import TestClient
from inline_snapshot import snapshot
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

app = FastAPI()
security = HTTPBasic()


class Reading(BaseModel):
    # A field whose alias happens to start with "_sa" -- coincidentally the
    # same prefix jsonable_encoder's sqlalchemy_safe hack strips, for a
    # reason that has nothing to do with SQLAlchemy.
    model_config = ConfigDict(populate_by_name=True)
    sample_rate: float = Field(alias="_sample_rate")
    value: float


@app.post("/readings")
def create_reading(reading: Reading) -> Reading:
    return reading


@app.post("/readings-with-examples")
def create_reading_with_examples(
    reading: Reading = Body(
        openapi_examples={
            "_sample": {"value": {"_sample_rate": 1.0, "value": 2.0}},
            "normal": {"value": {"_sample_rate": 3.0, "value": 4.0}},
        },
    ),
):
    return reading  # pragma: nocover


@app.get("/search")
def search(
    q: str = Query(
        openapi_examples={
            "_sample": {"value": "sample query"},
            "normal": {"value": "normal query"},
        },
    ),
):
    return {"q": q}  # pragma: nocover


@app.get("/dict")
def get_dict():
    # Unaffected control: the response-body drop of "_sa"-prefixed dict keys
    # (jsonable_encoder's default sqlalchemy_safe=True, used by
    # serialize_response in fastapi/routing.py) is untouched by this fix --
    # only OpenAPI document generation changes. This pins existing,
    # separately-tracked data-loss behavior as a scope guard for this fix;
    # it is not asserting that behavior is desirable.
    return {"_salt": "b64salt", "status": "OK"}


@app.get("/secure")
def secure(credentials: Annotated[HTTPBasicCredentials, Depends(security)]):
    # Exercises the jsonable_encoder(security_scheme.model, ...) call in
    # _get_openapi_security_definitions -- the same sqlalchemy_safe=False
    # fix applies there, even though a SecurityScheme dump is framework
    # data and unlikely to carry a "_sa"-prefixed key in practice.
    return {"user": credentials.username}  # pragma: nocover


# The single-value `example=` parameter is deprecated in favor of
# `examples`/`openapi_examples`, but still supported and still goes
# through the same jsonable_encoder call as openapi_examples.
with pytest.warns(FastAPIDeprecationWarning):

    @app.get("/search-legacy-example")
    def search_legacy_example(q: str = Query(example="legacy query")):
        return {"q": q}  # pragma: nocover

    @app.post("/readings-legacy-example")
    def create_reading_legacy_example(
        reading: Reading = Body(example={"_sample_rate": 1.0, "value": 2.0}),
    ):
        return reading  # pragma: nocover


# --- SQLAlchemy objects used as example values -----------------------------
#
# openapi_examples/example values are caller-supplied and can be arbitrary
# objects, including a live SQLAlchemy instance. A plain declarative
# instance falls through jsonable_encoder's vars(obj) path, which is
# exactly what the sqlalchemy_safe filter exists to guard: without it, a
# transient instance publishes its internal "_sa_instance_state" into the
# document, and a session-attached one makes app.openapi() raise entirely
# (turning /openapi.json into a 500). The four example call sites below
# keep the sqlalchemy_safe=True default for this reason.


class _Base(DeclarativeBase):
    pass


class _OrmUser(_Base):
    __tablename__ = "openapi_sa_prefix_orm_user"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str]


def _transient_orm_user() -> _OrmUser:
    # Never added to a session.
    return _OrmUser(id=1, name="alice")


# Kept alive for the life of the test module: closing the session would
# detach the object again and change what jsonable_encoder sees.
_persisted_sessions: list[Session] = []


def _persisted_orm_user() -> _OrmUser:
    # Committed and refreshed: fully session-attached, SQLAlchemy's
    # instrumented state wired up.
    engine = create_engine("sqlite://")
    _Base.metadata.create_all(engine)
    session = Session(engine)
    user = _OrmUser(id=1, name="alice")
    session.add(user)
    session.commit()
    session.refresh(user)
    _persisted_sessions.append(session)
    return user


@app.post("/orm-body-examples-transient")
def orm_body_examples_transient(
    u: dict = Body(openapi_examples={"ex": {"value": _transient_orm_user()}}),
):
    return u  # pragma: nocover


@app.post("/orm-body-examples-persisted")
def orm_body_examples_persisted(
    u: dict = Body(openapi_examples={"ex": {"value": _persisted_orm_user()}}),
):
    return u  # pragma: nocover


with pytest.warns(FastAPIDeprecationWarning):

    @app.post("/orm-body-example-transient")
    def orm_body_example_transient(u: dict = Body(example=_transient_orm_user())):
        return u  # pragma: nocover

    @app.post("/orm-body-example-persisted")
    def orm_body_example_persisted(u: dict = Body(example=_persisted_orm_user())):
        return u  # pragma: nocover


@app.get("/orm-query-examples-transient")
def orm_query_examples_transient(
    q: str = Query(openapi_examples={"ex": {"value": _transient_orm_user()}}),
):
    return {"q": q}  # pragma: nocover


@app.get("/orm-query-examples-persisted")
def orm_query_examples_persisted(
    q: str = Query(openapi_examples={"ex": {"value": _persisted_orm_user()}}),
):
    return {"q": q}  # pragma: nocover


client = TestClient(app)

_ORM_USER_JSON = {"id": 1, "name": "alice"}


def test_property_alias_starting_with_sa_is_not_dropped_from_openapi():
    schema = client.get("/openapi.json").json()
    reading_schema = schema["components"]["schemas"]["Reading"]
    assert reading_schema["required"] == ["_sample_rate", "value"]
    assert "_sample_rate" in reading_schema["properties"]
    assert reading_schema["properties"]["_sample_rate"] == snapshot(
        {"title": "Sample Rate", "type": "number"}
    )


def test_body_openapi_examples_starting_with_sa_are_still_dropped():
    # Known limitation, unchanged from master: an openapi_examples *name*
    # starting with "_sa" is still silently dropped. The four call sites
    # that build parameter/body examples keep the sqlalchemy_safe=True
    # default, because their input can be an arbitrary caller-supplied
    # object (see test_sqlalchemy_object_as_example_matches_master below),
    # and that default filters dict keys by prefix regardless of whether
    # the dict is an example map or a decoded SQLAlchemy object.
    schema = client.get("/openapi.json").json()
    examples = schema["paths"]["/readings-with-examples"]["post"]["requestBody"][
        "content"
    ]["application/json"]["examples"]
    assert sorted(examples) == ["normal"]


def test_query_openapi_examples_starting_with_sa_are_still_dropped():
    # Same known limitation as the body case above, for a query parameter.
    schema = client.get("/openapi.json").json()
    (param,) = [
        p for p in schema["paths"]["/search"]["get"]["parameters"] if p["name"] == "q"
    ]
    assert sorted(param["examples"]) == ["normal"]


def test_response_body_sa_prefixed_keys_are_still_dropped():
    # Unchanged behavior: this fix only touches OpenAPI document generation,
    # not the jsonable_encoder default used for response serialization.
    response = client.get("/dict")
    assert response.json() == {"status": "OK"}


def test_security_scheme_openapi_generation_still_works():
    schema = client.get("/openapi.json").json()
    assert "HTTPBasic" in schema["components"]["securitySchemes"]
    assert schema["components"]["securitySchemes"]["HTTPBasic"] == snapshot(
        {"type": "http", "scheme": "basic"}
    )


def test_legacy_single_example_params_still_generate_openapi():
    schema = client.get("/openapi.json").json()
    query_param = schema["paths"]["/search-legacy-example"]["get"]["parameters"][0]
    assert query_param["example"] == "legacy query"
    body = schema["paths"]["/readings-legacy-example"]["post"]["requestBody"]
    # Known limitation, same as the two tests above: the "_sample_rate" key
    # inside this example *value* is dropped too, same as on master --
    # only the schema's own "properties"/"required" consistency was fixed.
    assert body["content"]["application/json"]["example"] == {"value": 2.0}


def test_sqlalchemy_object_as_example_matches_master():
    # Regression test for the defect this branch was rejected for: with
    # sqlalchemy_safe=False at the four example call sites, a transient
    # SQLAlchemy instance published "_sa_instance_state" into the OpenAPI
    # document, and a session-attached instance made app.openapi() raise
    # (so /openapi.json returned 500 and /docs was unreachable). Neither
    # must happen: an ORM object used as an example value must behave
    # exactly as it does on master, transient or session-attached.
    response = client.get("/openapi.json")
    assert response.status_code == 200, response.text
    schema = response.json()

    body_examples_transient = schema["paths"]["/orm-body-examples-transient"]["post"][
        "requestBody"
    ]["content"]["application/json"]["examples"]["ex"]["value"]
    body_examples_persisted = schema["paths"]["/orm-body-examples-persisted"]["post"][
        "requestBody"
    ]["content"]["application/json"]["examples"]["ex"]["value"]
    body_example_transient = schema["paths"]["/orm-body-example-transient"]["post"][
        "requestBody"
    ]["content"]["application/json"]["example"]
    body_example_persisted = schema["paths"]["/orm-body-example-persisted"]["post"][
        "requestBody"
    ]["content"]["application/json"]["example"]
    (query_examples_transient_param,) = schema["paths"][
        "/orm-query-examples-transient"
    ]["get"]["parameters"]
    (query_examples_persisted_param,) = schema["paths"][
        "/orm-query-examples-persisted"
    ]["get"]["parameters"]

    for value in (
        body_examples_transient,
        body_examples_persisted,
        body_example_transient,
        body_example_persisted,
        query_examples_transient_param["examples"]["ex"]["value"],
        query_examples_persisted_param["examples"]["ex"]["value"],
    ):
        assert value == _ORM_USER_JSON
        assert "_sa_instance_state" not in value
