from typing import Annotated

import pytest
from fastapi import Body, Depends, FastAPI, Query
from fastapi.exceptions import FastAPIDeprecationWarning
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.testclient import TestClient
from inline_snapshot import snapshot
from pydantic import BaseModel, ConfigDict, Field

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
    # only OpenAPI document generation changes.
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


client = TestClient(app)


def test_property_alias_starting_with_sa_is_not_dropped_from_openapi():
    schema = client.get("/openapi.json").json()
    reading_schema = schema["components"]["schemas"]["Reading"]
    assert reading_schema["required"] == ["_sample_rate", "value"]
    assert "_sample_rate" in reading_schema["properties"]
    assert reading_schema["properties"]["_sample_rate"] == snapshot(
        {"title": "Sample Rate", "type": "number"}
    )


def test_body_openapi_examples_starting_with_sa_are_not_dropped():
    schema = client.get("/openapi.json").json()
    examples = schema["paths"]["/readings-with-examples"]["post"]["requestBody"][
        "content"
    ]["application/json"]["examples"]
    assert sorted(examples) == ["_sample", "normal"]


def test_query_openapi_examples_starting_with_sa_are_not_dropped():
    schema = client.get("/openapi.json").json()
    (param,) = [
        p for p in schema["paths"]["/search"]["get"]["parameters"] if p["name"] == "q"
    ]
    assert sorted(param["examples"]) == ["_sample", "normal"]


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
    assert body["content"]["application/json"]["example"] == {
        "_sample_rate": 1.0,
        "value": 2.0,
    }
