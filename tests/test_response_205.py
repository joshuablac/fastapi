from fastapi import FastAPI, Response
from fastapi.testclient import TestClient
from pydantic import BaseModel

app = FastAPI()


class Item(BaseModel):
    name: str


@app.post("/returns-none", status_code=205)
def returns_none():
    return None


@app.post("/returns-dict", status_code=205)
def returns_dict():
    return {"detail": "reset"}


@app.put("/dynamic", response_model=Item)
def dynamic(response: Response):
    # A response_model is declared, so this takes the dump_json fast path
    # (a different code path than the two routes above). The status code is
    # only known at runtime, via the injected Response parameter, so it
    # bypasses the route-build-time is_body_allowed_for_status_code() assert.
    response.status_code = 205
    return Item(name="x")


@app.get("/not-modified", status_code=304)
def not_modified():
    return None


@app.get("/no-content", status_code=204)
def no_content():
    return None


client = TestClient(app)


def test_205_returning_none_has_content_length_zero():
    response = client.post("/returns-none")
    assert response.status_code == 205
    assert response.content == b""
    assert response.headers.get_list("content-length") == ["0"]


def test_205_returning_dict_has_content_length_zero():
    response = client.post("/returns-dict")
    assert response.status_code == 205
    assert response.content == b""
    assert response.headers.get_list("content-length") == ["0"]


def test_205_set_at_runtime_via_response_param_has_content_length_zero():
    response = client.put("/dynamic")
    assert response.status_code == 205
    assert response.content == b""
    assert response.headers.get_list("content-length") == ["0"]


def test_304_is_not_touched():
    # Starlette never sets Content-Length for 304, and this fix must not
    # start setting it: RFC 9110 ties a 304's Content-Length to the length
    # a 200 response to the same request would have had, which is not "0".
    response = client.get("/not-modified")
    assert response.status_code == 304
    assert response.content == b""
    assert "content-length" not in response.headers


def test_204_is_not_touched():
    response = client.get("/no-content")
    assert response.status_code == 204
    assert response.content == b""
    assert "content-length" not in response.headers
