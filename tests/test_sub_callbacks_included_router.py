from fastapi import APIRouter, FastAPI
from fastapi.testclient import TestClient
from inline_snapshot import snapshot
from pydantic import BaseModel

app = FastAPI()


class Event(BaseModel):
    id: str


# A callback router built directly (flat) -- the shape that always worked.
flat_callback_router = APIRouter()


@flat_callback_router.post("{$callback_url}/events/{$request.body.id}")
def flat_event_notification(body: Event):
    pass  # pragma: nocover


# A callback router composed from a sub-router via include_router(), the same
# way users compose ordinary routers. Regression: since router.routes can
# contain _IncludedRouter nodes, the callbacks loop in get_openapi_path used to
# assume a flat list of APIRoute objects and silently dropped these.
inner_callback_router = APIRouter()


@inner_callback_router.post("{$callback_url}/events/{$request.body.id}")
def composed_event_notification(body: Event):
    pass  # pragma: nocover


composed_callback_router = APIRouter()
composed_callback_router.include_router(inner_callback_router, prefix="/v2")


@app.post("/subscribe-flat", callbacks=flat_callback_router.routes)
def subscribe_flat(callback_url: str):
    return {"msg": "flat"}


@app.post("/subscribe-composed", callbacks=composed_callback_router.routes)
def subscribe_composed(callback_url: str):
    return {"msg": "composed"}


client = TestClient(app)


def test_subscribe_flat():
    response = client.post(
        "/subscribe-flat", params={"callback_url": "http://example.com"}
    )
    assert response.status_code == 200, response.text
    assert response.json() == {"msg": "flat"}


def test_subscribe_composed():
    response = client.post(
        "/subscribe-composed", params={"callback_url": "http://example.com"}
    )
    assert response.status_code == 200, response.text
    assert response.json() == {"msg": "composed"}


def test_openapi_schema():
    with client:
        response = client.get("/openapi.json")
    assert response.status_code == 200, response.text
    assert response.json() == snapshot(
        {
            "openapi": "3.1.0",
            "info": {"title": "FastAPI", "version": "0.1.0"},
            "paths": {
                "/subscribe-flat": {
                    "post": {
                        "summary": "Subscribe Flat",
                        "operationId": "subscribe_flat_subscribe_flat_post",
                        "parameters": [
                            {
                                "required": True,
                                "schema": {
                                    "type": "string",
                                    "title": "Callback Url",
                                },
                                "name": "callback_url",
                                "in": "query",
                            }
                        ],
                        "responses": {
                            "200": {
                                "description": "Successful Response",
                                "content": {"application/json": {"schema": {}}},
                            },
                            "422": {
                                "description": "Validation Error",
                                "content": {
                                    "application/json": {
                                        "schema": {
                                            "$ref": "#/components/schemas/HTTPValidationError"
                                        }
                                    }
                                },
                            },
                        },
                        "callbacks": {
                            "flat_event_notification": {
                                "{$callback_url}/events/{$request.body.id}": {
                                    "post": {
                                        "summary": "Flat Event Notification",
                                        "operationId": "flat_event_notification__callback_url__events___request_body_id__post",
                                        "requestBody": {
                                            "required": True,
                                            "content": {
                                                "application/json": {
                                                    "schema": {
                                                        "$ref": "#/components/schemas/Event"
                                                    }
                                                }
                                            },
                                        },
                                        "responses": {
                                            "200": {
                                                "description": "Successful Response",
                                                "content": {
                                                    "application/json": {"schema": {}}
                                                },
                                            },
                                            "422": {
                                                "description": "Validation Error",
                                                "content": {
                                                    "application/json": {
                                                        "schema": {
                                                            "$ref": "#/components/schemas/HTTPValidationError"
                                                        }
                                                    }
                                                },
                                            },
                                        },
                                    }
                                }
                            }
                        },
                    }
                },
                "/subscribe-composed": {
                    "post": {
                        "summary": "Subscribe Composed",
                        "operationId": "subscribe_composed_subscribe_composed_post",
                        "parameters": [
                            {
                                "required": True,
                                "schema": {
                                    "type": "string",
                                    "title": "Callback Url",
                                },
                                "name": "callback_url",
                                "in": "query",
                            }
                        ],
                        "responses": {
                            "200": {
                                "description": "Successful Response",
                                "content": {"application/json": {"schema": {}}},
                            },
                            "422": {
                                "description": "Validation Error",
                                "content": {
                                    "application/json": {
                                        "schema": {
                                            "$ref": "#/components/schemas/HTTPValidationError"
                                        }
                                    }
                                },
                            },
                        },
                        "callbacks": {
                            "composed_event_notification": {
                                "/v2{$callback_url}/events/{$request.body.id}": {
                                    "post": {
                                        "summary": "Composed Event Notification",
                                        "operationId": "composed_event_notification_v2__callback_url__events___request_body_id__post",
                                        "requestBody": {
                                            "required": True,
                                            "content": {
                                                "application/json": {
                                                    "schema": {
                                                        "$ref": "#/components/schemas/Event"
                                                    }
                                                }
                                            },
                                        },
                                        "responses": {
                                            "200": {
                                                "description": "Successful Response",
                                                "content": {
                                                    "application/json": {"schema": {}}
                                                },
                                            },
                                            "422": {
                                                "description": "Validation Error",
                                                "content": {
                                                    "application/json": {
                                                        "schema": {
                                                            "$ref": "#/components/schemas/HTTPValidationError"
                                                        }
                                                    }
                                                },
                                            },
                                        },
                                    }
                                }
                            }
                        },
                    }
                },
            },
            "components": {
                "schemas": {
                    "Event": {
                        "title": "Event",
                        "required": ["id"],
                        "type": "object",
                        "properties": {"id": {"title": "Id", "type": "string"}},
                    },
                    "HTTPValidationError": {
                        "title": "HTTPValidationError",
                        "type": "object",
                        "properties": {
                            "detail": {
                                "title": "Detail",
                                "type": "array",
                                "items": {
                                    "$ref": "#/components/schemas/ValidationError"
                                },
                            }
                        },
                    },
                    "ValidationError": {
                        "title": "ValidationError",
                        "required": ["loc", "msg", "type"],
                        "type": "object",
                        "properties": {
                            "loc": {
                                "title": "Location",
                                "type": "array",
                                "items": {
                                    "anyOf": [{"type": "string"}, {"type": "integer"}]
                                },
                            },
                            "msg": {"title": "Message", "type": "string"},
                            "type": {"title": "Error Type", "type": "string"},
                            "input": {"title": "Input"},
                            "ctx": {"type": "object", "title": "Context"},
                        },
                    },
                }
            },
        }
    )


def _iter_refs(node: object):
    if isinstance(node, dict):
        ref = node.get("$ref")
        if isinstance(ref, str):
            yield ref
        for value in node.values():
            yield from _iter_refs(value)
    elif isinstance(node, list):
        for item in node:
            yield from _iter_refs(item)


def test_no_dangling_schema_refs():
    # The callback body model (Event) must land in components alongside the
    # now-fixed callback operation, and every $ref in the document -- not
    # just the ones for the two callback bodies -- must resolve. This is
    # the regression get_fields_from_routes already avoided (it traverses
    # route.callbacks with iter_route_contexts), but get_openapi_path's own
    # callbacks loop did not, until this fix.
    with client:
        schema = client.get("/openapi.json").json()
    schemas = schema["components"]["schemas"]
    assert "Event" in schemas
    for ref in _iter_refs(schema):
        assert ref.startswith("#/components/schemas/"), ref
        name = ref.removeprefix("#/components/schemas/")
        assert name in schemas, f"dangling ref: {ref}"
