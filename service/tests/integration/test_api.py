"""API tests against a real, container-backed Postgres."""

import base64
import json
from datetime import UTC, datetime, timedelta

import pytest
from app import api, db
from app.classifier import Classification
from fastapi.testclient import TestClient
from sqlalchemy import text

pytestmark = pytest.mark.integration

CLASSIFIED_EDIT = {
    "id": "100",
    "title": "Anarchism",
    "user": "Alice",
    "comment": "fix typo",
    "byte_delta": -3,
    "event_time": "2026-07-01T00:00:00+00:00",
}
FAILED_EDIT = {
    "id": "101",
    "title": "Socialism",
    "user": "203.0.113.9",
    "comment": "",
    "byte_delta": -900,
    "event_time": "2026-07-01T00:00:00+00:00",
}

BASE_TIME = datetime(2026, 7, 1, tzinfo=UTC)


def seed_edit(pg_conn, edit_id, processed_at, label="trivia", status="classified"):
    """db.upsert_edit hardcodes processed_at = now(), so insert directly."""
    pg_conn.execute(
        text(
            "INSERT INTO edits (id, label, status, processed_at) "
            "VALUES (:id, :label, :status, :processed_at)"
        ),
        {
            "id": edit_id,
            "label": label if status == "classified" else None,
            "status": status,
            "processed_at": processed_at,
        },
    )


def walk_pages(client, params):
    """Follow next_page to exhaustion; return the list of id-list pages."""
    pages = []
    cursor = None
    while True:
        body = client.get(
            "/edits", params={**params, **({"cursor": cursor} if cursor else {})}
        ).json()
        pages.append([row["id"] for row in body["items"]])
        cursor = body["next_page"]
        if cursor is None:
            return pages


def test_edits_endpoint_filters_and_validates(pg_conn):
    classified = Classification("trivia", 0.9, "typo fix", "test-model")
    db.upsert_edit(pg_conn, CLASSIFIED_EDIT, classified)
    db.upsert_failed_edit(pg_conn, FAILED_EDIT, "parse_failed", "unusable output")

    client = TestClient(api.app)

    body = client.get("/edits").json()
    assert {row["id"] for row in body["items"]} == {"100", "101"}
    assert body["next_page"] is None

    trivia = client.get("/edits", params={"label": "trivia"}).json()
    assert [row["id"] for row in trivia["items"]] == ["100"]

    failed = client.get("/edits", params={"status": "failed"}).json()
    assert [row["id"] for row in failed["items"]] == ["101"]

    bad_label = client.get("/edits", params={"label": "bogus"})
    assert bad_label.status_code == 400

    bad_status = client.get("/edits", params={"status": "bogus"})
    assert bad_status.status_code == 400


def test_response_shape(pg_conn):
    seed_edit(pg_conn, "e1", BASE_TIME)

    client = TestClient(api.app)
    body = client.get("/edits").json()

    assert set(body.keys()) == {
        "items",
        "total",
        "current_page",
        "current_page_backwards",
        "previous_page",
        "next_page",
    }
    assert body["total"] is None


def test_pagination_walks_all_pages(pg_conn):
    # e1 oldest ... e5 newest; e3 exercises microsecond round-tripping.
    for n in range(1, 6):
        micros = 123456 if n == 3 else 0
        processed_at = BASE_TIME + timedelta(minutes=n, microseconds=micros)
        seed_edit(pg_conn, f"e{n}", processed_at)

    client = TestClient(api.app)
    pages = walk_pages(client, {"size": 2})

    assert pages == [["e5", "e4"], ["e3", "e2"], ["e1"]]


def test_pagination_tie_break_on_id(pg_conn):
    for edit_id in ("a1", "a2", "a3"):
        seed_edit(pg_conn, edit_id, BASE_TIME)

    client = TestClient(api.app)
    pages = walk_pages(client, {"size": 1})

    assert pages == [["a3"], ["a2"], ["a1"]]


def test_pagination_with_filter(pg_conn):
    # Trivia and vandalism rows interleaved in time.
    for n, label in enumerate(
        ["trivia", "vandalism", "trivia", "trivia", "vandalism", "trivia"], start=1
    ):
        seed_edit(pg_conn, f"e{n}", BASE_TIME + timedelta(minutes=n), label=label)

    client = TestClient(api.app)
    pages = walk_pages(client, {"label": "trivia", "size": 2})

    assert pages == [["e6", "e4"], ["e3", "e1"]]


def test_invalid_cursor_returns_400(pg_conn):
    client = TestClient(api.app)

    # 1. Not valid base64 at all -> caught inside CursorParams.to_raw_params().
    response = client.get("/edits", params={"cursor": "not-base64!!!"})
    assert response.status_code == 400
    assert response.json()["detail"] == "Invalid cursor value"

    # 2. Valid base64, but garbage to sqlakeyset -> our InvalidPage catch.
    response = client.get(
        "/edits", params={"cursor": base64.urlsafe_b64encode(b"not json").decode()}
    )
    assert response.status_code == 400
    assert response.json()["detail"] == "invalid cursor"

    # 3. An OLD-format (pre-migration) cursor: decodes fine as base64/JSON but
    # is not a sqlakeyset bookmark -> must fail closed (400), not 500.
    old_format = base64.urlsafe_b64encode(
        json.dumps({"p": "2026-07-01T00:00:00+00:00", "i": "1"}).encode()
    ).decode()
    response = client.get("/edits", params={"cursor": old_format})
    assert response.status_code == 400


def test_size_bounds_enforced(pg_conn):
    client = TestClient(api.app)
    response = client.get("/edits", params={"size": 501})
    assert response.status_code == 422


def test_exact_size_last_page(pg_conn):
    for n in range(1, 4):
        seed_edit(pg_conn, f"e{n}", BASE_TIME + timedelta(minutes=n))

    client = TestClient(api.app)
    body = client.get("/edits", params={"size": 3}).json()

    assert [row["id"] for row in body["items"]] == ["e3", "e2", "e1"]
    assert body["next_page"] is None
