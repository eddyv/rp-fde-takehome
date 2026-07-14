"""API tests against a real, container-backed Postgres."""

import base64
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
    """Follow next_cursor to exhaustion; return the list of id-list pages."""
    pages = []
    cursor = None
    while True:
        body = client.get(
            "/edits", params={**params, **({"cursor": cursor} if cursor else {})}
        ).json()
        pages.append([row["id"] for row in body["items"]])
        cursor = body["next_cursor"]
        if cursor is None:
            return pages


def test_edits_endpoint_filters_and_validates(pg_conn):
    classified = Classification("trivia", 0.9, "typo fix", "test-model")
    db.upsert_edit(pg_conn, CLASSIFIED_EDIT, classified)
    db.upsert_failed_edit(pg_conn, FAILED_EDIT, "parse_failed", "unusable output")

    client = TestClient(api.app)

    body = client.get("/edits").json()
    assert {row["id"] for row in body["items"]} == {"100", "101"}
    assert body["next_cursor"] is None

    trivia = client.get("/edits", params={"label": "trivia"}).json()
    assert [row["id"] for row in trivia["items"]] == ["100"]

    failed = client.get("/edits", params={"status": "failed"}).json()
    assert [row["id"] for row in failed["items"]] == ["101"]

    bad_label = client.get("/edits", params={"label": "bogus"})
    assert bad_label.status_code == 400

    bad_status = client.get("/edits", params={"status": "bogus"})
    assert bad_status.status_code == 400


def test_pagination_walks_all_pages(pg_conn):
    # e1 oldest ... e5 newest; e3 exercises microsecond round-tripping.
    for n in range(1, 6):
        micros = 123456 if n == 3 else 0
        processed_at = BASE_TIME + timedelta(minutes=n, microseconds=micros)
        seed_edit(pg_conn, f"e{n}", processed_at)

    client = TestClient(api.app)
    pages = walk_pages(client, {"limit": 2})

    assert pages == [["e5", "e4"], ["e3", "e2"], ["e1"]]


def test_pagination_tie_break_on_id(pg_conn):
    for edit_id in ("a1", "a2", "a3"):
        seed_edit(pg_conn, edit_id, BASE_TIME)

    client = TestClient(api.app)
    pages = walk_pages(client, {"limit": 1})

    assert pages == [["a3"], ["a2"], ["a1"]]


def test_pagination_with_filter(pg_conn):
    # Trivia and vandalism rows interleaved in time.
    for n, label in enumerate(
        ["trivia", "vandalism", "trivia", "trivia", "vandalism", "trivia"], start=1
    ):
        seed_edit(pg_conn, f"e{n}", BASE_TIME + timedelta(minutes=n), label=label)

    client = TestClient(api.app)
    pages = walk_pages(client, {"label": "trivia", "limit": 2})

    assert pages == [["e6", "e4"], ["e3", "e1"]]


def test_invalid_cursor_returns_400(pg_conn):
    client = TestClient(api.app)
    bad_cursors = [
        "not-base64!!!",
        base64.urlsafe_b64encode(b"not json").decode(),
        base64.urlsafe_b64encode(b'{"p": "garbage", "i": "1"}').decode(),
    ]
    for bad in bad_cursors:
        response = client.get("/edits", params={"cursor": bad})
        assert response.status_code == 400
        assert response.json()["detail"] == "invalid cursor"


def test_exact_limit_last_page(pg_conn):
    for n in range(1, 4):
        seed_edit(pg_conn, f"e{n}", BASE_TIME + timedelta(minutes=n))

    client = TestClient(api.app)
    body = client.get("/edits", params={"limit": 3}).json()

    assert [row["id"] for row in body["items"]] == ["e3", "e2", "e1"]
    assert body["next_cursor"] is None
