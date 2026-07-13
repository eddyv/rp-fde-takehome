"""API tests against a real, container-backed Postgres."""

import pytest
from app import api, db
from app.classifier import Classification
from fastapi.testclient import TestClient

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


def test_edits_endpoint_filters_and_validates(pg_conn):
    classified = Classification("trivia", 0.9, "typo fix", "test-model")
    db.upsert_edit(pg_conn, CLASSIFIED_EDIT, classified)
    db.upsert_failed_edit(pg_conn, FAILED_EDIT, "parse_failed", "unusable output")

    client = TestClient(api.app)

    all_rows = client.get("/edits").json()
    assert {row["id"] for row in all_rows} == {"100", "101"}

    trivia = client.get("/edits", params={"label": "trivia"}).json()
    assert [row["id"] for row in trivia] == ["100"]

    failed = client.get("/edits", params={"status": "failed"}).json()
    assert [row["id"] for row in failed] == ["101"]

    bad_label = client.get("/edits", params={"label": "bogus"})
    assert bad_label.status_code == 400

    bad_status = client.get("/edits", params={"status": "bogus"})
    assert bad_status.status_code == 400
