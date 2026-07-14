"""Pure-function tests for the /stats aggregator (no DB, no TestClient)."""

from app import api
from sqlalchemy.dialects import postgresql


def test_summarize_stats_empty_input():
    assert api.summarize_stats([]) == {
        "total": 0,
        "by_label": {},
        "by_status": {},
    }


def test_summarize_stats_mixed_labels_and_statuses():
    rows = [
        ("trivia", "classified", 3),
        ("vandalism", "classified", 2),
        ("trivia", "failed", 1),
    ]

    assert api.summarize_stats(rows) == {
        "total": 6,
        "by_label": {"trivia": 4, "vandalism": 2},
        "by_status": {"classified": 5, "failed": 1},
    }


def test_summarize_stats_null_label_counts_in_total_and_status_not_label():
    rows = [(None, "failed", 5)]

    assert api.summarize_stats(rows) == {
        "total": 5,
        "by_label": {},
        "by_status": {"failed": 5},
    }


def test_summarize_stats_same_label_across_statuses_aggregates():
    rows = [
        ("trivia", "classified", 2),
        ("trivia", "failed", 1),
    ]

    assert api.summarize_stats(rows) == {
        "total": 3,
        "by_label": {"trivia": 3},
        "by_status": {"classified": 2, "failed": 1},
    }


def test_stats_statement_groups_label_and_status():
    sql = str(api.STATS_STMT.compile(dialect=postgresql.dialect()))
    assert "count(*)" in sql
    assert "GROUP BY edits.label, edits.status" in sql
