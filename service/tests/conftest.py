import app.classifier
import pytest


@pytest.fixture(autouse=True)
def no_backoff_sleep(monkeypatch):
    """The classifier's retry backoff must not slow the suite down."""
    monkeypatch.setattr(app.classifier.time, "sleep", lambda seconds: None)
