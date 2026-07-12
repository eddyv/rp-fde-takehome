import app.classifier
import pytest


@pytest.fixture(autouse=True)
def no_backoff_sleep(monkeypatch):
    """The classifier's retry backoff must not slow the suite down.
    With this fixture active, whenever your application code calls time.sleep(5),
    it instantly executes the lambda function instead, which takes 0 seconds.
    Your retry logic still runs, but the artificial delays are completely eliminated,
    making your test suite run faster.
    """
    monkeypatch.setattr(app.classifier.time, "sleep", lambda seconds: None)
