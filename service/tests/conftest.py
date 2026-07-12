import time

import pytest


@pytest.fixture(autouse=True)
def no_real_sleep(monkeypatch):
    """Neutralize time.sleep globally (it's one shared module): backoff paths
    still run, but the suite never actually waits. Tests that need to observe
    sleeps re-patch with their own recorder."""
    monkeypatch.setattr(time, "sleep", lambda seconds: None)
