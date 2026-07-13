import time

import pytest


@pytest.fixture(autouse=True)
def no_real_sleep(request, monkeypatch):
    """Neutralize time.sleep globally (it's one shared module): backoff paths
    still run, but the suite never actually waits. Tests that need to observe
    sleeps re-patch with their own recorder.

    Scoped away from `integration`-marked tests: those drive real Kafka
    clients (group-join backoff, retrier `wait_until`) that need real sleeps.
    """
    if request.node.get_closest_marker("integration"):
        return
    monkeypatch.setattr(time, "sleep", lambda seconds: None)
