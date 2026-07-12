"""The one real test: malformed LLM output through the parse/fallback path.

Asserts the retry-once-then-fallback behavior without any network calls.
"""

from types import SimpleNamespace

from app.classifier import classify


class FakeClient:
    """Stands in for anthropic.Anthropic; replays scripted outputs."""

    def __init__(self, outputs: list[str]):
        self.calls: list[str] = []
        self._outputs = list(outputs)
        self.messages = self

    def create(self, **kwargs) -> SimpleNamespace:
        self.calls.append(kwargs["messages"][0]["content"])
        text = self._outputs.pop(0)
        return SimpleNamespace(content=[SimpleNamespace(type="text", text=text)])


def test_malformed_output_retries_once_then_falls_back():
    client = FakeClient(
        [
            "Sure! The label is probably vandalism but here is no JSON.",
            "still {not valid json",
        ]
    )

    result = classify(client, {"id": "1", "title": "X", "comment": "", "byte_delta": 5})

    assert result.label == "unclear"
    assert result.confidence <= 0.2
    assert len(client.calls) == 2, "expected exactly one retry after parse failure"
    assert "ONLY the JSON" in client.calls[1], "retry should tighten the format ask"


def test_retry_recovers_when_second_output_is_dirty_but_parseable():
    client = FakeClient(
        [
            "no json here at all",
            'Here you go: {"label": " Vandalism ", "confidence": 0.9, '
            '"reasoning": "page blanked"} hope that helps!',
        ]
    )

    result = classify(
        client, {"id": "2", "title": "Y", "comment": "", "byte_delta": -4000}
    )

    assert result.label == "vandalism"  # extracted from prose, trimmed, lowercased
    assert result.confidence == 0.9
    assert len(client.calls) == 2
