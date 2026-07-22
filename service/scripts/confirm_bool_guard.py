"""Confirm the bool/int/float semantics behind normalize()'s guard.

Part 1: what json.loads actually produces for each literal, and what the
two isinstance checks say about it.
Part 2: feed each case through the real app.classifier.normalize().

Run from the repo root: .venv/bin/python service/scripts/confirm_bool_guard.py
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.classifier import normalize

CASES = ["1.0", "1", "0.75", "true", "false", '"1.0"']

print(f"{'JSON literal':<12} {'py type':<7} {'is bool?':<9} {'is int/float?':<14} float()")
for literal in CASES:
    value = json.loads(f'{{"confidence": {literal}}}')["confidence"]
    is_bool = isinstance(value, bool)
    is_num = isinstance(value, (int, float))
    coerced = float(value) if is_num else "n/a"
    print(f"{literal:<12} {type(value).__name__:<7} {str(is_bool):<9} {str(is_num):<14} {coerced}")

print("\nkey demonstration of the trap the guard exists for:")
print(f"  isinstance(True, int)          = {isinstance(True, int)}   <- bool subclasses int")
print(f"  isinstance(True, (int, float)) = {isinstance(True, (int, float))}   <- numeric check ALONE passes True")
print(f"  float(True)                    = {float(True)}   <- and would fabricate full confidence")

print("\nthrough the real normalize():")
for literal in CASES:
    parsed = json.loads(f'{{"label": "vandalism", "confidence": {literal}, "reasoning": "x"}}')
    result = normalize(parsed, "m")
    outcome = f"accepted, confidence={result.confidence!r}" if result else "REJECTED (None)"
    print(f"  confidence={literal:<7} -> {outcome}")
