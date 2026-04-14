"""Test fewshot matching logic."""
from analyzer.reporting import _question_has_fewshot, _load_profile_fewshots

# Test with marketing360 fewshots
fewshots = _load_profile_fewshots({"profile_name": "marketing360"})
print(f"Loaded {len(fewshots)} fewshots from marketing360")
for fs in fewshots:
    print(f"  - {fs['question']}")

# Test matching
print("\nMatch tests:")
tests = [
    ("what is the churn rate", True),
    ("top 5 campaigns by revenue", True),
    ("churn risk by segment", True),
    ("total revenue by country", False),
    ("how many orders this month", False),
]
for q, expected in tests:
    result = _question_has_fewshot(q, fewshots)
    status = "OK" if result == expected else "MISMATCH"
    print(f"  {status}: '{q}' -> has_fewshot={result} (expected={expected})")

# Test with empty profile
empty = _load_profile_fewshots({"profile_name": "cce_validation"})
print(f"\ncce_validation fewshots: {len(empty)}")
print(f"Match with empty: {_question_has_fewshot('test', empty)}")
