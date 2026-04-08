"""Quick test for French number extraction fix."""
from analyzer.grading import _extract_numbers

tests = [
    ("Il y a 1 535 lignes", [1535.0]),
    ("2 701,88 EUR", [2701.88]),
    ("31 429 480 679,92", [31429480679.92]),
    ("2 705,72 EUR", [2705.72]),
    ("3 785,06 (en euros)", [3785.06]),
    ("34,84 et 18 650,91", [34.84, 18650.91]),
    ("118,89", [118.89]),
    ("1 023 euros/unite", [1023.0]),
    # English should still work
    ("$1,234.56", [1234.56]),
    ("23.5M", [23500000.0]),
]

ok = 0
for text, expected in tests:
    nums = _extract_numbers(text)
    status = "OK" if nums == expected else "FAIL"
    if status == "OK":
        ok += 1
    print(f'  [{status}] "{text}" -> {nums} (expected {expected})')

print(f"\n{ok}/{len(tests)} passed")
