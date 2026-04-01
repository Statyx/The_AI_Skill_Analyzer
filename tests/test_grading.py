"""Non-regression tests for grading.py — answer comparison, number extraction,
pipeline tracing, and root cause analysis.

Run with:  python -m pytest tests/ -v
"""

import pytest
from analyzer.grading import (
    _extract_numbers,
    _compare_answer,
    identify_root_cause,
    trace_pipeline,
    extract_artifacts,
    grade_result,
)


# ══════════════════════════════════════════════════════════════
#  _extract_numbers
# ══════════════════════════════════════════════════════════════

class TestExtractNumbers:
    """Regression tests for magnitude-aware number extraction."""

    def test_plain_integer(self):
        assert _extract_numbers("revenue is 42") == [42.0]

    def test_comma_separated(self):
        assert _extract_numbers("$1,234,567") == [1234567.0]

    def test_decimal(self):
        assert _extract_numbers("margin is 23.5%") == [23.5]

    def test_negative(self):
        assert _extract_numbers("loss of -500") == [-500.0]

    def test_magnitude_M(self):
        nums = _extract_numbers("revenue is 23.5M")
        assert len(nums) == 1
        assert nums[0] == pytest.approx(23_500_000)

    def test_magnitude_B(self):
        nums = _extract_numbers("total 1.7B")
        assert len(nums) == 1
        assert nums[0] == pytest.approx(1_700_000_000)

    def test_magnitude_K(self):
        nums = _extract_numbers("about 500K users")
        assert len(nums) == 1
        assert nums[0] == pytest.approx(500_000)

    def test_magnitude_BN(self):
        nums = _extract_numbers("2.3BN in assets")
        assert len(nums) == 1
        assert nums[0] == pytest.approx(2_300_000_000)

    def test_magnitude_T(self):
        nums = _extract_numbers("1.2T")
        assert len(nums) == 1
        assert nums[0] == pytest.approx(1_200_000_000_000)

    def test_currency_dollar(self):
        nums = _extract_numbers("$1,234.56")
        assert len(nums) == 1
        assert nums[0] == pytest.approx(1234.56)

    def test_percentage_sign(self):
        nums = _extract_numbers("57%")
        assert nums == [57.0]

    def test_multiple_numbers(self):
        nums = _extract_numbers("revenue 100M, expenses 50M, profit 50M")
        assert len(nums) == 3
        assert nums[0] == pytest.approx(100_000_000)

    def test_no_numbers(self):
        assert _extract_numbers("no numbers here") == []

    def test_embedded_in_words_ignored(self):
        # Letters adjacent to digits should not match magnitude suffix
        nums = _extract_numbers("item ABC123 in stock")
        # "123" should still be found but "ABC" prefix blocks it
        # The regex uses (?<![a-zA-Z]) lookbehind
        assert all(isinstance(n, float) for n in nums)

    def test_zero(self):
        assert _extract_numbers("0 results") == [0.0]


# ══════════════════════════════════════════════════════════════
#  _compare_answer — exact
# ══════════════════════════════════════════════════════════════

class TestCompareExact:
    def test_exact_match(self):
        v, _ = _compare_answer("Yes", {"expected": "Yes", "match_type": "exact"})
        assert v == "pass"

    def test_exact_case_insensitive(self):
        v, _ = _compare_answer("YES", {"expected": "yes", "match_type": "exact"})
        assert v == "pass"

    def test_exact_fail(self):
        v, _ = _compare_answer("No", {"expected": "Yes", "match_type": "exact"})
        assert v == "fail"


# ══════════════════════════════════════════════════════════════
#  _compare_answer — contains
# ══════════════════════════════════════════════════════════════

class TestCompareContains:
    def test_contains_pass(self):
        v, _ = _compare_answer(
            "The total Revenue is 1.7B",
            {"expected": "Revenue", "match_type": "contains"},
        )
        assert v == "pass"

    def test_contains_fail(self):
        v, _ = _compare_answer(
            "The total is 1.7B",
            {"expected": "Revenue", "match_type": "contains"},
        )
        assert v == "fail"

    def test_contains_case_insensitive(self):
        v, _ = _compare_answer(
            "revenue grew 10%",
            {"expected": "Revenue", "match_type": "contains"},
        )
        assert v == "pass"


# ══════════════════════════════════════════════════════════════
#  _compare_answer — numeric
# ══════════════════════════════════════════════════════════════

class TestCompareNumeric:
    def test_numeric_exact(self):
        v, _ = _compare_answer(
            "Revenue is 1695514236",
            {"expected": "1695514236", "match_type": "numeric", "tolerance": 0},
        )
        assert v == "pass"

    def test_numeric_with_tolerance(self):
        v, _ = _compare_answer(
            "Revenue is 1695000000",
            {"expected": "1695514236", "match_type": "numeric", "tolerance": 1000000},
        )
        assert v == "pass"

    def test_numeric_magnitude_M(self):
        v, _ = _compare_answer(
            "Revenue is 1695.5M",
            {"expected": "1695514236", "match_type": "numeric", "tolerance": 1000000},
        )
        assert v == "pass"

    def test_numeric_fail_out_of_tolerance(self):
        v, _ = _compare_answer(
            "Revenue is 1500000000",
            {"expected": "1695514236", "match_type": "numeric", "tolerance": 1000000},
        )
        assert v == "fail"

    def test_numeric_no_numbers_in_answer(self):
        v, _ = _compare_answer(
            "I don't have that data",
            {"expected": "100", "match_type": "numeric"},
        )
        assert v == "fail"


# ══════════════════════════════════════════════════════════════
#  _compare_answer — numeric_pct
# ══════════════════════════════════════════════════════════════

class TestCompareNumericPct:
    def test_pct_57_matches_57(self):
        v, _ = _compare_answer(
            "Margin is 57%",
            {"expected": 57, "match_type": "numeric_pct", "tolerance": 2},
        )
        assert v == "pass"

    def test_pct_0_57_matches_57(self):
        """0.57 in answer should match expected=57 via percentage equivalence."""
        v, _ = _compare_answer(
            "Margin is 0.57",
            {"expected": 57, "match_type": "numeric_pct", "tolerance": 2},
        )
        assert v == "pass"

    def test_pct_57_matches_0_57_expected(self):
        """57 in answer should match expected=0.57 via percentage equivalence."""
        v, _ = _compare_answer(
            "Margin is 57%",
            {"expected": 0.57, "match_type": "numeric_pct", "tolerance": 0.02},
        )
        assert v == "pass"


# ══════════════════════════════════════════════════════════════
#  _compare_answer — regex
# ══════════════════════════════════════════════════════════════

class TestCompareRegex:
    def test_regex_pass(self):
        v, _ = _compare_answer(
            "churn rate is 5.2%",
            {"expected": r"churn.*rate", "match_type": "regex"},
        )
        assert v == "pass"

    def test_regex_fail(self):
        v, _ = _compare_answer(
            "retention is high",
            {"expected": r"churn.*rate", "match_type": "regex"},
        )
        assert v == "fail"


# ══════════════════════════════════════════════════════════════
#  _compare_answer — any_of
# ══════════════════════════════════════════════════════════════

class TestCompareAnyOf:
    def test_any_of_first_match(self):
        v, _ = _compare_answer(
            "Sales grew 10%",
            {"expected": ["Sales", "Revenue"], "match_type": "any_of"},
        )
        assert v == "pass"

    def test_any_of_second_match(self):
        v, _ = _compare_answer(
            "Revenue grew 10%",
            {"expected": ["Sales", "Revenue"], "match_type": "any_of"},
        )
        assert v == "pass"

    def test_any_of_none_match(self):
        v, _ = _compare_answer(
            "Profit grew 10%",
            {"expected": ["Sales", "Revenue"], "match_type": "any_of"},
        )
        assert v == "fail"


# ══════════════════════════════════════════════════════════════
#  _compare_answer — list_contains
# ══════════════════════════════════════════════════════════════

class TestCompareListContains:
    def test_all_present(self):
        v, _ = _compare_answer(
            "Top products: Alpha, Beta, Gamma",
            {"expected": ["Alpha", "Beta", "Gamma"], "match_type": "list_contains"},
        )
        assert v == "pass"

    def test_one_missing(self):
        v, d = _compare_answer(
            "Top products: Alpha, Beta",
            {"expected": ["Alpha", "Beta", "Gamma"], "match_type": "list_contains"},
        )
        assert v == "fail"
        assert "Gamma" in d

    def test_order_irrelevant(self):
        v, _ = _compare_answer(
            "Gamma, Alpha, Beta are the top",
            {"expected": ["Alpha", "Beta", "Gamma"], "match_type": "list_contains"},
        )
        assert v == "pass"


# ══════════════════════════════════════════════════════════════
#  _compare_answer — ordered_list
# ══════════════════════════════════════════════════════════════

class TestCompareOrderedList:
    def test_correct_order(self):
        v, _ = _compare_answer(
            "1. Alpha 2. Beta 3. Gamma",
            {"expected": ["Alpha", "Beta", "Gamma"], "match_type": "ordered_list"},
        )
        assert v == "pass"

    def test_wrong_order(self):
        v, d = _compare_answer(
            "1. Beta 2. Alpha 3. Gamma",
            {"expected": ["Alpha", "Beta", "Gamma"], "match_type": "ordered_list"},
        )
        assert v == "fail"
        assert "wrong order" in d.lower()

    def test_missing_item(self):
        v, d = _compare_answer(
            "1. Alpha 2. Beta",
            {"expected": ["Alpha", "Beta", "Gamma"], "match_type": "ordered_list"},
        )
        assert v == "fail"
        assert "Missing" in d


# ══════════════════════════════════════════════════════════════
#  _compare_answer — edge cases
# ══════════════════════════════════════════════════════════════

class TestCompareEdgeCases:
    def test_no_expected(self):
        v, _ = _compare_answer("anything", {"expected": None})
        assert v == "no_expected"

    def test_empty_expected(self):
        v, _ = _compare_answer("anything", {"expected": ""})
        assert v == "no_expected"

    def test_unknown_match_type(self):
        v, _ = _compare_answer("x", {"expected": "x", "match_type": "magic"})
        assert v == "no_expected"

    def test_default_match_type_is_contains(self):
        v, _ = _compare_answer("hello world", {"expected": "world"})
        assert v == "pass"


# ══════════════════════════════════════════════════════════════
#  trace_pipeline
# ══════════════════════════════════════════════════════════════

class TestTracePipeline:
    def test_empty_run_details(self):
        assert trace_pipeline({}) == []

    def test_message_creation_step(self):
        run = {"run_steps": {"data": [
            {"status": "completed", "created_at": 100, "completed_at": 102,
             "step_details": {"tool_calls": []}}
        ]}}
        trace = trace_pipeline(run)
        assert len(trace) == 1
        assert trace[0]["stage"] == "ANSWER_SYNTHESIS"
        assert trace[0]["tool"] == "message_creation"

    def test_nl2code_step(self):
        run = {"run_steps": {"data": [
            {"status": "completed", "created_at": 100, "completed_at": 105,
             "step_details": {"tool_calls": [
                 {"function": {"name": "analyze.database.nl2code",
                               "arguments": '{"query": "what is revenue"}',
                               "output": "```dax\nEVALUATE ROW(\"x\", [Revenue])\n```"}}
             ]}}
        ]}}
        trace = trace_pipeline(run)
        assert len(trace) == 1
        assert trace[0]["stage"] == "NL_TO_QUERY"
        assert trace[0]["duration_s"] == 5.0

    def test_unknown_tool_maps_to_TOOL_CALL(self):
        run = {"run_steps": {"data": [
            {"status": "completed", "created_at": 0, "completed_at": 0,
             "step_details": {"tool_calls": [
                 {"function": {"name": "custom_tool", "arguments": "{}", "output": ""}}
             ]}}
        ]}}
        trace = trace_pipeline(run)
        assert trace[0]["stage"] == "TOOL_CALL"


# ══════════════════════════════════════════════════════════════
#  identify_root_cause
# ══════════════════════════════════════════════════════════════

class TestIdentifyRootCause:
    """Tests for root cause analysis logic."""

    def test_pass_returns_none(self):
        cat, detail = identify_root_cause({}, {}, [], "pass")
        assert cat is None
        assert detail is None

    def test_no_expected_returns_none(self):
        cat, detail = identify_root_cause({}, {}, [], "no_expected")
        assert cat is None

    def test_agent_error_status(self):
        result = {"status": "error", "error": "something broke"}
        cat, _ = identify_root_cause({}, result, [], "fail")
        assert cat == "AGENT_ERROR"

    def test_agent_non_completed(self):
        result = {"status": "cancelled"}
        cat, _ = identify_root_cause({}, result, [], "fail")
        assert cat == "AGENT_ERROR"

    def test_query_error_step_failed(self):
        result = {"status": "completed"}
        trace = [{"tool": "nl2sa_query", "status": "failed",
                  "arguments": {}, "output": {}, "error": "syntax error",
                  "duration_s": None}]
        cat, _ = identify_root_cause({}, result, trace, "fail")
        assert cat == "QUERY_ERROR"

    def test_empty_result_detected(self):
        result = {"status": "completed"}
        trace = [{"tool": "evaluate_dax", "status": "completed",
                  "arguments": {}, "output": {"_raw": "0 rows returned"},
                  "error": None, "duration_s": 1.0}]
        cat, _ = identify_root_cause({}, result, trace, "fail")
        assert cat == "EMPTY_RESULT"

    def test_filter_context_time_intelligence(self):
        result = {"status": "completed"}
        trace = [{"tool": "nl2sa_query", "status": "completed",
                  "arguments": {"query": "EVALUATE CALCULATE([Revenue], __PBI_TimeIntelligenceEnabled=1)"},
                  "output": {"_raw": "some data"}, "error": None, "duration_s": 1.0}]
        cat, _ = identify_root_cause({}, result, trace, "fail")
        assert cat == "FILTER_CONTEXT"

    def test_reformulation_no_tool_calls(self):
        result = {"status": "completed"}
        trace = [{"tool": "message_creation", "status": "completed",
                  "arguments": None, "output": None, "error": None,
                  "duration_s": 1.0}]
        cat, _ = identify_root_cause({}, result, trace, "fail")
        assert cat == "REFORMULATION"

    def test_synthesis_has_answer(self):
        result = {"status": "completed", "answer": "wrong answer"}
        trace = [{"tool": "nl2sa_query", "status": "completed",
                  "arguments": {"query": "EVALUATE [Rev]"},
                  "output": {"_raw": "123"}, "error": None, "duration_s": 1.0}]
        cat, _ = identify_root_cause({}, result, trace, "fail")
        assert cat == "SYNTHESIS"

    def test_unknown_no_answer(self):
        result = {"status": "completed", "answer": ""}
        trace = [{"tool": "nl2sa_query", "status": "completed",
                  "arguments": {"query": "EVALUATE [Rev]"},
                  "output": {"_raw": "123"}, "error": None, "duration_s": 1.0}]
        cat, _ = identify_root_cause({}, result, trace, "fail")
        assert cat == "UNKNOWN"


# ══════════════════════════════════════════════════════════════
#  identify_root_cause — schema cross-referencing
# ══════════════════════════════════════════════════════════════

class TestRCASchemaSignals:
    """Tests for schema-aware RCA signals (measure selection, hidden columns)."""

    SCHEMA = {
        "elements": [{
            "display_name": "Finance",
            "type": "semantic_model.table",
            "children": [
                {"display_name": "Total Revenue", "type": "semantic_model.measure"},
                {"display_name": "Gross Margin", "type": "semantic_model.measure"},
                {"display_name": "Account_ID", "type": "semantic_model.column",
                 "is_hidden": True},
                {"display_name": "Category", "type": "semantic_model.column"},
            ],
        }]
    }

    def test_measure_case_mismatch(self):
        result = {"status": "completed", "answer": "wrong"}
        trace = [{"tool": "nl2sa_query", "status": "completed",
                  "arguments": {"query": "EVALUATE ROW(\"x\", [total revenue])"},
                  "output": {"_raw": "123"}, "error": None, "duration_s": 1.0}]
        cat, detail = identify_root_cause({}, result, trace, "fail",
                                          schema=self.SCHEMA)
        assert cat == "MEASURE_SELECTION"
        assert "case mismatch" in detail.lower()

    def test_unknown_identifier(self):
        result = {"status": "completed", "answer": "wrong"}
        trace = [{"tool": "nl2sa_query", "status": "completed",
                  "arguments": {"query": "EVALUATE ROW(\"x\", [NonExistent])"},
                  "output": {"_raw": "123"}, "error": None, "duration_s": 1.0}]
        cat, detail = identify_root_cause({}, result, trace, "fail",
                                          schema=self.SCHEMA)
        assert cat == "MEASURE_SELECTION"
        assert "Unknown identifier" in detail

    def test_hidden_column_detected(self):
        result = {"status": "completed", "answer": "wrong"}
        trace = [{"tool": "nl2sa_query", "status": "completed",
                  "arguments": {"query": "EVALUATE FILTER(Finance, [Account_ID] > 0)"},
                  "output": {"_raw": "some data"}, "error": None, "duration_s": 1.0}]
        cat, detail = identify_root_cause({}, result, trace, "fail",
                                          schema=self.SCHEMA)
        assert cat == "MEASURE_SELECTION"
        assert "hidden column" in detail.lower()

    def test_known_measure_no_signal(self):
        """Known measure with correct casing should not trigger MEASURE_SELECTION."""
        result = {"status": "completed", "answer": "wrong"}
        trace = [{"tool": "nl2sa_query", "status": "completed",
                  "arguments": {"query": "EVALUATE ROW(\"x\", [Total Revenue])"},
                  "output": {"_raw": "123"}, "error": None, "duration_s": 1.0}]
        cat, _ = identify_root_cause({}, result, trace, "fail",
                                     schema=self.SCHEMA)
        # Should fall through to SYNTHESIS (answer exists but wrong)
        assert cat == "SYNTHESIS"

    def test_userelationship_detected(self):
        result = {"status": "completed", "answer": "wrong"}
        trace = [{"tool": "nl2sa_query", "status": "completed",
                  "arguments": {"query": "CALCULATE([Rev], USERELATIONSHIP(A, B))"},
                  "output": {"_raw": "123"}, "error": None, "duration_s": 1.0}]
        cat, _ = identify_root_cause({}, result, trace, "fail")
        assert cat == "RELATIONSHIP"


# ══════════════════════════════════════════════════════════════
#  extract_artifacts
# ══════════════════════════════════════════════════════════════

class TestExtractArtifacts:
    def test_extracts_reformulated_question(self):
        trace = [{"tool": "nl2sa_query", "stage": "NL_TO_QUERY",
                  "arguments": {"query": "total revenue for 2024"},
                  "output": {}, "status": "completed"}]
        arts = extract_artifacts(trace)
        assert arts["reformulated_question"] == "total revenue for 2024"

    def test_extracts_dax_from_nl2code_fence(self):
        dax = "EVALUATE ROW(\"x\", [Revenue])"
        trace = [{"tool": "analyze.database.nl2code", "stage": "NL_TO_QUERY",
                  "arguments": {},
                  "output": {"_raw": f"```dax\n{dax}\n```"},
                  "status": "completed"}]
        arts = extract_artifacts(trace)
        assert arts["generated_query"] == dax

    def test_empty_pipeline(self):
        arts = extract_artifacts([])
        assert arts["reformulated_question"] is None
        assert arts["generated_query"] is None


# ══════════════════════════════════════════════════════════════
#  grade_result (integration)
# ══════════════════════════════════════════════════════════════

class TestGradeResult:
    def test_pass_with_contains(self):
        result = {"answer": "Total Revenue is 1.7B", "status": "completed",
                  "run_details": {"run_steps": {"data": []}}}
        tc = {"question": "what is revenue", "expected": "Revenue",
              "match_type": "contains", "tags": ["kpi"]}
        g = grade_result(result, tc)
        assert g["verdict"] == "pass"
        assert g["root_cause"] is None

    def test_fail_triggers_rca(self):
        result = {"answer": "No data available", "status": "completed",
                  "run_details": {"run_steps": {"data": [
                      {"status": "completed", "created_at": 0, "completed_at": 1,
                       "step_details": {"tool_calls": [
                           {"function": {"name": "nl2sa_query",
                                         "arguments": '{"query": "EVALUATE [X]"}',
                                         "output": '{"_raw": "0 rows"}'}}
                       ]}}
                  ]}}}
        tc = {"question": "show revenue", "expected": "1000",
              "match_type": "numeric", "tags": []}
        g = grade_result(result, tc)
        assert g["verdict"] == "fail"
        assert g["root_cause"] is not None

    def test_schema_passed_through(self):
        schema = {"elements": [{"display_name": "T", "children": [
            {"display_name": "Rev", "type": "semantic_model.measure"}
        ]}]}
        result = {"answer": "wrong", "status": "completed",
                  "run_details": {"run_steps": {"data": [
                      {"status": "completed", "created_at": 0, "completed_at": 1,
                       "step_details": {"tool_calls": [
                           {"function": {"name": "nl2sa_query",
                                         "arguments": '{"query": "EVALUATE [Unknown]"}',
                                         "output": '{"_raw": "123"}'}}
                       ]}}
                  ]}}}
        tc = {"question": "test", "expected": "right", "match_type": "contains",
              "tags": []}
        g = grade_result(result, tc, schema=schema)
        assert g["root_cause"] == "MEASURE_SELECTION"


# ══════════════════════════════════════════════════════════════
#  Signal priority ordering
# ══════════════════════════════════════════════════════════════

class TestSignalPriority:
    """QUERY_ERROR should take priority over EMPTY_RESULT when both present."""

    def test_query_error_beats_empty(self):
        result = {"status": "completed"}
        trace = [
            # Step 1: query error
            {"tool": "nl2sa_query", "status": "failed",
             "arguments": {}, "output": {"error": "syntax"},
             "error": "bad query", "duration_s": 1.0},
            # Step 2: empty result
            {"tool": "evaluate_dax", "status": "completed",
             "arguments": {}, "output": {"_raw": "no data"},
             "error": None, "duration_s": 1.0},
        ]
        cat, _ = identify_root_cause({}, result, trace, "fail")
        assert cat == "QUERY_ERROR"

    def test_filter_context_beats_measure(self):
        result = {"status": "completed", "answer": "wrong"}
        trace = [{"tool": "nl2sa_query", "status": "completed",
                  "arguments": {"query": "EVALUATE CALCULATE([Rev], TREATAS({1}, T[X]), [BadMeasure])"},
                  "output": {"_raw": "data"}, "error": None, "duration_s": 1.0}]
        schema = {"elements": [{"display_name": "T", "children": [
            {"display_name": "Rev", "type": "semantic_model.measure"}]}]}
        cat, _ = identify_root_cause({}, result, trace, "fail", schema=schema)
        assert cat == "FILTER_CONTEXT"
