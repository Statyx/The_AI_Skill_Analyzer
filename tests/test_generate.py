"""Non-regression tests for generate.py — auto-generate questions from schema."""

import json
import os
import tempfile
import pytest

from analyzer.generate import generate_questions, _measure_questions, _ranking_questions, _overview_questions


# ══════════════════════════════════════════════════════════════
#  Template helpers
# ══════════════════════════════════════════════════════════════

class TestTemplateHelpers:
    def test_measure_questions(self):
        qs = _measure_questions("Total Revenue", "Finance", ["kpi"])
        assert len(qs) >= 1
        assert "Total Revenue" in qs[0]["question"]
        assert qs[0]["match_type"] == "contains"
        assert "kpi" in qs[0]["tags"]

    def test_ranking_questions(self):
        qs = _ranking_questions("Category", "Revenue", "Finance", ["finance"])
        assert len(qs) >= 1
        assert "top" in qs[0]["question"].lower()
        assert "ranking" in qs[0]["tags"]

    def test_overview_questions(self):
        qs = _overview_questions("Finance", ["finance"])
        assert len(qs) >= 1
        assert "summary" in qs[0]["question"].lower()
        assert "overview" in qs[0]["tags"]

    def test_underscore_cleaning(self):
        qs = _measure_questions("Gross_Margin_Pct", "Sales", [])
        assert "_" not in qs[0]["question"]


# ══════════════════════════════════════════════════════════════
#  generate_questions
# ══════════════════════════════════════════════════════════════

class TestGenerateQuestions:
    """Tests using a temporary schema.json file."""

    SCHEMA = {
        "elements": [
            {
                "display_name": "Sales",
                "type": "semantic_model.table",
                "children": [
                    {"display_name": "Total Revenue", "type": "semantic_model.measure"},
                    {"display_name": "Gross Margin", "type": "semantic_model.measure"},
                    {"display_name": "Category", "type": "semantic_model.column"},
                    {"display_name": "Internal_ID", "type": "semantic_model.column",
                     "is_hidden": True},
                ],
            },
            {
                "display_name": "Products",
                "type": "semantic_model.table",
                "children": [
                    {"display_name": "Product Count", "type": "semantic_model.measure"},
                    {"display_name": "Product Name", "type": "semantic_model.column"},
                ],
            },
        ]
    }

    @pytest.fixture()
    def cfg_with_schema(self, tmp_path, monkeypatch):
        """Set up a fake profile directory with schema.json."""
        snap_dir = tmp_path / "snapshots"
        snap_dir.mkdir()
        schema_file = snap_dir / "schema.json"
        schema_file.write_text(json.dumps(self.SCHEMA), encoding="utf-8")

        # Monkey-patch snapshot_path to return our temp directory
        import analyzer.generate as gen_mod
        monkeypatch.setattr(gen_mod, "snapshot_path", lambda cfg: snap_dir)

        return {"profile_name": "test_profile", "semantic_model_name": "TestModel"}

    def test_generates_questions(self, cfg_with_schema):
        cases, stats = generate_questions(cfg_with_schema)
        assert stats["tables_scanned"] == 2
        assert stats["questions_generated"] > 0
        assert len(cases) == stats["questions_generated"]

    def test_max_total_limits_output(self, cfg_with_schema):
        cases, stats = generate_questions(cfg_with_schema, max_total=3)
        assert len(cases) <= 3

    def test_hidden_columns_excluded_from_ranking(self, cfg_with_schema):
        cases, _ = generate_questions(cfg_with_schema)
        all_questions = " ".join(c["question"] for c in cases)
        assert "Internal ID" not in all_questions

    def test_missing_schema_raises(self, tmp_path, monkeypatch):
        import analyzer.generate as gen_mod
        monkeypatch.setattr(gen_mod, "snapshot_path", lambda cfg: tmp_path)
        with pytest.raises(FileNotFoundError):
            generate_questions({})

    def test_empty_schema(self, tmp_path, monkeypatch):
        import analyzer.generate as gen_mod
        schema_file = tmp_path / "schema.json"
        schema_file.write_text(json.dumps({"elements": []}), encoding="utf-8")
        monkeypatch.setattr(gen_mod, "snapshot_path", lambda cfg: tmp_path)
        cases, stats = generate_questions({})
        assert len(cases) == 0
        assert stats["questions_generated"] == 0

    def test_every_case_has_required_fields(self, cfg_with_schema):
        cases, _ = generate_questions(cfg_with_schema)
        for tc in cases:
            assert "question" in tc
            assert "match_type" in tc
            assert "tags" in tc
