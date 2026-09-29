"""Unit tests for the grader. No Databricks workspace required.

The tests that matter most here are not the ones proving a correct run is
graded correctly. They are the ones proving a BROKEN run is not: a missing
count, an unobserved table, a state the grader has never seen. Every one of
those has to arrive as Unknown, and Unknown has to fail.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from expectations import (
    ExpectationAction,
    MatrixError,
    Outcome,
    check_expectation,
    load_matrix,
    resolve_admission,
    resolve_update_verdict,
)

MATRIX_PATH = Path(__file__).resolve().parents[1] / "expectation-matrix.json"

# A run in which the warn action admitted every violating row: 2000 in, 2000 out.
ADMITTED = dict(
    table_exists=True,
    rows_in_target=2000,
    rows_in_source=2000,
    violating_rows_in_source=340,
    update_state="COMPLETED",
)

# The same fixture through the drop action: the 340 violating rows are gone.
DROPPED = dict(ADMITTED, rows_in_target=1660)

# The fail action: update failed, nothing published.
REFUSED = dict(ADMITTED, table_exists=False, rows_in_target=None, update_state="FAILED")


# --------------------------------------------------------------- happy paths


def test_warn_action_is_graded_admitted():
    assert resolve_admission(**ADMITTED) is Outcome.ADMITTED


def test_drop_action_is_graded_dropped():
    assert resolve_admission(**DROPPED) is Outcome.DROPPED


def test_fail_action_is_graded_refused():
    assert resolve_admission(**REFUSED) is Outcome.REFUSED


# ------------------------------------------- a missing measurement is Unknown


@pytest.mark.parametrize("field", ["rows_in_target", "rows_in_source", "violating_rows_in_source"])
def test_a_missing_count_is_unknown_not_a_verdict(field):
    """The central discipline. None is not zero and must not become a verdict."""
    assert resolve_admission(**dict(ADMITTED, **{field: None})) is Outcome.UNKNOWN


def test_an_unobserved_table_is_unknown_even_when_counts_are_present():
    assert resolve_admission(**dict(ADMITTED, table_exists=None)) is Outcome.UNKNOWN


def test_a_missing_update_state_is_unknown():
    assert resolve_admission(**dict(ADMITTED, update_state=None)) is Outcome.UNKNOWN


def test_a_running_update_is_not_a_verdict():
    for state in ("RUNNING", "INITIALIZING", "WAITING_FOR_RESOURCES", "SETTING_UP_TABLES"):
        assert resolve_admission(**dict(ADMITTED, update_state=state)) is Outcome.UNKNOWN


def test_an_unrecognised_state_is_unknown_rather_than_assumed_bad():
    assert resolve_admission(**dict(ADMITTED, update_state="SOME_NEW_STATE")) is Outcome.UNKNOWN


def test_zero_rows_with_an_unobserved_table_is_unknown_not_dropped():
    """The bug this design exists to prevent.

    An empty target and a target nobody looked at both present as a falsy row
    count. One means the expectation dropped everything; the other means the
    harness failed. `if not rows_in_target` would collapse them.
    """
    observed = resolve_admission(
        table_exists=None,
        rows_in_target=0,
        rows_in_source=2000,
        violating_rows_in_source=340,
        update_state="COMPLETED",
    )
    assert observed is Outcome.UNKNOWN


def test_zero_rows_in_an_observed_table_is_still_not_dropped_unless_the_arithmetic_agrees():
    """Every row gone is not the same as the violating rows gone."""
    observed = resolve_admission(
        table_exists=True,
        rows_in_target=0,
        rows_in_source=2000,
        violating_rows_in_source=340,
        update_state="COMPLETED",
    )
    assert observed is Outcome.UNKNOWN


def test_a_partial_drop_is_unknown_rather_than_rounded_to_the_nearer_answer():
    """171 of 340 dropped is neither outcome and must not be reported as one."""
    observed = resolve_admission(**dict(ADMITTED, rows_in_target=1829))
    assert observed is Outcome.UNKNOWN


# ------------------------------------ a failed update that published anyway


def test_a_failed_update_that_published_rows_is_admitted_not_refused():
    """A worse finding than either declared outcome, so it gets its own answer."""
    observed = resolve_admission(**dict(ADMITTED, update_state="FAILED", table_exists=True))
    assert observed is Outcome.ADMITTED


def test_a_failed_update_with_an_unobserved_table_is_unknown():
    observed = resolve_admission(**dict(REFUSED, table_exists=None))
    assert observed is Outcome.UNKNOWN


def test_a_failed_update_with_a_published_but_empty_table_is_unknown():
    observed = resolve_admission(
        **dict(ADMITTED, update_state="FAILED", table_exists=True, rows_in_target=0)
    )
    assert observed is Outcome.UNKNOWN


# ------------------------------------------------- a fixture that cannot fail


def test_a_fixture_with_no_violating_rows_raises_rather_than_passing():
    """A drill that cannot fail is not evidence, so this is an error not a pass."""
    with pytest.raises(MatrixError, match="cannot fail"):
        resolve_admission(**dict(ADMITTED, violating_rows_in_source=0))


# ------------------------------------------------------------ update verdicts


def test_update_verdicts():
    assert resolve_update_verdict("COMPLETED") == "Succeeded"
    assert resolve_update_verdict("FAILED") == "Failed"
    assert resolve_update_verdict("CANCELED") == "Failed"
    assert resolve_update_verdict("RUNNING") == "Unknown"
    assert resolve_update_verdict(None) == "Unknown"


def test_update_verdict_is_case_insensitive():
    assert resolve_update_verdict("completed") == "Succeeded"


# ------------------------------------------------------------ the comparison


def test_a_matching_outcome_passes():
    ok, detail = check_expectation({"expect": "Admitted"}, Outcome.ADMITTED)
    assert ok is True
    assert "as declared" in detail


def test_a_mismatching_outcome_fails_and_says_both_values():
    ok, detail = check_expectation({"expect": "Dropped"}, Outcome.ADMITTED)
    assert ok is False
    assert "Admitted" in detail and "Dropped" in detail


def test_unknown_never_passes_whatever_was_declared():
    for declared in ("Admitted", "Dropped", "Refused"):
        ok, detail = check_expectation({"expect": declared}, Outcome.UNKNOWN)
        assert ok is False
        assert "not measured" in detail


# --------------------------------------------------------- the matrix itself


def test_the_shipped_matrix_loads():
    matrix = load_matrix(MATRIX_PATH)
    assert matrix["fixture"]["rows"] > matrix["fixture"]["violatingRows"] > 0


def test_the_shipped_matrix_exercises_all_three_actions():
    """The finding is a contrast. Two of the three actions report a successful
    update, so a matrix missing one of them would show a uniform result."""
    matrix = load_matrix(MATRIX_PATH)
    actions = {item["action"] for item in matrix["expectations"]}
    assert actions == {a.value for a in ExpectationAction}


def test_the_shipped_matrix_expects_three_distinct_outcomes():
    """The premise assertion.

    If two expectations declared the same outcome, the run could not tell the
    actions apart and the lab would measure nothing, while still reporting a
    row of green ticks.
    """
    matrix = load_matrix(MATRIX_PATH)
    outcomes = [item["expect"] for item in matrix["expectations"]]
    assert len(set(outcomes)) == len(outcomes) == 3


def test_the_shipped_matrix_declares_a_reason_for_every_graded_item():
    matrix = load_matrix(MATRIX_PATH)
    for item in list(matrix["expectations"]) + list(matrix["assertions"]):
        assert item.get("why"), f"{item.get('id')} has no stated reason"


def test_the_shipped_matrix_records_what_was_refused():
    """The discarded list is the part of this repo worth reading."""
    matrix = load_matrix(MATRIX_PATH)
    assert len(matrix["discarded"]) >= 4
    for item in matrix["discarded"]:
        assert item.get("idea") and item.get("why")


# ------------------------------------------------- the matrix loader refuses


def _write(tmp_path: Path, matrix: dict) -> Path:
    path = tmp_path / "m.json"
    path.write_text(json.dumps(matrix), encoding="utf-8")
    return path


def _minimal() -> dict:
    return {
        "fixture": {"rows": 100, "violatingRows": 10},
        "expectations": [
            {"id": "a", "action": "warn", "table": "t1", "expect": "Admitted"},
            {"id": "b", "action": "drop", "table": "t2", "expect": "Dropped"},
            {"id": "c", "action": "fail", "table": "t3", "expect": "Refused"},
        ],
        "assertions": [],
    }


def test_the_minimal_matrix_is_actually_valid():
    """Mutation control. If this fixture were invalid for an unrelated reason,
    every refusal test below would pass without testing its own refusal."""
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        load_matrix(_write(Path(tmp), _minimal()))


@pytest.mark.parametrize("block", ["fixture", "expectations", "assertions"])
def test_a_matrix_missing_a_block_is_refused(tmp_path, block):
    matrix = _minimal()
    del matrix[block]
    with pytest.raises(MatrixError, match=block):
        load_matrix(_write(tmp_path, matrix))


def test_duplicate_expectation_ids_are_refused(tmp_path):
    matrix = _minimal()
    matrix["expectations"][1]["id"] = "a"
    with pytest.raises(MatrixError, match="Duplicate"):
        load_matrix(_write(tmp_path, matrix))


def test_an_unknown_action_is_refused(tmp_path):
    matrix = _minimal()
    matrix["expectations"][0]["action"] = "quarantine"
    with pytest.raises(MatrixError, match="action"):
        load_matrix(_write(tmp_path, matrix))


def test_declaring_unknown_as_the_expected_outcome_is_refused(tmp_path):
    """Otherwise a drill that measured nothing would grade itself green."""
    matrix = _minimal()
    matrix["expectations"][0]["expect"] = "Unknown"
    with pytest.raises(MatrixError, match="Unknown"):
        load_matrix(_write(tmp_path, matrix))


def test_an_outcome_the_grader_cannot_return_is_refused(tmp_path):
    matrix = _minimal()
    matrix["expectations"][0]["expect"] = "Quarantined"
    with pytest.raises(MatrixError, match="cannot return"):
        load_matrix(_write(tmp_path, matrix))


def test_a_matrix_that_skips_an_action_is_refused(tmp_path):
    matrix = _minimal()
    matrix["expectations"] = matrix["expectations"][:2]
    with pytest.raises(MatrixError, match="fail"):
        load_matrix(_write(tmp_path, matrix))


def test_a_fixture_with_no_violating_rows_is_refused(tmp_path):
    matrix = _minimal()
    matrix["fixture"]["violatingRows"] = 0
    with pytest.raises(MatrixError, match="no violating rows"):
        load_matrix(_write(tmp_path, matrix))


def test_a_fixture_where_every_row_violates_is_refused(tmp_path):
    """A dropped result and an empty table would be the same observation."""
    matrix = _minimal()
    matrix["fixture"]["violatingRows"] = 100
    with pytest.raises(MatrixError, match="indistinguishable"):
        load_matrix(_write(tmp_path, matrix))


def test_an_expectation_missing_a_field_is_refused(tmp_path):
    matrix = _minimal()
    del matrix["expectations"][0]["table"]
    with pytest.raises(MatrixError, match="table"):
        load_matrix(_write(tmp_path, matrix))


def test_malformed_json_is_refused(tmp_path):
    path = tmp_path / "m.json"
    path.write_text("{not json", encoding="utf-8")
    with pytest.raises(MatrixError, match="not valid JSON"):
        load_matrix(path)
