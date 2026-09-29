"""What a measured pipeline run means, decided without a pipeline present."""

from __future__ import annotations

import json
from enum import Enum
from pathlib import Path
from collections.abc import Mapping, Sequence
from typing import Any


class Outcome(str, Enum):
    """What happened to the rows that violated a declared expectation."""

    ADMITTED = "Admitted"
    DROPPED = "Dropped"
    REFUSED = "Refused"
    UNKNOWN = "Unknown"


class ExpectationAction(str, Enum):
    """The three decorators, which differ by one word and not by one degree."""

    WARN = "warn"
    DROP = "drop"
    FAIL = "fail"


class MatrixError(Exception):
    """The declared matrix is unusable. Raised rather than defaulted around."""


TERMINAL_OK = frozenset({"COMPLETED"})
TERMINAL_BAD = frozenset({"FAILED", "CANCELED"})


def positive(value: Any) -> bool:
    """True only for a count that was measured and is above zero."""
    if value is None:
        return False
    try:
        return int(value) > 0
    except (TypeError, ValueError):
        return False


def resolve_admission(
    table_exists: Any,
    rows_in_target: Any,
    rows_in_source: Any,
    violating_rows_in_source: Any,
    update_state: Any,
) -> Outcome:
    """Decide what became of the violating rows.

    Every count is checked against None explicitly. The trap being avoided is
    `if not rows_in_target:`, which is true for 0 and true for None, so a table
    that was never created and a table that correctly dropped every row collapse
    into one answer. They are opposite findings: a broken harness and a working
    feature. A count of 0 is a fact; an absent count is not, and has to travel
    as Unknown rather than be coerced into one.
    """
    if update_state is None:
        return Outcome.UNKNOWN
    state = str(update_state).upper()

    if state in TERMINAL_BAD:
        if table_exists is False:
            return Outcome.REFUSED
        if table_exists is None:
            return Outcome.UNKNOWN
        return Outcome.ADMITTED if positive(rows_in_target) else Outcome.UNKNOWN

    if state not in TERMINAL_OK:
        return Outcome.UNKNOWN

    if table_exists is not True:
        return Outcome.UNKNOWN

    if rows_in_target is None or rows_in_source is None:
        return Outcome.UNKNOWN
    if violating_rows_in_source is None:
        return Outcome.UNKNOWN

    target = int(rows_in_target)
    source = int(rows_in_source)
    violating = int(violating_rows_in_source)

    if violating <= 0:
        raise MatrixError(
            "The fixture contains no violating rows, so this run cannot tell an "
            "admitted row from a dropped one. A drill that cannot fail is not evidence."
        )

    if target == source:
        return Outcome.ADMITTED
    if target == source - violating:
        return Outcome.DROPPED
    return Outcome.UNKNOWN


def resolve_update_verdict(update_state: Any) -> str:
    """Normalise a pipeline update state into Succeeded / Failed / Unknown."""
    if update_state is None:
        return "Unknown"
    state = str(update_state).upper()
    if state in TERMINAL_OK:
        return "Succeeded"
    if state in TERMINAL_BAD:
        return "Failed"
    return "Unknown"


def check_expectation(declared: Mapping[str, Any], observed: Outcome) -> tuple[bool, str]:
    """Compare one declared expectation against what was measured."""
    expected = Outcome(str(declared["expect"]))
    if observed is Outcome.UNKNOWN:
        return False, "Unknown -- not measured, which is a failure and not a pass"
    if observed is expected:
        return True, observed.value + " as declared"
    return False, observed.value + ", declared " + expected.value


def load_matrix(path: Any) -> Mapping[str, Any]:
    """Read the declared matrix and refuse an incoherent one."""
    raw = Path(path).read_text(encoding="utf-8")
    try:
        matrix = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise MatrixError(str(path) + " is not valid JSON: " + str(exc)) from exc

    for key in ("fixture", "expectations", "assertions"):
        if key not in matrix:
            raise MatrixError(str(path) + " has no '" + key + "' block.")

    expectations = matrix["expectations"]
    if not isinstance(expectations, Sequence) or not expectations:
        raise MatrixError("'expectations' must be a non-empty list.")

    seen = set()
    actions = set()
    for item in expectations:
        for field in ("id", "action", "table", "expect"):
            if field not in item:
                raise MatrixError("An expectation entry has no '" + field + "'.")
        if item["id"] in seen:
            raise MatrixError("Duplicate expectation id: " + str(item["id"]))
        seen.add(item["id"])
        try:
            ExpectationAction(item["action"])
        except ValueError as exc:
            raise MatrixError(
                "Expectation " + str(item["id"]) + " declares an action this grader "
                "does not know: " + str(item["action"])
            ) from exc
        actions.add(item["action"])
        try:
            expected = Outcome(item["expect"])
        except ValueError as exc:
            raise MatrixError(
                "Expectation " + str(item["id"]) + " expects an outcome this grader "
                "cannot return: " + str(item["expect"])
            ) from exc
        if expected is Outcome.UNKNOWN:
            raise MatrixError(
                "Expectation " + str(item["id"]) + " declares Unknown as expected. "
                "Unknown means the measurement failed, so declaring it expected "
                "would let a broken drill pass."
            )

    missing = set(a.value for a in ExpectationAction) - actions
    if missing:
        raise MatrixError(
            "The matrix does not exercise: " + ", ".join(sorted(missing)) + ". The "
            "finding is a contrast between the three actions, so leaving one out "
            "removes the comparison that makes it falsifiable."
        )

    fixture = matrix["fixture"]
    for field in ("rows", "violatingRows"):
        if field not in fixture:
            raise MatrixError("The fixture block has no '" + field + "'.")
    rows = int(fixture["rows"])
    violating = int(fixture["violatingRows"])
    if violating <= 0:
        raise MatrixError("The fixture declares no violating rows, so nothing can be graded.")
    if violating >= rows:
        raise MatrixError(
            "Every fixture row violates the expectation, so a dropped result and "
            "an empty table would be indistinguishable."
        )
    return matrix
