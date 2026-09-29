"""Grading logic for Lakeflow declarative pipeline expectations.

Nothing in this package talks to Databricks. It takes what a run observed and
decides what that means, so the judgement is unit-testable without a workspace.
"""

from .outcome import (
    Outcome,
    ExpectationAction,
    resolve_admission,
    resolve_update_verdict,
    load_matrix,
    check_expectation,
    MatrixError,
)

__all__ = [
    "ExpectationAction",
    "MatrixError",
    "Outcome",
    "check_expectation",
    "load_matrix",
    "resolve_admission",
    "resolve_update_verdict",
]
