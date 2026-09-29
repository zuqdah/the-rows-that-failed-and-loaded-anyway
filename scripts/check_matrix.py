"""Refuse an incoherent matrix before anything is provisioned.

Run by CI. Exits non-zero with a reason rather than printing a warning, because
a matrix that does not line up with the pipeline would leave the drill
unfalsifiable while its report still looked rigorous.
"""

from __future__ import annotations

import os
import pathlib
import re
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from expectations import load_matrix  # noqa: E402

REPO = pathlib.Path(__file__).resolve().parents[1]

# Measured against a real warehouse on 2026-09-29: 2000 rows, of which 170 carry
# a negative amount and a disjoint 170 carry a null merchant. Pinned here so an
# edit to the fixture has to be re-measured rather than guessed.
MEASURED_ROWS = 2000
MEASURED_VIOLATING = 340


def main() -> int:
    matrix = load_matrix(REPO / "expectation-matrix.json")
    expectations = matrix["expectations"]
    assertions = matrix["assertions"]
    graded = len(expectations) + len(assertions)

    print(f"{len(expectations)} expectations, {len(assertions)} assertions, {graded} graded outcomes.")
    print(
        f"{len(matrix['discarded'])} discarded ideas, "
        f"{len(matrix['knownVariance'])} documented variance."
    )

    source = (REPO / "pipeline" / "expectations_pipeline.py").read_text(encoding="utf-8")
    defined = set(re.findall(r'name="(transactions_[a-z_]+)"', source))
    graded_tables = {item["table"] for item in expectations}

    missing = graded_tables - defined
    if missing:
        print(f"Graded but not defined in the pipeline: {sorted(missing)}", file=sys.stderr)
        return 1

    ungraded = defined - graded_tables - {"transactions_source"}
    if ungraded:
        print(f"Defined in the pipeline but never graded: {sorted(ungraded)}", file=sys.stderr)
        return 1

    fixture = matrix["fixture"]
    if int(fixture["rows"]) != MEASURED_ROWS or int(fixture["violatingRows"]) != MEASURED_VIOLATING:
        print(
            "The fixture numbers do not match what was measured. They were "
            "measured, not chosen: re-measure against a warehouse before "
            "editing them.",
            file=sys.stderr,
        )
        return 1

    # The pipeline has to actually use all three decorators. A source file that
    # declared the tables but reused one decorator would satisfy every check
    # above and measure nothing.
    for decorator in ("expect_all", "expect_all_or_drop", "expect_all_or_fail"):
        if f"@dlt.{decorator}(" not in source:
            print(f"The pipeline source never calls @dlt.{decorator}.", file=sys.stderr)
            return 1

    print("Matrix is coherent with the pipeline source.")

    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with pathlib.Path(summary_path).open("a", encoding="utf-8") as fh:
            fh.write("### Expectation matrix\n\n")
            fh.write(
                f"**{len(expectations)}** expectations and **{len(assertions)}** "
                f"assertions, {graded} graded outcomes.\n\n"
            )
            fh.write(
                f"Fixture: **{fixture['rows']}** rows, "
                f"**{fixture['violatingRows']}** violating.\n\n"
            )
            fh.write(f"Ideas the measurements refused: **{len(matrix['discarded'])}**\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
