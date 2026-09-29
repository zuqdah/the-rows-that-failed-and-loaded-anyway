"""Run both passes against a real Databricks workspace and grade the result.

Pass 1 (`admit`)  builds the warn and drop targets and must end COMPLETED.
Pass 2 (`refuse`) builds the fail target alone and must end FAILED.

Nothing here decides what a result means. The grading lives in `expectations`,
which touches no workspace and is covered by unit tests.

Auth comes from the environment the way the SDK expects it: DATABRICKS_HOST
plus either DATABRICKS_CLIENT_ID/DATABRICKS_CLIENT_SECRET for a service
principal, or DATABRICKS_TOKEN, or a configured profile. No credential is read,
printed or written by this script.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import pipelines as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from expectations import (  # noqa: E402
    Outcome,
    check_expectation,
    load_matrix,
    resolve_admission,
    resolve_update_verdict,
)

REPO = Path(__file__).resolve().parents[1]
SOURCE = REPO / "pipeline" / "expectations_pipeline.py"
MATRIX = REPO / "expectation-matrix.json"

TERMINAL = {"COMPLETED", "FAILED", "CANCELED"}


def log(message: str) -> None:
    print(message, flush=True)


# ----------------------------------------------------------------- the runner


def ensure_source(w: WorkspaceClient, workspace_dir: str) -> str:
    """Put the pipeline source in the workspace and return its path."""
    from databricks.sdk.service.workspace import ImportFormat, Language

    w.workspace.mkdirs(workspace_dir)
    target = f"{workspace_dir}/expectations_pipeline"
    w.workspace.upload(
        target,
        SOURCE.read_bytes(),
        format=ImportFormat.SOURCE,
        language=Language.PYTHON,
        overwrite=True,
    )
    log(f"   source uploaded to {target}")
    return target


def ensure_pipeline(w: WorkspaceClient, name: str, source_path: str, catalog: str, schema: str) -> str:
    """Create the pipeline, or reuse the one this lab already made.

    Free Edition allows one active pipeline per pipeline type, so a lab that
    created a second one would fail for a quota reason unrelated to anything it
    measures.
    """
    for existing in w.pipelines.list_pipelines():
        if existing.name == name:
            log(f"   reusing pipeline {existing.pipeline_id}")
            return existing.pipeline_id

    created = w.pipelines.create(
        name=name,
        serverless=True,
        catalog=catalog,
        schema=schema,
        continuous=False,
        development=True,
        libraries=[pl.PipelineLibrary(notebook=pl.NotebookLibrary(path=source_path))],
    )
    log(f"   created pipeline {created.pipeline_id}")
    return created.pipeline_id


def run_pass(
    w: WorkspaceClient,
    pipeline_id: str,
    name: str,
    source_path: str,
    catalog: str,
    schema: str,
    which: str,
    timeout_s: int = 900,
) -> tuple[str, str]:
    """Set the pass, run one update to a terminal state, return (update_id, state).

    The name is passed through unchanged. An earlier version derived it from the
    catalog and schema, which renamed the pipeline on the first pass so the
    reuse-by-name lookup missed it on the next run and tried to create a second
    one -- which Free Edition refuses, for a quota reason that has nothing to do
    with expectations.
    """
    w.pipelines.update(
        pipeline_id=pipeline_id,
        name=name,
        serverless=True,
        catalog=catalog,
        schema=schema,
        continuous=False,
        development=True,
        configuration={"lab.pass": which},
        libraries=[pl.PipelineLibrary(notebook=pl.NotebookLibrary(path=source_path))],
    )
    started = w.pipelines.start_update(pipeline_id, full_refresh=True)
    update_id = started.update_id
    log(f"   pass '{which}' update {update_id}")

    deadline = time.time() + timeout_s
    state = "UNKNOWN"
    while time.time() < deadline:
        info = w.pipelines.get(pipeline_id)
        found = None
        for candidate in info.latest_updates or []:
            if candidate.update_id == update_id:
                found = candidate
                break
        if found is not None and found.state is not None:
            state = str(found.state.value if hasattr(found.state, "value") else found.state)
            state = state.upper()
            if state in TERMINAL:
                log(f"   pass '{which}' -> {state}")
                return update_id, state
        time.sleep(10)

    # A timeout is not a failed expectation. It is an unmeasured run, and it has
    # to be reported as one rather than graded.
    log(f"   pass '{which}' did not reach a terminal state within {timeout_s}s")
    return update_id, "TIMED_OUT"


# ------------------------------------------------------------- the measuring


def scalar(w: WorkspaceClient, warehouse_id: str, sql: str):
    """Run one query and return its single value, or None if it did not answer.

    None means the count was not obtained. It is deliberately not 0: the grader
    treats a missing count as Unknown and a zero count as a fact, and collapsing
    them is the bug this whole lab is about.
    """
    try:
        result = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id, statement=sql, wait_timeout="50s"
        )
    except Exception as exc:  # noqa: BLE001 - any failure here means "not measured"
        log(f"   query failed: {exc}")
        return None
    state = result.status.state.value if result.status and result.status.state else "?"
    if str(state).upper() != "SUCCEEDED":
        log(f"   query did not succeed ({state}): {sql.strip()[:60]}")
        return None
    rows = result.result.data_array if result.result else None
    if not rows or rows[0][0] is None:
        return None
    return int(rows[0][0])


def table_exists(w: WorkspaceClient, catalog: str, schema: str, table: str):
    """True, False, or None when the listing itself could not be obtained."""
    try:
        names = {t.name for t in w.tables.list(catalog_name=catalog, schema_name=schema)}
    except Exception as exc:  # noqa: BLE001
        log(f"   could not list tables: {exc}")
        return None
    return table in names


def data_quality_events(w: WorkspaceClient, pipeline_id: str, update_id: str) -> list[dict]:
    """Read the event log through the REST API rather than the CLI.

    The CLI's list-pipeline-events drops the `details` field, and `details` is
    where data_quality lives -- so read that way, a pipeline with violated
    expectations looks exactly like one that never declared any. Measured on
    CLI v1.18.0: 0 of 37 events carried a details field.
    """
    response = w.api_client.do(
        "GET", f"/api/2.0/pipelines/{pipeline_id}/events", query={"max_results": 250}
    )
    events = response.get("events", []) if isinstance(response, dict) else []

    with_details = [e for e in events if e.get("details")]
    if events and not with_details:
        raise RuntimeError(
            "Every event came back without a details field. That is what the CLI "
            "does, and reading the event log that way cannot tell a violated "
            "expectation from an absent one. Refusing to grade on it."
        )

    out = []
    for event in events:
        origin = event.get("origin") or {}
        if origin.get("update_id") != update_id:
            continue
        progress = (event.get("details") or {}).get("flow_progress") or {}
        quality = progress.get("data_quality")
        if not quality:
            continue
        out.append(
            {
                "flow": origin.get("flow_name", "?"),
                "level": event.get("level", "?"),
                "quality": quality,
            }
        )
    return out


def failed_records_for(events: list[dict], table: str):
    """Sum failed_records across the expectations recorded for one table."""
    for event in events:
        if not event["flow"].endswith("." + table):
            continue
        expectations = event["quality"].get("expectations") or []
        if not expectations:
            return None
        return sum(int(e.get("failed_records", 0)) for e in expectations)
    return None


def level_for(events: list[dict], table: str):
    for event in events:
        if event["flow"].endswith("." + table):
            return event["level"]
    return None


# ------------------------------------------------------------------- the main


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", default="workspace")
    parser.add_argument("--schema", default="lab_expectations")
    parser.add_argument("--warehouse-id", required=True)
    parser.add_argument("--pipeline-name", default="lab-expectations")
    parser.add_argument("--workspace-dir", default=None)
    parser.add_argument("--keep", action="store_true", help="Leave the pipeline and tables behind.")
    args = parser.parse_args()

    matrix = load_matrix(MATRIX)
    declared = {item["id"]: item for item in matrix["expectations"]}
    fixture_rows = int(matrix["fixture"]["rows"])
    fixture_violating = int(matrix["fixture"]["violatingRows"])

    w = WorkspaceClient()
    me = w.current_user.me()
    log(f"== Signed in as {me.user_name}")

    workspace_dir = args.workspace_dir or f"/Users/{me.user_name}/lab-expectations"
    source_path = ensure_source(w, workspace_dir)
    pipeline_id = ensure_pipeline(
        w, args.pipeline_name, source_path, args.catalog, args.schema
    )

    results: dict[str, Outcome] = {}
    observed: dict[str, object] = {}

    # ------------------------------------------------------------ pass 1
    log("== Pass 1: warn and drop")
    admit_update, admit_state = run_pass(
        w, pipeline_id, args.pipeline_name, source_path, args.catalog, args.schema, "admit"
    )
    admit_events = data_quality_events(w, pipeline_id, admit_update)

    fq = f"{args.catalog}.{args.schema}"
    source_rows = scalar(w, args.warehouse_id, f"SELECT count(*) FROM {fq}.transactions_source")
    warn_rows = scalar(w, args.warehouse_id, f"SELECT count(*) FROM {fq}.transactions_warn")
    drop_rows = scalar(w, args.warehouse_id, f"SELECT count(*) FROM {fq}.transactions_drop")

    log(f"   source={source_rows}  warn={warn_rows}  drop={drop_rows}")

    results["warn-admits-the-violating-rows"] = resolve_admission(
        table_exists=table_exists(w, args.catalog, args.schema, "transactions_warn"),
        rows_in_target=warn_rows,
        rows_in_source=source_rows,
        violating_rows_in_source=fixture_violating,
        update_state=admit_state,
    )
    results["drop-removes-the-violating-rows"] = resolve_admission(
        table_exists=table_exists(w, args.catalog, args.schema, "transactions_drop"),
        rows_in_target=drop_rows,
        rows_in_source=source_rows,
        violating_rows_in_source=fixture_violating,
        update_state=admit_state,
    )

    # ------------------------------------------------------------ pass 2
    log("== Pass 2: fail")
    refuse_update, refuse_state = run_pass(
        w, pipeline_id, args.pipeline_name, source_path, args.catalog, args.schema, "refuse"
    )
    refuse_events = data_quality_events(w, pipeline_id, refuse_update)
    fail_published = table_exists(w, args.catalog, args.schema, "transactions_fail")
    log(f"   transactions_fail published: {fail_published}")

    results["fail-refuses-to-publish"] = resolve_admission(
        table_exists=fail_published,
        rows_in_target=None if fail_published is False else scalar(
            w, args.warehouse_id, f"SELECT count(*) FROM {fq}.transactions_fail"
        ),
        rows_in_source=source_rows,
        violating_rows_in_source=fixture_violating,
        update_state=refuse_state,
    )

    # -------------------------------------------------------- assertions
    warn_failed = failed_records_for(admit_events, "transactions_warn")
    fail_failed = failed_records_for(refuse_events, "transactions_fail")
    warn_level = level_for(admit_events, "transactions_warn")
    drop_level = level_for(admit_events, "transactions_drop")
    fail_level = level_for(refuse_events, "transactions_fail")

    observed["the-update-succeeds-while-admitting-violations"] = (
        resolve_update_verdict(admit_state) == "Succeeded"
        and results["warn-admits-the-violating-rows"] is Outcome.ADMITTED
    )
    observed["the-row-count-does-not-reveal-it"] = (
        None if warn_rows is None or source_rows is None else warn_rows == source_rows
    )
    observed["the-violation-is-recorded-at-info"] = (
        None
        if warn_failed is None or warn_level is None or fail_level is None
        else (
            warn_failed == fixture_violating
            and warn_level == "INFO"
            and drop_level == "INFO"
            and fail_level == "ERROR"
        )
    )
    observed["fail-fast-undercounts-the-violations"] = (
        None
        if warn_failed is None or fail_failed is None
        else fail_failed < warn_failed
    )

    # ------------------------------------------------------------- report
    log("")
    log(f"   fixture: {fixture_rows} rows, {fixture_violating} violating")
    log(f"   event log: warn failed_records={warn_failed} level={warn_level}")
    log(f"              drop level={drop_level}")
    log(f"              fail failed_records={fail_failed} level={fail_level}")
    log("")

    passed = failed = inconclusive = 0
    for item_id, item in declared.items():
        outcome = results.get(item_id, Outcome.UNKNOWN)
        ok, detail = check_expectation(item, outcome)
        if outcome is Outcome.UNKNOWN:
            inconclusive += 1
        elif ok:
            passed += 1
        else:
            failed += 1
        log(f"   {item_id:<46} {detail}")

    for item in matrix["assertions"]:
        value = observed.get(item["id"])
        expected = bool(item["expected"])
        if value is None:
            inconclusive += 1
            log(f"   {item['id']:<46} Not measured")
        elif bool(value) is expected:
            passed += 1
            log(f"   {item['id']:<46} {value} as declared")
        else:
            failed += 1
            log(f"   {item['id']:<46} {value}, declared {expected}")

    total = len(declared) + len(matrix["assertions"])
    log("")
    log(f"   {passed}/{total} as declared; {failed} failed; {inconclusive} inconclusive.")

    if not args.keep:
        log("== Cleaning up")
        try:
            w.pipelines.delete(pipeline_id)
            log("   pipeline deleted")
        except Exception as exc:  # noqa: BLE001
            log(f"   could not delete pipeline: {exc}")
        for table in ("transactions_source", "transactions_warn", "transactions_drop", "transactions_fail"):
            try:
                w.tables.delete(f"{fq}.{table}")
            except Exception:  # noqa: BLE001 - absent is the expected case for some
                pass
        remaining = table_exists(w, args.catalog, args.schema, "transactions_warn")
        log(f"   transactions_warn still present after cleanup: {remaining}")

    # An inconclusive result is a failure. A drill that could not measure
    # something must not report the finding as confirmed.
    return 0 if failed == 0 and inconclusive == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
