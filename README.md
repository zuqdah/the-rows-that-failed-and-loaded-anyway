# the-rows-that-failed-and-loaded-anyway

A Databricks Lakeflow declarative pipeline that declares two data-quality
constraints, reports **`COMPLETED`**, and loads every single row that violated
them.

Not a bug. It is the documented behaviour of `dlt.expect`, and it is the
default thing you get for writing the obvious decorator.

## The problem

A declarative pipeline lets you write your data-quality rules next to the table
they protect:

```python
@dlt.expect_all({"amount_non_negative": "amount >= 0"})
def transactions():
    ...
```

Reading that, there are two reasonable conclusions and both are wrong: that rows
violating the rule will not reach the table, and that if any did, you would
hear about it.

`expect` records the violation and loads the row. To actually keep bad rows out
you need `expect_or_drop`, and to make the pipeline refuse you need
`expect_or_fail`. The three differ by one word, they have no difference in
severity or naming to suggest one is lenient, and the lenient one is the one
whose name reads like an assertion.

## What this proves

Three targets built from one source, in one pipeline, differing **only** in
which decorator they carry — same fixture, same constraints, same query — graded
against [`expectation-matrix.json`](expectation-matrix.json), which was written
after measuring the platform and before the graded runs.

Fixture: **2000 rows, 340 of which violate a constraint** — 170 with a negative
amount and a disjoint 170 with a null merchant, verified by query before being
declared.

| decorator | update | rows in target | violating rows in target | event level |
|---|---|---|---|---|
| `dlt.expect` | `COMPLETED` | **2000** | **340** | INFO |
| `dlt.expect_or_drop` | `COMPLETED` | 1660 | 0 | INFO |
| `dlt.expect_or_fail` | **`FAILED`** | table never published | — | **ERROR** |

| | |
|---|---|
| Expectations graded | 3, plus 4 assertions — 7 graded outcomes |
| Unit tests | 40, no workspace required |
| Ideas the measurements refused | **5** |
| Cost | nothing — Databricks Free Edition, which is permanent and not a trial |

**The middle column is the finding.** The 340 rows that failed a declared
constraint are in the target table and you can query them:

```sql
SELECT count(*) FROM transactions_warn WHERE amount < 0 OR merchant IS NULL
-- 340
```

The same query against `transactions_drop` returns 0. The pipeline reported the
same `COMPLETED` for both.

## Three things that do not tell you

**The pipeline state does not.** `COMPLETED`, identical to a run with no
violations at all. This is the value a scheduler alerts on, a dashboard shows
and a person checks.

**The row count does not.** 2000 rows in, 2000 rows out. Reconciling source
against target — the first check a data engineer reaches for — agrees perfectly,
because nothing was discarded. The discrepancy people look for cannot exist for
this action.

**The severity does not.** The violation *is* recorded, and this is where the
honest claim is narrower than the dramatic one. Databricks writes it to the
event log in full:

```
transactions_warn   level INFO    warned=340  dropped=0
    amount_non_negative:  failed=170  passed=1830
    merchant_present:     failed=170  passed=1830

transactions_drop   level INFO    warned=0    dropped=340
    amount_non_negative:  failed=170  passed=1830
    merchant_present:     failed=170  passed=1830
```

Everything you would need is there, including the distinction between warned and
dropped. The problem is the level. **Both actions that let rows through log at
`INFO`, and the only action that logs at `ERROR` is the one that already stopped
the update.** The event log is loud exactly when it did not need to be and quiet
exactly when it did.

## The strictest action tells you the least

`expect_or_fail` aborts on the first violation rather than evaluating the batch:

```
transactions_fail   level ERROR
    amount_non_negative:  failed=1
```

**One**, against 340 for the identical fixture under `expect`. No
`passed_records` field at all, and the second constraint is not mentioned —
nothing evaluated it. So the action that protects the table hardest gives you no
way to size the problem it found. If you switch to `expect_or_fail` to be safe
and then want to know how much of your data is bad, you have to switch back.

## Where the event log is not

The drill reads the event log through the REST API and not through the
Databricks CLI, for a measured reason.

`databricks pipelines list-pipeline-events` returns the events **without their
`details` field** — and `details.flow_progress.data_quality` is where all the
numbers above live. Measured on **CLI v1.18.0: 0 of 37 events carried a
`details` field.** The same update read through
`GET /api/2.0/pipelines/{id}/events` returned 37 of 37.

Read with the official CLI, a pipeline that violated its expectations 340 times
is indistinguishable from one that never declared an expectation at all. I
found this by believing the CLI first and concluding the violations were
unrecorded — so the drill now asserts the field is *present* before it reads
anything out of it, and refuses to grade if every event arrives without one.

## Why an outcome and not a count

[`resolve_admission`](expectations/outcome.py) grades on the target row count
and takes the presence of the table as a separate input from its contents:

| outcome | meaning |
|---|---|
| `Admitted` | the violating rows are in the target |
| `Dropped` | they were filtered out and the update still succeeded |
| `Refused` | the update failed and the table was not published |
| `Unknown` | could not be determined. Always a failure, never a pass |

The trap being avoided is `if not rows_in_target:`, which is true for `0` and
true for `None`. **A table that dropped every row and a table nobody managed to
query are the same falsy count with opposite meanings** — one is the feature
working, the other is a broken harness. A count of 0 is a fact; an absent count
is not, and travels as `Unknown`.

This is the same discipline as the PowerShell labs in this series, where a
`[bool]` parameter coerces `$null` to `$false` and turns "never recorded" into
"no samples" and then into a verdict. Different language, identical bug.

A partial drop is also `Unknown` rather than rounded to whichever answer is
nearer: 171 of 340 rows removed is neither outcome, and reporting it as one
would hide a real defect.

## Cost

**Nothing, and with no clock on it.** Databricks Free Edition is permanent —
it replaced Community Edition, which
[retired on 1 January 2026](https://community.databricks.com/t5/announcements/psa-community-edition-retires-on-january-1-2026-move-to-the-free/td-p/141888).
This lab deliberately does *not* use the 14-day full-platform trial, because a
lab that stops working is a lab that misleads whoever reads it next.

What Free Edition costs you instead is
[limits](https://learn.microsoft.com/en-us/azure/databricks/getting-started/free-edition-limitations):
serverless compute only with no custom configuration, one workspace, one
metastore, **one active pipeline per pipeline type**, five concurrent job tasks,
a single `2X-Small` SQL warehouse, restricted outbound internet, and a fair-use
quota that parks your compute for the rest of the day if you exceed it. None of
that constrains this lab — it needs one pipeline and one small warehouse — but
the one-pipeline limit is why the drill reuses its pipeline by name rather than
creating one per run.

It is also non-commercial use only, and Databricks deletes accounts left
inactive for long enough.

## Running it

```bash
pip install databricks-sdk
python scripts/run_drill.py --warehouse-id <your warehouse id>
```

The drill uploads the pipeline source, creates or reuses the pipeline, runs both
passes, reads the event log, grades, and deletes what it made. Pass `--keep` to
leave it behind.

Two passes rather than one, because `expect_or_fail` ends the update: the tables
it does not concern are left unpublished in that run and cannot be graded. The
pass is selected from the pipeline configuration rather than from a second copy
of the source file, so the two runs cannot drift apart.

### Getting a machine identity on Free Edition

Free Edition documents **no account console and no account-level APIs**, and
OAuth secrets for service principals are normally minted at account level. They
are reachable anyway, through a workspace-level endpoint:

```bash
databricks service-principals create --json '{"displayName":"lab-ci","active":true}'
databricks service-principal-secrets-proxy create <service-principal-id>
```

**Then grant entitlements, and then mint the token — in that order.** A new
service principal has none, and this is the trap:

```
create service principal    -> 200, applicationId returned
mint OAuth secret           -> 200, status ACTIVE, two-year expiry
exchange for a bearer token -> 200, a valid token
call any API with it        -> 403
```

Four successes and an inert credential. Granting `workspace-access` fixes it —
but a token minted *before* the grant still returns 403, because entitlements
are fixed into the token when it is issued. So you grant the permission, read
the service principal back to confirm it landed, retry, and **still get 403**,
which reads exactly like "Free Edition blocks service principals". It does not.
Mint a new token and everything answers.

I was one step from writing that wrong conclusion down. It is the same mistake
this lab is about, made while building it: a measurement taken at the wrong
moment and trusted because it was the measurement available.

## Bugs the build found in itself

**The premise was measured before it was declared.** A five-row probe ran first:
`expect` produced 5 rows of 5 with both violations present, `expect_or_drop`
produced 3, `expect_or_fail` failed the update and published nothing. Only then
was the matrix written.

**An assertion that the event log is silent for `expect_or_drop` was refuted on
the first run.** It reports `failed_records: 170` per constraint for `drop`
exactly as it does for `warn`. The event log says what the expectation *saw*;
the table says what the pipeline *did*. Only the second is the finding, and an
assertion written from an assumption about how it ought to work would have
claimed otherwise.

**Grading on `failed_records` would have shown no difference between the two
actions.** Same number, 340, for the action that loaded the rows and the action
that removed them. The whole finding would have vanished into a matching pair of
green ticks.

**The fixture arithmetic was verified rather than trusted.** The modulo
expressions were run against a warehouse before being written into the matrix:
2000 total, 170 negative amounts, 170 null merchants, **0 overlap**, 340
violating. CI now pins those numbers so an edit has to be re-measured.

**The drill renamed the pipeline it was meant to reuse.** `run_pass` derived the
name from the catalog and schema, so the first pass renamed the pipeline, the
reuse-by-name lookup missed it on the next run, and it tried to create a second
one — which Free Edition refuses, for a quota reason with nothing to do with
expectations. Found by reading the code rather than by running it.

**And one that is not about Databricks.** Writing these files through shell
heredocs failed three times because apostrophes in the *content* were being
counted as shell quotes, and the failure surfaced as a syntax error pointing at
an unrelated line. Two of those failures I misdiagnosed as a trailing-command
problem before testing the actual hypothesis.

## What this does not do

**It does not run the Python drill end to end.** Every number in this README was
measured against a real Free Edition workspace, but by driving the same sequence
by hand through the CLI and REST API — because this machine has no local Python
or PowerShell 7 to run it with. The drill automates exactly those calls and its
grading logic is covered by 40 unit tests; what has not yet been observed is the
script itself completing a full run. That is the one claim made nowhere above.

**It does not test expectations on streaming tables.** Everything here is a
materialized view built by a triggered update. Expectations behave differently
in a continuous pipeline, where there is no single terminal state to grade.

**It does not measure quarantine.** The usual production pattern is to route
violating rows to a separate table rather than admit or drop them, which needs
two flows and a branch condition. That is a design worth a lab of its own; this
one is about what the three built-in actions actually do.

**It does not exercise the fair-use quota.** Free Edition parks compute for the
rest of the day if you exceed it, and finding that boundary empirically would
cost the rest of the day.
