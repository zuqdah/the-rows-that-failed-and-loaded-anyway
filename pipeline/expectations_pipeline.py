"""The pipeline under measurement.

One source table and, depending on the pass, targets that differ ONLY in which
expectation decorator they carry. Same fixture, same constraints, same query.
Holding everything else identical is the point: whatever differs downstream was
caused by the one word that changed.

The pass is read from the pipeline configuration rather than from a second copy
of this file, so the two runs cannot drift apart. `admit` builds the warn and
drop targets; `refuse` builds the fail target alone, because the fail action
ends the update and would leave its neighbours unpublished and ungradeable.
"""

import dlt

# `spark` and `dlt.read` are injected by the pipeline runtime. Both forms used
# here were verified on a real update before this file was written -- an earlier
# draft used spark.read.table("LIVE.x"), which is the form the current docs
# prefer but which this lab has not measured.
ROWS = 2000

# 170 rows carry a negative amount (id % 200 in 0..16) and a disjoint 170 carry
# a null merchant (id % 200 in 100..116). 340 violating rows in 2000, which is
# neither none of them nor all of them: a fixture where every row violated would
# make a dropped result and an empty table the same observation.
FIXTURE_SQL = """
    SELECT
        id,
        CASE WHEN id % 200 < 17
             THEN -((id % 37) + 1)
             ELSE ((id % 900) + 1)
        END                                                   AS amount,
        CASE WHEN id % 200 >= 100 AND id % 200 < 117
             THEN NULL
             ELSE concat('merchant-', cast(id % 40 AS string))
        END                                                   AS merchant,
        timestampadd(SECOND, cast(id AS int), TIMESTAMP '2026-01-01 00:00:00') AS txn_ts
    FROM range(0, {rows})
"""

CONSTRAINTS = {
    "amount_non_negative": "amount >= 0",
    "merchant_present": "merchant IS NOT NULL",
}


def _pass():
    """Which pass this update is. Defaults to admit so a bare hand-run does something."""
    try:
        return spark.conf.get("lab.pass")  # noqa: F821 - runtime global
    except Exception:  # noqa: BLE001 - the key is simply absent on a bare run
        return "admit"


@dlt.table(
    name="transactions_source",
    comment="2000 synthetic transactions, 340 of which violate a declared constraint.",
)
def transactions_source():
    return spark.sql(FIXTURE_SQL.format(rows=ROWS))  # noqa: F821 - runtime global


if _pass() == "admit":

    @dlt.table(
        name="transactions_warn",
        comment="dlt.expect. Records the violation and loads the row anyway.",
    )
    @dlt.expect_all(CONSTRAINTS)
    def transactions_warn():
        return dlt.read("transactions_source")

    @dlt.table(
        name="transactions_drop",
        comment="dlt.expect_or_drop. Filters the violating rows out of the target.",
    )
    @dlt.expect_all_or_drop(CONSTRAINTS)
    def transactions_drop():
        return dlt.read("transactions_source")

else:

    @dlt.table(
        name="transactions_fail",
        comment="dlt.expect_or_fail. Ends the update and publishes nothing.",
    )
    @dlt.expect_all_or_fail(CONSTRAINTS)
    def transactions_fail():
        return dlt.read("transactions_source")
