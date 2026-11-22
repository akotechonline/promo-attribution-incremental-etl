"""Promo attribution ETL (v2): raw -> staging -> curated, with full and augmented builds.

Batch contract
--------------
Each promo batch folder (data/raw/promo/batch_id=N) is a complete snapshot of the
marketing feed. Raw keeps every landed batch (an append-only ledger tagged with
`landed_batch_id`); staging and curated always reflect the latest landed snapshot.

Build modes
-----------
full       Rebuild every layer from raw.
augmented  Land only batches not yet in raw, diff the new snapshot against the previous
           one, and rebuild staging and curated only for the purchase dates that changed.
           The result is verified against a shadow full rebuild in the same run.

Run:
  python build.py --root . --mode full --promo-max-batch 1
  python build.py --root . --mode augmented --promo-max-batch 2
  python build.py --root . --mode full --promo-max-batch 2 --strict-audit
"""
from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
from pathlib import Path
import re
import time

import duckdb
import numpy as np

# -----------------------------------------------------------------------------
# SQL. {{STG}} and {{CUR}} are schema names, so the same SQL can build the real
# layers or a shadow copy used to verify an augmented build.
# -----------------------------------------------------------------------------

STAGING_BASE_SQL = r"""
CREATE OR REPLACE TABLE {{STG}}.stg_orders AS
SELECT
    CAST(order_id AS VARCHAR) AS order_id,
    CAST(customer_id AS VARCHAR) AS customer_id,
    CAST(order_status AS VARCHAR) AS order_status,
    CAST(order_purchase_timestamp AS TIMESTAMP) AS order_purchase_ts,
    CAST(order_purchase_timestamp AS DATE) AS purchase_date
FROM raw.orders
WHERE order_id IS NOT NULL AND order_status = 'delivered';

CREATE OR REPLACE TABLE {{STG}}.stg_order_items AS
SELECT
    CAST(order_id AS VARCHAR) AS order_id,
    CAST(order_item_id AS INTEGER) AS order_item_id,
    CAST(product_id AS VARCHAR) AS product_id,
    CAST(seller_id AS VARCHAR) AS seller_id,
    CAST(price AS DOUBLE) AS line_gmv
FROM raw.order_items
WHERE order_id IS NOT NULL AND price IS NOT NULL;

CREATE OR REPLACE TABLE {{STG}}.stg_order_gmv AS
SELECT
    o.order_id,
    o.purchase_date,
    SUM(i.line_gmv) AS order_gmv,
    COUNT(*) AS line_count
FROM {{STG}}.stg_orders o
JOIN {{STG}}.stg_order_items i ON i.order_id = o.order_id
GROUP BY 1, 2;
"""

# Campaigns are a three-row dimension, so they are always rebuilt from the latest snapshot.
STAGING_CAMPAIGNS_SQL = r"""
CREATE OR REPLACE TABLE {{STG}}.stg_campaigns AS
SELECT
    CAST(campaign_id AS VARCHAR) AS campaign_id,
    CAST(campaign_name AS VARCHAR) AS campaign_name,
    CAST(channel AS VARCHAR) AS channel,
    CAST(coupon_code AS VARCHAR) AS coupon_code,
    CAST(valid_from AS DATE) AS valid_from,
    CAST(valid_to AS DATE) AS valid_to,
    landed_batch_id
FROM raw.campaigns
WHERE landed_batch_id = {{BATCH}};
"""

# Touch tables carry purchase_date so they can be rebuilt for a set of dates only.
# {{DATE_FILTER}} is empty for a full build, or restricts rows to the affected dates.
STAGING_TOUCHES_SELECT = r"""
-- stg_touches_raw: latest snapshot, typed, with the order's purchase date and
-- whether the touch falls inside the campaign's validity window.
SELECT
    CAST(t.touch_id AS VARCHAR) AS touch_id,
    CAST(t.order_id AS VARCHAR) AS order_id,
    CAST(t.campaign_id AS VARCHAR) AS campaign_id,
    CAST(t.touch_at AS TIMESTAMP) AS touch_at,
    CAST(t.channel AS VARCHAR) AS channel,
    CAST(t.source_batch_id AS INTEGER) AS source_batch_id,
    o.purchase_date,
    (o.purchase_date BETWEEN c.valid_from AND c.valid_to) AS within_validity
FROM raw.campaign_touches t
JOIN {{STG}}.stg_orders o ON o.order_id = CAST(t.order_id AS VARCHAR)
LEFT JOIN {{STG}}.stg_campaigns c ON c.campaign_id = CAST(t.campaign_id AS VARCHAR)
WHERE t.landed_batch_id = {{BATCH}}
  AND t.order_id IS NOT NULL
  {{DATE_FILTER}}
"""

STAGING_TOUCHES_DERIVED = r"""
-- dq_touch_dup_candidates: logical duplicates inside the latest snapshot.
SELECT order_id, campaign_id, touch_at, purchase_date, COUNT(*) AS duplicate_count
FROM {{STG}}.stg_touches_raw
WHERE TRUE {{DATE_FILTER}}
GROUP BY 1, 2, 3, 4
HAVING COUNT(*) > 1
;;
-- stg_touches_dedup: one row per logical touch (deterministic winner).
SELECT * EXCLUDE (rn)
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id, campaign_id, touch_at
            ORDER BY touch_id
        ) AS rn
    FROM {{STG}}.stg_touches_raw
    WHERE TRUE {{DATE_FILTER}}
) t
WHERE rn = 1
"""

CURATED_SELECT = r"""
WITH valid_touches AS (
    SELECT order_id, campaign_id, purchase_date
    FROM {{STG}}.stg_touches_dedup
    WHERE within_validity {{DATE_FILTER}}
),
touch_counts AS (
    SELECT order_id, purchase_date, campaign_id, COUNT(*) AS touch_count
    FROM valid_touches
    GROUP BY 1, 2, 3
),
campaign_counts AS (
    SELECT order_id, purchase_date, COUNT(DISTINCT campaign_id) AS campaign_count
    FROM touch_counts
    GROUP BY 1, 2
)
SELECT
    tc.order_id,
    tc.campaign_id,
    tc.purchase_date,
    g.order_gmv / NULLIF(cc.campaign_count, 0) AS attributed_gmv,
    tc.touch_count,
    cc.campaign_count,
    (d.order_id IS NOT NULL) AS dq_inflation_risk
FROM touch_counts tc
JOIN campaign_counts cc
  ON cc.order_id = tc.order_id AND cc.purchase_date = tc.purchase_date
JOIN {{STG}}.stg_order_gmv g
  ON g.order_id = tc.order_id AND g.purchase_date = tc.purchase_date
LEFT JOIN (SELECT DISTINCT order_id FROM {{STG}}.dq_touch_dup_candidates) d
  ON d.order_id = tc.order_id
"""

CURATED_VIEW_SQL = r"""
CREATE OR REPLACE VIEW curated.vw_promo_impact_daily AS
SELECT
    purchase_date,
    campaign_id,
    SUM(attributed_gmv) AS total_attributed_gmv,
    SUM(CASE WHEN dq_inflation_risk THEN attributed_gmv ELSE 0 END) AS risky_gmv,
    COUNT(DISTINCT order_id) AS order_count
FROM curated.fct_promo_attributed_gmv
GROUP BY 1, 2
ORDER BY 1, 2;
"""

# Snapshot diff used by the augmented build: which purchase dates changed between the
# previous and the latest landed batch, and why.
CHANGE_DETECTION_SQL = r"""
WITH prev AS (
    SELECT CAST(touch_id AS VARCHAR) touch_id, CAST(order_id AS VARCHAR) order_id,
           CAST(campaign_id AS VARCHAR) campaign_id, CAST(touch_at AS TIMESTAMP) touch_at,
           CAST(channel AS VARCHAR) channel
    FROM raw.campaign_touches WHERE landed_batch_id = {{PREV}}
),
cur AS (
    SELECT CAST(touch_id AS VARCHAR) touch_id, CAST(order_id AS VARCHAR) order_id,
           CAST(campaign_id AS VARCHAR) campaign_id, CAST(touch_at AS TIMESTAMP) touch_at,
           CAST(channel AS VARCHAR) channel
    FROM raw.campaign_touches WHERE landed_batch_id = {{BATCH}}
),
removed AS (SELECT * FROM prev EXCEPT SELECT * FROM cur),
added   AS (SELECT * FROM cur EXCEPT SELECT * FROM prev),
touch_changes AS (
    SELECT o.purchase_date, 'touch removed or corrected' AS reason
    FROM removed r JOIN staging.stg_orders o USING (order_id)
    UNION ALL
    SELECT o.purchase_date, 'touch added (late-arriving)'
    FROM added a JOIN staging.stg_orders o USING (order_id)
),
camp_prev AS (
    SELECT CAST(campaign_id AS VARCHAR) campaign_id, CAST(valid_from AS DATE) vf, CAST(valid_to AS DATE) vt
    FROM raw.campaigns WHERE landed_batch_id = {{PREV}}
),
camp_cur AS (
    SELECT CAST(campaign_id AS VARCHAR) campaign_id, CAST(valid_from AS DATE) vf, CAST(valid_to AS DATE) vt
    FROM raw.campaigns WHERE landed_batch_id = {{BATCH}}
),
validity_changes AS (
    -- Orders touched by a campaign whose window changed, on dates inside exactly one of the two windows.
    SELECT DISTINCT o.purchase_date, 'campaign validity changed' AS reason
    FROM (SELECT * FROM prev UNION SELECT * FROM cur) t
    JOIN staging.stg_orders o USING (order_id)
    JOIN camp_prev p USING (campaign_id)
    JOIN camp_cur c USING (campaign_id)
    WHERE (p.vf IS DISTINCT FROM c.vf OR p.vt IS DISTINCT FROM c.vt)
      AND COALESCE(o.purchase_date BETWEEN p.vf AND p.vt, FALSE)
          <> COALESCE(o.purchase_date BETWEEN c.vf AND c.vt, FALSE)
)
SELECT purchase_date, string_agg(DISTINCT reason, '; ' ORDER BY reason) AS reason
FROM (SELECT * FROM touch_changes UNION ALL SELECT * FROM validity_changes)
GROUP BY 1
ORDER BY 1
"""

CHECKS_SQL = r"""
-- C1) Curated key not null.
INSERT INTO meta.audit_results
SELECT '{{RUN_ID}}', 'C1_curated_key_not_null',
       CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END, CAST(COUNT(*) AS DOUBLE),
       'null rows in curated key columns', {{LOGGED_AT_MS}}
FROM curated.fct_promo_attributed_gmv
WHERE order_id IS NULL OR campaign_id IS NULL OR purchase_date IS NULL;

-- C2) Logical duplicate touches in the latest snapshot (informational).
INSERT INTO meta.audit_results
SELECT '{{RUN_ID}}', 'C2_duplicate_touch_detection', 'PASS', CAST(COUNT(*) AS DOUBLE),
       'duplicate logical touches in the latest snapshot, removed by dedupe', {{LOGGED_AT_MS}}
FROM staging.dq_touch_dup_candidates;

-- C3) Declared curated grain must be unique.
INSERT INTO meta.audit_results
SELECT '{{RUN_ID}}', 'C3_curated_grain_uniqueness',
       CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END, CAST(COUNT(*) AS DOUBLE),
       'duplicate rows at declared curated grain', {{LOGGED_AT_MS}}
FROM (
    SELECT order_id, campaign_id, purchase_date
    FROM curated.fct_promo_attributed_gmv
    GROUP BY 1, 2, 3
    HAVING COUNT(*) > 1
);

-- C4) Touches outside their campaign's validity window (informational).
INSERT INTO meta.audit_results
SELECT '{{RUN_ID}}', 'C4_touches_outside_validity', 'PASS', CAST(COUNT(*) AS DOUBLE),
       'deduped touches excluded from attribution because the campaign was not valid that day',
       {{LOGGED_AT_MS}}
FROM staging.stg_touches_dedup
WHERE NOT COALESCE(within_validity, FALSE);

-- C5) Every campaign needs a usable validity window.
INSERT INTO meta.audit_results
SELECT '{{RUN_ID}}', 'C5_campaign_validity_window_valid',
       CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END, CAST(COUNT(*) AS DOUBLE),
       'campaigns with a missing or reversed validity window', {{LOGGED_AT_MS}}
FROM staging.stg_campaigns
WHERE valid_from IS NULL OR valid_to IS NULL OR valid_from > valid_to;

-- R1) Attributed total must not exceed order truth GMV.
INSERT INTO meta.audit_results
SELECT '{{RUN_ID}}', 'R1_attributed_not_exceed_order_gmv',
       CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END, CAST(COUNT(*) AS DOUBLE),
       'violating order count', {{LOGGED_AT_MS}}
FROM (
    SELECT order_id, purchase_date, SUM(attributed_gmv) AS attributed_sum
    FROM curated.fct_promo_attributed_gmv
    GROUP BY 1, 2
) c
JOIN staging.stg_order_gmv g USING (order_id, purchase_date)
WHERE c.attributed_sum > g.order_gmv + 0.000001;

-- R2) Set reconciliation: expected grain keys missing from curated.
INSERT INTO meta.audit_results
SELECT '{{RUN_ID}}', 'R2_lost_curated_grain_keys',
       CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END, CAST(COUNT(*) AS DOUBLE),
       'expected grain keys missing in curated', {{LOGGED_AT_MS}}
FROM (
    SELECT DISTINCT t.order_id, t.campaign_id, t.purchase_date
    FROM staging.stg_touches_dedup t
    JOIN staging.stg_order_gmv g USING (order_id, purchase_date)
    WHERE t.within_validity
    EXCEPT
    SELECT DISTINCT order_id, campaign_id, purchase_date
    FROM curated.fct_promo_attributed_gmv
);
"""

META_TABLES_SQL = r"""
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS curated;
CREATE SCHEMA IF NOT EXISTS meta;
CREATE SCHEMA IF NOT EXISTS ai;

CREATE TABLE IF NOT EXISTS meta.runs(
    run_id VARCHAR PRIMARY KEY, mode VARCHAR, scope VARCHAR,
    started_at_ms BIGINT, finished_at_ms BIGINT, status VARCHAR,
    rows_curated BIGINT, audit_failures BIGINT, notes VARCHAR
);
CREATE TABLE IF NOT EXISTS meta.audit_results(
    run_id VARCHAR, check_name VARCHAR, status VARCHAR,
    metric_value DOUBLE, details VARCHAR, logged_at_ms BIGINT
);
CREATE TABLE IF NOT EXISTS meta.row_count_reconciliation(
    run_id VARCHAR, table_name VARCHAR, row_count BIGINT, checksum VARCHAR, logged_at_ms BIGINT
);
CREATE TABLE IF NOT EXISTS meta.latency_logs(
    run_id VARCHAR, stage_name VARCHAR, elapsed_ms BIGINT, logged_at_ms BIGINT
);
CREATE TABLE IF NOT EXISTS meta.change_log(
    run_id VARCHAR, table_name VARCHAR, action VARCHAR,
    rows_deleted BIGINT, rows_inserted BIGINT, rows_after BIGINT, logged_at_ms BIGINT
);
CREATE TABLE IF NOT EXISTS meta.affected_dates(
    run_id VARCHAR, purchase_date DATE, reason VARCHAR, logged_at_ms BIGINT
);
CREATE TABLE IF NOT EXISTS ai.retrieval_eval(
    run_id VARCHAR, query_text VARCHAR, expected_campaign_id VARCHAR,
    top1_campaign_id VARCHAR, hit_at_1 INTEGER, scored_at_ms BIGINT
);
CREATE TABLE IF NOT EXISTS ai.cost_summary(
    run_id VARCHAR, stage VARCHAR, cost_units DOUBLE, notes VARCHAR, logged_at_ms BIGINT
);
CREATE TABLE IF NOT EXISTS ai.agent_trace(
    run_id VARCHAR, step_name VARCHAR, payload VARCHAR, logged_at_ms BIGINT
);
CREATE TABLE IF NOT EXISTS ai.grounded_answers(
    run_id VARCHAR, query_text VARCHAR, top1_campaign_id VARCHAR, top1_chunk_id VARCHAR,
    top1_chunk_text VARCHAR, graph_hop_neighbors VARCHAR, logged_at_ms BIGINT
);
CREATE TABLE IF NOT EXISTS ai.graphrag_eval(
    run_id VARCHAR, query_text VARCHAR, expected_campaign_id VARCHAR,
    vector_top1_campaign_id VARCHAR, graphrag_top1_campaign_id VARCHAR,
    vector_hit_at_1 INTEGER, graphrag_hit_at_1 INTEGER, scored_at_ms BIGINT
);
"""

RECONCILED_TABLES = [
    "staging.stg_orders",
    "staging.stg_order_items",
    "staging.stg_touches_dedup",
    "curated.fct_promo_attributed_gmv",
]


def now_epoch_ms() -> int:
    return int(time.time() * 1000)


def render(sql: str, **values: str) -> str:
    for key, value in values.items():
        sql = sql.replace("{{" + key + "}}", value)
    return sql


def text_to_vec(text: str, dim: int = 256) -> list[float]:
    """Hashed bag-of-words embedding: deterministic and dependency-free.

    Words are split on anything that isn't a letter or digit, so "paid_search" in a
    chunk matches "paid search" in a question.
    """
    vec = np.zeros(dim, dtype=np.float64)
    for token in re.findall(r"[a-z0-9]+", text.lower()):
        idx = int(hashlib.md5(token.encode("utf-8")).hexdigest(), 16) % dim
        vec[idx] += 1.0
    norm = float(np.linalg.norm(vec))
    if norm > 0:
        vec = vec / norm
    return [float(x) for x in vec]


def dot(a: list[float], b: list[float]) -> float:
    return float(np.dot(np.array(a, dtype=np.float64), np.array(b, dtype=np.float64)))


def table_checksum(con: duckdb.DuckDBPyConnection, table: str) -> tuple[int, str]:
    count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    digest = con.execute(
        f"SELECT md5(string_agg(md5(CAST(t AS VARCHAR)), '' ORDER BY CAST(t AS VARCHAR))) FROM {table} t"
    ).fetchone()[0]
    return count, digest or "EMPTY"


class Pipeline:
    def __init__(self, root: Path, mode: str, strict_audit: bool) -> None:
        self.root = root.resolve()
        self.mode = mode
        self.strict_audit = strict_audit
        self.db_path = self.root / "data" / "processed" / "warehouse.duckdb"
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(str(self.db_path))
        self.run_id = datetime.now().isoformat(timespec="milliseconds")
        self.started_at = now_epoch_ms()
        self.audit_failures = 0
        self.scope = "ALL"
        self.batch = 0

    # ------------------------------------------------------------------ run registry
    def start_run(self) -> None:
        self.con.execute(
            "UPDATE meta.runs SET status = 'SUPERSEDED', notes = 'Marked stale by newer run' "
            "WHERE status = 'RUNNING'"
        )
        self.con.execute(
            "INSERT INTO meta.runs(run_id, mode, scope, started_at_ms, status, notes) "
            "VALUES (?, ?, ?, ?, 'RUNNING', '')",
            [self.run_id, self.mode, self.scope, self.started_at],
        )

    def finish_run(self, status: str, notes: str = "") -> None:
        try:
            rows = self.con.execute("SELECT COUNT(*) FROM curated.fct_promo_attributed_gmv").fetchone()[0]
        except duckdb.Error:
            rows = 0
        self.con.execute(
            "UPDATE meta.runs SET finished_at_ms = ?, status = ?, scope = ?, rows_curated = ?, "
            "audit_failures = ?, notes = ? WHERE run_id = ?",
            [now_epoch_ms(), status, self.scope, rows, self.audit_failures, notes, self.run_id],
        )

    def log_latency(self, stage: str, started_ms: int) -> None:
        self.con.execute(
            "INSERT INTO meta.latency_logs VALUES (?, ?, ?, ?)",
            [self.run_id, stage, now_epoch_ms() - started_ms, now_epoch_ms()],
        )

    def log_change(self, table: str, action: str, deleted: int = 0, inserted: int = 0) -> None:
        after = self.con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        self.con.execute(
            "INSERT INTO meta.change_log VALUES (?, ?, ?, ?, ?, ?, ?)",
            [self.run_id, table, action, deleted, inserted, after, now_epoch_ms()],
        )

    def log_audit(self, name: str, status: str, value: float, details: str) -> None:
        if status == "FAIL":
            self.audit_failures += 1
        self.con.execute(
            "INSERT INTO meta.audit_results VALUES (?, ?, ?, ?, ?, ?)",
            [self.run_id, name, status, value, details, now_epoch_ms()],
        )

    # ------------------------------------------------------------------ raw
    def load_raw_olist(self) -> None:
        olist = self.root / "data" / "raw" / "olist"
        for table, fname in [
            ("orders", "orders.csv"),
            ("order_items", "order_items.csv"),
            ("payments", "payments.csv"),
            ("customers", "customers.csv"),
            ("reviews", "reviews.csv"),
        ]:
            path = (olist / fname).as_posix().replace("'", "''")
            self.con.execute(
                f"CREATE OR REPLACE TABLE raw.{table} AS SELECT * FROM read_csv_auto('{path}', header=true)"
            )

    def _batch_select(self, batch_id: int, fname: str) -> str:
        path = (self.root / "data" / "raw" / "promo" / f"batch_id={batch_id}" / fname).as_posix()
        if not Path(path).exists():
            raise FileNotFoundError(f"Missing promo file: {path}")
        path = path.replace("'", "''")
        return f"SELECT *, {batch_id} AS landed_batch_id FROM read_csv_auto('{path}', header=true)"

    def land_promo_full(self, max_batch: int) -> None:
        """Rebuild the raw promo ledger from batch 1 up to max_batch."""
        for table, fname in [("campaigns", "campaigns.csv"), ("campaign_touches", "campaign_touches.csv")]:
            union = " UNION ALL BY NAME ".join(self._batch_select(b, fname) for b in range(1, max_batch + 1))
            self.con.execute(f"CREATE OR REPLACE TABLE raw.{table} AS {union}")
            self.log_change(f"raw.{table}", f"REBUILT (batches 1-{max_batch})")

    def land_promo_new(self, max_batch: int) -> None:
        """Append only batches that are not in the raw ledger yet (idempotent)."""
        for table, fname in [("campaigns", "campaigns.csv"), ("campaign_touches", "campaign_touches.csv")]:
            landed = {r[0] for r in self.con.execute(f"SELECT DISTINCT landed_batch_id FROM raw.{table}").fetchall()}
            new = [b for b in range(1, max_batch + 1) if b not in landed]
            inserted = 0
            for b in new:
                before = self.con.execute(f"SELECT COUNT(*) FROM raw.{table}").fetchone()[0]
                self.con.execute(f"INSERT INTO raw.{table} BY NAME {self._batch_select(b, fname)}")
                inserted += self.con.execute(f"SELECT COUNT(*) FROM raw.{table}").fetchone()[0] - before
            action = f"APPENDED batch {','.join(map(str, new))}" if new else "SKIPPED (no new batch)"
            self.log_change(f"raw.{table}", action, inserted=inserted)

    # ------------------------------------------------------------------ full build
    def build_staging_and_curated(self, stg: str, cur: str, log: bool) -> None:
        """Full build of staging and curated into the given schemas."""
        values = {"STG": stg, "CUR": cur, "BATCH": str(self.batch), "DATE_FILTER": ""}
        self.con.execute(render(STAGING_BASE_SQL, **values))
        self.con.execute(render(STAGING_CAMPAIGNS_SQL, **values))
        self.con.execute(f"CREATE OR REPLACE TABLE {stg}.stg_touches_raw AS " + render(STAGING_TOUCHES_SELECT, **values))
        dup_sql, dedup_sql = render(STAGING_TOUCHES_DERIVED, **values).split(";;")
        self.con.execute(f"CREATE OR REPLACE TABLE {stg}.dq_touch_dup_candidates AS {dup_sql}")
        self.con.execute(f"CREATE OR REPLACE TABLE {stg}.stg_touches_dedup AS {dedup_sql}")
        self.con.execute(
            f"CREATE OR REPLACE TABLE {cur}.fct_promo_attributed_gmv AS " + render(CURATED_SELECT, **values)
        )
        if log:
            for t in ["stg_orders", "stg_order_items", "stg_order_gmv", "stg_campaigns",
                      "stg_touches_raw", "dq_touch_dup_candidates", "stg_touches_dedup"]:
                self.log_change(f"{stg}.{t}", "REBUILT")
            self.log_change(f"{cur}.fct_promo_attributed_gmv", "REBUILT")

    # ------------------------------------------------------------------ augmented build
    def detect_affected_dates(self, date_start: str | None, date_end: str | None) -> list[str]:
        if date_start and date_end:
            rows = self.con.execute(
                "SELECT DISTINCT purchase_date, 'manual window (--date-start/--date-end)' "
                "FROM staging.stg_orders WHERE purchase_date BETWEEN ? AND ? ORDER BY 1",
                [date_start, date_end],
            ).fetchall()
        else:
            prev = self.con.execute(
                "SELECT MAX(landed_batch_id) FROM raw.campaign_touches WHERE landed_batch_id < ?", [self.batch]
            ).fetchone()[0]
            if prev is None:
                raise RuntimeError("Augmented build needs an earlier batch to compare with; run a full build first.")
            rows = self.con.execute(
                render(CHANGE_DETECTION_SQL, PREV=str(prev), BATCH=str(self.batch))
            ).fetchall()
        ts = now_epoch_ms()
        for d, reason in rows:
            self.con.execute("INSERT INTO meta.affected_dates VALUES (?, ?, ?, ?)", [self.run_id, d, reason, ts])
        return [str(d) for d, _ in rows]

    def rebuild_scoped(self, dates: list[str]) -> None:
        """Delete and re-insert staging touches and curated rows for the affected dates only."""
        self.con.execute(render(STAGING_CAMPAIGNS_SQL, STG="staging", BATCH=str(self.batch)))
        self.log_change("staging.stg_campaigns", "REBUILT (3-row dimension)")
        for t in ["stg_orders", "stg_order_items", "stg_order_gmv"]:
            self.log_change(f"staging.{t}", "SKIPPED (order data unchanged)")
        if not dates:
            for t in ["stg_touches_raw", "dq_touch_dup_candidates", "stg_touches_dedup"]:
                self.log_change(f"staging.{t}", "SKIPPED (no affected dates)")
            self.log_change("curated.fct_promo_attributed_gmv", "SKIPPED (no affected dates)")
            return

        date_list = ", ".join(f"DATE '{d}'" for d in dates)
        date_filter = f"AND purchase_date IN ({date_list})"
        values = {"STG": "staging", "BATCH": str(self.batch)}

        def replace_rows(table: str, select_sql: str) -> None:
            deleted = self.con.execute(
                f"SELECT COUNT(*) FROM {table} WHERE purchase_date IN ({date_list})"
            ).fetchone()[0]
            self.con.execute(f"DELETE FROM {table} WHERE purchase_date IN ({date_list})")
            before = self.con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            self.con.execute(f"INSERT INTO {table} BY NAME {select_sql}")
            inserted = self.con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] - before
            self.log_change(table, f"SCOPED ({len(dates)} dates)", deleted, inserted)

        replace_rows(
            "staging.stg_touches_raw",
            render(STAGING_TOUCHES_SELECT, DATE_FILTER=f"AND o.purchase_date IN ({date_list})", **values),
        )
        dup_sql, dedup_sql = render(STAGING_TOUCHES_DERIVED, DATE_FILTER=date_filter, **values).split(";;")
        replace_rows("staging.dq_touch_dup_candidates", dup_sql)
        replace_rows("staging.stg_touches_dedup", dedup_sql)
        replace_rows(
            "curated.fct_promo_attributed_gmv",
            render(CURATED_SELECT, DATE_FILTER=f"AND purchase_date IN ({date_list})", **values),
        )

    def verify_against_full(self) -> None:
        """Build a shadow full rebuild and check the augmented result matches it exactly.

        The `shadow` schema is overwritten on every augmented run and kept for inspection.
        """
        self.con.execute("CREATE SCHEMA IF NOT EXISTS shadow")
        self.build_staging_and_curated("shadow", "shadow", log=False)
        diff = self.con.execute(
            """
            SELECT COUNT(*) FROM (
                (SELECT * FROM curated.fct_promo_attributed_gmv EXCEPT ALL SELECT * FROM shadow.fct_promo_attributed_gmv)
                UNION ALL
                (SELECT * FROM shadow.fct_promo_attributed_gmv EXCEPT ALL SELECT * FROM curated.fct_promo_attributed_gmv)
            )
            """
        ).fetchone()[0]
        self.log_audit(
            "R3_augmented_matches_full_rebuild",
            "FAIL" if diff else "PASS",
            float(diff),
            "rows that differ between the augmented result and a shadow full rebuild",
        )

    def check_manifest(self, dates: list[str]) -> None:
        manifest = self.root / "data" / "raw" / "promo" / f"batch_id={self.batch}" / "affected_window_manifest.csv"
        if not manifest.exists():
            return
        path = manifest.as_posix().replace("'", "''")
        expected = {
            str(r[0])
            for r in self.con.execute(
                f"SELECT CAST(purchase_date AS DATE) FROM read_csv_auto('{path}', header=true)"
            ).fetchall()
        }
        missed = sorted(expected - set(dates))
        self.log_audit(
            "R4_detected_dates_cover_manifest",
            "FAIL" if missed else "PASS",
            float(len(missed)),
            f"dates listed by the sender but not detected: {missed}" if missed else "detected dates cover the sender's manifest",
        )

    # ------------------------------------------------------------------ audits
    def audit(self) -> None:
        self.con.execute(render(CHECKS_SQL, RUN_ID=self.run_id, LOGGED_AT_MS=str(now_epoch_ms())))
        self.audit_failures = self.con.execute(
            "SELECT COUNT(*) FROM meta.audit_results WHERE run_id = ? AND status = 'FAIL'", [self.run_id]
        ).fetchone()[0]

    def reconcile(self) -> None:
        ts = now_epoch_ms()
        for table in RECONCILED_TABLES:
            count, digest = table_checksum(self.con, table)
            self.con.execute(
                "INSERT INTO meta.row_count_reconciliation VALUES (?, ?, ?, ?, ?)",
                [self.run_id, table, count, digest, ts],
            )

    # ------------------------------------------------------------------ AI layer
    def build_rag(self) -> None:
        self.con.execute("DROP TABLE IF EXISTS ai.vector_chunks")
        self.con.execute(
            "CREATE TABLE ai.vector_chunks(chunk_id VARCHAR, campaign_id VARCHAR, content VARCHAR, vec DOUBLE[])"
        )
        docs = self.con.execute(
            """
            SELECT c.campaign_id, c.campaign_name, c.channel, CAST(SUM(v.total_attributed_gmv) AS DOUBLE)
            FROM staging.stg_campaigns c
            LEFT JOIN curated.vw_promo_impact_daily v USING (campaign_id)
            GROUP BY 1, 2, 3
            ORDER BY 1
            """
        ).fetchall()
        for idx, (campaign_id, name, channel, gmv) in enumerate(docs, start=1):
            text = (
                f"campaign {name} id {campaign_id} channel {channel} "
                f"attributed_gmv {0.0 if gmv is None else round(gmv, 2)}"
            )
            self.con.execute(
                "INSERT INTO ai.vector_chunks VALUES (?, ?, ?, ?)",
                [f"chunk_{idx}", campaign_id, text, text_to_vec(text)],
            )

        # Graph edges: campaigns that reached the same order (weighted by shared orders),
        # plus campaigns on the same channel.
        self.con.execute(
            """
            CREATE OR REPLACE TABLE ai.note_edges AS
            SELECT a.campaign_id AS src_campaign_id, b.campaign_id AS dst_campaign_id,
                   'co_touch' AS edge_type, COUNT(DISTINCT a.order_id) AS weight
            FROM curated.fct_promo_attributed_gmv a
            JOIN curated.fct_promo_attributed_gmv b
              ON a.order_id = b.order_id AND a.purchase_date = b.purchase_date
             AND a.campaign_id <> b.campaign_id
            GROUP BY 1, 2
            UNION ALL
            SELECT a.campaign_id, b.campaign_id, 'shared_channel', 1
            FROM staging.stg_campaigns a
            JOIN staging.stg_campaigns b ON a.channel = b.channel AND a.campaign_id <> b.campaign_id
            """
        )

    def _rank_chunks(self, query: str) -> list[tuple[float, str, str, str]]:
        qv = text_to_vec(query)
        rows = self.con.execute("SELECT chunk_id, campaign_id, content, vec FROM ai.vector_chunks").fetchall()
        return sorted(((dot(qv, vec), cid, content, chunk) for chunk, cid, content, vec in rows), reverse=True)

    def evaluate_rag(self) -> None:
        ts = now_epoch_ms()
        # 1) Lookup questions: answered by plain vector retrieval.
        lookups = [
            ("Which campaign uses social channel?", "cmp20"),
            ("What is the email campaign id?", "cmp30"),
            ("Find the paid search campaign", "cmp10"),
        ]
        hits = 0
        for query, expected in lookups:
            ranked = self._rank_chunks(query)
            _, top1, content, chunk_id = ranked[0]
            neighbors = self.con.execute(
                "SELECT string_agg(dst_campaign_id, ',' ORDER BY dst_campaign_id) FROM ai.note_edges "
                "WHERE src_campaign_id = ? AND edge_type = 'co_touch'",
                [top1],
            ).fetchone()[0] or ""
            hit = int(top1 == expected)
            hits += hit
            self.con.execute(
                "INSERT INTO ai.retrieval_eval VALUES (?, ?, ?, ?, ?, ?)",
                [self.run_id, query, expected, top1, hit, ts],
            )
            self.con.execute(
                "INSERT INTO ai.grounded_answers VALUES (?, ?, ?, ?, ?, ?, ?)",
                [self.run_id, query, top1, chunk_id, content, neighbors, ts],
            )
            self.con.execute(
                "INSERT INTO ai.agent_trace VALUES (?, 'vector_retrieval', ?, ?)",
                [self.run_id, f"query={query}; top1={top1}", ts],
            )

        # 2) Relationship questions: "which campaign reaches the same customers as X?"
        # Vector search can only return the chunk most similar to the text; GraphRAG
        # retrieves X, then follows its strongest co_touch edge.
        relations = [
            ("Which campaign most often reaches the same orders as NewYearPush?", "cmp10"),
            ("Which campaign most often reaches the same orders as InfluencerDrop?", "cmp20"),
            ("Which campaign most often reaches the same orders as LoyaltyEmail?", "cmp30"),
        ]
        graph_hits = vector_hits = scored = 0
        for query, anchor in relations:
            truth = self.con.execute(
                """
                SELECT b.campaign_id
                FROM curated.fct_promo_attributed_gmv a
                JOIN curated.fct_promo_attributed_gmv b
                  ON a.order_id = b.order_id AND a.campaign_id <> b.campaign_id
                WHERE a.campaign_id = ?
                GROUP BY 1
                ORDER BY COUNT(DISTINCT a.order_id) DESC, 1
                LIMIT 1
                """,
                [anchor],
            ).fetchone()
            if truth is None:
                continue  # the campaign shares no orders in this batch; nothing to evaluate
            expected = truth[0]
            ranked = self._rank_chunks(query)
            vector_answer = ranked[0][1]
            graph_answer = self.con.execute(
                "SELECT dst_campaign_id FROM ai.note_edges WHERE src_campaign_id = ? AND edge_type = 'co_touch' "
                "ORDER BY weight DESC, dst_campaign_id LIMIT 1",
                [vector_answer],
            ).fetchone()
            graph_answer = graph_answer[0] if graph_answer else vector_answer
            vh, gh = int(vector_answer == expected), int(graph_answer == expected)
            vector_hits, graph_hits, scored = vector_hits + vh, graph_hits + gh, scored + 1
            self.con.execute(
                "INSERT INTO ai.graphrag_eval VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [self.run_id, query, expected, vector_answer, graph_answer, vh, gh, ts],
            )
            self.con.execute(
                "INSERT INTO ai.agent_trace VALUES (?, 'graph_hop', ?, ?)",
                [self.run_id, f"query={query}; anchor={vector_answer}; hop=co_touch -> {graph_answer}", ts],
            )

        self.con.execute(
            "INSERT INTO ai.cost_summary VALUES (?, 'retrieval_eval', ?, 'unit=vector dot products', ?)",
            [self.run_id, float((len(lookups) + scored) * 3), ts],
        )
        self.log_audit("retrieval_eval_hit_at_1", "PASS" if hits / len(lookups) >= 0.66 else "FAIL",
                       hits / len(lookups), "lookup questions, minimum Hit@1 0.66")
        if scored:
            self.log_audit("graphrag_eval_hit_at_1", "PASS" if graph_hits / scored >= 0.66 else "FAIL",
                           graph_hits / scored,
                           f"relationship questions; vector-only Hit@1 on the same questions = {vector_hits / scored:.2f}")

    # ------------------------------------------------------------------ orchestration
    def run(self, max_batch: int, date_start: str | None, date_end: str | None, with_ai: bool) -> None:
        self.batch = max_batch
        self.con.execute(META_TABLES_SQL)
        self.start_run()
        # Everything after the run registry row happens in one transaction, so a failed
        # run leaves the previously published raw, staging and curated data untouched.
        self.con.execute("BEGIN TRANSACTION")
        try:
            t = now_epoch_ms()
            if self.mode == "full":
                self.load_raw_olist()
                self.land_promo_full(max_batch)
                self.log_latency("raw_load", t)
                t = now_epoch_ms()
                self.build_staging_and_curated("staging", "curated", log=True)
                self.log_latency("staging_curated", t)
            else:
                if not self.con.execute(
                    "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'curated'"
                ).fetchone()[0]:
                    raise RuntimeError("No curated layer yet: run a full build before an augmented build.")
                self.land_promo_new(max_batch)
                self.log_latency("raw_load", t)
                t = now_epoch_ms()
                dates = self.detect_affected_dates(date_start, date_end)
                self.scope = "purchase_date IN [" + ", ".join(dates) + "]" if dates else "NONE"
                self.rebuild_scoped(dates)
                self.log_latency("staging_curated", t)
                self.check_manifest(dates)
            self.con.execute(CURATED_VIEW_SQL)

            t = now_epoch_ms()
            self.audit()
            if self.mode == "augmented":
                self.verify_against_full()
            self.reconcile()
            self.log_latency("audit_reconcile", t)

            if with_ai:
                t = now_epoch_ms()
                self.build_rag()
                self.evaluate_rag()
                self.log_latency("ai_pipeline", t)

            if self.strict_audit and self.audit_failures > 0:
                raise RuntimeError(f"Audit failures found: {self.audit_failures}")
            self.finish_run("SUCCESS")
            self.con.execute("COMMIT")
        except Exception as exc:
            try:
                self.con.execute("ROLLBACK")
            except duckdb.Error:
                pass  # a failed COMMIT has already ended the transaction
            self.audit_failures = 0
            self.finish_run("FAILED", notes=str(exc))
            raise
        finally:
            self.con.close()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Promo attribution ETL pipeline (v2)")
    p.add_argument("--mode", required=True, choices=["full", "augmented"], help="Build mode")
    p.add_argument("--root", type=Path, default=Path("."), help="Project root path")
    p.add_argument("--promo-max-batch", type=int, default=1, help="Latest promo batch to use")
    p.add_argument("--date-start", default=None, help="Augmented mode: override detected dates (YYYY-MM-DD)")
    p.add_argument("--date-end", default=None, help="Augmented mode: override detected dates (YYYY-MM-DD)")
    p.add_argument("--strict-audit", action="store_true", help="Fail the run when any audit check fails")
    p.add_argument("--skip-ai", action="store_true", help="Skip the vector/GraphRAG layer")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if bool(args.date_start) != bool(args.date_end):
        raise SystemExit("--date-start and --date-end must be given together")
    pipeline = Pipeline(root=args.root, mode=args.mode, strict_audit=args.strict_audit)
    pipeline.run(args.promo_max_batch, args.date_start, args.date_end, with_ai=not args.skip_ai)
    print(f"Build completed: mode={args.mode} batch={args.promo_max_batch} scope={pipeline.scope} "
          f"audit_failures={pipeline.audit_failures}")
    print(f"DuckDB path: {pipeline.db_path.relative_to(pipeline.root)}")


if __name__ == "__main__":
    main()
