from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
from pathlib import Path
import time
from typing import Iterable

import duckdb
import numpy as np

STAGING_SQL = r"""
CREATE OR REPLACE TABLE staging.stg_orders AS
SELECT
    CAST(order_id AS VARCHAR) AS order_id,
    CAST(customer_id AS VARCHAR) AS customer_id,
    CAST(order_status AS VARCHAR) AS order_status,
    CAST(order_purchase_timestamp AS TIMESTAMP) AS order_purchase_ts,
    CAST(order_purchase_timestamp AS DATE) AS purchase_date
FROM raw.orders
WHERE order_id IS NOT NULL AND order_status = 'delivered';

CREATE OR REPLACE TABLE staging.stg_order_items AS
SELECT
    CAST(order_id AS VARCHAR) AS order_id,
    CAST(order_item_id AS INTEGER) AS order_item_id,
    CAST(product_id AS VARCHAR) AS product_id,
    CAST(seller_id AS VARCHAR) AS seller_id,
    CAST(price AS DOUBLE) AS line_gmv
FROM raw.order_items
WHERE order_id IS NOT NULL AND price IS NOT NULL;

CREATE OR REPLACE TABLE staging.stg_order_gmv AS
SELECT
    o.order_id,
    o.purchase_date,
    SUM(i.line_gmv) AS order_gmv,
    COUNT(*) AS line_count
FROM staging.stg_orders o
JOIN staging.stg_order_items i ON i.order_id = o.order_id
GROUP BY 1,2;

CREATE OR REPLACE TABLE staging.stg_campaigns AS
SELECT
    CAST(campaign_id AS VARCHAR) AS campaign_id,
    CAST(campaign_name AS VARCHAR) AS campaign_name,
    CAST(channel AS VARCHAR) AS channel,
    CAST(coupon_code AS VARCHAR) AS coupon_code,
    CAST(valid_from AS DATE) AS valid_from,
    CAST(valid_to AS DATE) AS valid_to,
    CAST(source_batch_id AS INTEGER) AS source_batch_id
FROM raw.campaigns;

CREATE OR REPLACE TABLE staging.stg_touches_raw AS
SELECT
    CAST(touch_id AS VARCHAR) AS touch_id,
    CAST(order_id AS VARCHAR) AS order_id,
    CAST(campaign_id AS VARCHAR) AS campaign_id,
    CAST(touch_at AS TIMESTAMP) AS touch_at,
    CAST(channel AS VARCHAR) AS channel,
    CAST(source_batch_id AS INTEGER) AS source_batch_id
FROM raw.campaign_touches
WHERE order_id IS NOT NULL;

CREATE OR REPLACE TABLE staging.dq_touch_dup_candidates AS
SELECT
    order_id,
    campaign_id,
    touch_at,
    COUNT(*) AS duplicate_count
FROM staging.stg_touches_raw
GROUP BY 1,2,3
HAVING COUNT(*) > 1;

CREATE OR REPLACE TABLE staging.stg_touches_dedup AS
SELECT *
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id, campaign_id, touch_at
            ORDER BY touch_id
        ) AS rn
    FROM staging.stg_touches_raw
) t
WHERE rn = 1;
"""

CURATED_SQL = r"""
CREATE OR REPLACE TABLE curated.fct_promo_attributed_gmv AS
WITH touch_counts AS (
    SELECT
        t.order_id,
        CAST(o.purchase_date AS DATE) AS purchase_date,
        t.campaign_id,
        COUNT(*) AS touch_count
    FROM staging.stg_touches_dedup t
    JOIN staging.stg_orders o ON o.order_id = t.order_id
    GROUP BY 1,2,3
),
campaign_counts AS (
    SELECT
        order_id,
        purchase_date,
        COUNT(DISTINCT campaign_id) AS campaign_count
    FROM touch_counts
    GROUP BY 1,2
)
SELECT
    tc.order_id,
    tc.campaign_id,
    tc.purchase_date,
    g.order_gmv / NULLIF(cc.campaign_count, 0) AS attributed_gmv,
    tc.touch_count,
    cc.campaign_count,
    CASE WHEN d.order_id IS NOT NULL THEN TRUE ELSE FALSE END AS dq_inflation_risk
FROM touch_counts tc
JOIN campaign_counts cc
  ON cc.order_id = tc.order_id AND cc.purchase_date = tc.purchase_date
JOIN staging.stg_order_gmv g
  ON g.order_id = tc.order_id AND g.purchase_date = tc.purchase_date
LEFT JOIN (SELECT DISTINCT order_id FROM staging.dq_touch_dup_candidates) d
  ON d.order_id = tc.order_id;

CREATE OR REPLACE VIEW curated.vw_promo_impact_daily AS
SELECT
    purchase_date,
    campaign_id,
    SUM(attributed_gmv) AS total_attributed_gmv,
    SUM(CASE WHEN dq_inflation_risk THEN attributed_gmv ELSE 0 END) AS risky_gmv,
    COUNT(DISTINCT order_id) AS order_count
FROM curated.fct_promo_attributed_gmv
GROUP BY 1,2
ORDER BY 1,2;
"""

CURATED_AUGMENTED_SQL = r"""
CREATE TABLE IF NOT EXISTS curated.fct_promo_attributed_gmv(
    order_id VARCHAR,
    campaign_id VARCHAR,
    purchase_date DATE,
    attributed_gmv DOUBLE,
    touch_count BIGINT,
    campaign_count BIGINT,
    dq_inflation_risk BOOLEAN
);

DELETE FROM curated.fct_promo_attributed_gmv
WHERE purchase_date BETWEEN DATE '{{DATE_START}}' AND DATE '{{DATE_END}}';

INSERT INTO curated.fct_promo_attributed_gmv
WITH touch_counts AS (
    SELECT
        t.order_id,
        CAST(o.purchase_date AS DATE) AS purchase_date,
        t.campaign_id,
        COUNT(*) AS touch_count
    FROM staging.stg_touches_dedup t
    JOIN staging.stg_orders o ON o.order_id = t.order_id
    GROUP BY 1,2,3
),
campaign_counts AS (
    SELECT
        order_id,
        purchase_date,
        COUNT(DISTINCT campaign_id) AS campaign_count
    FROM touch_counts
    GROUP BY 1,2
)
SELECT
    tc.order_id,
    tc.campaign_id,
    tc.purchase_date,
    g.order_gmv / NULLIF(cc.campaign_count, 0) AS attributed_gmv,
    tc.touch_count,
    cc.campaign_count,
    CASE WHEN d.order_id IS NOT NULL THEN TRUE ELSE FALSE END AS dq_inflation_risk
FROM touch_counts tc
JOIN campaign_counts cc
  ON cc.order_id = tc.order_id AND cc.purchase_date = tc.purchase_date
JOIN staging.stg_order_gmv g
  ON g.order_id = tc.order_id AND g.purchase_date = tc.purchase_date
LEFT JOIN (SELECT DISTINCT order_id FROM staging.dq_touch_dup_candidates) d
  ON d.order_id = tc.order_id
WHERE tc.purchase_date BETWEEN DATE '{{DATE_START}}' AND DATE '{{DATE_END}}';

CREATE OR REPLACE VIEW curated.vw_promo_impact_daily AS
SELECT
    purchase_date,
    campaign_id,
    SUM(attributed_gmv) AS total_attributed_gmv,
    SUM(CASE WHEN dq_inflation_risk THEN attributed_gmv ELSE 0 END) AS risky_gmv,
    COUNT(DISTINCT order_id) AS order_count
FROM curated.fct_promo_attributed_gmv
GROUP BY 1,2
ORDER BY 1,2;
"""

CHECKS_SQL = r"""
-- C0) Ensure audit table exists.
CREATE TABLE IF NOT EXISTS meta.audit_results(
    run_id VARCHAR,
    check_name VARCHAR,
    status VARCHAR,
    metric_value DOUBLE,
    details VARCHAR,
    logged_at_ms BIGINT
);

-- C1) Curated key not null.
INSERT INTO meta.audit_results
SELECT
    '{{RUN_ID}}',
    'C1_curated_key_not_null',
    CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END,
    CAST(COUNT(*) AS DOUBLE),
    'null rows in curated key columns',
    {{LOGGED_AT_MS}}
FROM curated.fct_promo_attributed_gmv
WHERE order_id IS NULL OR campaign_id IS NULL OR purchase_date IS NULL;

-- C2) Duplicate touches should be detected (informational pass if >0).
INSERT INTO meta.audit_results
SELECT
    '{{RUN_ID}}',
    'C2_duplicate_touch_detection',
    'PASS',
    CAST(COUNT(*) AS DOUBLE),
    'duplicate logical touches detected in staging and handled by dedupe',
    {{LOGGED_AT_MS}}
FROM staging.dq_touch_dup_candidates;

-- R1) Attributed total must not exceed order truth GMV.
WITH over_alloc AS (
    SELECT COUNT(*) AS fail_count
    FROM (
        SELECT order_id, purchase_date, SUM(attributed_gmv) AS attributed_sum
        FROM curated.fct_promo_attributed_gmv
        GROUP BY 1,2
    ) c
    JOIN staging.stg_order_gmv g
      ON g.order_id = c.order_id AND g.purchase_date = c.purchase_date
    WHERE c.attributed_sum > g.order_gmv + 0.000001
)
INSERT INTO meta.audit_results
SELECT
    '{{RUN_ID}}',
    'R1_attributed_not_exceed_order_gmv',
    CASE WHEN fail_count > 0 THEN 'FAIL' ELSE 'PASS' END,
    CAST(fail_count AS DOUBLE),
    'violating order count',
    {{LOGGED_AT_MS}}
FROM over_alloc;

-- C3) Declared curated grain must be unique.
WITH dup_grain AS (
    SELECT order_id, campaign_id, purchase_date
    FROM curated.fct_promo_attributed_gmv
    GROUP BY 1,2,3
    HAVING COUNT(*) > 1
)
INSERT INTO meta.audit_results
SELECT
    '{{RUN_ID}}',
    'C3_curated_grain_uniqueness',
    CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END,
    CAST(COUNT(*) AS DOUBLE),
    'duplicate rows at declared curated grain',
    {{LOGGED_AT_MS}}
FROM dup_grain;

-- R2) Set reconciliation: lost grain keys from staging to curated.
WITH expected_keys AS (
    SELECT DISTINCT
        tc.order_id,
        tc.campaign_id,
        o.purchase_date
    FROM staging.stg_touches_dedup tc
    JOIN staging.stg_orders o ON o.order_id = tc.order_id
),
actual_keys AS (
    SELECT DISTINCT order_id, campaign_id, purchase_date
    FROM curated.fct_promo_attributed_gmv
),
lost AS (
    SELECT * FROM expected_keys
    EXCEPT
    SELECT * FROM actual_keys
)
INSERT INTO meta.audit_results
SELECT
    '{{RUN_ID}}',
    'R2_lost_curated_grain_keys',
    CASE WHEN COUNT(*) > 0 THEN 'FAIL' ELSE 'PASS' END,
    CAST(COUNT(*) AS DOUBLE),
    'expected grain keys missing in curated',
    {{LOGGED_AT_MS}}
FROM lost;
"""


def now_epoch_ms() -> int:
    return int(time.time() * 1000)


def text_to_vec(text: str, dim: int = 64) -> list[float]:
    vec = np.zeros(dim, dtype=np.float64)
    for token in text.lower().split():
        idx = int(hashlib.md5(token.encode("utf-8")).hexdigest(), 16) % dim
        vec[idx] += 1.0
    norm = float(np.linalg.norm(vec))
    if norm > 0:
        vec = vec / norm
    return [float(x) for x in vec]


def dot(a: list[float], b: list[float]) -> float:
    return float(np.dot(np.array(a, dtype=np.float64), np.array(b, dtype=np.float64)))


def run_sql(con: duckdb.DuckDBPyConnection, sql: str) -> None:
    con.execute(sql)


def run_sql_template(
    con: duckdb.DuckDBPyConnection, sql: str, replacements: dict[str, str]
) -> None:
    rendered = sql
    for key, value in replacements.items():
        rendered = rendered.replace(f"{{{{{key}}}}}", value)
    con.execute(rendered)


class Pipeline:
    def __init__(self, root: Path, mode: str, strict_audit: bool) -> None:
        self.root = root.resolve()
        self.mode = mode
        self.strict_audit = strict_audit
        self.db_path = self.root / "data" / "processed" / "warehouse.duckdb"
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(str(self.db_path))
        # Include milliseconds to avoid run_id collisions under rapid reruns.
        self.run_id = datetime.now().isoformat(timespec="milliseconds")
        self.started_at = now_epoch_ms()
        self.audit_failures = 0
        self.scope = "ALL"

    def setup_meta(self) -> None:
        self.con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        self.con.execute("CREATE SCHEMA IF NOT EXISTS staging")
        self.con.execute("CREATE SCHEMA IF NOT EXISTS curated")
        self.con.execute("CREATE SCHEMA IF NOT EXISTS meta")
        self.con.execute("CREATE SCHEMA IF NOT EXISTS ai")
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS meta.runs(
                run_id VARCHAR PRIMARY KEY,
                mode VARCHAR,
                scope VARCHAR,
                started_at_ms BIGINT,
                finished_at_ms BIGINT,
                status VARCHAR,
                rows_curated BIGINT,
                audit_failures BIGINT,
                notes VARCHAR
            )
            """
        )
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS meta.audit_results(
                run_id VARCHAR,
                check_name VARCHAR,
                status VARCHAR,
                metric_value DOUBLE,
                details VARCHAR,
                logged_at_ms BIGINT
            )
            """
        )
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS meta.row_count_reconciliation(
                run_id VARCHAR,
                table_name VARCHAR,
                row_count BIGINT,
                checksum VARCHAR,
                logged_at_ms BIGINT
            )
            """
        )
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS meta.latency_logs(
                run_id VARCHAR,
                stage_name VARCHAR,
                elapsed_ms BIGINT,
                logged_at_ms BIGINT
            )
            """
        )
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS ai.retrieval_eval(
                run_id VARCHAR,
                query_text VARCHAR,
                expected_campaign_id VARCHAR,
                top1_campaign_id VARCHAR,
                hit_at_1 INTEGER,
                scored_at_ms BIGINT
            )
            """
        )
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS ai.cost_summary(
                run_id VARCHAR,
                stage VARCHAR,
                cost_units DOUBLE,
                notes VARCHAR,
                logged_at_ms BIGINT
            )
            """
        )
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS ai.agent_trace(
                run_id VARCHAR,
                step_name VARCHAR,
                payload VARCHAR,
                logged_at_ms BIGINT
            )
            """
        )
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS ai.grounded_answers(
                run_id VARCHAR,
                query_text VARCHAR,
                top1_campaign_id VARCHAR,
                top1_chunk_id VARCHAR,
                top1_chunk_text VARCHAR,
                graph_hop_neighbors VARCHAR,
                logged_at_ms BIGINT
            )
            """
        )
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS ai.graphrag_eval(
                run_id VARCHAR,
                query_text VARCHAR,
                expected_campaign_id VARCHAR,
                vector_top1_campaign_id VARCHAR,
                graphrag_top1_campaign_id VARCHAR,
                vector_hit_at_1 INTEGER,
                graphrag_hit_at_1 INTEGER,
                scored_at_ms BIGINT
            )
            """
        )

    def start_run(self) -> None:
        self.con.execute(
            """
            UPDATE meta.runs
            SET status = 'SUPERSEDED', notes = 'Marked stale by newer run'
            WHERE status = 'RUNNING'
            """
        )
        self.con.execute(
            """
            INSERT INTO meta.runs(run_id, mode, scope, started_at_ms, status, notes)
            VALUES (?, ?, ?, ?, 'RUNNING', '')
            """,
            [self.run_id, self.mode, self.scope, self.started_at],
        )

    def finish_run(self, status: str, notes: str = "") -> None:
        rows_curated = 0
        try:
            rows_curated = self.con.execute(
                "SELECT COUNT(*) FROM curated.fct_promo_attributed_gmv"
            ).fetchone()[0]
        except duckdb.Error:
            rows_curated = 0
        self.con.execute(
            """
            UPDATE meta.runs
            SET finished_at_ms = ?, status = ?, scope = ?, rows_curated = ?, audit_failures = ?, notes = ?
            WHERE run_id = ?
            """,
            [
                now_epoch_ms(),
                status,
                self.scope,
                rows_curated,
                self.audit_failures,
                notes,
                self.run_id,
            ],
        )

    def load_raw_olist(self) -> None:
        olist_dir = self.root / "data" / "raw" / "olist"
        self.con.execute(
            f"CREATE OR REPLACE TABLE raw.orders AS SELECT * FROM read_csv_auto('{(olist_dir / 'orders.csv').as_posix()}', header=true)"
        )
        self.con.execute(
            f"CREATE OR REPLACE TABLE raw.order_items AS SELECT * FROM read_csv_auto('{(olist_dir / 'order_items.csv').as_posix()}', header=true)"
        )
        self.con.execute(
            f"CREATE OR REPLACE TABLE raw.payments AS SELECT * FROM read_csv_auto('{(olist_dir / 'payments.csv').as_posix()}', header=true)"
        )
        self.con.execute(
            f"CREATE OR REPLACE TABLE raw.customers AS SELECT * FROM read_csv_auto('{(olist_dir / 'customers.csv').as_posix()}', header=true)"
        )
        self.con.execute(
            f"CREATE OR REPLACE TABLE raw.reviews AS SELECT * FROM read_csv_auto('{(olist_dir / 'reviews.csv').as_posix()}', header=true)"
        )

    def load_raw_promo(self, promo_max_batch: int) -> None:
        promo_root = self.root / "data" / "raw" / "promo"
        campaign_selects = []
        touch_selects = []
        for batch_id in range(1, promo_max_batch + 1):
            batch_dir = promo_root / f"batch_id={batch_id}"
            campaign_file = (batch_dir / "campaigns.csv").as_posix()
            touch_file = (batch_dir / "campaign_touches.csv").as_posix()
            campaign_selects.append(
                f"SELECT * FROM read_csv_auto('{campaign_file}', header=true)"
            )
            touch_selects.append(
                f"SELECT * FROM read_csv_auto('{touch_file}', header=true)"
            )
        campaign_sql = " UNION ALL ".join(campaign_selects)
        touch_sql = " UNION ALL ".join(touch_selects)
        self.con.execute(
            f"CREATE OR REPLACE TABLE raw.campaigns AS {campaign_sql}"
        )
        self.con.execute(
            f"CREATE OR REPLACE TABLE raw.campaign_touches AS {touch_sql}"
        )

    def staging(self) -> None:
        run_sql(self.con, STAGING_SQL)

    def _window_filter_sql(self, date_start: str | None, date_end: str | None) -> str:
        if not date_start or not date_end:
            return ""
        return (
            "WHERE g.purchase_date BETWEEN "
            f"DATE '{date_start}' AND DATE '{date_end}'"
        )

    def curated_full(self) -> None:
        run_sql(self.con, CURATED_SQL)

    def curated_augmented(self, date_start: str, date_end: str) -> None:
        self.scope = f"purchase_date=[{date_start},{date_end}]"
        run_sql_template(
            self.con,
            CURATED_AUGMENTED_SQL,
            {"DATE_START": date_start, "DATE_END": date_end},
        )

    def audit(self) -> None:
        run_sql_template(
            self.con,
            CHECKS_SQL,
            {"RUN_ID": self.run_id, "LOGGED_AT_MS": str(now_epoch_ms())},
        )
        self.audit_failures = self.con.execute(
            """
            SELECT COUNT(*)
            FROM meta.audit_results
            WHERE run_id = ? AND status = 'FAIL'
            """,
            [self.run_id],
        ).fetchone()[0]

    def log_audit(
        self,
        check_name: str,
        status: str,
        metric_value: float,
        details: str,
        logged_at_ms: int,
    ) -> None:
        if status == "FAIL":
            self.audit_failures += 1
        self.con.execute(
            """
            INSERT INTO meta.audit_results(run_id, check_name, status, metric_value, details, logged_at_ms)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [self.run_id, check_name, status, metric_value, details, logged_at_ms],
        )

    def reconcile(self) -> None:
        ts = now_epoch_ms()
        tables = [
            "staging.stg_orders",
            "staging.stg_order_items",
            "staging.stg_touches_dedup",
            "curated.fct_promo_attributed_gmv",
        ]
        for table in tables:
            count = self.con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            digest = self.con.execute(
                f"SELECT md5(string_agg(md5(CAST(t AS VARCHAR)), '' ORDER BY CAST(t AS VARCHAR))) FROM (SELECT * FROM {table}) t"
            ).fetchone()[0]
            digest = digest if digest is not None else "EMPTY"
            self.con.execute(
                """
                INSERT INTO meta.row_count_reconciliation(run_id, table_name, row_count, checksum, logged_at_ms)
                VALUES (?, ?, ?, ?, ?)
                """,
                [self.run_id, table, count, digest, ts],
            )

    def log_latency(self, stage_name: str, elapsed_ms: int) -> None:
        self.con.execute(
            """
            INSERT INTO meta.latency_logs(run_id, stage_name, elapsed_ms, logged_at_ms)
            VALUES (?, ?, ?, ?)
            """,
            [self.run_id, stage_name, elapsed_ms, now_epoch_ms()],
        )

    def build_rag(self) -> None:
        self.con.execute("DROP TABLE IF EXISTS ai.vector_chunks")
        self.con.execute(
            """
            CREATE TABLE ai.vector_chunks(
                chunk_id VARCHAR,
                campaign_id VARCHAR,
                content VARCHAR,
                vec DOUBLE[]
            )
            """
        )
        docs = self.con.execute(
            """
            SELECT
                c.campaign_id,
                c.campaign_name,
                c.channel,
                CAST(SUM(v.total_attributed_gmv) AS DOUBLE) AS gmv
            FROM staging.stg_campaigns c
            LEFT JOIN curated.vw_promo_impact_daily v
              ON v.campaign_id = c.campaign_id
            GROUP BY 1,2,3
            ORDER BY 1
            """
        ).fetchall()
        for idx, row in enumerate(docs, start=1):
            campaign_id, campaign_name, channel, gmv = row
            text = (
                f"campaign {campaign_name} id {campaign_id} channel {channel} "
                f"attributed_gmv {0.0 if gmv is None else round(gmv, 2)}"
            )
            vec = text_to_vec(text)
            self.con.execute(
                "INSERT INTO ai.vector_chunks VALUES (?, ?, ?, ?)",
                [f"chunk_{idx}", campaign_id, text, vec],
            )

        # Graph edges by shared channel for simple GraphRAG hop expansion.
        self.con.execute("DROP TABLE IF EXISTS ai.note_edges")
        self.con.execute(
            """
            CREATE TABLE ai.note_edges AS
            SELECT
                a.campaign_id AS src_campaign_id,
                b.campaign_id AS dst_campaign_id,
                'shared_channel' AS edge_type
            FROM staging.stg_campaigns a
            JOIN staging.stg_campaigns b
              ON a.channel = b.channel AND a.campaign_id <> b.campaign_id
            """
        )

    def evaluate_rag(self) -> None:
        eval_cases = [
            ("Which campaign uses social channel?", "cmp20"),
            ("What is the email campaign id?", "cmp30"),
            ("Find the paid search campaign", "cmp10"),
        ]
        ts = now_epoch_ms()
        hits = 0
        for query, expected in eval_cases:
            query_vec = text_to_vec(query)
            rows = self.con.execute(
                "SELECT chunk_id, campaign_id, content, vec FROM ai.vector_chunks"
            ).fetchall()
            scored = []
            for _, campaign_id, content, vec in rows:
                score = dot(query_vec, vec)
                scored.append((score, campaign_id, content))
            scored.sort(key=lambda x: x[0], reverse=True)
            top1 = scored[0][1]
            top1_content = scored[0][2]
            top1_chunk = self.con.execute(
                "SELECT chunk_id FROM ai.vector_chunks WHERE campaign_id = ? ORDER BY chunk_id LIMIT 1",
                [top1],
            ).fetchone()
            top1_chunk_id = top1_chunk[0] if top1_chunk else "unknown_chunk"
            neighbors = self.con.execute(
                """
                SELECT string_agg(dst_campaign_id, ',')
                FROM ai.note_edges
                WHERE src_campaign_id = ?
                """,
                [top1],
            ).fetchone()[0]
            neighbors = neighbors if neighbors else ""
            hit = int(top1 == expected)
            hits += hit
            self.con.execute(
                """
                INSERT INTO ai.retrieval_eval(run_id, query_text, expected_campaign_id, top1_campaign_id, hit_at_1, scored_at_ms)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [self.run_id, query, expected, top1, hit, ts],
            )
            self.con.execute(
                """
                INSERT INTO ai.agent_trace(run_id, step_name, payload, logged_at_ms)
                VALUES (?, 'vector_retrieval', ?, ?)
                """,
                [self.run_id, f"query={query}; top1={top1}", ts],
            )
            self.con.execute(
                """
                INSERT INTO ai.grounded_answers(
                    run_id, query_text, top1_campaign_id, top1_chunk_id, top1_chunk_text, graph_hop_neighbors, logged_at_ms
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    self.run_id,
                    query,
                    top1,
                    top1_chunk_id,
                    top1_content,
                    neighbors,
                    ts,
                ],
            )
            # GraphRAG score: reserve score budget for 1-hop neighbors of vector top1.
            neighbor_ids = self.con.execute(
                """
                SELECT dst_campaign_id
                FROM ai.note_edges
                WHERE src_campaign_id = ?
                """,
                [top1],
            ).fetchall()
            neighbor_ids = {x[0] for x in neighbor_ids}
            graphrag_scored = []
            for base_score, cid, content in scored:
                boost = 0.15 if cid in neighbor_ids else 0.0
                graphrag_scored.append((base_score + boost, cid, content))
            graphrag_scored.sort(key=lambda x: x[0], reverse=True)
            graph_top1 = graphrag_scored[0][1]
            graph_hit = int(graph_top1 == expected)
            self.con.execute(
                """
                INSERT INTO ai.graphrag_eval(
                    run_id, query_text, expected_campaign_id, vector_top1_campaign_id,
                    graphrag_top1_campaign_id, vector_hit_at_1, graphrag_hit_at_1, scored_at_ms
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [self.run_id, query, expected, top1, graph_top1, hit, graph_hit, ts],
            )

        # GraphRAG 1-hop: if top1 is wrong (rare here), expand via note_edges.
        self.con.execute(
            """
            INSERT INTO ai.agent_trace(run_id, step_name, payload, logged_at_ms)
            VALUES (?, 'graph_hop_policy', '1-hop expansion over shared_channel edges', ?)
            """,
            [self.run_id, ts],
        )
        hit_rate = hits / len(eval_cases)
        graph_hit_rate = self.con.execute(
            """
            SELECT AVG(graphrag_hit_at_1)
            FROM ai.graphrag_eval
            WHERE run_id = ?
            """,
            [self.run_id],
        ).fetchone()[0]
        graph_hit_rate = float(graph_hit_rate if graph_hit_rate is not None else 0.0)
        self.con.execute(
            """
            INSERT INTO ai.cost_summary(run_id, stage, cost_units, notes, logged_at_ms)
            VALUES (?, 'retrieval_eval', ?, 'unit=vector dot operations (synthetic)', ?)
            """,
            [self.run_id, float(len(eval_cases)), ts],
        )
        self.log_audit(
            "retrieval_eval_hit_at_1",
            "PASS" if hit_rate >= 0.66 else "FAIL",
            float(hit_rate),
            "minimum expected Hit@1 >= 0.66",
            ts,
        )
        self.log_audit(
            "graphrag_eval_hit_at_1",
            "PASS" if graph_hit_rate >= 0.66 else "FAIL",
            graph_hit_rate,
            "minimum expected GraphRAG Hit@1 >= 0.66",
            ts,
        )

    def run(
        self,
        promo_max_batch: int,
        date_start: str | None = None,
        date_end: str | None = None,
        with_ai: bool = True,
    ) -> None:
        self.setup_meta()
        if self.mode == "augmented" and date_start and date_end:
            self.scope = f"purchase_date=[{date_start},{date_end}]"
        self.start_run()
        try:
            t0 = now_epoch_ms()
            self.load_raw_olist()
            self.load_raw_promo(promo_max_batch=promo_max_batch)
            self.log_latency("raw_load", now_epoch_ms() - t0)

            t1 = now_epoch_ms()
            self.staging()
            self.log_latency("staging", now_epoch_ms() - t1)

            t2 = now_epoch_ms()
            if self.mode == "full":
                self.curated_full()
            else:
                assert date_start and date_end
                self.curated_augmented(date_start=date_start, date_end=date_end)
            self.log_latency("curated", now_epoch_ms() - t2)

            t3 = now_epoch_ms()
            self.audit()
            self.reconcile()
            self.log_latency("audit_reconcile", now_epoch_ms() - t3)
            if with_ai:
                t4 = now_epoch_ms()
                self.build_rag()
                self.evaluate_rag()
                self.log_latency("ai_pipeline", now_epoch_ms() - t4)
            if self.strict_audit and self.audit_failures > 0:
                raise RuntimeError(f"Audit failures found: {self.audit_failures}")
            self.finish_run(status="SUCCESS")
        except Exception as exc:
            self.finish_run(status="FAILED", notes=str(exc))
            raise
        finally:
            self.con.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="IS455 final project ETL pipeline")
    parser.add_argument(
        "--mode",
        required=True,
        choices=["full", "augmented"],
        help="Build mode",
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path("."),
        help="Project root path",
    )
    parser.add_argument(
        "--promo-max-batch",
        type=int,
        default=1,
        help="For full mode, include promo batches up to this id.",
    )
    parser.add_argument(
        "--date-start",
        type=str,
        default=None,
        help="For augmented mode: start purchase date YYYY-MM-DD",
    )
    parser.add_argument(
        "--date-end",
        type=str,
        default=None,
        help="For augmented mode: end purchase date YYYY-MM-DD",
    )
    parser.add_argument(
        "--strict-audit",
        action="store_true",
        help="Fail run when any audit check fails",
    )
    parser.add_argument(
        "--skip-ai",
        action="store_true",
        help="Skip vectorDB/RAG/GraphRAG pipeline",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.mode == "augmented" and (not args.date_start or not args.date_end):
        raise SystemExit("augmented mode requires --date-start and --date-end")
    p = Pipeline(root=args.root, mode=args.mode, strict_audit=args.strict_audit)
    p.run(
        promo_max_batch=args.promo_max_batch,
        date_start=args.date_start,
        date_end=args.date_end,
        with_ai=not args.skip_ai,
    )
    print("Build completed.")
    print(f"DuckDB path: {p.db_path}")


if __name__ == "__main__":
    main()
