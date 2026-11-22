"""End-to-end tests: each test builds a fresh warehouse in a temporary copy of the project."""
from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture()
def project(tmp_path: Path) -> Path:
    for name in ("build.py", "data_generate.py"):
        shutil.copy(ROOT / name, tmp_path / name)
    subprocess.run([sys.executable, "data_generate.py", "--root", "."], cwd=tmp_path, check=True, capture_output=True)
    return tmp_path


def build(project: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, "build.py", "--root", ".", *args],
        cwd=project, check=check, capture_output=True, text=True,
    )


def query(project: Path, sql: str):
    con = duckdb.connect(str(project / "data" / "processed" / "warehouse.duckdb"), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def latest_checksum(project: Path, table: str = "curated.fct_promo_attributed_gmv") -> str:
    return query(project, f"""
        SELECT x.checksum FROM meta.row_count_reconciliation x JOIN meta.runs r USING (run_id)
        WHERE x.table_name = '{table}' ORDER BY r.started_at_ms DESC LIMIT 1""")[0][0]


def test_full_rebuild_is_idempotent(project: Path) -> None:
    build(project, "--mode", "full", "--promo-max-batch", "2")
    first = latest_checksum(project)
    build(project, "--mode", "full", "--promo-max-batch", "2")
    assert latest_checksum(project) == first


def test_augmented_build_matches_full_rebuild(project: Path) -> None:
    build(project, "--mode", "full", "--promo-max-batch", "1")
    build(project, "--mode", "augmented", "--promo-max-batch", "2", "--strict-audit")
    augmented = {t: latest_checksum(project, t) for t in ("curated.fct_promo_attributed_gmv", "staging.stg_touches_dedup")}
    build(project, "--mode", "full", "--promo-max-batch", "2")
    for table, checksum in augmented.items():
        assert latest_checksum(project, table) == checksum, table
    assert query(project, "SELECT status FROM meta.audit_results WHERE check_name = 'R3_augmented_matches_full_rebuild'") == [("PASS",)]


def test_augmented_build_detects_only_changed_dates(project: Path) -> None:
    build(project, "--mode", "full", "--promo-max-batch", "1")
    build(project, "--mode", "augmented", "--promo-max-batch", "2")
    dates = [str(d) for (d,) in query(project, "SELECT purchase_date FROM meta.affected_dates ORDER BY 1")]
    assert dates == ["2018-01-03", "2018-01-04", "2018-01-05"]
    # 2018-01-02 is untouched: its curated rows were not deleted and re-inserted.
    skipped = query(project, "SELECT action FROM meta.change_log WHERE table_name = 'staging.stg_orders' ORDER BY logged_at_ms DESC LIMIT 1")
    assert skipped[0][0].startswith("SKIPPED")


def test_duplicate_touch_is_flagged_then_cleared_by_correction(project: Path) -> None:
    build(project, "--mode", "full", "--promo-max-batch", "1")
    assert query(project, "SELECT DISTINCT order_id FROM curated.fct_promo_attributed_gmv WHERE dq_inflation_risk") == [("o1004",)]
    build(project, "--mode", "full", "--promo-max-batch", "2")
    assert query(project, "SELECT COUNT(*) FROM curated.fct_promo_attributed_gmv WHERE dq_inflation_risk") == [(0,)]


def test_campaign_validity_correction_changes_attribution(project: Path) -> None:
    build(project, "--mode", "full", "--promo-max-batch", "1")
    assert query(project, "SELECT COUNT(*) FROM curated.fct_promo_attributed_gmv WHERE order_id = 'o1007'") == [(0,)]
    build(project, "--mode", "full", "--promo-max-batch", "2")
    rows = query(project, "SELECT campaign_id, attributed_gmv FROM curated.fct_promo_attributed_gmv WHERE order_id = 'o1007' ORDER BY 1")
    assert rows == [("cmp20", 20.0), ("cmp30", 20.0)]


def test_attributed_gmv_never_exceeds_order_gmv(project: Path) -> None:
    build(project, "--mode", "full", "--promo-max-batch", "2", "--strict-audit")
    assert query(project, "SELECT SUM(attributed_gmv) FROM curated.fct_promo_attributed_gmv") == [(880.0,)]


def test_repeated_augmented_build_changes_nothing(project: Path) -> None:
    build(project, "--mode", "full", "--promo-max-batch", "1")
    build(project, "--mode", "augmented", "--promo-max-batch", "2")
    first = latest_checksum(project)
    build(project, "--mode", "augmented", "--promo-max-batch", "2")
    assert latest_checksum(project) == first
    assert query(project, "SELECT COUNT(*) FROM raw.campaign_touches WHERE landed_batch_id = 2") == [(11,)]


def test_augmented_build_needs_a_full_build_first(project: Path) -> None:
    result = build(project, "--mode", "augmented", "--promo-max-batch", "2", check=False)
    assert result.returncode != 0
    assert query(project, "SELECT status FROM meta.runs") == [("FAILED",)]


def test_missing_batch_file_marks_the_run_failed(project: Path) -> None:
    build(project, "--mode", "full", "--promo-max-batch", "1")
    (project / "data" / "raw" / "promo" / "batch_id=2" / "campaign_touches.csv").unlink()
    result = build(project, "--mode", "augmented", "--promo-max-batch", "2", check=False)
    assert result.returncode != 0
    statuses = query(project, "SELECT mode, status FROM meta.runs ORDER BY started_at_ms")
    assert statuses == [("full", "SUCCESS"), ("augmented", "FAILED")]
    # The failed run was rolled back: batch 2 was not half-landed and curated still shows batch 1.
    assert query(project, "SELECT COUNT(*) FROM raw.campaigns WHERE landed_batch_id = 2") == [(0,)]
    assert query(project, "SELECT COUNT(*) FROM curated.fct_promo_attributed_gmv") == [(8,)]


def test_strict_audit_failure_rolls_back_the_run(project: Path) -> None:
    build(project, "--mode", "full", "--promo-max-batch", "1")
    # Corrupt batch 2: campaign cmp10 arrives without a start date.
    campaigns = project / "data" / "raw" / "promo" / "batch_id=2" / "campaigns.csv"
    campaigns.write_text(campaigns.read_text().replace("cmp10,NewYearPush,paid_search,NY10,2018-01-01,2018-01-05",
                                                       "cmp10,NewYearPush,paid_search,NY10,,2018-01-05"))
    result = build(project, "--mode", "augmented", "--promo-max-batch", "2", "--strict-audit", check=False)
    assert result.returncode != 0
    assert query(project, "SELECT status FROM meta.runs ORDER BY started_at_ms DESC LIMIT 1") == [("FAILED",)]
    # Rolled back: curated still shows batch 1.
    assert query(project, "SELECT COUNT(*) FROM curated.fct_promo_attributed_gmv") == [(8,)]

    # Without --strict-audit the run completes, the bad campaign is reported, and the
    # incremental result still matches a full rebuild.
    build(project, "--mode", "augmented", "--promo-max-batch", "2")
    failed = query(project, """
        SELECT check_name FROM meta.audit_results
        WHERE run_id = (SELECT run_id FROM meta.runs ORDER BY started_at_ms DESC LIMIT 1) AND status = 'FAIL'""")
    assert failed == [("C5_campaign_validity_window_valid",)]
