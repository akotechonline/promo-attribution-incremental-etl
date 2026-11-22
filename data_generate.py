from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import random
from typing import Iterable

import pandas as pd


@dataclass
class Paths:
    root: Path

    @property
    def olist_dir(self) -> Path:
        return self.root / "data" / "raw" / "olist"

    @property
    def promo_batch_1(self) -> Path:
        return self.root / "data" / "raw" / "promo" / "batch_id=1"

    @property
    def promo_batch_2(self) -> Path:
        return self.root / "data" / "raw" / "promo" / "batch_id=2"

    def ensure(self) -> None:
        for folder in (self.olist_dir, self.promo_batch_1, self.promo_batch_2):
            folder.mkdir(parents=True, exist_ok=True)


def _write(df: pd.DataFrame, path: Path) -> None:
    df.to_csv(path, index=False)


def _date(ts: str) -> str:
    return ts.split(" ")[0]


def generate_olist_base(paths: Paths) -> None:
    orders = pd.DataFrame(
        [
            ("o1001", "c1", "delivered", "2018-01-02 10:15:00"),
            ("o1002", "c2", "delivered", "2018-01-02 13:20:00"),
            ("o1003", "c3", "delivered", "2018-01-03 09:05:00"),
            ("o1004", "c2", "delivered", "2018-01-03 15:30:00"),
            ("o1005", "c4", "delivered", "2018-01-04 12:05:00"),
            ("o1006", "c5", "delivered", "2018-01-04 17:45:00"),
            ("o1007", "c6", "delivered", "2018-01-05 11:10:00"),
            ("o1008", "c1", "delivered", "2018-01-05 20:20:00"),
        ],
        columns=[
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
        ],
    )
    order_items = pd.DataFrame(
        [
            ("o1001", 1, "p1", "s1", 100.0),
            ("o1001", 2, "p2", "s2", 25.0),
            ("o1002", 1, "p3", "s2", 80.0),
            ("o1003", 1, "p4", "s3", 160.0),
            ("o1004", 1, "p1", "s1", 60.0),
            ("o1004", 2, "p4", "s3", 35.0),
            ("o1005", 1, "p5", "s4", 120.0),
            ("o1006", 1, "p6", "s5", 50.0),
            ("o1007", 1, "p3", "s2", 40.0),
            ("o1008", 1, "p2", "s2", 210.0),
        ],
        columns=["order_id", "order_item_id", "product_id", "seller_id", "price"],
    )
    payments = pd.DataFrame(
        [
            ("o1001", 1, "credit_card", 125.0),
            ("o1002", 1, "credit_card", 80.0),
            ("o1003", 1, "voucher", 160.0),
            ("o1004", 1, "credit_card", 95.0),
            ("o1005", 1, "boleto", 120.0),
            ("o1006", 1, "credit_card", 50.0),
            ("o1007", 1, "debit_card", 40.0),
            ("o1008", 1, "credit_card", 210.0),
        ],
        columns=[
            "order_id",
            "payment_sequential",
            "payment_type",
            "payment_value",
        ],
    )
    customers = pd.DataFrame(
        [
            ("c1", "13001"),
            ("c2", "13002"),
            ("c3", "13003"),
            ("c4", "13004"),
            ("c5", "13005"),
            ("c6", "13006"),
        ],
        columns=["customer_id", "customer_zip_code_prefix"],
    )
    reviews = pd.DataFrame(
        [
            ("r1", "o1001", 4),
            ("r2", "o1002", 5),
            ("r3", "o1003", 2),
            ("r4", "o1004", 3),
            ("r5", "o1005", 4),
            ("r6", "o1006", 5),
        ],
        columns=["review_id", "order_id", "review_score"],
    )

    _write(orders, paths.olist_dir / "orders.csv")
    _write(order_items, paths.olist_dir / "order_items.csv")
    _write(payments, paths.olist_dir / "payments.csv")
    _write(customers, paths.olist_dir / "customers.csv")
    _write(reviews, paths.olist_dir / "reviews.csv")


def _campaign_rows() -> Iterable[tuple[str, str, str, str, str, str]]:
    return [
        ("cmp10", "NewYearPush", "paid_search", "NY10", "2018-01-01", "2018-01-05"),
        ("cmp20", "InfluencerDrop", "social", "INF5", "2018-01-02", "2018-01-04"),
        ("cmp30", "LoyaltyEmail", "email", "LOY3", "2018-01-01", "2018-01-31"),
    ]


def generate_promo_batches(paths: Paths, seed: int) -> None:
    rng = random.Random(seed)
    campaigns_1 = pd.DataFrame(
        _campaign_rows(),
        columns=[
            "campaign_id",
            "campaign_name",
            "channel",
            "coupon_code",
            "valid_from",
            "valid_to",
        ],
    )
    campaigns_1["source_batch_id"] = 1

    touches_1 = pd.DataFrame(
        [
            ("t100", "o1001", "cmp10", "2018-01-02 09:00:00", "paid_search"),
            ("t101", "o1001", "cmp20", "2018-01-02 09:10:00", "social"),
            ("t102", "o1002", "cmp20", "2018-01-02 12:05:00", "social"),
            ("t103", "o1003", "cmp30", "2018-01-03 07:00:00", "email"),
            ("t104", "o1004", "cmp10", "2018-01-03 12:00:00", "paid_search"),
            ("t105", "o1004", "cmp20", "2018-01-03 12:02:00", "social"),
            ("t106", "o1005", "cmp30", "2018-01-04 08:00:00", "email"),
            ("t107", "o1008", "cmp10", "2018-01-05 19:50:00", "paid_search"),
            # Touch on a day cmp20 is not valid in batch 1 (valid_to 2018-01-04).
            # Batch 2 extends cmp20 to 2018-01-05, so this touch starts to count.
            ("t109", "o1007", "cmp20", "2018-01-05 09:30:00", "social"),
            # Injected duplicate logical touch (failure mode).
            ("t108", "o1004", "cmp20", "2018-01-03 12:02:00", "social"),
        ],
        columns=["touch_id", "order_id", "campaign_id", "touch_at", "channel"],
    )
    touches_1["source_batch_id"] = 1

    # Batch 2 is corrected metadata and adjusted touch logs for a bounded window.
    campaigns_2 = campaigns_1.copy()
    campaigns_2.loc[campaigns_2["campaign_id"] == "cmp20", "valid_to"] = "2018-01-05"
    campaigns_2["source_batch_id"] = 2

    touches_2 = touches_1.copy()
    touches_2 = touches_2[touches_2["touch_id"] != "t108"]  # remove duplicate correction
    touches_2 = pd.concat(
        [
            touches_2,
            pd.DataFrame(
                [
                    ("t200", "o1006", "cmp20", "2018-01-04 16:40:00", "social", 2),
                    ("t201", "o1007", "cmp30", "2018-01-05 10:30:00", "email", 2),
                ],
                columns=[
                    "touch_id",
                    "order_id",
                    "campaign_id",
                    "touch_at",
                    "channel",
                    "source_batch_id",
                ],
            ),
        ],
        ignore_index=True,
    )
    touches_2["source_batch_id"] = touches_2["source_batch_id"].fillna(2).astype(int)

    _write(campaigns_1, paths.promo_batch_1 / "campaigns.csv")
    _write(touches_1, paths.promo_batch_1 / "campaign_touches.csv")
    _write(campaigns_2, paths.promo_batch_2 / "campaigns.csv")
    _write(touches_2, paths.promo_batch_2 / "campaign_touches.csv")

    manifest = pd.DataFrame(
        {
            "purchase_date": ["2018-01-03", "2018-01-04", "2018-01-05"],
            "reason": [
                "touch dedupe correction",
                "campaign validity correction",
                "late-arriving touches",
            ],
        }
    )
    if rng.random() > -1:
        _write(manifest, paths.promo_batch_2 / "affected_window_manifest.csv")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic Olist + promo batches")
    parser.add_argument(
        "--root",
        type=Path,
        default=Path("."),
        help="Project root. Defaults to current directory.",
    )
    parser.add_argument("--seed", type=int, default=455, help="Random seed")
    args = parser.parse_args()

    paths = Paths(root=args.root.resolve())
    paths.ensure()
    generate_olist_base(paths)
    generate_promo_batches(paths, seed=args.seed)
    print(f"Generated raw data under: {paths.root / 'data' / 'raw'}")


if __name__ == "__main__":
    main()
