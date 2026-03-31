#!/usr/bin/env python3
"""Offline smoke test: ME X-Ray Excel → same KPI logic as Marketing Performance Overview.

Usage:
  python scripts/smoke_verify_excel.py path/to/workbook.xlsx
  python scripts/smoke_verify_excel.py path/to/workbook.xlsx --month 2025-09 --market Kuwait

Requires: pandas, openpyxl (see requirements.txt). Imports ``oracle_app`` (Streamlit is loaded but the app is not run).
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import pandas as pd  # noqa: E402

import oracle_app as oa  # noqa: E402


def main() -> int:
    p = argparse.ArgumentParser(description="Smoke-test X-Ray Excel KPI totals vs dashboard logic.")
    p.add_argument("xlsx", type=Path, help="Path to .xlsx (ME X-Ray template)")
    p.add_argument(
        "--month",
        type=str,
        default="",
        help="Optional period string YYYY-MM (matches normalized ``month`` column, e.g. 2025-09)",
    )
    p.add_argument(
        "--market",
        type=str,
        default="",
        help="Optional market filter (exact match on normalized ``country`` column)",
    )
    p.add_argument(
        "--days",
        type=int,
        default=730,
        help="Date window in days ending today (default 730, same as app shell)",
    )
    args = p.parse_args()

    path = args.xlsx.expanduser().resolve()
    if not path.is_file():
        print(f"File not found: {path}", file=sys.stderr)
        return 1

    raw = path.read_bytes()
    df = oa._load_excel_all_sheets_from_bytes(raw)
    if df.empty:
        print("No data after load (check tab names: Raw Spend, Raw Leads, Raw Post Qualification, RAW CW).")
        return 2

    end = date.today()
    start = end - timedelta(days=max(1, args.days))
    df = oa._filter_by_date_range(df, start, end)
    if df.empty:
        print("No rows after date filter.")
        return 3

    if args.month.strip():
        m = args.month.strip()
        df = df[df["month"].astype(str) == m]
    if args.market.strip():
        mk = args.market.strip()
        df = df[df["country"].astype(str) == mk]

    if df.empty:
        print("No rows after month/market filter.")
        return 4

    kpi, spend_df, leads_df, post_df, cw_df = oa.compute_mpo_totals(df, gid0_spend_sum=0.0)

    print("=== Tab stats (rows per source_tab) ===")
    if "source_tab" in df.columns:
        print(df.groupby("source_tab").size().to_string())
    print()
    print("=== KPI (compute_mpo_totals) ===")
    for k in sorted(kpi.keys()):
        print(f"  {k}: {kpi[k]}")
    print()
    print("=== Slice row counts ===")
    print(f"  spend_df: {len(spend_df)}  leads_df: {len(leads_df)}  post_df: {len(post_df)}  cw_df: {len(cw_df)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
