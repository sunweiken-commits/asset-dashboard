from __future__ import annotations

from pathlib import Path

import pandas as pd

from app import DEFAULT_WORKBOOK, parse_assets_sheet
from database import create_supabase


def main() -> None:
    workbook = Path(DEFAULT_WORKBOOK)
    asset_history, _ = parse_assets_sheet(workbook)
    client = create_supabase()

    asset_order = (
        asset_history[["category", "account"]]
        .drop_duplicates()
        .reset_index(drop=True)
        .reset_index(names="sort_order")
    )
    asset_records = asset_order.assign(sort_order=lambda df: df["sort_order"] + 1).to_dict(orient="records")
    client.table("assets").upsert(asset_records, on_conflict="account").execute()

    assets_df = pd.DataFrame(client.table("assets").select("id, account").execute().data or [])
    if assets_df.empty:
        raise RuntimeError("assets 导入失败，数据库中没有读到任何账户。")

    merged = asset_history.merge(assets_df, on="account", how="inner")
    value_records = (
        merged.assign(snapshot_date=lambda df: df["date"].dt.strftime("%Y-%m-%d"))
        [["id", "snapshot_date", "amount"]]
        .rename(columns={"id": "asset_id"})
        .to_dict(orient="records")
    )
    client.table("asset_values").upsert(value_records, on_conflict="asset_id,snapshot_date").execute()
    print(f"Imported {len(asset_records)} assets and {len(value_records)} asset values.")


if __name__ == "__main__":
    main()
