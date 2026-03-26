from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from typing import Any

import pandas as pd
import streamlit as st
from supabase import Client, create_client


@dataclass
class SnapshotMonth:
    label: str
    snapshot_date: date
    has_values: bool


def read_secret(name: str) -> str | None:
    value = os.getenv(name)
    if value:
        return value
    try:
        secrets = st.secrets
        if name in secrets:
            return str(secrets[name])
    except Exception:
        pass
    return None


def is_supabase_configured() -> bool:
    return bool(read_secret("SUPABASE_URL") and read_secret("SUPABASE_KEY"))


def create_supabase() -> Client:
    url = read_secret("SUPABASE_URL")
    key = read_secret("SUPABASE_KEY")
    if not url or not key:
        raise RuntimeError("缺少 SUPABASE_URL 或 SUPABASE_KEY。")
    return create_client(url, key)


def has_app_password() -> bool:
    return bool(read_secret("APP_PASSWORD"))


def validate_app_password(password: str) -> bool:
    expected = read_secret("APP_PASSWORD")
    if not expected:
        return True
    return password == expected


def fetch_assets_df(client: Client) -> pd.DataFrame:
    response = client.table("assets").select("*").order("sort_order").order("id").execute()
    return pd.DataFrame(response.data or [])


def fetch_asset_values_df(client: Client) -> pd.DataFrame:
    response = client.table("asset_values").select("*").order("snapshot_date").execute()
    return pd.DataFrame(response.data or [])


def build_workbook_data_from_database(client: Client) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    assets_df = fetch_assets_df(client)
    values_df = fetch_asset_values_df(client)
    if assets_df.empty or values_df.empty:
        raise ValueError("数据库中缺少 assets 或 asset_values 数据。")

    merged = values_df.merge(
        assets_df[["id", "category", "account", "sort_order"]],
        left_on="asset_id",
        right_on="id",
        how="inner",
    )
    asset_history = (
        merged.rename(columns={"snapshot_date": "date"})[["category", "account", "date", "amount", "asset_id", "sort_order"]]
        .assign(date=lambda df: pd.to_datetime(df["date"]))
        .sort_values(["date", "sort_order", "account"])
        .reset_index(drop=True)
    )
    latest_snapshot = (
        asset_history.sort_values("date")
        .groupby(["category", "account"], as_index=False)
        .tail(1)
        .sort_values(["category", "amount"], ascending=[True, False])
        .reset_index(drop=True)
    )
    total_trend = (
        asset_history.groupby("date", as_index=False)["amount"]
        .sum()
        .rename(columns={"amount": "total_assets"})
        .sort_values("date")
        .reset_index(drop=True)
    )
    return asset_history, latest_snapshot, total_trend


def list_snapshot_months(client: Client) -> list[SnapshotMonth]:
    values_df = fetch_asset_values_df(client)
    if values_df.empty:
        return []
    values_df["snapshot_date"] = pd.to_datetime(values_df["snapshot_date"]).dt.date
    counts = values_df.groupby("snapshot_date").size().reset_index(name="count")
    return [
        SnapshotMonth(
            label=item.snapshot_date.strftime("%Y-%m-%d"),
            snapshot_date=item.snapshot_date,
            has_values=bool(item["count"]),
        )
        for _, item in counts.sort_values("snapshot_date").iterrows()
    ]


def build_update_frame_from_database(client: Client, snapshot_date: date) -> pd.DataFrame:
    assets_df = fetch_assets_df(client)
    values_df = fetch_asset_values_df(client)
    if assets_df.empty:
        return pd.DataFrame(columns=["asset_id", "分类", "账户", "上月金额", "本月金额"])

    values_df["snapshot_date"] = pd.to_datetime(values_df["snapshot_date"]).dt.date
    existing_dates = sorted(values_df["snapshot_date"].unique().tolist())
    previous_date = max([d for d in existing_dates if d < snapshot_date], default=None)

    current_map = (
        values_df[values_df["snapshot_date"] == snapshot_date][["asset_id", "amount"]]
        .rename(columns={"amount": "本月金额"})
        .set_index("asset_id")
    )
    previous_map = (
        values_df[values_df["snapshot_date"] == previous_date][["asset_id", "amount"]]
        .rename(columns={"amount": "上月金额"})
        .set_index("asset_id")
        if previous_date
        else pd.DataFrame(columns=["上月金额"])
    )

    frame = assets_df[["id", "category", "account", "sort_order"]].rename(
        columns={"id": "asset_id", "category": "分类", "account": "账户"}
    )
    frame = frame.join(previous_map, on="asset_id").join(current_map, on="asset_id")
    return frame.sort_values(["sort_order", "账户"]).reset_index(drop=True)


def snapshot_has_values(client: Client, snapshot_date: date) -> bool:
    response = client.table("asset_values").select("id", count="exact").eq("snapshot_date", snapshot_date.isoformat()).execute()
    count = getattr(response, "count", None)
    if count is not None:
        return count > 0
    return bool(response.data)


def upsert_snapshot_values(client: Client, values: pd.DataFrame, snapshot_date: date) -> None:
    records: list[dict[str, Any]] = []
    for _, row in values.iterrows():
        amount = row["本月金额"]
        if pd.isna(amount):
            continue
        records.append(
            {
                "asset_id": int(row["asset_id"]),
                "snapshot_date": snapshot_date.isoformat(),
                "amount": float(amount),
            }
        )
    if not records:
        return

    client.table("asset_values").upsert(records, on_conflict="asset_id,snapshot_date").execute()


def insert_audit_log(client: Client, action: str, snapshot_date: date | None, details: dict[str, Any]) -> None:
    try:
        client.table("audit_logs").insert(
            {
                "action": action,
                "snapshot_date": snapshot_date.isoformat() if snapshot_date else None,
                "details": details,
            }
        ).execute()
    except Exception:
        # 审计失败不阻断主流程，避免影响正常录入。
        pass


def fetch_recent_audit_logs(client: Client, limit: int = 20) -> pd.DataFrame:
    try:
        response = (
            client.table("audit_logs")
            .select("*")
            .order("created_at", desc=True)
            .limit(limit)
            .execute()
        )
        return pd.DataFrame(response.data or [])
    except Exception:
        return pd.DataFrame()
