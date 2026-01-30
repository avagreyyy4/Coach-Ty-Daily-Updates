#!/usr/bin/env python3
# ================== PACKAGES ==================
import json, os, subprocess
from pathlib import Path
from typing import Dict, Any

import pandas as pd

# ================== CONFIG ==================
FILTERS: Dict[str, Dict[str, Any]] = {
    "Mobile Phone": {"notnull": True},
}

TOP_N = 5
DISTANCE_ORDER = ["far", "never", "recent"]

# ================== HELPERS ==================
def _ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def _normalize_str(x: Any) -> str:
    return str(x).strip()

def apply_filters(df: pd.DataFrame, rules: Dict[str, Dict[str, Any]]) -> pd.DataFrame:
    if not rules:
        return df
    m = pd.Series(True, index=df.index)
    for col, cond in rules.items():
        if col not in df.columns:
            m &= False
            continue
        s = df[col].astype(str).fillna("").map(_normalize_str)
        for op, val in cond.items():
            op = op.lower()
            if op == "eq":
                m &= s.str.lower() == str(val).lower()
            elif op == "ne":
                m &= s.str.lower() != str(val).lower()
            elif op == "contains":
                m &= s.str.contains(str(val), case=False, na=False)
            elif op == "notnull":
                m &= s.str.len() > 0
            elif op == "null":
                m &= s.str.len() == 0
            else:
                raise ValueError(f"Unsupported filter op: {op}")
    return df[m]

# ================== RECENCY LOGIC ==================
def add_recency_bucket(
    df: pd.DataFrame,
    date_col: str,
    prefix: str,
    recent_days: int = 365,
) -> pd.DataFrame:
    df = df.copy()
    dt = pd.to_datetime(df[date_col].replace({"": None}), errors="coerce", utc=True)

    today = pd.Timestamp.now(tz="UTC").normalize()
    cutoff = today - pd.Timedelta(days=recent_days)

    dist = pd.Series("far", index=df.index)
    dist[dt.isna()] = "never"
    dist[dt >= cutoff] = "recent"

    df[f"{prefix}_dt"] = dt
    df[f"{prefix}_distance"] = dist
    return df

def sort_by_contact_and_texted(df: pd.DataFrame) -> pd.DataFrame:
    order = {k: i for i, k in enumerate(DISTANCE_ORDER)}
    today = pd.Timestamp.now(tz="UTC").normalize()

    return (
        df.assign(
            _contact_rank=df["contact_distance"].map(order).fillna(999),
            _texted_rank=df["texted_distance"].map(order).fillna(999),
            _contact_days=(today - df["contact_dt"]).dt.days.fillna(9999),
            _texted_days=(today - df["texted_dt"]).dt.days.fillna(9999),
        )
        .sort_values(
            by=[
                "_contact_rank",
                "_texted_rank",
                "_contact_days",
                "_texted_days",
            ],
            ascending=[True, True, False, False],
            kind="stable",
        )
        .drop(
            columns=[
                "_contact_rank",
                "_texted_rank",
                "_contact_days",
                "_texted_days",
            ]
        )
    )

# ================== MAIN ==================
def main():
    run_id = os.getenv("RUN_ID")
    if not run_id:
        raise RuntimeError("RUN_ID not set")

    raw_csv = Path(f"data/export_{run_id}.csv")
    if not raw_csv.exists():
        raise FileNotFoundError(raw_csv)

    out_dir = Path(f"data/processed/{run_id}")
    _ensure_dir(out_dir)

    df = pd.read_csv(raw_csv, dtype=str, keep_default_na=False)
    df.columns = [c.strip() for c in df.columns]

    # Build Full Name from First/Last Name columns
    first_col = next((c for c in df.columns if "first" in c.lower() and "name" in c.lower()), None)
    last_col = next((c for c in df.columns if "last" in c.lower() and "name" in c.lower()), None)
    if first_col and last_col:
        df["Full Name"] = (df[first_col].str.strip() + " " + df[last_col].str.strip()).str.strip()

    # Filters
    df = apply_filters(df, FILTERS)

    # Add recency logic
    df = add_recency_bucket(df, "Last Contact", prefix="contact")

    # Sort + top 5 recruits not called recently
    df = sort_by_contact_and_texted(df)
    df_top = df.head(TOP_N).reset_index(drop=True)

    # Save CSV
    top_csv = out_dir / "top5.csv"
    df_top.to_csv(top_csv, index=False)
    print(f"[info] saved {top_csv}")

    # Save JSON for the frontend (name only)
    names = df_top["Full Name"].tolist() if "Full Name" in df_top.columns else []
    top5_json = Path("data/top5.json")
    top5_json.write_text(json.dumps({"players": names}, indent=2))
    print(f"[info] saved {top5_json}")

    # Push to GitHub
    try:
        repo_root = Path(__file__).resolve().parent
        subprocess.run(["git", "add", "data/"], cwd=repo_root, check=True)
        subprocess.run(
            ["git", "commit", "-m", f"data: processed top 5 for run {run_id}"],
            cwd=repo_root, check=True,
        )
        subprocess.run(["git", "push"], cwd=repo_root, check=True)
        print(f"[info] pushed processed data to GitHub")
    except subprocess.CalledProcessError as e:
        print(f"[error] git push failed: {e}")

    print("[done] process complete.")

if __name__ == "__main__":
    main()
