#!/usr/bin/env python3
# ================== PACKAGES ==================
import json, random, subprocess
from pathlib import Path
from typing import Dict, Any

import pandas as pd

# ================== CONFIG ==================
FILTERS: Dict[str, Dict[str, Any]] = {
    "Mobile Phone": {"notnull": True},
}

TOP_N = 5
DISTANCE_ORDER = ["far", "never", "recent"]
EXCLUDE_CALLERS = ["alexandra bassetti", "Kizmahr Grell"]

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

def pick_random_far(df: pd.DataFrame, n: int) -> pd.DataFrame:
    """Pick n recruits randomly from the 'far' bucket only."""
    pool = df[df["called_distance"] == "far"]
    if pool.empty:
        print("[warn] no recruits in 'far' bucket")
        return df.head(0)
    if len(pool) <= n:
        return pool.reset_index(drop=True)
    return pool.sample(n=n, random_state=random.randint(0, 2**31)).reset_index(drop=True)

# ================== MAIN ==================
def main():
    raw_csv = Path("data/export.csv")
    if not raw_csv.exists():
        raise FileNotFoundError(raw_csv)

    out_dir = Path("data")
    _ensure_dir(out_dir)

    df = pd.read_csv(raw_csv, dtype=str, keep_default_na=False)
    df.columns = [c.strip() for c in df.columns]
    print(f"[debug] columns: {list(df.columns)}")

    # Build Full Name from First/Last Name columns
    first_col = next((c for c in df.columns if "first" in c.lower() and "name" in c.lower()), None)
    last_col = next((c for c in df.columns if "last" in c.lower() and "name" in c.lower()), None)
    if first_col and last_col:
        df["Full Name"] = (df[first_col].str.strip() + " " + df[last_col].str.strip()).str.strip()

    # Filters
    df = apply_filters(df, FILTERS)

    # Find the "Last Called" column
    called_col = next((c for c in df.columns if "call" in c.lower() and "last" in c.lower()), None)
    if called_col:
        df = add_recency_bucket(df, called_col, prefix="called")
    else:
        print(f"[warn] no 'Last Called' column found, treating all as never called")
        df["called_dt"] = pd.NaT
        df["called_distance"] = "never"

    # Exclude recruits last called by specific people
    call_with_col = next((c for c in df.columns if "call" in c.lower() and "with" in c.lower()), None)
    if call_with_col:
        caller = df[call_with_col].str.strip().str.lower()
        df = df[~caller.isin(EXCLUDE_CALLERS)]
        print(f"[info] filtered out recruits called by {EXCLUDE_CALLERS} via '{call_with_col}'")

    # Pick top 5 randomly, prioritizing by distance bucket
    df_top = pick_random_far(df, TOP_N)

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
            ["git", "commit", "-m", "data: processed top 5"],
            cwd=repo_root, check=True,
        )
        subprocess.run(["git", "push"], cwd=repo_root, check=True)
        print(f"[info] pushed processed data to GitHub")
    except subprocess.CalledProcessError as e:
        print(f"[error] git push failed: {e}")

    print("[done] process complete.")

if __name__ == "__main__":
    main()
