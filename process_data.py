#!/usr/bin/env python3
# ================== PACKAGES ==================
import csv, json, re, subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, Any
import pandas as pd

# ================== CONFIG ==================
FILTERS: Dict[str, Dict[str, Any]] = {
    "Mobile Phone": {"notnull": True},
}

# Month → required Contact Sheet Color (repeating Navy / Columbia Blue / Anthracite)
MONTH_TO_COLOR: Dict[int, str] = {
    1: "Navy",            7: "Navy",
    2: "Columbia Blue",   8: "Columbia Blue",
    3: "Anthracite",      9: "Anthracite",
    4: "Navy",           10: "Navy",
    5: "Columbia Blue",  11: "Columbia Blue",
    6: "Anthracite",     12: "Anthracite",
}

# First/Last name pairs to merge → (first_col, last_col, output_col)
NAME_PAIRS = [
    ("First Name",           "Last Name",           "Full Name"),
    ("Mother's First Name",  "Mother's Last Name",  "Mother's Full Name"),
    ("Father's First Name",  "Father's Last Name",  "Father's Full Name"),
]

TOP_N = 5
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

def sort_by_last_contacted(df: pd.DataFrame) -> pd.DataFrame:
    """Sort so least-recently-contacted players come first.

    Order: never called → oldest call date → most recent call date.
    """
    # NaT sorts last by default; we want never-called first,
    # so use a sentinel far in the past for NaT values.
    sort_key = df["called_dt"].fillna(pd.Timestamp("1900-01-01", tz="UTC"))
    return df.assign(_sort=sort_key).sort_values("_sort").drop(columns="_sort").reset_index(drop=True)

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

    # Merge first/last name pairs into full name columns, drop originals
    col_map = {c.lower(): c for c in df.columns}
    for first_key, last_key, out_col in NAME_PAIRS:
        fc = col_map.get(first_key.lower())
        lc = col_map.get(last_key.lower())
        if fc and lc:
            df[out_col] = (df[fc].str.strip() + " " + df[lc].str.strip()).str.strip()
            df.drop(columns=[fc, lc], inplace=True)
            print(f"[info] merged '{fc}' + '{lc}' → '{out_col}'")

    # Consolidate parent info: fall back to Parent/Guardian if Mother/Father empty
    def _col(name):
        return col_map.get(name.lower())

    pg1_name = _col("Parent/Guardian 1 Name")
    pg1_phone = _col("Parent/Guardian 1 Phone")
    pg2_name = _col("Parent/Guardian 2 Name")
    pg2_phone = _col("Parent/Guardian 2 Phone")
    mom_phone = _col("Mother's Mobile Phone")
    dad_phone = _col("Father's Mobile Phone")

    if pg1_name:
        # If Mother's Full Name is empty, fill from Parent/Guardian 1
        if "Mother's Full Name" in df.columns:
            empty_mom = df["Mother's Full Name"].str.strip() == ""
            df.loc[empty_mom, "Mother's Full Name"] = df.loc[empty_mom, pg1_name].str.strip()
            if mom_phone and pg1_phone:
                df.loc[empty_mom, mom_phone] = df.loc[empty_mom, pg1_phone].str.strip()
        # If Father's Full Name is empty, fill from Parent/Guardian 2
        if "Father's Full Name" in df.columns and pg2_name:
            empty_dad = df["Father's Full Name"].str.strip() == ""
            df.loc[empty_dad, "Father's Full Name"] = df.loc[empty_dad, pg2_name].str.strip()
            if dad_phone and pg2_phone:
                df.loc[empty_dad, dad_phone] = df.loc[empty_dad, pg2_phone].str.strip()
        # Drop the Parent/Guardian columns now that data is merged
        drop_cols = [c for c in [pg1_name, pg1_phone, pg2_name, pg2_phone] if c]
        df.drop(columns=drop_cols, inplace=True, errors="ignore")
        print(f"[info] consolidated Parent/Guardian fallbacks into Mother/Father columns")

    # ── Enrich game schedule with recruit color counts (BEFORE color filter) ──
    color_col = next((c for c in df.columns if "contact sheet color" in c.lower()), None)
    schedule_csv = Path("data/feb_HS_sched_processed.csv")

    if color_col and "Full Name" in df.columns and schedule_csv.exists():
        # Build name → color lookup (lowercase name, no year tag)
        name_to_color: Dict[str, str] = {}
        for _, row in df.iterrows():
            name = row["Full Name"].strip().lower()
            color = row[color_col].strip()
            if name and color:
                name_to_color[name] = color

        # Read schedule, count colors per game
        games = []
        with open(schedule_csv, newline="") as f:
            for game in csv.DictReader(f):
                raw = game.get("Recruits_2027", "").strip()
                counts: Dict[str, int] = {"Navy": 0, "Columbia Blue": 0, "Anthracite": 0}
                matched: list[str] = []
                if raw:
                    for recruit in raw.split(", "):
                        # Strip "(2027)" to match export names
                        clean = re.sub(r"\s*\(\d{4}\)\s*$", "", recruit).strip().lower()
                        c = name_to_color.get(clean)
                        if c and c in counts:
                            counts[c] += 1
                            matched.append(recruit)
                total = sum(counts.values())
                games.append({
                    "date": game.get("Date_NY", ""),
                    "school_1": game.get("School_1", ""),
                    "school_2": game.get("School_2", ""),
                    "recruits_2027": matched,
                    "color_counts": counts,
                    "total_tracked": total,
                })

        # Sort by total tracked recruits descending
        games.sort(key=lambda g: g["total_tracked"], reverse=True)
        games_json = Path("data/games.json")
        games_json.write_text(json.dumps(games, indent=2))
        print(f"[info] enriched {len(games)} games with color counts → {games_json}")

    # Filter by Contact Sheet Color for the current month
    if color_col:
        keep_color = MONTH_TO_COLOR[datetime.now().month]
        before = len(df)
        df = df[df[color_col].str.strip().str.lower() == keep_color.lower()]
        print(f"[info] kept {len(df)}/{before} rows where '{color_col}' == '{keep_color}'")

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

    # Sort by last contacted (never/oldest first), take top N
    df = sort_by_last_contacted(df)
    df_top = df.head(TOP_N)

    # Save CSV
    top_csv = out_dir / "top5.csv"
    df_top.to_csv(top_csv, index=False)
    print(f"[info] saved {top_csv}")

    # ── Helper to build a player dict from a row ──
    def _get(row, *candidates):
        """Return the first non-empty value from candidate column names."""
        for c in candidates:
            actual = col_map.get(c.lower())
            if actual and actual in row.index:
                v = str(row[actual]).strip()
                if v:
                    return v
        return ""

    def player_dict(row):
        return {
            "name":          row.get("Full Name", ""),
            "phone":         _get(row, "Mobile Phone"),
            "position":      _get(row, "Board Position"),
            "state":         _get(row, "State"),
            "color":         _get(row, "Contact Sheet Color"),
            "has_transcript": _get(row, "Has Transcript"),
            "last_eval":     _get(row, "Last Evaluation"),
            "last_called":   _get(row, "Last Called"),
            "last_call_with": _get(row, "Last Call With"),
            "mother_name":   row.get("Mother's Full Name", ""),
            "mother_phone":  _get(row, "Mother's Mobile Phone"),
            "mother_email":  _get(row, "Mother's Email"),
            "father_name":   row.get("Father's Full Name", ""),
            "father_phone":  _get(row, "Father's Mobile Phone"),
            "father_email":  _get(row, "Father's Email"),
            "hs_name":       _get(row, "HS Name"),
            "hs_phone":      _get(row, "HS Phone"),
        }

    # Save enriched top 5 JSON (for index.html)
    top5_list = [player_dict(r) for _, r in df_top.iterrows()]
    Path("data/top5.json").write_text(json.dumps({"players": top5_list}, indent=2))
    print(f"[info] saved data/top5.json ({len(top5_list)} players)")

    # Save all filtered players JSON (for players.html)
    all_list = [player_dict(r) for _, r in df.iterrows()]
    Path("data/players.json").write_text(json.dumps({"players": all_list}, indent=2))
    print(f"[info] saved data/players.json ({len(all_list)} players)")

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
