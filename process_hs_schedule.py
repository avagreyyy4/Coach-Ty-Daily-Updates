#!/usr/bin/env python3
"""
Process the high-school schedule CSV:
  1. Convert all game times → America/New_York
  2. Split "Name" on " vs " → School_1, School_2
  3. Expand Recruits into recruit_1 … recruit_N columns
  4. Drop any recruit who does NOT end with (2027)
"""

import csv, re, subprocess
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

# ── Config ──────────────────────────────────────────────
INPUT  = Path("data/feb_HS_sched.csv")
OUTPUT = Path("data/feb_HS_sched_processed.csv")
TARGET_TZ = ZoneInfo("America/New_York")
KEEP_YEAR = "(2027)"

# ── Helpers ─────────────────────────────────────────────
def convert_to_ny(date_str: str, time_str: str, tz_str: str) -> str:
    """Parse date + time in its original tz, return as NY time string."""
    naive = datetime.strptime(f"{date_str} {time_str}", "%m/%d/%y %I:%M %p")
    local = naive.replace(tzinfo=ZoneInfo(tz_str))
    ny = local.astimezone(TARGET_TZ)
    return ny.strftime("%m/%d/%y %I:%M %p %Z")


def split_recruits(raw: str) -> list[str]:
    """Split the comma-separated recruits, keeping only (2027)."""
    if not raw.strip():
        return []
    # Recruits look like "Name (YYYY), Name (YYYY), ..."
    # Some names contain commas inside the CSV quoting, but the CSV reader
    # already handled that — we just need to split on ", " carefully.
    # Pattern: split on comma that is followed by a space and a capital letter
    # (avoids splitting "Olivia Schnurer, Evangeline Kotarski" incorrectly —
    #  but that actually IS two recruits).  Safest: split on ", " then re-join
    #  fragments that don't look like a recruit entry.
    parts = [p.strip() for p in raw.split(",")]

    # Rejoin fragments: a valid recruit ends with (YYYY)
    recruits: list[str] = []
    buf = ""
    for p in parts:
        if buf:
            buf += ", " + p
        else:
            buf = p
        if re.search(r"\(\d{4}\)\s*$", buf):
            recruits.append(buf.strip())
            buf = ""
    # leftover (no year) — still a recruit entry, just missing year
    if buf.strip():
        recruits.append(buf.strip())

    # Keep only 2027
    return [r for r in recruits if r.strip().endswith(KEEP_YEAR)]


# ── Main ────────────────────────────────────────────────
def main():
    rows: list[dict] = []
    max_recruits = 0

    # Only keep these source columns (easy to trim if new cols appear)
    KEEP_COLS = {"Date", "Time", "Timezone", "Name", "Recruits"}

    with open(INPUT, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row = {k: v for k, v in row.items() if k in KEEP_COLS}
            date = row["Date"].strip()
            time = row["Time"].strip()
            tz   = row["Timezone"].strip()

            # 1) Convert time to NY
            ny_time = convert_to_ny(date, time, tz)

            # 2) Split schools
            name = row["Name"].strip()
            if " vs " in name:
                school_1, school_2 = name.split(" vs ", 1)
            else:
                school_1, school_2 = name, ""

            # 3) Filter recruits to 2027 only, keep in one column
            recruits_2027 = split_recruits(row.get("Recruits", ""))

            rows.append({
                "Date_NY": ny_time,
                "Original_Timezone": tz,
                "School_1": school_1.strip(),
                "School_2": school_2.strip(),
                "Recruits_2027": ", ".join(recruits_2027),
            })

    fieldnames = ["Date_NY", "Original_Timezone", "School_1", "School_2", "Recruits_2027"]

    with open(OUTPUT, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"[done] wrote {len(rows)} games → {OUTPUT}")
    print(f"[info] max recruits per game: {max_recruits}")
    print(f"[info] only kept recruits ending with {KEEP_YEAR}")


if __name__ == "__main__":
    main()
