#!/usr/bin/env python3
"""
Filter titles from screened_candidates.csv by:
  1) 'review'
  2) 'machine learning' (also 'machine-learning')
  3) 'deep learning' BUT KEEP if 'deep reinforcement learning' is present
  4) must include a mechanical-ventilation keyword in the TITLE (e.g., 'mechanical ventilation',
     'ventilator', 'ventilation', 'weaning', 'extubation', 'SBT', etc.)

Usage:
  python remove_review_ml_deeplearning_mv.py \
      -i paper_list/normalized/screened_candidates.csv \
      -o paper_list/normalized/screened_candidates_title_filtered.csv
"""

import csv
import argparse
import re
from pathlib import Path
from collections import Counter

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-i", "--input",  default="paper_list/normalized/screened_candidates.csv",
                    help="Input CSV path (must include a 'title' column)")
    ap.add_argument("-o", "--output", default="paper_list/normalized/screened_candidates_title_filtered.csv",
                    help="Output CSV path")
    args = ap.parse_args()

    in_path  = Path(args.input)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # --- Patterns ---
    rx_review   = re.compile(r"\breview\b", re.IGNORECASE)
    rx_ml       = re.compile(r"\bmachine[-\s]?learning\b", re.IGNORECASE)
    rx_deep_l   = re.compile(r"\bdeep\s+learning\b", re.IGNORECASE)
    rx_deep_rl  = re.compile(r"\bdeep\s+reinforcement\s+learning\b", re.IGNORECASE)

    # Mechanical-ventilation keywords required in title
    mv_title_terms = [
        r"mechanical ventilation",
        r"ventilator",
        r"ventilatory support",
        r"\bventilation\b",
        r"\bventilated\b",
        r"patient-ventilator",
        r"\bwean(?:ing)?\b",
        r"\bextubat(?:e|ion|ing)?\b",
        r"spontaneous breathing trial",
        r"\bSBT\b",
        r"ventilator liberation",
    ]
    rx_mv_title = re.compile("|".join(mv_title_terms), re.IGNORECASE)

    kept_rows = []
    removed_rows = []  # list of (row, [reasons])
    reason_counts = Counter()

    with in_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        if not fieldnames:
            raise RuntimeError("Empty or invalid CSV.")
        if "title" not in [c.lower() for c in fieldnames]:
            raise RuntimeError("CSV must contain a 'title' column.")

        # Preserve exact key for title column from the file
        title_key = next(c for c in fieldnames if c.lower() == "title")

        for row in reader:
            title = (row.get(title_key) or "").strip()
            title_lc = title.lower()
            reasons = []

            # 1) 'review'
            if rx_review.search(title):
                reasons.append("review")

            # 2) 'machine learning'
            if rx_ml.search(title):
                reasons.append("machine learning")

            # 3) 'deep learning' (unless also 'deep reinforcement learning')
            if rx_deep_l.search(title) and not rx_deep_rl.search(title):
                reasons.append("deep learning (not DRL)")

            # 4) must include MV keyword in TITLE
            if not rx_mv_title.search(title):
                reasons.append("no MV keyword in title")

            if reasons:
                removed_rows.append((row, reasons))
                for r in set(reasons):
                    reason_counts[r] += 1
            else:
                kept_rows.append(row)

    # --- Print removals, step-by-step ---
    print("=== Removals (by title) ===")
    if not removed_rows:
        print("(none)")
    else:
        for row, reasons in removed_rows:
            print(f"REMOVED [{'; '.join(reasons)}]: {row.get('title','').strip()}")

    # --- Summary ---
    print("\n=== Summary ===")
    print(f"Input : {in_path}")
    print(f"Output: {out_path}")
    print(f"Kept   : {len(kept_rows)}")
    print(f"Removed: {len(removed_rows)}")
    if removed_rows:
        print("Removed by reason:")
        for r, c in reason_counts.most_common():
            print(f"  - {r}: {c}")

    # --- Write output ---
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in kept_rows:
            writer.writerow(r)

if __name__ == "__main__":
    main()
