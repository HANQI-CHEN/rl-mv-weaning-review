#!/usr/bin/env python3
"""
Filter titles that contain:
  1) 'review'
  2) 'machine learning' (also matches 'machine-learning')

Usage (defaults work out of the box):
    python remove_review_ml.py
    # or specify paths:
    python remove_review_ml.py -i paper_list/normalized/screened_candidates.csv \
                               -o paper_list/normalized/screened_candidates_no_review_ml.csv
"""

import csv
import argparse
import re
from pathlib import Path

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-i", "--input",  default="paper_list/normalized/screened_candidates.csv",
                    help="Input CSV with columns including 'title'")
    ap.add_argument("-o", "--output", default="paper_list/normalized/screened_candidates_no_review_ml.csv",
                    help="Output CSV after removals")
    args = ap.parse_args()

    in_path  = Path(args.input)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Compile regex (case-insensitive)
    rx_review = re.compile(r"\breview\b", re.IGNORECASE)
    rx_ml     = re.compile(r"\bmachine[-\s]?learning\b", re.IGNORECASE)

    kept_rows = []
    removed_rows = []

    with in_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        if "title" not in (name.lower() for name in fieldnames):
            raise RuntimeError("CSV must contain a 'title' column")

        # normalize the title key (exactly as in file)
        title_key = next(k for k in fieldnames if k.lower() == "title")

        for row in reader:
            title = (row.get(title_key) or "").strip()
            reasons = []
            if title:
                if rx_review.search(title):
                    reasons.append("review")
                if rx_ml.search(title):
                    reasons.append("machine learning")

            if reasons:
                removed_rows.append((row, reasons))
            else:
                kept_rows.append(row)

    # Print removals step-by-step
    print("=== Removals ===")
    if not removed_rows:
        print("(none)")
    else:
        for row, reasons in removed_rows:
            title_display = (row.get("title") or "").strip()
            print(f"REMOVED [{ ' | '.join(reasons) }]: {title_display}")

    # Write output
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=kept_rows[0].keys() if kept_rows else (fieldnames or ["title"]))
        writer.writeheader()
        for r in kept_rows:
            writer.writerow(r)

    # Summary
    print("\n=== Summary ===")
    print(f"Input : {in_path}")
    print(f"Output: {out_path}")
    print(f"Kept  : {len(kept_rows)}")
    print(f"Removed: {len(removed_rows)}")

if __name__ == "__main__":
    main()
