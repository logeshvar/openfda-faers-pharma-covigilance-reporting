"""Validate that a local demo window is small enough for reliable execution."""

from __future__ import annotations

import argparse
from datetime import date


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate an inclusive date window size.")
    parser.add_argument("--window-start", required=True)
    parser.add_argument("--window-end", required=True)
    parser.add_argument("--max-days", type=int, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    window_start = date.fromisoformat(args.window_start)
    window_end = date.fromisoformat(args.window_end)
    day_count = (window_end - window_start).days + 1

    if day_count < 1:
        raise ValueError("window_end must be on or after window_start")

    if day_count > args.max_days:
        print(
            "Demo window is too large for a single local Airflow run: "
            f"{day_count} days requested, max is {args.max_days}. "
            "Run multiple smaller windows or override MAX_DEMO_WINDOW_DAYS intentionally."
        )
        return 2

    print(f"Demo window validated: {day_count} day(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
