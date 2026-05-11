from __future__ import annotations

import argparse
import sys
import time

from airflow.models.dagrun import DagRun
from airflow.utils.session import create_session


SUCCESS_STATES = {"success"}
FAILED_STATES = {"failed"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Wait for an Airflow DAG run to finish.")
    parser.add_argument("dag_id")
    parser.add_argument("run_id")
    parser.add_argument("--poll-seconds", type=int, default=15)
    parser.add_argument("--timeout-seconds", type=int, default=7200)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    deadline = time.monotonic() + args.timeout_seconds
    last_state: str | None = None

    while time.monotonic() < deadline:
        with create_session() as session:
            dag_run = (
                session.query(DagRun)
                .filter(DagRun.dag_id == args.dag_id, DagRun.run_id == args.run_id)
                .one_or_none()
            )
            state = str(dag_run.state) if dag_run else "not_found"

        if state != last_state:
            print(f"{args.dag_id} run_id={args.run_id} state={state}", flush=True)
            last_state = state

        if state in SUCCESS_STATES:
            return 0
        if state in FAILED_STATES:
            return 1
        time.sleep(args.poll_seconds)

    print(f"Timed out waiting for {args.dag_id} run_id={args.run_id}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
