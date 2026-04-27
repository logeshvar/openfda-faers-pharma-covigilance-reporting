from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

logger = logging.getLogger(__name__)


def ensure_repo_root_on_path(anchor_file: str) -> None:
    current_path = Path(anchor_file).resolve()
    for parent in current_path.parents:
        if (parent / "src").is_dir() and (parent / "conf").is_dir():
            repo_root = str(parent)
            if repo_root not in sys.path:
                sys.path.insert(0, repo_root)
            return


def add_common_databricks_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--env", default=None)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--batch-id", default=None)
    parser.add_argument("--raw-base-path", default=None)
    parser.add_argument("--curated-base-path", default=None)
    parser.add_argument("--gold-base-path", default=None)
    parser.add_argument("--ops-base-path", default=None)
    parser.add_argument("--window-start", default=None)
    parser.add_argument("--window-end", default=None)


def log_common_databricks_args(args: argparse.Namespace) -> None:
    logger.info(
        "Databricks task context env=%s run_id=%s batch_id=%s window_start=%s window_end=%s",
        getattr(args, "env", None),
        getattr(args, "run_id", None),
        getattr(args, "batch_id", None),
        getattr(args, "window_start", None),
        getattr(args, "window_end", None),
    )
