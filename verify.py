#!/usr/bin/env python3
"""Re-run exhaustive verification for a completed provider migration."""

from __future__ import annotations

import argparse
import dataclasses
import json
import sqlite3
import sys
from pathlib import Path

from migrate import MigrationError
from migrate import TOOL_VERSION
from migrate import verify_against_backup


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", action="version", version=TOOL_VERSION)
    parser.add_argument(
        "--backup-dir",
        type=Path,
        required=True,
        help="private backup directory created by migrate.py",
    )
    parser.add_argument(
        "--codex-home",
        type=Path,
        help="override the migrated Codex state path recorded in the manifest",
    )
    parser.add_argument(
        "--sqlite-home",
        type=Path,
        help="override the migrated SQLite path recorded in the manifest",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="emit machine-readable JSON",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        report = verify_against_backup(
            backup_dir=args.backup_dir.resolve(),
            codex_home=args.codex_home.resolve() if args.codex_home else None,
            sqlite_home=args.sqlite_home.resolve() if args.sqlite_home else None,
        )
    except (MigrationError, OSError, ValueError, sqlite3.Error) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    values = {"result": "verification passed", **dataclasses.asdict(report)}
    if args.json:
        print(json.dumps(values, indent=2, sort_keys=True))
    else:
        for key, value in values.items():
            print(f"{key}: {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
