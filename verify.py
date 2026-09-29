#!/usr/bin/env python3
"""Re-run exhaustive verification for a completed provider migration."""

from __future__ import annotations

import argparse
import dataclasses
import json
import sqlite3
from pathlib import Path

import progress
from migrate import TOOL_VERSION, MigrationError, verify_against_backup


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
    progress.add_arguments(parser)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        with progress.reporting(args), progress.phase("Verify migration") as status:
            result = run(args)
            status.outcome = "failed" if result else "completed"
            return result
    except (MigrationError, OSError, ValueError, sqlite3.Error, MemoryError) as exc:
        progress.error(progress.exception_message(exc))
        return 1


def run(args: argparse.Namespace) -> int:
    try:
        report = verify_against_backup(
            backup_dir=args.backup_dir.resolve(),
            codex_home=args.codex_home.resolve() if args.codex_home else None,
            sqlite_home=args.sqlite_home.resolve() if args.sqlite_home else None,
        )
    except KeyboardInterrupt:
        progress.error("interrupted; active file workers have stopped")
        return 130
    except (MigrationError, OSError, ValueError, sqlite3.Error, MemoryError) as exc:
        progress.error(progress.exception_message(exc))
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
