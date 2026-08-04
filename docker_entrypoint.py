#!/usr/bin/env python3
"""Dispatch the migration utility's container subcommands."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import TextIO


COMMANDS = {
    "migrate": "migrate.py",
    "restore": "restore.py",
    "verify": "verify.py",
}


def print_usage(stream: TextIO = sys.stdout) -> None:
    print(
        "usage: docker run IMAGE {migrate|verify|restore} [arguments...]",
        file=stream,
    )


def main(argv: list[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if not arguments or arguments[0] in {"help", "-h", "--help"}:
        print_usage()
        return 0

    command = arguments.pop(0)
    script = COMMANDS.get(command)
    if script is None:
        print(f"error: unknown container command: {command}", file=sys.stderr)
        print_usage(sys.stderr)
        return 2

    script_path = Path(__file__).resolve().with_name(script)
    os.execv(
        sys.executable,
        [sys.executable, str(script_path), *arguments],
    )
    raise AssertionError("os.execv unexpectedly returned")


if __name__ == "__main__":
    raise SystemExit(main())
