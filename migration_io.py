"""Bounded file I/O and sparse, independently validated byte edits."""

from __future__ import annotations

import bisect
import dataclasses
import hashlib
from collections.abc import Iterator
from pathlib import Path

import migration_workers

CHUNK_SIZE = 1024 * 1024
MAX_RECORD_BYTES = 64 * CHUNK_SIZE


class MigrationError(RuntimeError):
    """A safety precondition or verification check failed."""


def records(path: Path) -> Iterator[bytes]:
    """Read LF-delimited records without allocating an entire rollout."""
    with path.open("rb") as stream:
        for number in range(1, 2**63):
            migration_workers.check_cancelled()
            line = stream.readline(MAX_RECORD_BYTES + 1)
            if not line:
                return
            if len(line) > MAX_RECORD_BYTES:
                raise MigrationError(
                    f"JSONL record exceeds {MAX_RECORD_BYTES} bytes at {path}:{number}; "
                    "use --max-record-mib to raise the per-record memory bound"
                )
            yield line


def file_chunks(path: Path) -> Iterator[bytes]:
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(CHUNK_SIZE), b""):
            migration_workers.check_cancelled()
            yield chunk


@dataclasses.dataclass(frozen=True)
class ByteEdit:
    offset: int
    before: bytes
    after: bytes


@dataclasses.dataclass(frozen=True)
class FilePlan:
    size: int
    digest: str
    edits: tuple[ByteEdit, ...]
    _ends: tuple[int, ...] = dataclasses.field(init=False, repr=False, compare=False)
    _deltas: tuple[int, ...] = dataclasses.field(init=False, repr=False, compare=False)

    def __post_init__(self) -> None:
        end = 0
        ends = []
        deltas = [0]
        for edit in self.edits:
            if edit.offset < end or not edit.before:
                raise MigrationError("overlapping or empty rollout byte edit")
            end = edit.offset + len(edit.before)
            if end > self.size:
                raise MigrationError("rollout byte edit is outside the original file")
            ends.append(end)
            deltas.append(deltas[-1] + len(edit.after) - len(edit.before))
        object.__setattr__(self, "_ends", tuple(ends))
        object.__setattr__(self, "_deltas", tuple(deltas))

    def translate(self, offset: int) -> int:
        return offset + self._deltas[bisect.bisect_right(self._ends, offset)]

    def chunks(self, path: Path) -> Iterator[bytes]:
        """Render a plan while checking the complete source against preflight."""
        digest = hashlib.sha256()
        position = 0
        with path.open("rb") as source:
            for edit in self.edits:
                remaining = edit.offset - position
                while remaining:
                    migration_workers.check_cancelled()
                    chunk = source.read(min(remaining, CHUNK_SIZE))
                    if not chunk:
                        raise MigrationError(f"rollout shrank since preflight: {path}")
                    digest.update(chunk)
                    remaining -= len(chunk)
                    position += len(chunk)
                    yield chunk
                before = source.read(len(edit.before))
                if before != edit.before:
                    raise MigrationError(
                        f"rollout edit no longer matches preflight: {path}"
                    )
                digest.update(before)
                position += len(before)
                yield edit.after
            for chunk in iter(lambda: source.read(CHUNK_SIZE), b""):
                migration_workers.check_cancelled()
                digest.update(chunk)
                position += len(chunk)
                yield chunk
        if position != self.size or digest.hexdigest() != self.digest:
            raise MigrationError(f"rollout changed since preflight: {path}")


def matches_chunks(path: Path, expected: Iterator[bytes]) -> bool:
    try:
        with path.open("rb") as stream:
            for chunk in expected:
                migration_workers.check_cancelled()
                if stream.read(len(chunk)) != chunk:
                    return False
            return not stream.read(1)
    finally:
        close = getattr(expected, "close", None)
        if close is not None:
            close()


def files_equal(first: Path, second: Path) -> bool:
    return first.stat().st_size == second.stat().st_size and matches_chunks(
        second, file_chunks(first)
    )
