#!/usr/bin/env python3
"""Back up, migrate, and verify legacy Codex model-provider metadata.

The command does not change Codex records unless --apply is supplied. A
read-only SQLite open may create standard WAL coordination sidecars. The tool
refuses compressed rollouts and unknown paginated schemas; for the supported
thread-history schema it migrates and exhaustively verifies stored byte offsets.
"""

from __future__ import annotations

import argparse
import collections
import copy
import dataclasses
import datetime as dt
import functools
import graphlib
import hashlib
import itertools
import json
import os
import re
import shutil
import sqlite3
import stat
import tempfile
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import tomllib

import migration_workers as workers
import progress
from migration_io import (
    ByteEdit,
    FilePlan,
    MigrationError,
    file_chunks,
    files_equal,
    matches_chunks,
    records,
)

TOOL_VERSION = "1.5.1"
MANIFEST_VERSION = 3
LEGACY_MANIFEST_VERSION = 2
MANIFEST_NAME = "migration-manifest.json"
STATE_DB_NAME = "state_5.sqlite"
HISTORY_DB_NAME = "thread_history_1.sqlite"
SESSION_DIR_NAMES = ("sessions", "archived_sessions")
PROVIDER_KEYS = ("model_provider", "model_provider_id")
KNOWN_PROVIDER_PATHS = {
    ("payload", "model_provider"),
    ("payload", "thread_settings", "model_provider_id"),
}
PERSISTENT_SQLITE_PRAGMAS = (
    "application_id",
    "auto_vacuum",
    "default_cache_size",
    "encoding",
    "page_size",
    "schema_version",
    "user_version",
)


@dataclasses.dataclass(frozen=True)
class RolloutAnalysis:
    rollout_files: int
    rollout_heads_from_provider: int
    files_requiring_changes: int
    session_meta_values: int
    thread_settings_values: int
    malformed_lines: int
    valid_lines: int
    total_lines: int
    history_base_values: int
    history_base_offsets_changed: int
    file_hashes: dict[str, str]
    file_metadata: dict[str, dict[str, int]]

    @property
    def replacements(self) -> int:
        return self.session_meta_values + self.thread_settings_values


@dataclasses.dataclass(frozen=True)
class HistoryBaseReference:
    relative_path: str
    line_number: int
    owner_thread_id: str
    source_rollout_id: str
    end_ordinal_exclusive: int
    original_end_byte_offset: int


@dataclasses.dataclass(frozen=True)
class RolloutMigrationPlan:
    files: dict[str, FilePlan]
    positions: dict[str, RolloutPositions]
    analysis: RolloutAnalysis
    history_base_values: int
    history_base_offsets_changed: int
    rollouts_by_id: dict[str, RolloutIdentity]


@dataclasses.dataclass(frozen=True)
class RolloutIdentity:
    relative_path: str
    thread_id: str
    rollout_id: str
    ordinal_base: int


@dataclasses.dataclass(frozen=True)
class RolloutPositions:
    checkpoints: dict[int, int]
    starts: dict[int, int]
    ends: dict[int, int]


@dataclasses.dataclass(frozen=True)
class DatabaseAnalysis:
    rows_from_provider: int
    integrity_check: str
    tables: int
    persistent_settings: dict[str, int | str]


@dataclasses.dataclass(frozen=True)
class HistoryDatabaseAnalysis:
    present: bool
    integrity_check: str
    tables: int
    rows: int
    paginated_threads: int
    offset_fields_to_update: int
    persistent_settings: dict[str, int | str]


@dataclasses.dataclass(frozen=True)
class HistoryOffsetUpdate:
    table: str
    key_columns: tuple[str, ...]
    key_values: tuple[str, ...]
    offset_column: str
    original_offset: int
    migrated_offset: int


@dataclasses.dataclass(frozen=True)
class VerificationReport:
    rollout_files_checked: int
    changed_rollout_files: int
    unchanged_rollout_files: int
    jsonl_lines_checked: int
    malformed_lines_preserved: int
    session_meta_values_changed: int
    thread_settings_values_changed: int
    history_base_offsets_changed: int
    sqlite_tables_checked: int
    sqlite_rows_checked: int
    sqlite_thread_rows_changed: int
    history_sqlite_tables_checked: int
    history_sqlite_rows_checked: int
    history_offset_fields_changed: int
    config_matches_expected: bool


@dataclasses.dataclass(frozen=True)
class RestorationReport:
    rollout_files_restored: int
    config_restored: bool
    sqlite_tables_checked: int
    sqlite_rows_checked: int
    history_database_restored: bool
    history_sqlite_tables_checked: int
    history_sqlite_rows_checked: int


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            workers.check_cancelled()
            digest.update(chunk)
    return digest.hexdigest()


def capture_file_metadata(path: Path) -> dict[str, int]:
    metadata = path.stat()
    return {
        "mode": stat.S_IMODE(metadata.st_mode),
        "uid": metadata.st_uid,
        "gid": metadata.st_gid,
        "mtime_ns": metadata.st_mtime_ns,
    }


def apply_file_metadata(path: Path, metadata: dict[str, int]) -> None:
    set_file_owner(path, metadata["uid"], metadata["gid"])
    # chown can clear setuid/setgid bits, even when the requested owner is
    # unchanged. Apply the recorded mode only after ownership is settled.
    os.chmod(path, metadata["mode"])
    current = path.stat()
    os.utime(path, ns=(current.st_atime_ns, metadata["mtime_ns"]))


def artifact_descriptor(
    content_path: Path,
    metadata: dict[str, int],
) -> dict[str, Any]:
    return {
        "present": True,
        "sha256": sha256_file(content_path),
        **metadata,
    }


@functools.lru_cache(maxsize=32)
def json_string_bytes(value: str) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


@functools.lru_cache(maxsize=32)
def provider_pattern(key: str, provider: str) -> re.Pattern[bytes]:
    return re.compile(
        rb'("'
        + re.escape(key.encode("utf-8"))
        + rb'"\s*:\s*)'
        + re.escape(json_string_bytes(provider))
    )


@functools.lru_cache(maxsize=32)
def replacement_patterns(
    source_provider: str, target_provider: str
) -> list[tuple[re.Pattern[bytes], bytes]]:
    target = json_string_bytes(target_provider)
    return [(provider_pattern(key, source_provider), target) for key in PROVIDER_KEYS]


def replace_provider_bytes(
    value: bytes, source_provider: str, target_provider: str
) -> tuple[bytes, int]:
    updated = value
    replacements = 0
    for pattern, target in replacement_patterns(source_provider, target_provider):
        updated, count = pattern.subn(
            lambda match, target=target: match.group(1) + target, updated
        )
        replacements += count
    return updated, replacements


def session_roots(codex_home: Path) -> list[Path]:
    roots: list[Path] = []
    for name in SESSION_DIR_NAMES:
        root = codex_home / name
        if root.is_symlink():
            raise MigrationError(f"refusing symlinked session root: {root}")
        if root.is_dir():
            roots.append(root)
    return roots


def rollout_paths(codex_home: Path) -> list[Path]:
    paths: list[Path] = []
    for root in session_roots(codex_home):
        for path in root.rglob("*.jsonl"):
            if path.is_symlink():
                raise MigrationError(f"refusing symlinked rollout: {path}")
            if path.is_file():
                paths.append(path)
    return sorted(paths)


def relative_rollout_path(codex_home: Path, path: Path) -> str:
    return path.relative_to(codex_home).as_posix()


def count_raw_provider_matches(line: bytes, source_provider: str) -> int:
    if b"model_provider" not in line or json_string_bytes(source_provider) not in line:
        return 0
    return sum(
        len(provider_pattern(key, source_provider).findall(line))
        for key in PROVIDER_KEYS
    )


def walk_provider_values(
    value: Any,
    source_provider: str,
    path: tuple[str, ...] = (),
) -> list[tuple[str, ...]]:
    unknown: list[tuple[str, ...]] = []
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = (*path, str(key))
            if (
                key in PROVIDER_KEYS
                and child == source_provider
                and child_path not in KNOWN_PROVIDER_PATHS
            ):
                unknown.append(child_path)
            unknown.extend(walk_provider_values(child, source_provider, child_path))
    elif isinstance(value, list):
        for child in value:
            unknown.extend(walk_provider_values(child, source_provider, path))
    return unknown


def structural_provider_changes(record: Any, source_provider: str) -> tuple[int, int]:
    if not isinstance(record, dict):
        return 0, 0
    payload = record.get("payload")
    if not isinstance(payload, dict):
        return 0, 0

    session_meta = int(
        record.get("type") == "session_meta"
        and payload.get("model_provider") == source_provider
    )
    settings = payload.get("thread_settings")
    thread_settings = int(
        record.get("type") == "event_msg"
        and payload.get("type") == "thread_settings_applied"
        and isinstance(settings, dict)
        and settings.get("model_provider_id") == source_provider
    )
    return session_meta, thread_settings


def assert_supported_rollout(
    record: Any,
    path: Path,
    line_number: int,
    *,
    allow_paginated: bool,
) -> None:
    if not isinstance(record, dict) or record.get("type") != "session_meta":
        return
    payload = record.get("payload")
    if not isinstance(payload, dict):
        return
    history_mode = payload.get("history_mode")
    history_base = payload.get("history_base")
    if history_mode not in (None, "legacy", "paginated"):
        raise MigrationError(f"unsupported history_mode at {path}:{line_number}")
    if history_base is not None and history_mode != "paginated":
        raise MigrationError(
            f"refusing history_base on a non-paginated rollout at {path}:{line_number}"
        )
    if history_mode == "paginated" and not allow_paginated:
        raise MigrationError(
            "refusing paginated rollout without a supported thread-history "
            f"database at {path}:{line_number}"
        )


def scan_rollout_file(task):
    codex_home, path, source_provider, target_provider, allow_paginated = task
    hashes: dict[str, str] = {}
    metadata: dict[str, dict[str, int]] = {}
    files: dict[str, FilePlan] = {}
    identities: dict[str, RolloutIdentity] = {}
    references: list[tuple[HistoryBaseReference, ByteEdit]] = []
    heads = changed = meta_count = settings_count = malformed = valid = total = 0
    patterns = replacement_patterns(source_provider, target_provider)
    source_bytes = json_string_bytes(source_provider)
    target_bytes = json_string_bytes(target_provider)
    relative = relative_rollout_path(codex_home, path)
    before_stat = path.stat()
    metadata[relative] = capture_file_metadata(path)
    digest = hashlib.sha256()
    edits: list[ByteEdit] = []
    first_meta: int | None = None
    file_replacements = offset = pending_bytes = 0
    for number, line in enumerate(records(path), 1):
        digest.update(line)
        line_offset = offset
        offset += len(line)
        pending_bytes += len(line)
        if pending_bytes >= 1024 * 1024:
            workers.advance(pending_bytes)
            pending_bytes = 0
        total += 1
        if not line.strip():
            continue
        raw_matches = count_raw_provider_matches(line, source_provider)
        try:
            record = json.loads(line)
        except (json.JSONDecodeError, UnicodeDecodeError):
            malformed += 1
            if raw_matches or re.search(rb'"history_base"\s*:', line):
                raise MigrationError(
                    "refusing migration metadata inside an unparseable JSONL line at "
                    f"{path}:{number}"
                ) from None
            continue
        valid += 1
        assert_supported_rollout(record, path, number, allow_paginated=allow_paginated)
        unknown = walk_provider_values(record, source_provider)
        if unknown:
            raise MigrationError(
                f"refusing unknown provider metadata path at {path}:{number}: "
                + ", ".join(".".join(item) for item in unknown)
            )
        meta, settings = structural_provider_changes(record, source_provider)
        if raw_matches != meta + settings:
            raise MigrationError(
                "provider byte matches do not map one-to-one to known metadata at "
                f"{path}:{number} (raw={raw_matches}, structural={meta + settings})"
            )
        meta_count += meta
        settings_count += settings
        file_replacements += meta + settings
        if raw_matches and source_bytes != target_bytes:
            for pattern, _ in patterns:
                for match in pattern.finditer(line):
                    edits.append(
                        ByteEdit(
                            line_offset + match.end(1),
                            source_bytes,
                            target_bytes,
                        )
                    )
        if (
            isinstance(record, dict)
            and record.get("type") == "session_meta"
            and first_meta is None
        ):
            first_meta = number
            payload = record.get("payload")
            if (
                isinstance(payload, dict)
                and payload.get("model_provider") == source_provider
            ):
                heads += 1
            found = index_paginated_rollouts({relative: line})
            if identities.keys() & found.keys():
                raise MigrationError("ambiguous duplicate rollout ID")
            identities.update(found)
        reference = parse_history_base_reference(record, relative, number, first_meta)
        if reference is not None:
            # Require the same unique, canonical numeric token as the original verifier.
            replace_history_base_offset(
                line, reference, reference.original_end_byte_offset
            )
            match = re.search(rb'"end_byte_offset"\s*:\s*([0-9]+)', line)
            if match is None:
                raise MigrationError("history_base offset token is missing")
            references.append(
                (
                    reference,
                    ByteEdit(line_offset + match.start(1), match[1], match[1]),
                )
            )
    after_stat = path.stat()
    if (before_stat.st_size, before_stat.st_mtime_ns, before_stat.st_ino) != (
        after_stat.st_size,
        after_stat.st_mtime_ns,
        after_stat.st_ino,
    ) or offset != before_stat.st_size:
        raise MigrationError(
            f"rollout changed during preflight: {path}; stop Codex and retry"
        )
    hashes[relative] = digest.hexdigest()
    files[relative] = FilePlan(
        offset, hashes[relative], tuple(sorted(edits, key=lambda e: e.offset))
    )
    changed += bool(file_replacements)
    workers.advance(pending_bytes, files=1)
    analysis = RolloutAnalysis(
        1,
        heads,
        changed,
        meta_count,
        settings_count,
        malformed,
        valid,
        total,
        0,
        0,
        hashes,
        metadata,
    )
    return analysis, files, identities, references


def scan_rollouts(
    codex_home: Path,
    source_provider: str,
    target_provider: str,
    *,
    allow_paginated: bool,
) -> tuple[
    RolloutAnalysis,
    dict[str, FilePlan],
    dict[str, RolloutIdentity],
    list[tuple[HistoryBaseReference, ByteEdit]],
]:
    paths = rollout_paths(codex_home)
    hashes = {}
    metadata = {}
    files = {}
    identities = {}
    references = []
    counts = [0] * 10
    jobs = (
        (codex_home, path, source_provider, target_provider, allow_paginated)
        for path in paths
    )
    with (
        progress.phase(
            "Scan and validate rollouts",
            total_files=len(paths),
            total_bytes=sum(path.stat().st_size for path in paths),
        ) as status,
        workers.map_files(scan_rollout_file, jobs, status, processes=True) as results,
    ):
        for analysis, found_files, found_identities, found_references in results:
            if identities.keys() & found_identities.keys():
                raise MigrationError("ambiguous duplicate rollout ID")
            for i, field in enumerate(dataclasses.fields(RolloutAnalysis)[:10]):
                counts[i] += getattr(analysis, field.name)
            hashes.update(analysis.file_hashes)
            metadata.update(analysis.file_metadata)
            files.update(found_files)
            identities.update(found_identities)
            references.extend(found_references)
    # Completion order must not affect plans, manifests, or dependency resolution.
    analysis = RolloutAnalysis(
        *counts, dict(sorted(hashes.items())), dict(sorted(metadata.items()))
    )
    return (
        analysis,
        dict(sorted(files.items())),
        dict(sorted(identities.items())),
        sorted(
            references, key=lambda item: (item[0].relative_path, item[0].line_number)
        ),
    )


def analyze_rollouts(
    codex_home: Path, source_provider: str, *, allow_paginated: bool = False
) -> RolloutAnalysis:
    return scan_rollouts(
        codex_home, source_provider, source_provider, allow_paginated=allow_paginated
    )[0]


def ensure_supported_storage(codex_home: Path, sqlite_home: Path) -> None:
    compressed: list[Path] = []
    for root in session_roots(codex_home):
        for path in root.rglob("*"):
            if path.is_symlink():
                raise MigrationError(f"refusing symlink inside session tree: {path}")
        compressed.extend(root.rglob("*.zst"))
        compressed.extend(root.rglob("*.gz"))
    if compressed:
        raise MigrationError(f"refusing compressed rollout: {sorted(compressed)[0]}")
    history_db = sqlite_home / HISTORY_DB_NAME
    if history_db.is_symlink():
        raise MigrationError(
            f"refusing symlinked thread-history database: {history_db}"
        )
    if history_db.exists() and not history_db.is_file():
        raise MigrationError(
            f"thread-history database is not a regular file: {history_db}"
        )


def validate_state_db_path(path: Path) -> None:
    if path.is_symlink():
        raise MigrationError(f"refusing symlinked state database: {path}")
    if not path.is_file():
        raise MigrationError(f"state database not found: {path}")


def sqlite_read_only_uri(path: Path, *, immutable: bool = False) -> str:
    query = "?mode=ro"
    if immutable:
        query += "&immutable=1"
    return path.resolve().as_uri() + query


def sqlite_header_journal_mode(path: Path) -> str:
    with path.open("rb") as stream:
        header = stream.read(100)
    if len(header) != 100 or header[:16] != b"SQLite format 3\x00":
        raise MigrationError(f"invalid SQLite file header: {path}")
    read_version, write_version = header[18], header[19]
    if (read_version, write_version) == (2, 2):
        return "wal"
    if (read_version, write_version) == (1, 1):
        return "delete"
    raise MigrationError(f"unsupported SQLite file format versions: {path}")


def read_persistent_sqlite_settings(
    connection: sqlite3.Connection,
) -> dict[str, int | str]:
    settings: dict[str, int | str] = {}
    for pragma in PERSISTENT_SQLITE_PRAGMAS:
        rows = connection.execute(f"PRAGMA {pragma}").fetchall()
        if (
            len(rows) != 1
            or len(rows[0]) != 1
            or not isinstance(rows[0][0], (int, str))
        ):
            raise MigrationError(f"cannot read SQLite persistent setting: {pragma}")
        settings[pragma] = rows[0][0]
    return settings


def normalize_database_backup_settings(
    database_path: Path,
    expected: dict[str, int | str],
) -> None:
    if set(expected) != {*PERSISTENT_SQLITE_PRAGMAS, "journal_mode"}:
        raise MigrationError("invalid SQLite persistent-settings preflight")
    connection = sqlite3.connect(database_path)
    try:
        journal_mode = expected["journal_mode"]
        if journal_mode not in {"delete", "persist", "truncate", "wal"}:
            raise MigrationError(
                f"unsupported persistent SQLite journal mode: {journal_mode}"
            )
        actual_mode = connection.execute(
            f"PRAGMA journal_mode = {journal_mode}"
        ).fetchone()[0]
        if actual_mode != journal_mode:
            raise MigrationError("could not preserve SQLite journal_mode in backup")

        current = read_persistent_sqlite_settings(connection)
        for pragma in (
            "application_id",
            "default_cache_size",
            "schema_version",
            "user_version",
        ):
            value = expected[pragma]
            if type(value) is not int:
                raise MigrationError(
                    f"invalid integer SQLite persistent setting: {pragma}"
                )
            if current[pragma] != value:
                connection.execute(f"PRAGMA {pragma} = {value}")

        actual = read_persistent_sqlite_settings(connection)
        actual["journal_mode"] = connection.execute("PRAGMA journal_mode").fetchone()[0]
        if actual != expected:
            differing = sorted(
                name for name in expected if actual.get(name) != expected[name]
            )
            raise MigrationError(
                "SQLite backup changed persistent settings: " + ", ".join(differing)
            )
    finally:
        connection.close()


@progress.phase("Check state database")
def analyze_database(db_path: Path, source_provider: str) -> DatabaseAnalysis:
    validate_state_db_path(db_path)
    connection = sqlite3.connect(sqlite_read_only_uri(db_path), uri=True)
    try:
        connection.execute("BEGIN")
        integrity_rows = connection.execute("PRAGMA integrity_check").fetchall()
        if integrity_rows != [("ok",)]:
            raise MigrationError(f"SQLite integrity_check failed: {integrity_rows!r}")
        foreign_key_rows = connection.execute("PRAGMA foreign_key_check").fetchall()
        if foreign_key_rows:
            raise MigrationError(
                f"SQLite foreign_key_check failed: {foreign_key_rows!r}"
            )
        columns = {row[1] for row in connection.execute('PRAGMA table_info("threads")')}
        if "model_provider" not in columns:
            raise MigrationError("threads.model_provider is absent from state database")
        rows = connection.execute(
            "SELECT count(*) FROM threads WHERE model_provider = ?",
            (source_provider,),
        ).fetchone()[0]
        tables = connection.execute(
            "SELECT count(*) FROM sqlite_master WHERE type = 'table'"
        ).fetchone()[0]
        persistent_settings = read_persistent_sqlite_settings(connection)
        persistent_settings["journal_mode"] = sqlite_header_journal_mode(db_path)
    finally:
        connection.close()
    return DatabaseAnalysis(
        rows_from_provider=int(rows),
        integrity_check="ok",
        tables=int(tables),
        persistent_settings=persistent_settings,
    )


def jsonl_lines(value: bytes) -> list[bytes]:
    """Split only at LF, matching Codex, and retain every original byte."""
    parts = value.split(b"\n")
    return [part + b"\n" for part in parts[:-1]] + ([parts[-1]] if parts[-1] else [])


def rollout_offset_translation(original: bytes, migrated: bytes) -> dict[int, int]:
    before = jsonl_lines(original)
    after = jsonl_lines(migrated)
    if len(before) != len(after):
        raise MigrationError("rollout line boundaries changed unexpectedly")
    offsets = {0: 0}
    old_offset = new_offset = 0
    for old_line, new_line in zip(before, after, strict=True):
        if old_line.endswith(b"\n") != new_line.endswith(b"\n"):
            raise MigrationError("rollout newline changed unexpectedly")
        old_offset += len(old_line)
        new_offset += len(new_line)
        offsets[old_offset] = new_offset
    return offsets


# Durable envelopes and events in Codex 0.149--0.157. Unknown envelopes do
# not advance a recovered projection. A later recognized explicit ordinal
# can establish a new checkpoint without interpreting unknown payload data.
ROLLOUT_TYPES = {
    "session_meta",
    "response_item",
    "inter_agent_communication",
    "inter_agent_communication_metadata",
    "compacted",
    "turn_context",
    "token_usage_record",
    "world_state",
    "retained_context",
    "security_risk_score",
    "realtime_item",
    "event_msg",
}
# EventMsg tags, including historical task_* names, from upstream 18344a972d.
# Count recognized transient events too: older/imported files can contain them.
KNOWN_EVENT_TYPES = frozenset(
    """
agent_message agent_message_content_delta agent_reasoning agent_reasoning_raw_content
agent_reasoning_section_break apply_patch_approval_request auth_recovery_completed
auth_recovery_started collab_agent_interaction_begin collab_agent_interaction_end
collab_agent_spawn_begin collab_agent_spawn_end collab_close_begin collab_close_end
collab_resume_begin collab_resume_end collab_waiting_begin collab_waiting_end
context_compacted deprecation_notice dynamic_tool_call_request
dynamic_tool_call_response elicitation_request entered_review_mode
environment_connected environment_disconnected error exec_approval_request
exec_command_begin exec_command_end exec_command_output_delta exited_review_mode
guardian_assessment guardian_warning hook_completed hook_started image_generation_begin
image_generation_end item_completed item_started mcp_startup_complete
mcp_startup_update mcp_tool_call_begin mcp_tool_call_end model_reroute
model_verification patch_apply_begin patch_apply_end patch_apply_updated plan_delta
plan_update raw_response_completed raw_response_item realtime_conversation_closed
realtime_conversation_list_voices_response realtime_conversation_realtime
realtime_conversation_sdp realtime_conversation_started reasoning_content_delta
reasoning_raw_content_delta request_permissions request_user_input safety_buffering
session_configured shutdown_complete stream_error sub_agent_activity task_complete
task_started terminal_interaction thread_goal_updated thread_queue_changed
thread_rolled_back thread_settings_applied token_count turn_aborted turn_complete
turn_diff turn_moderation_metadata turn_started user_message view_image_tool_call
warning web_search_begin web_search_end
""".split()
)


def rollout_positions(
    value: bytes | Iterable[bytes], ordinal_base: int, requested: set[int] | None = None
) -> RolloutPositions:
    """Validate positions using explicit ordinals, independently of byte shifts.

    Follow the projection's recovery rules for skipped lines, reused ordinals,
    and forward gaps. Payloads are not fully decoded as Rust types: an offset
    whose recorded ordinal cannot be established from these envelopes fails
    closed. An unterminated last record is never a projected position.
    """
    checkpoints = {0: ordinal_base}
    starts: dict[int, int] = {}
    ends: dict[int, int] = {}
    next_ordinal = ordinal_base
    offset = 0
    first_meta = True
    for line in jsonl_lines(value) if isinstance(value, bytes) else value:
        if not line.endswith(b"\n"):
            break
        end = offset + len(line)
        try:
            record = json.loads(line)
        except (json.JSONDecodeError, UnicodeDecodeError):
            record = None
        if isinstance(record, dict):
            ordinal = record.get("ordinal")
            kind = record.get("type")
            payload = record.get("payload")
            if kind == "session_meta" and first_meta:
                first_meta = False
                if type(ordinal) is not int or ordinal != ordinal_base:
                    raise MigrationError(
                        "paginated session_meta ordinal does not match history base"
                    )
            recognized = isinstance(kind, str) and kind in ROLLOUT_TYPES
            recognized = recognized and isinstance(payload, dict)
            if recognized and kind == "event_msg":
                event_type = payload.get("type")
                recognized = (
                    isinstance(event_type, str) and event_type in KNOWN_EVENT_TYPES
                )
            if (
                recognized
                and type(ordinal) is int
                and next_ordinal <= ordinal < 2**63 - 1
            ):
                if requested is None or offset in requested:
                    starts[offset] = ordinal
                if requested is None or end in requested:
                    ends[end] = ordinal
                next_ordinal = ordinal + 1
        if requested is None or end in requested:
            checkpoints[end] = next_ordinal
        offset = end
    return RolloutPositions(checkpoints, starts, ends)


def index_paginated_rollouts(
    original_by_relative: dict[str, bytes],
) -> dict[str, RolloutIdentity]:
    uuid = r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}"
    canonical = re.compile(
        rf"rollout-\d{{4}}-\d{{2}}-\d{{2}}T\d{{2}}-\d{{2}}-\d{{2}}-"
        rf"(?P<thread>{uuid})(?:_(?P<rollout>{uuid}))?\.jsonl"
    )
    result: dict[str, RolloutIdentity] = {}
    for relative, data in original_by_relative.items():
        for line in jsonl_lines(data):
            try:
                record = json.loads(line)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            if isinstance(record, dict) and record.get("type") == "session_meta":
                break
        else:
            continue
        payload = record.get("payload")
        if not isinstance(payload, dict) or payload.get("history_mode") != "paginated":
            continue
        thread_id = payload.get("id")
        if not isinstance(thread_id, str) or not thread_id or "\x00" in thread_id:
            raise MigrationError(f"invalid paginated thread identity: {relative}")
        name = Path(relative).name
        match = canonical.fullmatch(name)
        if match:
            if match["thread"] != thread_id:
                raise MigrationError(
                    f"rollout filename disagrees with session_meta.id: {relative}"
                )
            rollout_id = match["rollout"] or match["thread"]
        else:
            # Retain support for older noncanonical single-rollout names. They
            # cannot encode replacements, and duplicate IDs are always refused.
            if re.match(r"rollout-\d{4}-", name) or "_" in name:
                raise MigrationError(f"unrecognized rollout filename: {relative}")
            rollout_id = thread_id
        if rollout_id in result:
            raise MigrationError(f"ambiguous duplicate rollout ID: {rollout_id}")
        base = payload.get("history_base")
        ordinal_base = (
            base.get("end_ordinal_exclusive") if isinstance(base, dict) else 0
        )
        if type(ordinal_base) is not int or ordinal_base < 0:
            raise MigrationError(f"invalid rollout ordinal base: {relative}")
        result[rollout_id] = RolloutIdentity(
            relative, thread_id, rollout_id, ordinal_base
        )
    return result


def resolve_recorded_rollout_path(codex_home: Path, recorded: Any) -> Path:
    if not isinstance(recorded, str) or not recorded or "\x00" in recorded:
        raise MigrationError("invalid threads.rollout_path in state database")
    # A state database created on the host can be inspected from a Linux
    # container, so recognize either path separator before retaining only the
    # session-root-relative suffix.
    recorded_path = Path(recorded.replace("\\", "/"))
    marker_indices = [
        index
        for index, part in enumerate(recorded_path.parts)
        if part in SESSION_DIR_NAMES
    ]
    if marker_indices:
        relative = Path(*recorded_path.parts[marker_indices[-1] :])
        candidate = codex_home / relative
    elif recorded_path.is_absolute():
        try:
            relative = recorded_path.resolve().relative_to(codex_home.resolve())
        except ValueError as exc:
            raise MigrationError(
                f"threads.rollout_path is outside recognizable session roots: {recorded}"
            ) from exc
        candidate = codex_home / relative
    else:
        candidate = codex_home / recorded_path

    resolved_home = codex_home.resolve()
    resolved = candidate.resolve()
    try:
        resolved.relative_to(resolved_home)
    except ValueError as exc:
        raise MigrationError(
            f"threads.rollout_path escapes Codex state: {recorded}"
        ) from exc
    current = codex_home
    relative_candidate = candidate.relative_to(codex_home)
    for part in relative_candidate.parts:
        current /= part
        if current.is_symlink():
            raise MigrationError(
                f"recorded paginated rollout traverses a symlink: {candidate}"
            )
    if not candidate.is_file():
        raise MigrationError(
            f"recorded paginated rollout is missing or unsafe: {candidate}"
        )
    return candidate


def history_base_paths(
    value: Any,
    path: tuple[str, ...] = (),
) -> list[tuple[str, ...]]:
    paths: list[tuple[str, ...]] = []
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = (*path, str(key))
            if key == "history_base" and child is not None:
                paths.append(child_path)
            paths.extend(history_base_paths(child, child_path))
    elif isinstance(value, list):
        for child in value:
            paths.extend(history_base_paths(child, path))
    return paths


def parse_history_base_reference(
    record: Any,
    relative_path: str,
    line_number: int,
    first_session_meta_line: int | None,
) -> HistoryBaseReference | None:
    paths = history_base_paths(record)
    if not paths:
        return None
    if paths != [("payload", "history_base")]:
        rendered = ", ".join(".".join(path) for path in paths)
        raise MigrationError(
            "refusing unknown history_base metadata path at "
            f"{relative_path}:{line_number}: {rendered}"
        )
    if not isinstance(record, dict) or record.get("type") != "session_meta":
        raise MigrationError(
            f"history_base is not on session_meta at {relative_path}:{line_number}"
        )
    payload = record.get("payload")
    if not isinstance(payload, dict) or payload.get("history_mode") != "paginated":
        raise MigrationError(
            f"history_base is not paginated at {relative_path}:{line_number}"
        )
    if first_session_meta_line != line_number:
        raise MigrationError(
            "history_base must be on the rollout's first session_meta at "
            f"{relative_path}:{line_number}"
        )
    history_base = payload.get("history_base")
    expected_keys = {
        "thread_id",
        "end_ordinal_exclusive",
        "end_byte_offset",
    }
    if not isinstance(history_base, dict) or set(history_base) != expected_keys:
        raise MigrationError(
            f"unsupported history_base schema at {relative_path}:{line_number}"
        )
    owner_thread_id = payload.get("id")
    source_thread_id = history_base.get("thread_id")
    end_ordinal = history_base.get("end_ordinal_exclusive")
    end_offset = history_base.get("end_byte_offset")
    if (
        not isinstance(owner_thread_id, str)
        or not owner_thread_id
        or "\x00" in owner_thread_id
        or not isinstance(source_thread_id, str)
        or not source_thread_id
        or "\x00" in source_thread_id
        or type(end_ordinal) is not int
        or end_ordinal <= 0
        or type(end_offset) is not int
        or end_offset <= 0
    ):
        raise MigrationError(
            f"invalid history_base values at {relative_path}:{line_number}"
        )
    return HistoryBaseReference(
        relative_path=relative_path,
        line_number=line_number,
        owner_thread_id=owner_thread_id,
        source_rollout_id=source_thread_id,
        end_ordinal_exclusive=end_ordinal,
        original_end_byte_offset=end_offset,
    )


def replace_history_base_offset(
    line: bytes,
    reference: HistoryBaseReference,
    migrated_offset: int,
) -> bytes:
    key_pattern = re.compile(rb'"end_byte_offset"\s*:')
    if len(key_pattern.findall(line)) != 1:
        raise MigrationError(
            "could not uniquely locate history_base.end_byte_offset at "
            f"{reference.relative_path}:{reference.line_number}"
        )
    pattern = re.compile(
        rb'("end_byte_offset"\s*:\s*)'
        + str(reference.original_end_byte_offset).encode("ascii")
        + rb"(?=\s*[,}])"
    )
    updated, count = pattern.subn(
        lambda match: match.group(1) + str(migrated_offset).encode("ascii"),
        line,
    )
    if count != 1:
        raise MigrationError(
            "history_base.end_byte_offset bytes do not match parsed metadata at "
            f"{reference.relative_path}:{reference.line_number}"
        )
    return updated


def history_position_requests(
    history_path: Path, *, immutable: bool
) -> dict[str, set[int]]:
    """Collect only referenced boundaries; never index every JSONL record in memory."""
    requests: dict[str, set[int]] = collections.defaultdict(set)
    if not history_path.is_file():
        return requests
    with sqlite3.connect(
        sqlite_read_only_uri(history_path, immutable=immutable), uri=True
    ) as db:
        for table, columns in [
            ("thread_history_projection_state", ("next_rollout_byte_offset",)),
            ("thread_turns", ("rollout_byte_offset", "rollout_end_byte_offset")),
        ]:
            available = {row[1] for row in db.execute(f"PRAGMA table_info({table})")}
            if not {"thread_id", *columns}.issubset(available):
                # The full schema check below supplies the detailed refusal.
                continue
            for row in db.execute(
                f"SELECT thread_id, {', '.join(columns)} FROM {table}"
            ):
                for offset in row[1:]:
                    if type(offset) is int:
                        requests[row[0]].add(offset)
    return requests


def validate_rollout_boundaries(task):
    path, rollout_id, ordinal_base, requests, expected_digest = task
    digest = hashlib.sha256()

    def measured_records():
        pending = 0
        for line in records(path):
            digest.update(line)
            pending += len(line)
            if pending >= 1024 * 1024:
                workers.advance(pending)
                pending = 0
            yield line
        workers.advance(pending)

    measured = measured_records()
    try:
        positions = rollout_positions(measured, ordinal_base, requests)
        # Include any unterminated tail in the complete source digest.
        for _ in measured:
            pass
    finally:
        measured.close()
    if digest.hexdigest() != expected_digest:
        raise MigrationError(f"rollout changed during boundary validation: {path}")
    workers.advance(files=1)
    return rollout_id, positions


def build_rollout_migration_plan(
    codex_home: Path,
    state_db_path: Path,
    source_provider: str,
    target_provider: str,
    *,
    immutable: bool = False,
    allow_paginated: bool | None = None,
) -> RolloutMigrationPlan:
    history_path = state_db_path.with_name(HISTORY_DB_NAME)
    if allow_paginated is None:
        allow_paginated = history_path.is_file()
    analysis, files, identities, references = scan_rollouts(
        codex_home,
        source_provider,
        target_provider,
        allow_paginated=allow_paginated,
    )
    requests = history_position_requests(history_path, immutable=immutable)
    dependencies: dict[str, set[str]] = {relative: set() for relative in files}
    by_relative = {identity.relative_path: identity for identity in identities.values()}
    references_by_relative: dict[str, list[tuple[HistoryBaseReference, ByteEdit]]] = (
        collections.defaultdict(list)
    )
    for reference, edit in references:
        owner = by_relative.get(reference.relative_path)
        if owner is None or owner.thread_id != reference.owner_thread_id:
            raise MigrationError("history_base owner has no matching paginated rollout")
        source = identities.get(reference.source_rollout_id)
        if source is None:
            raise MigrationError("history_base references an unknown source rollout")
        dependencies[reference.relative_path].add(source.relative_path)
        requests[source.rollout_id].add(reference.original_end_byte_offset)
        references_by_relative[reference.relative_path].append((reference, edit))
    try:
        order = list(graphlib.TopologicalSorter(dependencies).static_order())
    except graphlib.CycleError as exc:
        raise MigrationError("cycle detected in history_base references") from exc

    positions: dict[str, RolloutPositions] = {}
    with progress.phase(
        "Validate paginated boundaries",
        total_files=len(identities),
        total_bytes=sum(files[i.relative_path].size for i in identities.values()),
    ) as status:
        jobs = (
            (
                codex_home / identity.relative_path,
                rollout_id,
                identity.ordinal_base,
                requests[rollout_id],
                files[identity.relative_path].digest,
            )
            for rollout_id, identity in identities.items()
        )
        with workers.map_files(
            validate_rollout_boundaries, jobs, status, processes=True
        ) as results:
            positions.update(results)
    positions = dict(sorted(positions.items()))

    base_changes = 0
    for relative in order:
        extra = []
        for reference, edit in references_by_relative[relative]:
            source = identities[reference.source_rollout_id]
            if (
                reference.end_ordinal_exclusive <= source.ordinal_base
                or positions[source.rollout_id].checkpoints.get(
                    reference.original_end_byte_offset
                )
                != reference.end_ordinal_exclusive
            ):
                raise MigrationError(
                    "history_base offset is not on its recorded source boundary at "
                    f"{relative}:{reference.line_number}"
                )
            migrated = files[source.relative_path].translate(
                reference.original_end_byte_offset
            )
            if migrated != reference.original_end_byte_offset:
                extra.append(
                    dataclasses.replace(edit, after=str(migrated).encode("ascii"))
                )
                base_changes += 1
        if extra:
            file = files[relative]
            files[relative] = dataclasses.replace(
                file, edits=tuple(sorted((*file.edits, *extra), key=lambda e: e.offset))
            )
    analysis = dataclasses.replace(
        analysis,
        files_requiring_changes=sum(bool(f.edits) for f in files.values()),
        history_base_values=len(references),
        history_base_offsets_changed=base_changes,
    )
    return RolloutMigrationPlan(
        files, positions, analysis, len(references), base_changes, identities
    )


def analyze_migratable_rollouts(
    codex_home: Path,
    state_db_path: Path,
    source_provider: str,
    target_provider: str,
    *,
    allow_paginated: bool,
    immutable: bool = False,
) -> tuple[RolloutAnalysis, RolloutMigrationPlan]:
    plan = build_rollout_migration_plan(
        codex_home,
        state_db_path,
        source_provider,
        target_provider,
        immutable=immutable,
        allow_paginated=allow_paginated,
    )
    return plan.analysis, plan


def quoted_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


@progress.phase("Check history database")
def inspect_history_database(
    history_db_path: Path,
    state_db_path: Path,
    codex_home: Path,
    source_provider: str,
    target_provider: str,
    *,
    immutable: bool = False,
    rollout_migration: RolloutMigrationPlan | None = None,
) -> tuple[HistoryDatabaseAnalysis, list[HistoryOffsetUpdate]]:
    if not history_db_path.exists():
        return (
            HistoryDatabaseAnalysis(
                present=False,
                integrity_check="not present",
                tables=0,
                rows=0,
                paginated_threads=0,
                offset_fields_to_update=0,
                persistent_settings={},
            ),
            [],
        )
    if history_db_path.is_symlink() or not history_db_path.is_file():
        raise MigrationError(
            f"thread-history database is missing or unsafe: {history_db_path}"
        )
    validate_state_db_path(state_db_path)
    if rollout_migration is None:
        rollout_migration = build_rollout_migration_plan(
            codex_home,
            state_db_path,
            source_provider,
            target_provider,
            immutable=immutable,
        )

    history = sqlite3.connect(
        sqlite_read_only_uri(history_db_path, immutable=immutable), uri=True
    )
    state = sqlite3.connect(
        sqlite_read_only_uri(state_db_path, immutable=immutable), uri=True
    )
    try:
        history.execute("BEGIN")
        state.execute("BEGIN")
        if history.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
            raise MigrationError("thread-history SQLite integrity_check failed")
        if history.execute("PRAGMA foreign_key_check").fetchall():
            raise MigrationError("thread-history SQLite foreign_key_check failed")

        table_names = [
            row[0]
            for row in history.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
            )
        ]
        required_tables = {
            "thread_history_projection_state",
            "thread_items",
            "thread_turns",
        }
        missing_tables = sorted(required_tables - set(table_names))
        if missing_tables:
            raise MigrationError(
                "unsupported thread-history schema; missing tables: "
                + ", ".join(missing_tables)
            )

        columns_by_table: dict[str, list[str]] = {}
        for table in table_names:
            columns_by_table[table] = [
                row[1]
                for row in history.execute(
                    f"PRAGMA table_info({quoted_identifier(table)})"
                )
            ]
        required_columns = {
            "thread_history_projection_state": {
                "thread_id",
                "next_rollout_byte_offset",
                "next_rollout_ordinal",
            },
            "thread_items": {"thread_id"},
            "thread_turns": {
                "thread_id",
                "turn_id",
                "rollout_ordinal",
                "rollout_byte_offset",
                "rollout_end_ordinal",
                "rollout_end_byte_offset",
            },
        }
        for table, expected in required_columns.items():
            missing = sorted(expected - set(columns_by_table[table]))
            if missing:
                raise MigrationError(
                    f"unsupported thread-history schema; {table} is missing: "
                    + ", ".join(missing)
                )

        expected_offset_columns = {
            ("thread_history_projection_state", "next_rollout_byte_offset"),
            ("thread_turns", "rollout_byte_offset"),
            ("thread_turns", "rollout_end_byte_offset"),
        }
        actual_offset_columns = {
            (table, column)
            for table, columns in columns_by_table.items()
            for column in columns
            if "byte_offset" in column.lower()
        }
        unexpected_offsets = sorted(actual_offset_columns - expected_offset_columns)
        if unexpected_offsets:
            rendered = ", ".join(
                f"{table}.{column}" for table, column in unexpected_offsets
            )
            raise MigrationError(
                "unsupported thread-history byte-offset columns: " + rendered
            )

        state_columns = {
            row[1] for row in state.execute('PRAGMA table_info("threads")')
        }
        expected_state_columns = {"id", "rollout_path", "history_mode"}
        if not expected_state_columns.issubset(state_columns):
            raise MigrationError("state database lacks paginated thread path metadata")
        state_threads = {
            row[0]: (row[1], row[2])
            for row in state.execute(
                "SELECT id, rollout_path, history_mode FROM threads"
            )
        }
        paginated_threads = sum(
            history_mode == "paginated" for _, history_mode in state_threads.values()
        )

        # State rows select the current physical file of a logical thread.
        # Projection rows and history_base pointers instead identify immutable
        # physical rollouts, including retained files absent from threads.id.
        identities = rollout_migration.rollouts_by_id
        by_relative = {item.relative_path: item for item in identities.values()}
        for thread_id, (recorded_path, history_mode) in state_threads.items():
            if history_mode != "paginated":
                continue
            selected = resolve_recorded_rollout_path(codex_home, recorded_path)
            identity = by_relative.get(relative_rollout_path(codex_home, selected))
            if identity is None or identity.thread_id != thread_id:
                raise MigrationError(
                    "paginated rollout metadata does not match its state thread"
                )

        referenced_rollouts: set[str] = set()
        for table, columns in columns_by_table.items():
            if "thread_id" in columns:
                referenced_rollouts.update(
                    row[0]
                    for row in history.execute(
                        f"SELECT DISTINCT thread_id FROM {quoted_identifier(table)}"
                    )
                )
        for rollout_id in referenced_rollouts:
            if not isinstance(rollout_id, str) or rollout_id not in identities:
                raise MigrationError(
                    "thread-history database references an unknown paginated rollout"
                )

        def translate(
            rollout_id: str,
            offset: Any,
            ordinal: Any,
            *,
            position: str,
            label: str,
        ) -> int:
            if type(offset) is not int or type(ordinal) is not int:
                raise MigrationError(f"invalid thread-history offset pair: {label}")
            identity = identities.get(rollout_id)
            if identity is None:
                raise MigrationError(
                    f"thread-history offset has no paginated rollout: {label}"
                )
            positions = rollout_migration.positions[rollout_id]
            boundaries = getattr(positions, position)
            if boundaries.get(offset) != ordinal or offset not in boundaries:
                raise MigrationError(
                    f"thread-history offset is not on its recorded JSONL boundary: {label}"
                )
            return rollout_migration.files[identity.relative_path].translate(offset)

        updates: list[HistoryOffsetUpdate] = []
        for thread_id, offset, ordinal in history.execute(
            "SELECT thread_id, next_rollout_byte_offset, next_rollout_ordinal "
            "FROM thread_history_projection_state"
        ):
            migrated = translate(
                thread_id,
                offset,
                ordinal,
                position="checkpoints",
                label="thread_history_projection_state.next_rollout_byte_offset",
            )
            if migrated != offset:
                updates.append(
                    HistoryOffsetUpdate(
                        table="thread_history_projection_state",
                        key_columns=("thread_id",),
                        key_values=(thread_id,),
                        offset_column="next_rollout_byte_offset",
                        original_offset=offset,
                        migrated_offset=migrated,
                    )
                )

        turn_rows = history.execute(
            "SELECT thread_id, turn_id, rollout_ordinal, rollout_byte_offset, "
            "rollout_end_ordinal, rollout_end_byte_offset FROM thread_turns"
        )
        for (
            thread_id,
            turn_id,
            start_ordinal,
            start_offset,
            end_ordinal,
            end_offset,
        ) in turn_rows:
            if not isinstance(thread_id, str) or not isinstance(turn_id, str):
                raise MigrationError("invalid thread-history turn identity")
            if start_offset is not None:
                migrated = translate(
                    thread_id,
                    start_offset,
                    start_ordinal,
                    position="starts",
                    label="thread_turns.rollout_byte_offset",
                )
                if migrated != start_offset:
                    updates.append(
                        HistoryOffsetUpdate(
                            table="thread_turns",
                            key_columns=("thread_id", "turn_id"),
                            key_values=(thread_id, turn_id),
                            offset_column="rollout_byte_offset",
                            original_offset=start_offset,
                            migrated_offset=migrated,
                        )
                    )
            if (end_ordinal is None) != (end_offset is None):
                raise MigrationError(
                    "thread-history turn has an incomplete end offset pair"
                )
            if end_offset is not None:
                migrated = translate(
                    thread_id,
                    end_offset,
                    end_ordinal,
                    position="ends",
                    label="thread_turns.rollout_end_byte_offset",
                )
                if migrated != end_offset:
                    updates.append(
                        HistoryOffsetUpdate(
                            table="thread_turns",
                            key_columns=("thread_id", "turn_id"),
                            key_values=(thread_id, turn_id),
                            offset_column="rollout_end_byte_offset",
                            original_offset=end_offset,
                            migrated_offset=migrated,
                        )
                    )

        rows = 0
        for table in table_names:
            rows += int(
                history.execute(
                    f"SELECT count(*) FROM {quoted_identifier(table)}"
                ).fetchone()[0]
            )
        persistent_settings = read_persistent_sqlite_settings(history)
        persistent_settings["journal_mode"] = sqlite_header_journal_mode(
            history_db_path
        )
        updates.sort(
            key=lambda update: (
                update.table,
                update.key_values,
                update.offset_column,
                update.original_offset,
            )
        )
        analysis = HistoryDatabaseAnalysis(
            present=True,
            integrity_check="ok",
            tables=len(table_names),
            rows=rows,
            paginated_threads=paginated_threads,
            offset_fields_to_update=len(updates),
            persistent_settings=persistent_settings,
        )
        return analysis, updates
    finally:
        state.close()
        history.close()


def analyze_history_database(
    history_db_path: Path,
    state_db_path: Path,
    codex_home: Path,
    source_provider: str,
    target_provider: str,
) -> HistoryDatabaseAnalysis:
    analysis, _ = inspect_history_database(
        history_db_path,
        state_db_path,
        codex_home,
        source_provider,
        target_provider,
    )
    return analysis


def require_writable_database(db_path: Path, *, label: str = "state database") -> None:
    validate_state_db_path(db_path)
    connection = sqlite3.connect(db_path.resolve().as_uri() + "?mode=rw", uri=True)
    try:
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute("BEGIN IMMEDIATE")
        user_version = connection.execute("PRAGMA user_version").fetchone()[0]
        connection.execute(f"PRAGMA user_version = {int(user_version)}")
        connection.rollback()
    except sqlite3.Error as exc:
        raise MigrationError(f"{label} is not writable: {db_path}") from exc
    finally:
        connection.close()


@progress.phase("Update history offsets")
def apply_history_offset_updates(
    history_db_path: Path,
    updates: list[HistoryOffsetUpdate],
) -> int:
    if not updates:
        return 0
    allowed_shapes = {
        (
            "thread_history_projection_state",
            ("thread_id",),
            "next_rollout_byte_offset",
        ),
        ("thread_turns", ("thread_id", "turn_id"), "rollout_byte_offset"),
        ("thread_turns", ("thread_id", "turn_id"), "rollout_end_byte_offset"),
    }
    connection = sqlite3.connect(history_db_path)
    try:
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute("BEGIN IMMEDIATE")
        changed = 0
        for update in updates:
            shape = (update.table, update.key_columns, update.offset_column)
            if shape not in allowed_shapes:
                connection.rollback()
                raise MigrationError("invalid thread-history offset update plan")
            where = " AND ".join(
                f"{quoted_identifier(column)} = ?" for column in update.key_columns
            )
            sql = (
                f"UPDATE {quoted_identifier(update.table)} "
                f"SET {quoted_identifier(update.offset_column)} = ? "
                f"WHERE {where} "
                f"AND {quoted_identifier(update.offset_column)} = ?"
            )
            result = connection.execute(
                sql,
                (
                    update.migrated_offset,
                    *update.key_values,
                    update.original_offset,
                ),
            )
            if result.rowcount != 1:
                connection.rollback()
                raise MigrationError(
                    "thread-history offset changed after migration preflight"
                )
            changed += 1
        connection.commit()
        return changed
    finally:
        connection.close()


def read_config_file(path: Path, *, required: bool = False) -> dict[str, Any]:
    if path.is_symlink():
        raise MigrationError(f"refusing symlinked config: {path}")
    if not path.exists() and not required:
        return {}
    try:
        return tomllib.loads(path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise MigrationError(f"cannot parse config {path}: {exc}") from exc


def read_profile_configs(codex_home: Path) -> dict[Path, dict[str, Any]]:
    return {
        path: read_config_file(path, required=True)
        for path in sorted(codex_home.glob("*.config.toml"))
    }


def validate_config_profiles(codex_home: Path, before: bytes, after: bytes) -> None:
    """Check dependencies without editing or taking ownership of profile files."""
    original = tomllib.loads(before.decode("utf-8"))
    migrated = tomllib.loads(after.decode("utf-8"))
    if "profile" in original or "profiles" in original:
        raise MigrationError(
            "legacy profile selectors/tables are unsupported; move them to "
            "<name>.config.toml before --migrate-config"
        )
    source = original["model_provider"]
    for path, profile in read_profile_configs(codex_home).items():
        providers = profile.get("model_providers", {})
        if not isinstance(providers, dict):
            raise MigrationError(f"invalid profile provider definitions: {path}")
        if profile.get("model_provider") == source or source in providers:
            raise MigrationError(
                f"profile {path.name} depends on provider {source!r}; "
                "update that profile before --migrate-config, or omit --migrate-config"
            )
        provider_before = profile.get("model_provider", source)
        provider_after = profile.get("model_provider", "openai")
        if provider_after == "openai":
            old_url = (
                original["model_providers"][source]["base_url"]
                if provider_before == source
                else profile.get("openai_base_url", original.get("openai_base_url"))
            )
            new_url = profile.get("openai_base_url", migrated.get("openai_base_url"))
            if old_url != new_url:
                raise MigrationError(
                    f"config conversion would change the endpoint inherited by profile "
                    f"{path.name}; set its intended openai_base_url before --migrate-config"
                )


def transform_config(raw: bytes, source_provider: str, target_provider: str) -> bytes:
    if target_provider != "openai":
        raise MigrationError(
            "automatic config migration only supports target provider 'openai'"
        )
    try:
        text = raw.decode("utf-8")
        parsed = tomllib.loads(text)
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise MigrationError(f"cannot parse config.toml: {exc}") from exc

    selected_provider = parsed.get("model_provider", "openai")
    if selected_provider == target_provider:
        return raw
    if selected_provider != source_provider:
        raise MigrationError(
            f"config.toml does not select model_provider={source_provider!r}; "
            f"it selects {selected_provider!r}. Omit --migrate-config to migrate only sessions"
        )
    providers = parsed.get("model_providers")
    if not isinstance(providers, dict) or not isinstance(
        providers.get(source_provider), dict
    ):
        raise MigrationError(
            f"config.toml has no [model_providers.{source_provider}] table"
        )
    provider = providers[source_provider]
    allowed_keys = {"base_url", "name", "requires_openai_auth", "wire_api"}
    unsupported_keys = sorted(set(provider) - allowed_keys)
    if unsupported_keys:
        raise MigrationError(
            "automatic config migration cannot preserve provider keys: "
            + ", ".join(unsupported_keys)
        )
    base_url = provider.get("base_url")
    if not isinstance(base_url, str) or not base_url:
        raise MigrationError("custom provider base_url is missing or empty")
    if provider.get("requires_openai_auth") is not True:
        raise MigrationError(
            "custom provider requires_openai_auth must be true to match built-in openai"
        )
    if provider.get("wire_api") not in (None, "responses"):
        raise MigrationError("custom provider wire_api must be 'responses'")
    existing_base_url = parsed.get("openai_base_url")
    if existing_base_url is not None and existing_base_url != base_url:
        raise MigrationError(
            "existing openai_base_url conflicts with provider base_url"
        )
    if not re.fullmatch(r"[A-Za-z0-9_-]+", source_provider):
        raise MigrationError(
            "automatic config migration requires a bare TOML provider id"
        )

    lines = text.splitlines(keepends=True)
    first_table = next(
        (index for index, line in enumerate(lines) if line.lstrip().startswith("[")),
        len(lines),
    )
    selector_re = re.compile(r"^\s*model_provider\s*=.*(?:\r?\n)?$")
    selector_indices = [
        index
        for index, line in enumerate(lines[:first_table])
        if selector_re.match(line)
    ]
    if len(selector_indices) != 1:
        raise MigrationError("could not uniquely locate root model_provider assignment")

    table_re = re.compile(
        rf"^\s*\[model_providers\.{re.escape(source_provider)}\]\s*(?:\r?\n)?$"
    )
    table_indices = [index for index, line in enumerate(lines) if table_re.match(line)]
    if len(table_indices) != 1:
        raise MigrationError("could not uniquely locate custom provider table")
    table_start = table_indices[0]
    table_end = next(
        (
            index
            for index in range(table_start + 1, len(lines))
            if lines[index].lstrip().startswith("[")
        ),
        len(lines),
    )

    remove_indices = set(range(table_start, table_end))
    remove_indices.add(selector_indices[0])
    retained = [line for index, line in enumerate(lines) if index not in remove_indices]
    if existing_base_url is None:
        newline = "\r\n" if "\r\n" in text else "\n"
        retained.insert(
            0,
            f"openai_base_url = {json.dumps(base_url, ensure_ascii=False)}{newline}",
        )
    updated = "".join(retained).encode("utf-8")

    expected = copy.deepcopy(parsed)
    expected["openai_base_url"] = base_url
    del expected["model_provider"]
    del expected["model_providers"][source_provider]
    if not expected["model_providers"]:
        del expected["model_providers"]
    try:
        reparsed = tomllib.loads(updated.decode("utf-8"))
    except tomllib.TOMLDecodeError as exc:
        raise MigrationError(f"generated config.toml is invalid: {exc}") from exc
    if reparsed != expected:
        raise MigrationError(
            "generated config.toml changes values outside migration scope"
        )
    return updated


def plan_config_migration(
    codex_home: Path, original: bytes, source_provider: str, target_provider: str
) -> bytes | None:
    transformed = transform_config(original, source_provider, target_provider)
    if transformed == original:
        progress.warning(
            f"config.toml already uses model_provider={target_provider!r} "
            "(explicitly or by default); skipping config migration"
        )
        return None
    validate_config_profiles(codex_home, original, transformed)
    return transformed


def set_file_owner(path: Path, uid: int, gid: int) -> None:
    chown = getattr(os, "chown", None)
    if chown is None:
        return
    try:
        chown(path, uid, gid)
    except PermissionError:
        if (path.stat().st_uid, path.stat().st_gid) != (uid, gid):
            raise


def atomic_write(
    path: Path,
    value: bytes | Iterable[bytes],
    *,
    preserve_mtime: bool,
) -> None:
    metadata = path.stat()
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.provider-migration-", dir=path.parent
    )
    temporary = Path(temporary_name)
    chunks = iter((value,) if isinstance(value, bytes) else value)
    try:
        with os.fdopen(fd, "wb") as stream:
            for chunk in chunks:
                stream.write(chunk)
            stream.flush()
            os.fsync(stream.fileno())
        set_file_owner(temporary, metadata.st_uid, metadata.st_gid)
        os.chmod(temporary, stat.S_IMODE(metadata.st_mode))
        if preserve_mtime:
            os.utime(
                temporary,
                ns=(metadata.st_atime_ns, metadata.st_mtime_ns),
            )
        os.replace(temporary, path)
    finally:
        close = getattr(chunks, "close", None)
        if close is not None:
            close()
        temporary.unlink(missing_ok=True)


def command_looks_like_codex(command_args: list[str]) -> bool:
    if not command_args:
        return False

    normalized = [argument.replace("\\", "/").lower() for argument in command_args]
    executable_name = normalized[0].rsplit("/", 1)[-1]
    if executable_name in {"codex", "codex.exe", "codex.js"}:
        return True

    # Only JavaScript entrypoints and package paths are meaningful after argv[0].
    # A generic argument named `codex` may instead be a bind-mount destination or
    # the value of --codex-home in a Docker invocation.
    return any(
        argument.rsplit("/", 1)[-1] == "codex.js" or "/@openai/codex/" in argument
        for argument in normalized[1:]
    )


def find_processes_with_open_state(codex_home: Path, sqlite_home: Path) -> list[int]:
    proc = Path("/proc")
    if not proc.is_dir():
        return []
    watched_roots = [
        *(root.resolve() for root in session_roots(codex_home)),
        sqlite_home.resolve(),
    ]
    current_pid = os.getpid()
    matches: list[int] = []
    for entry in proc.iterdir():
        if not entry.name.isdigit() or int(entry.name) == current_pid:
            continue
        looks_like_codex = False
        try:
            command_args = [
                value.decode("utf-8", errors="replace")
                for value in (entry / "cmdline").read_bytes().split(b"\0")
                if value
            ]
            looks_like_codex = command_looks_like_codex(command_args)
        except (FileNotFoundError, PermissionError, OSError):
            pass
        fd_root = entry / "fd"
        try:
            descriptors = list(fd_root.iterdir())
        except (FileNotFoundError, PermissionError):
            if looks_like_codex:
                matches.append(int(entry.name))
            continue
        found = False
        for descriptor in descriptors:
            try:
                target = descriptor.resolve(strict=True)
            except (FileNotFoundError, PermissionError, OSError):
                continue
            for root in watched_roots:
                try:
                    target.relative_to(root)
                except ValueError:
                    continue
                found = True
                break
            if found:
                break
        if found or looks_like_codex:
            matches.append(int(entry.name))
    return sorted(matches)


def copy_database_backup(
    source: Path,
    destination: Path,
    *,
    source_immutable: bool = False,
) -> None:
    source_connection = sqlite3.connect(
        sqlite_read_only_uri(source, immutable=source_immutable),
        uri=True,
    )
    destination_connection = sqlite3.connect(destination)
    try:
        page_size = source_connection.execute("PRAGMA page_size").fetchone()[0]
        pages = source_connection.execute("PRAGMA page_count").fetchone()[0]
        with progress.phase(
            "Copy SQLite backup", total_bytes=pages * page_size
        ) as status:

            def update(_result, remaining, total):
                status.total_bytes = total * page_size
                status.completed_bytes = (total - remaining) * page_size

            source_connection.backup(destination_connection, pages=256, progress=update)
    finally:
        destination_connection.close()
        source_connection.close()


def write_json_atomic(path: Path, value: dict[str, Any]) -> None:
    encoded = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")
    if path.exists():
        atomic_write(path, encoded, preserve_mtime=False)
        return
    path.write_bytes(encoded)
    os.chmod(path, 0o600)


@progress.phase("Create backup")
def create_backup(
    *,
    codex_home: Path,
    sqlite_home: Path,
    db_path: Path,
    history_db_path: Path,
    backup_dir: Path,
    source_provider: str,
    target_provider: str,
    rollout_analysis: RolloutAnalysis,
    database_analysis: DatabaseAnalysis,
    history_database_analysis: HistoryDatabaseAnalysis,
    migrate_config: bool,
) -> dict[str, Any]:
    if backup_dir.exists():
        raise MigrationError(f"backup directory already exists: {backup_dir}")
    backup_dir.mkdir(parents=True, mode=0o700)

    config_path = codex_home / "config.toml"
    if config_path.is_symlink():
        raise MigrationError(f"refusing symlinked config: {config_path}")
    config_artifact: dict[str, Any] = {"present": False}
    if config_path.is_file():
        config_metadata = capture_file_metadata(config_path)
        config_backup = backup_dir / "config.toml"
        shutil.copy2(config_path, config_backup)
        apply_file_metadata(config_backup, config_metadata)
        config_artifact = artifact_descriptor(config_backup, config_metadata)
    elif config_path.exists():
        raise MigrationError(f"config path is not a regular file: {config_path}")
    copy_paths = [
        path
        for root in session_roots(codex_home)
        for path in root.rglob("*")
        if path.is_file()
    ]
    with progress.phase(
        "Copy session backup",
        total_files=len(copy_paths),
        total_bytes=sum(path.stat().st_size for path in copy_paths),
    ) as status:
        copy_jobs = []

        def queue_copy(source, destination):
            copy_jobs.append((Path(source), Path(destination)))
            return destination

        for name in SESSION_DIR_NAMES:
            source = codex_home / name
            if source.is_dir():
                shutil.copytree(
                    source,
                    backup_dir / name,
                    copy_function=queue_copy,
                    symlinks=True,
                )

        directories = [
            directory
            for root in session_roots(codex_home)
            for directory in (
                root,
                *(p for p in root.rglob("*") if p.is_dir() and not p.is_symlink()),
            )
        ]
        # copytree applies directory modes before deferred copies run. Keep the
        # private destinations writable until their files have been created.
        for directory in directories:
            destination = backup_dir / directory.relative_to(codex_home)
            if destination.is_symlink():
                raise MigrationError(
                    f"refusing symlinked backup directory: {destination}"
                )
            destination.chmod(stat.S_IMODE(destination.stat().st_mode) | stat.S_IRWXU)

        def copy_file(job):
            source, destination = job
            workers.check_cancelled()
            shutil.copy2(source, destination)
            workers.advance(source.stat().st_size, files=1)

        workers.for_each(copy_file, copy_jobs, status)
        # File creation changes directory mtimes after copytree copied them.
        for directory in reversed(directories):
            shutil.copystat(directory, backup_dir / directory.relative_to(codex_home))

    database_metadata = capture_file_metadata(db_path)
    database_backup = backup_dir / db_path.name
    copy_database_backup(db_path, database_backup)
    normalize_database_backup_settings(
        database_backup, database_analysis.persistent_settings
    )
    apply_file_metadata(database_backup, database_metadata)
    database_artifact = artifact_descriptor(database_backup, database_metadata)

    history_database_artifact: dict[str, Any] = {"present": False}
    if history_database_analysis.present:
        history_database_metadata = capture_file_metadata(history_db_path)
        history_database_backup = backup_dir / history_db_path.name
        copy_database_backup(history_db_path, history_database_backup)
        normalize_database_backup_settings(
            history_database_backup,
            history_database_analysis.persistent_settings,
        )
        apply_file_metadata(history_database_backup, history_database_metadata)
        history_database_artifact = artifact_descriptor(
            history_database_backup,
            history_database_metadata,
        )

    def check_copied_rollout(item):
        relative, expected_hash = item
        copied = backup_dir / relative
        expected_metadata = rollout_analysis.file_metadata[relative]
        if copied.is_file():
            apply_file_metadata(copied, expected_metadata)
        if not copied.is_file() or sha256_file(copied) != expected_hash:
            raise MigrationError(f"backup hash mismatch for {relative}")
        if capture_file_metadata(copied) != expected_metadata:
            raise MigrationError(f"backup metadata mismatch for {relative}")
        workers.advance(copied.stat().st_size, files=1)

    with progress.phase(
        "Verify session backup",
        total_files=len(rollout_analysis.file_hashes),
        total_bytes=sum(
            (backup_dir / relative).stat().st_size
            for relative in rollout_analysis.file_hashes
        ),
    ) as status:
        workers.for_each(
            check_copied_rollout, rollout_analysis.file_hashes.items(), status
        )

    manifest: dict[str, Any] = {
        "manifest_version": MANIFEST_VERSION,
        "tool_version": TOOL_VERSION,
        "status": "prepared",
        "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "codex_home": str(codex_home),
        "sqlite_home": str(sqlite_home),
        "state_db_name": db_path.name,
        "source_provider": source_provider,
        "target_provider": target_provider,
        "migrate_config": migrate_config,
        "rollout_analysis": dataclasses.asdict(rollout_analysis),
        "database_analysis": dataclasses.asdict(database_analysis),
        "history_database_analysis": dataclasses.asdict(history_database_analysis),
        "artifacts": {
            "config": config_artifact,
            "database": database_artifact,
            "history_database": history_database_artifact,
        },
    }
    write_json_atomic(backup_dir / MANIFEST_NAME, manifest)
    return manifest


def compare_table_rows(
    original: sqlite3.Connection,
    migrated: sqlite3.Connection,
    table: str,
    columns: list[str],
    expressions: list[str],
    parameters: tuple[Any, ...] = (),
) -> int:
    # Order by every transformed value with binary collation, retaining duplicate multiplicity.
    # Using PK-only ordering would miss schemas with nullable/non-unique logical keys.
    order = ", ".join(
        quoted_identifier(column) + " COLLATE BINARY" for column in columns
    )
    table_name = quoted_identifier(table)
    before = original.execute(
        f"SELECT {', '.join(expressions)} FROM {table_name} ORDER BY {order}",
        parameters,
    )
    after = migrated.execute(f"SELECT * FROM {table_name} ORDER BY {order}")
    sentinel = object()
    count = 0
    for expected, actual in itertools.zip_longest(before, after, fillvalue=sentinel):
        if expected != actual:
            raise MigrationError(f"unexpected SQLite change in table {table}")
        count += 1
    return count


@progress.phase("Verify state database")
def compare_sqlite_databases(
    original_path: Path,
    migrated_path: Path,
    source_provider: str,
    target_provider: str,
    *,
    allow_restore_intermediate: bool = False,
) -> tuple[int, int, int]:
    original = sqlite3.connect(
        sqlite_read_only_uri(original_path, immutable=True),
        uri=True,
    )
    migrated = sqlite3.connect(sqlite_read_only_uri(migrated_path), uri=True)
    tables_checked = 0
    rows_checked = 0
    changed_rows = 0
    try:
        for connection in (original, migrated):
            connection.execute("PRAGMA temp_store = FILE")
            connection.execute("PRAGMA cache_size = -8192")
        original.execute("BEGIN")
        migrated.execute("BEGIN")
        if original.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
            raise MigrationError("backup SQLite integrity_check failed")
        if migrated.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
            raise MigrationError("migrated SQLite integrity_check failed")
        if original.execute("PRAGMA foreign_key_check").fetchall():
            raise MigrationError("backup SQLite foreign_key_check failed")
        if migrated.execute("PRAGMA foreign_key_check").fetchall():
            raise MigrationError("migrated SQLite foreign_key_check failed")

        original_settings = read_persistent_sqlite_settings(original)
        migrated_settings = read_persistent_sqlite_settings(migrated)
        for pragma in PERSISTENT_SQLITE_PRAGMAS:
            if original_settings[pragma] == migrated_settings[pragma]:
                continue
            if (
                allow_restore_intermediate
                and pragma == "schema_version"
                and type(original_settings[pragma]) is int
                and migrated_settings[pragma] == original_settings[pragma] + 1
            ):
                continue
            raise MigrationError(f"SQLite persistent setting changed: {pragma}")
        if sqlite_header_journal_mode(original_path) != sqlite_header_journal_mode(
            migrated_path
        ):
            raise MigrationError("SQLite persistent setting changed: journal_mode")

        schema_query = (
            "SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY type, name"
        )
        if (
            original.execute(schema_query).fetchall()
            != migrated.execute(schema_query).fetchall()
        ):
            raise MigrationError("SQLite schema changed")
        tables = [
            row[0]
            for row in original.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
        ]
        migrated_tables = [
            row[0]
            for row in migrated.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
        ]
        if tables != migrated_tables:
            raise MigrationError("SQLite table set changed")

        for table in tables:
            quoted = '"' + table.replace('"', '""') + '"'
            columns = [
                row[1] for row in original.execute(f"PRAGMA table_info({quoted})")
            ]
            expressions = [quoted_identifier(column) for column in columns]
            parameters: tuple[Any, ...] = ()
            if table == "threads":
                index = columns.index("model_provider")
                expressions[index] = (
                    'CASE WHEN "model_provider" = ? THEN ? ELSE "model_provider" END AS "model_provider"'
                )
                parameters = (source_provider, target_provider)
                changed_rows += original.execute(
                    "SELECT count(*) FROM threads WHERE model_provider = ?",
                    (source_provider,),
                ).fetchone()[0]
            rows_checked += compare_table_rows(
                original, migrated, table, columns, expressions, parameters
            )
            tables_checked += 1

    finally:
        original.close()
        migrated.close()
    return tables_checked, rows_checked, changed_rows


@progress.phase("Verify history database")
def compare_history_databases(
    original_path: Path,
    migrated_path: Path,
    state_db_path: Path,
    rollout_root: Path,
    source_provider: str,
    target_provider: str,
    *,
    allow_restore_intermediate: bool = False,
    rollout_migration: RolloutMigrationPlan | None = None,
) -> tuple[int, int, int]:
    analysis, updates = inspect_history_database(
        original_path,
        state_db_path,
        rollout_root,
        source_provider,
        target_provider,
        immutable=True,
        rollout_migration=rollout_migration,
    )
    if not analysis.present:
        raise MigrationError("backup thread-history database is missing")

    original = sqlite3.connect(
        sqlite_read_only_uri(original_path, immutable=True), uri=True
    )
    migrated = sqlite3.connect(sqlite_read_only_uri(migrated_path), uri=True)
    tables_checked = 0
    rows_checked = 0
    try:
        for connection in (original, migrated):
            connection.execute("PRAGMA temp_store = FILE")
            connection.execute("PRAGMA cache_size = -8192")
        original.execute("BEGIN")
        migrated.execute("BEGIN")
        if original.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
            raise MigrationError("backup thread-history integrity_check failed")
        if migrated.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
            raise MigrationError("migrated thread-history integrity_check failed")
        if original.execute("PRAGMA foreign_key_check").fetchall():
            raise MigrationError("backup thread-history foreign_key_check failed")
        if migrated.execute("PRAGMA foreign_key_check").fetchall():
            raise MigrationError("migrated thread-history foreign_key_check failed")

        original_settings = read_persistent_sqlite_settings(original)
        migrated_settings = read_persistent_sqlite_settings(migrated)
        for pragma in PERSISTENT_SQLITE_PRAGMAS:
            if original_settings[pragma] == migrated_settings[pragma]:
                continue
            if (
                allow_restore_intermediate
                and pragma == "schema_version"
                and type(original_settings[pragma]) is int
                and migrated_settings[pragma] == original_settings[pragma] + 1
            ):
                continue
            raise MigrationError(f"thread-history persistent setting changed: {pragma}")
        if sqlite_header_journal_mode(original_path) != sqlite_header_journal_mode(
            migrated_path
        ):
            raise MigrationError(
                "thread-history persistent setting changed: journal_mode"
            )

        schema_query = (
            "SELECT type, name, tbl_name, sql FROM sqlite_master ORDER BY type, name"
        )
        if (
            original.execute(schema_query).fetchall()
            != migrated.execute(schema_query).fetchall()
        ):
            raise MigrationError("thread-history SQLite schema changed")
        tables = [
            row[0]
            for row in original.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
        ]
        migrated_tables = [
            row[0]
            for row in migrated.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
        ]
        if tables != migrated_tables:
            raise MigrationError("thread-history SQLite table set changed")

        planned = {
            (
                update.table,
                update.key_values,
                update.offset_column,
                update.original_offset,
            ): update.migrated_offset
            for update in updates
        }
        for table in tables:
            quoted = quoted_identifier(table)
            columns = [
                row[1] for row in original.execute(f"PRAGMA table_info({quoted})")
            ]
            expressions = [quoted_identifier(column) for column in columns]
            if table == "thread_history_projection_state":
                key_columns = ("thread_id",)
                offset_columns = ("next_rollout_byte_offset",)
            elif table == "thread_turns":
                key_columns = ("thread_id", "turn_id")
                offset_columns = ("rollout_byte_offset", "rollout_end_byte_offset")
            else:
                key_columns = offset_columns = ()
            for column in offset_columns:

                def expected_offset(*args, table=table, column=column):
                    return planned.get(
                        (table, tuple(args[:-1]), column, args[-1]), args[-1]
                    )

                function = "expected_" + column
                original.create_function(
                    function, len(key_columns) + 1, expected_offset, deterministic=True
                )
                arguments = ", ".join(
                    quoted_identifier(key) for key in (*key_columns, column)
                )
                expressions[columns.index(column)] = (
                    f"{function}({arguments}) AS {quoted_identifier(column)}"
                )
            try:
                rows_checked += compare_table_rows(
                    original, migrated, table, columns, expressions
                )
            except MigrationError as exc:
                raise MigrationError(
                    f"unexpected thread-history SQLite change in table {table}"
                ) from exc
            tables_checked += 1

    finally:
        original.close()
        migrated.close()
    return tables_checked, rows_checked, len(updates)


def load_backup_manifest(backup_dir: Path) -> dict[str, Any]:
    manifest_path = backup_dir / MANIFEST_NAME
    try:
        manifest = json.loads(manifest_path.read_bytes().decode("utf-8"))
    except (FileNotFoundError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MigrationError(f"cannot read backup manifest: {exc}") from exc
    if not isinstance(manifest, dict):
        raise MigrationError("invalid backup manifest: root must be an object")
    required_string_fields = {
        "codex_home",
        "sqlite_home",
        "state_db_name",
        "source_provider",
        "target_provider",
    }
    missing = sorted(required_string_fields - manifest.keys())
    if missing:
        raise MigrationError("invalid backup manifest: missing " + ", ".join(missing))
    invalid_strings = sorted(
        name
        for name in required_string_fields
        if not isinstance(manifest[name], str)
        or not manifest[name].strip()
        or "\x00" in manifest[name]
    )
    if invalid_strings:
        raise MigrationError(
            "invalid backup manifest fields: " + ", ".join(invalid_strings)
        )
    if manifest.get("manifest_version") not in {
        LEGACY_MANIFEST_VERSION,
        MANIFEST_VERSION,
    }:
        raise MigrationError("unsupported backup manifest version")
    if not isinstance(manifest.get("migrate_config"), bool):
        raise MigrationError("invalid backup manifest field: migrate_config")
    if manifest["state_db_name"] != STATE_DB_NAME:
        raise MigrationError("invalid backup manifest state database name")
    invalid_paths = sorted(
        name
        for name in ("codex_home", "sqlite_home")
        if not Path(manifest[name]).is_absolute()
    )
    if invalid_paths:
        raise MigrationError(
            "backup manifest paths must be absolute: " + ", ".join(invalid_paths)
        )
    return manifest


def history_database_descriptor(manifest: dict[str, Any]) -> dict[str, Any]:
    if manifest.get("manifest_version") == LEGACY_MANIFEST_VERSION:
        return {"present": False}
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, dict):
        raise MigrationError("invalid backup manifest artifacts")
    descriptor = artifacts.get("history_database")
    if not isinstance(descriptor, dict):
        raise MigrationError("invalid backup manifest thread-history artifact")
    return descriptor


def validate_recorded_metadata(value: Any, label: str) -> dict[str, int]:
    if not isinstance(value, dict):
        raise MigrationError(f"invalid backup manifest metadata: {label}")
    expected_keys = {"mode", "uid", "gid", "mtime_ns"}
    if set(value) != expected_keys or any(
        type(value[name]) is not int for name in expected_keys
    ):
        raise MigrationError(f"invalid backup manifest metadata: {label}")
    if (
        value["mode"] < 0
        or value["mode"] > 0o7777
        or value["uid"] < 0
        or value["gid"] < 0
        or value["mtime_ns"] < 0
    ):
        raise MigrationError(f"invalid backup manifest metadata: {label}")
    return value


def validate_sha256(value: Any, label: str) -> str:
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise MigrationError(f"invalid backup manifest digest: {label}")
    return value


@progress.phase("Validate backup artifacts")
def validate_backup_artifacts(
    backup_dir: Path,
    manifest: dict[str, Any],
) -> None:
    rollout_analysis = manifest.get("rollout_analysis")
    if not isinstance(rollout_analysis, dict):
        raise MigrationError("invalid backup manifest rollout analysis")
    expected_hashes = rollout_analysis.get("file_hashes")
    expected_metadata = rollout_analysis.get("file_metadata")
    if not isinstance(expected_hashes, dict) or not isinstance(expected_metadata, dict):
        raise MigrationError("invalid backup manifest rollout artifacts")

    backup_rollouts = {
        relative_rollout_path(backup_dir, path): path
        for path in rollout_paths(backup_dir)
    }
    if set(expected_hashes) != set(backup_rollouts) or set(expected_metadata) != set(
        backup_rollouts
    ):
        raise MigrationError("backup rollout file set differs from the manifest")

    def validate_rollout(item):
        relative, path = item
        expected_hash = validate_sha256(
            expected_hashes[relative], f"rollout {relative}"
        )
        metadata = validate_recorded_metadata(
            expected_metadata[relative], f"rollout {relative}"
        )
        if sha256_file(path) != expected_hash:
            raise MigrationError(f"backup rollout digest mismatch: {relative}")
        if capture_file_metadata(path) != metadata:
            raise MigrationError(f"backup rollout metadata mismatch: {relative}")
        workers.advance(path.stat().st_size, files=1)

    with progress.phase(
        "Validate backup rollouts",
        total_files=len(backup_rollouts),
        total_bytes=sum(path.stat().st_size for path in backup_rollouts.values()),
    ) as status:
        workers.for_each(validate_rollout, backup_rollouts.items(), status)

    artifacts = manifest.get("artifacts")
    expected_artifacts = {"config", "database"}
    if manifest.get("manifest_version") == MANIFEST_VERSION:
        expected_artifacts.add("history_database")
    if not isinstance(artifacts, dict) or set(artifacts) != expected_artifacts:
        raise MigrationError("invalid backup manifest artifacts")

    config_descriptor = artifacts["config"]
    if (
        not isinstance(config_descriptor, dict)
        or type(config_descriptor.get("present")) is not bool
    ):
        raise MigrationError("invalid backup manifest config artifact")
    config_path = backup_dir / "config.toml"
    if config_descriptor["present"]:
        if config_path.is_symlink() or not config_path.is_file():
            raise MigrationError("backup config.toml is missing or unsafe")
        config_metadata = validate_recorded_metadata(
            {
                key: config_descriptor.get(key)
                for key in ("mode", "uid", "gid", "mtime_ns")
            },
            "config.toml",
        )
        config_hash = validate_sha256(config_descriptor.get("sha256"), "config.toml")
        if sha256_file(config_path) != config_hash:
            raise MigrationError("backup config.toml digest mismatch")
        if capture_file_metadata(config_path) != config_metadata:
            raise MigrationError("backup config.toml metadata mismatch")
    elif config_path.exists() or config_path.is_symlink():
        raise MigrationError("unexpected config.toml exists in backup")

    database_descriptor = artifacts["database"]
    if (
        not isinstance(database_descriptor, dict)
        or database_descriptor.get("present") is not True
    ):
        raise MigrationError("invalid backup manifest database artifact")
    database_path = backup_dir / manifest["state_db_name"]
    if database_path.is_symlink() or not database_path.is_file():
        raise MigrationError("backup state database is missing or unsafe")
    database_metadata = validate_recorded_metadata(
        {
            key: database_descriptor.get(key)
            for key in ("mode", "uid", "gid", "mtime_ns")
        },
        manifest["state_db_name"],
    )
    database_hash = validate_sha256(
        database_descriptor.get("sha256"), manifest["state_db_name"]
    )
    if sha256_file(database_path) != database_hash:
        raise MigrationError("backup state database digest mismatch")
    if capture_file_metadata(database_path) != database_metadata:
        raise MigrationError("backup state database metadata mismatch")
    for suffix in ("-wal", "-shm", "-journal"):
        if Path(f"{database_path}{suffix}").exists():
            raise MigrationError(f"unexpected SQLite sidecar in backup: {suffix}")

    history_descriptor = history_database_descriptor(manifest)
    if type(history_descriptor.get("present")) is not bool:
        raise MigrationError("invalid backup manifest thread-history artifact")
    history_path = backup_dir / HISTORY_DB_NAME
    if history_descriptor["present"]:
        if history_path.is_symlink() or not history_path.is_file():
            raise MigrationError("backup thread-history database is missing or unsafe")
        history_metadata = validate_recorded_metadata(
            {
                key: history_descriptor.get(key)
                for key in ("mode", "uid", "gid", "mtime_ns")
            },
            HISTORY_DB_NAME,
        )
        history_hash = validate_sha256(
            history_descriptor.get("sha256"), HISTORY_DB_NAME
        )
        if sha256_file(history_path) != history_hash:
            raise MigrationError("backup thread-history database digest mismatch")
        if capture_file_metadata(history_path) != history_metadata:
            raise MigrationError("backup thread-history database metadata mismatch")
        for suffix in ("-wal", "-shm", "-journal"):
            if Path(f"{history_path}{suffix}").exists():
                raise MigrationError(
                    "unexpected thread-history SQLite sidecar in backup: " + suffix
                )
    elif history_path.exists() or history_path.is_symlink():
        raise MigrationError("unexpected thread-history database exists in backup")

    if manifest.get("manifest_version") == MANIFEST_VERSION:
        history_analysis = manifest.get("history_database_analysis")
        if (
            not isinstance(history_analysis, dict)
            or type(history_analysis.get("present")) is not bool
            or history_analysis["present"] != history_descriptor["present"]
        ):
            raise MigrationError("invalid backup manifest thread-history analysis")


def verify_against_backup(
    *,
    backup_dir: Path,
    codex_home: Path | None = None,
    sqlite_home: Path | None = None,
) -> VerificationReport:
    manifest = load_backup_manifest(backup_dir)
    validate_backup_artifacts(backup_dir, manifest)
    source_provider = manifest["source_provider"]
    target_provider = manifest["target_provider"]
    codex_home = (codex_home or Path(manifest["codex_home"])).resolve()
    sqlite_home = (sqlite_home or Path(manifest["sqlite_home"])).resolve()
    db_name = manifest["state_db_name"]
    history_descriptor = history_database_descriptor(manifest)

    original_paths = rollout_paths(backup_dir)
    migrated_paths = rollout_paths(codex_home)
    original_by_relative = {
        relative_rollout_path(backup_dir, path): path for path in original_paths
    }
    migrated_by_relative = {
        relative_rollout_path(codex_home, path): path for path in migrated_paths
    }
    if original_by_relative.keys() != migrated_by_relative.keys():
        missing = sorted(original_by_relative.keys() - migrated_by_relative.keys())
        extra = sorted(migrated_by_relative.keys() - original_by_relative.keys())
        raise MigrationError(
            f"rollout file set changed; missing={missing}, extra={extra}"
        )

    backup_analysis, backup_rollout_migration = analyze_migratable_rollouts(
        backup_dir,
        backup_dir / db_name,
        source_provider,
        target_provider,
        allow_paginated=history_descriptor["present"],
        immutable=True,
    )
    changed_files = backup_analysis.files_requiring_changes
    unchanged_files = backup_analysis.rollout_files - changed_files
    malformed_preserved = backup_analysis.malformed_lines
    lines_checked = backup_analysis.total_lines
    session_meta_changed = backup_analysis.session_meta_values
    thread_settings_changed = backup_analysis.thread_settings_values
    history_base_changed = backup_analysis.history_base_offsets_changed
    with progress.phase(
        "Verify rollout bytes and metadata",
        total_files=len(original_by_relative),
        total_bytes=sum(f.size for f in backup_rollout_migration.files.values()),
    ) as status:

        def verify_rollout(relative):
            original_path = original_by_relative[relative]
            migrated_path = migrated_by_relative[relative]
            file = backup_rollout_migration.files[relative]
            if not matches_chunks(migrated_path, file.chunks(original_path)):
                raise MigrationError(f"unexpected rollout byte change: {relative}")
            original_stat = original_path.stat()
            migrated_stat = migrated_path.stat()
            if stat.S_IMODE(original_stat.st_mode) != stat.S_IMODE(
                migrated_stat.st_mode
            ):
                raise MigrationError(f"rollout mode changed: {relative}")
            if (original_stat.st_uid, original_stat.st_gid) != (
                migrated_stat.st_uid,
                migrated_stat.st_gid,
            ):
                raise MigrationError(f"rollout ownership changed: {relative}")
            if original_stat.st_mtime_ns != migrated_stat.st_mtime_ns:
                raise MigrationError(f"rollout mtime changed: {relative}")
            workers.advance(file.size, files=1)

        workers.for_each(verify_rollout, sorted(original_by_relative), status)

    config_backup = backup_dir / "config.toml"
    config_current = codex_home / "config.toml"
    config_descriptor = manifest["artifacts"]["config"]
    config_matches_expected = not config_descriptor["present"]
    if config_descriptor["present"]:
        expected_config = config_backup.read_bytes()
        if manifest.get("migrate_config"):
            expected_config = transform_config(
                expected_config, source_provider, target_provider
            )
        config_matches_expected = (
            not config_current.is_symlink()
            and config_current.is_file()
            and config_current.read_bytes() == expected_config
        )
        if not config_matches_expected:
            raise MigrationError("config.toml differs from the expected migration")
        backup_stat = config_backup.stat()
        current_stat = config_current.stat()
        if stat.S_IMODE(backup_stat.st_mode) != stat.S_IMODE(current_stat.st_mode):
            raise MigrationError("config.toml mode changed during migration")
        if (backup_stat.st_uid, backup_stat.st_gid) != (
            current_stat.st_uid,
            current_stat.st_gid,
        ):
            raise MigrationError("config.toml ownership changed during migration")
        if (
            not manifest["migrate_config"]
            and backup_stat.st_mtime_ns != current_stat.st_mtime_ns
        ):
            raise MigrationError("config.toml mtime changed during migration")
    elif config_current.exists() or config_current.is_symlink():
        raise MigrationError("config.toml appeared after the backup was created")

    validate_state_db_path(sqlite_home / db_name)
    tables, rows, changed_rows = compare_sqlite_databases(
        backup_dir / db_name,
        sqlite_home / db_name,
        source_provider,
        target_provider,
    )
    database_metadata = manifest["artifacts"]["database"]
    current_database_metadata = capture_file_metadata(sqlite_home / db_name)
    for key in ("mode", "uid", "gid"):
        if current_database_metadata[key] != database_metadata[key]:
            raise MigrationError(f"SQLite {key} changed during migration")

    history_tables = 0
    history_rows = 0
    history_offsets_changed = 0
    history_path = sqlite_home / HISTORY_DB_NAME
    if history_descriptor["present"]:
        if history_path.is_symlink() or not history_path.is_file():
            raise MigrationError("migrated thread-history database is missing")
        history_tables, history_rows, history_offsets_changed = (
            compare_history_databases(
                backup_dir / HISTORY_DB_NAME,
                history_path,
                backup_dir / db_name,
                backup_dir,
                source_provider,
                target_provider,
                rollout_migration=backup_rollout_migration,
            )
        )
        current_history_metadata = capture_file_metadata(history_path)
        for key in ("mode", "uid", "gid"):
            if current_history_metadata[key] != history_descriptor[key]:
                raise MigrationError(
                    f"thread-history SQLite {key} changed during migration"
                )
    elif history_path.exists() or history_path.is_symlink():
        raise MigrationError(
            "thread-history database appeared after the backup was created"
        )
    return VerificationReport(
        rollout_files_checked=len(original_by_relative),
        changed_rollout_files=changed_files,
        unchanged_rollout_files=unchanged_files,
        jsonl_lines_checked=lines_checked,
        malformed_lines_preserved=malformed_preserved,
        session_meta_values_changed=session_meta_changed,
        thread_settings_values_changed=thread_settings_changed,
        history_base_offsets_changed=history_base_changed,
        sqlite_tables_checked=tables,
        sqlite_rows_checked=rows,
        sqlite_thread_rows_changed=changed_rows,
        history_sqlite_tables_checked=history_tables,
        history_sqlite_rows_checked=history_rows,
        history_offset_fields_changed=history_offsets_changed,
        config_matches_expected=config_matches_expected,
    )


@progress.phase("Check recoverable state")
def verify_recoverable_state(
    *,
    backup_dir: Path,
    codex_home: Path,
    sqlite_home: Path,
    manifest: dict[str, Any],
) -> None:
    """Require every live artifact to be exactly original or exactly migrated.

    File replacement and the SQLite provider update are individually atomic,
    but a process interruption can leave different artifacts on different
    sides of the migration. Accepting only these two known states makes a
    restore resumable without overwriting later Codex activity.
    """

    validate_backup_artifacts(backup_dir, manifest)
    source_provider = manifest["source_provider"]
    target_provider = manifest["target_provider"]
    history_descriptor = history_database_descriptor(manifest)
    _, backup_rollout_migration = analyze_migratable_rollouts(
        backup_dir,
        backup_dir / manifest["state_db_name"],
        source_provider,
        target_provider,
        allow_paginated=history_descriptor["present"],
        immutable=True,
    )

    backup_paths = {
        relative_rollout_path(backup_dir, path): path
        for path in rollout_paths(backup_dir)
    }
    live_paths = {
        relative_rollout_path(codex_home, path): path
        for path in rollout_paths(codex_home)
    }
    if backup_paths.keys() != live_paths.keys():
        raise MigrationError(
            "live rollout file set is not recoverable from this backup"
        )

    def check_recoverable_rollout(item):
        relative, backup_path = item
        live_path = live_paths[relative]
        if not files_equal(backup_path, live_path) and not matches_chunks(
            live_path, backup_rollout_migration.files[relative].chunks(backup_path)
        ):
            raise MigrationError(
                f"live rollout is neither original nor migrated: {relative}"
            )
        expected_metadata = manifest["rollout_analysis"]["file_metadata"][relative]
        if capture_file_metadata(live_path) != expected_metadata:
            raise MigrationError(
                f"live rollout metadata is not recoverable: {relative}"
            )
        workers.advance(backup_path.stat().st_size, files=1)

    with progress.phase(
        "Check recoverable rollouts",
        total_files=len(backup_paths),
        total_bytes=sum(path.stat().st_size for path in backup_paths.values()),
    ) as status:
        workers.for_each(check_recoverable_rollout, backup_paths.items(), status)

    config_descriptor = manifest["artifacts"]["config"]
    config_backup = backup_dir / "config.toml"
    config_current = codex_home / "config.toml"
    if not config_descriptor["present"]:
        if config_current.exists() or config_current.is_symlink():
            raise MigrationError(
                "live config.toml is not recoverable because it was absent "
                "from the backup"
            )
    else:
        if config_current.is_symlink() or not config_current.is_file():
            raise MigrationError("live config.toml is missing or unsafe")
        original_config = config_backup.read_bytes()
        migrated_config = original_config
        if manifest["migrate_config"]:
            migrated_config = transform_config(
                original_config, source_provider, target_provider
            )
        current_config = config_current.read_bytes()
        if current_config not in (original_config, migrated_config):
            raise MigrationError("live config.toml is neither original nor migrated")
        expected_metadata = validate_recorded_metadata(
            {key: config_descriptor[key] for key in ("mode", "uid", "gid", "mtime_ns")},
            "config.toml",
        )
        current_metadata = capture_file_metadata(config_current)
        for key in ("mode", "uid", "gid"):
            if current_metadata[key] != expected_metadata[key]:
                raise MigrationError(
                    f"live config.toml {key} is neither original nor migrated"
                )
        if (
            current_config == original_config
            and current_metadata["mtime_ns"] != expected_metadata["mtime_ns"]
        ):
            raise MigrationError(
                "live config.toml mtime is not the original recorded value"
            )

    database_path = sqlite_home / manifest["state_db_name"]
    validate_state_db_path(database_path)
    expected_database_metadata = manifest["artifacts"]["database"]
    current_database_metadata = capture_file_metadata(database_path)
    for key in ("uid", "gid"):
        if current_database_metadata[key] != expected_database_metadata[key]:
            raise MigrationError(f"live SQLite {key} is neither original nor migrated")
    expected_mode = expected_database_metadata["mode"]
    known_modes = {expected_mode, expected_mode & ~(stat.S_ISUID | stat.S_ISGID)}
    if current_database_metadata["mode"] not in known_modes:
        raise MigrationError("live SQLite mode is neither original nor migrated")
    try:
        compare_sqlite_databases(
            backup_dir / manifest["state_db_name"],
            database_path,
            source_provider,
            target_provider,
        )
    except (MigrationError, sqlite3.Error):
        try:
            compare_sqlite_databases(
                backup_dir / manifest["state_db_name"],
                database_path,
                source_provider,
                source_provider,
                allow_restore_intermediate=True,
            )
        except (MigrationError, sqlite3.Error) as original_error:
            raise MigrationError(
                "live SQLite state is neither original nor migrated"
            ) from original_error

    history_path = sqlite_home / HISTORY_DB_NAME
    if history_descriptor["present"]:
        if history_path.is_symlink() or not history_path.is_file():
            raise MigrationError("live thread-history database is missing or unsafe")
        current_history_metadata = capture_file_metadata(history_path)
        for key in ("uid", "gid"):
            if current_history_metadata[key] != history_descriptor[key]:
                raise MigrationError(
                    f"live thread-history SQLite {key} is neither original nor migrated"
                )
        expected_history_mode = history_descriptor["mode"]
        known_history_modes = {
            expected_history_mode,
            expected_history_mode & ~(stat.S_ISUID | stat.S_ISGID),
        }
        if current_history_metadata["mode"] not in known_history_modes:
            raise MigrationError(
                "live thread-history SQLite mode is neither original nor migrated"
            )
        try:
            compare_history_databases(
                backup_dir / HISTORY_DB_NAME,
                history_path,
                backup_dir / manifest["state_db_name"],
                backup_dir,
                source_provider,
                target_provider,
            )
        except (MigrationError, sqlite3.Error):
            try:
                compare_history_databases(
                    backup_dir / HISTORY_DB_NAME,
                    history_path,
                    backup_dir / manifest["state_db_name"],
                    backup_dir,
                    source_provider,
                    source_provider,
                    allow_restore_intermediate=True,
                )
            except (MigrationError, sqlite3.Error) as original_error:
                raise MigrationError(
                    "live thread-history state is neither original nor migrated"
                ) from original_error
    elif history_path.exists() or history_path.is_symlink():
        raise MigrationError(
            "live thread-history database is not recoverable from this backup"
        )


def restore_file_from_backup(source: Path, destination: Path) -> None:
    if source.is_symlink() or not source.is_file():
        raise MigrationError(f"backup file is missing or unsafe: {source}")
    if destination.is_symlink():
        raise MigrationError(f"refusing symlinked restore destination: {destination}")
    if destination.exists() and not destination.is_file():
        raise MigrationError(f"restore destination is not a file: {destination}")
    if not destination.parent.is_dir() or destination.parent.is_symlink():
        raise MigrationError(
            f"restore destination directory is missing or unsafe: {destination.parent}"
        )

    source_metadata = source.stat()
    fd, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.provider-restore-",
        dir=destination.parent,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "wb") as stream:
            for chunk in file_chunks(source):
                stream.write(chunk)
            stream.flush()
            os.fsync(stream.fileno())
        set_file_owner(temporary, source_metadata.st_uid, source_metadata.st_gid)
        os.chmod(temporary, stat.S_IMODE(source_metadata.st_mode))
        os.utime(
            temporary,
            ns=(source_metadata.st_atime_ns, source_metadata.st_mtime_ns),
        )
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def restore_database_from_backup(source: Path, destination: Path) -> None:
    validate_state_db_path(source)
    if destination.is_symlink():
        raise MigrationError(f"refusing symlinked state database: {destination}")
    if destination.exists() and not destination.is_file():
        raise MigrationError(f"state database is not a file: {destination}")
    if not destination.parent.is_dir() or destination.parent.is_symlink():
        raise MigrationError(
            f"SQLite restore directory is missing or unsafe: {destination.parent}"
        )

    source_metadata = source.stat()
    destination_existed = destination.is_file()
    source_connection = sqlite3.connect(
        sqlite_read_only_uri(source, immutable=True), uri=True
    )
    destination_connection = sqlite3.connect(destination)
    try:
        source_settings = read_persistent_sqlite_settings(source_connection)
        source_settings["journal_mode"] = sqlite_header_journal_mode(source)
        source_connection.backup(destination_connection)
    finally:
        destination_connection.close()
        source_connection.close()

    normalize_database_backup_settings(destination, source_settings)
    if not destination_existed:
        set_file_owner(destination, source_metadata.st_uid, source_metadata.st_gid)
    os.chmod(destination, stat.S_IMODE(source_metadata.st_mode))
    os.utime(
        destination,
        ns=(source_metadata.st_atime_ns, source_metadata.st_mtime_ns),
    )
    with destination.open("rb") as stream:
        os.fsync(stream.fileno())


@progress.phase("Verify restored state")
def verify_restored_state(
    *,
    backup_dir: Path,
    codex_home: Path,
    sqlite_home: Path,
    manifest: dict[str, Any],
) -> RestorationReport:
    source_provider = manifest["source_provider"]
    db_name = manifest["state_db_name"]
    backup_paths = {
        relative_rollout_path(backup_dir, path): path
        for path in rollout_paths(backup_dir)
    }
    restored_paths = {
        relative_rollout_path(codex_home, path): path
        for path in rollout_paths(codex_home)
    }
    if backup_paths.keys() != restored_paths.keys():
        raise MigrationError("rollout file set differs after restoration")

    def check_restored_rollout(item):
        relative, source = item
        destination = restored_paths[relative]
        if not files_equal(source, destination):
            raise MigrationError(f"restored rollout differs: {relative}")
        source_stat = source.stat()
        destination_stat = destination.stat()
        if stat.S_IMODE(source_stat.st_mode) != stat.S_IMODE(destination_stat.st_mode):
            raise MigrationError(f"restored rollout mode differs: {relative}")
        if (source_stat.st_uid, source_stat.st_gid) != (
            destination_stat.st_uid,
            destination_stat.st_gid,
        ):
            raise MigrationError(f"restored rollout ownership differs: {relative}")
        if source_stat.st_mtime_ns != destination_stat.st_mtime_ns:
            raise MigrationError(f"restored rollout mtime differs: {relative}")
        workers.advance(source.stat().st_size, files=1)

    with progress.phase(
        "Verify restored rollouts",
        total_files=len(backup_paths),
        total_bytes=sum(path.stat().st_size for path in backup_paths.values()),
    ) as status:
        workers.for_each(check_restored_rollout, backup_paths.items(), status)

    config_backup = backup_dir / "config.toml"
    config_current = codex_home / "config.toml"
    config_restored = manifest["artifacts"]["config"]["present"]
    if config_restored:
        if not config_current.is_file() or (
            config_backup.read_bytes() != config_current.read_bytes()
        ):
            raise MigrationError("config.toml differs after restoration")
        backup_stat = config_backup.stat()
        current_stat = config_current.stat()
        if stat.S_IMODE(backup_stat.st_mode) != stat.S_IMODE(current_stat.st_mode):
            raise MigrationError("config.toml mode differs after restoration")
        if (backup_stat.st_uid, backup_stat.st_gid) != (
            current_stat.st_uid,
            current_stat.st_gid,
        ):
            raise MigrationError("config.toml ownership differs after restoration")
        if backup_stat.st_mtime_ns != current_stat.st_mtime_ns:
            raise MigrationError("config.toml mtime differs after restoration")
    elif config_current.exists() or config_current.is_symlink():
        raise MigrationError("config.toml should be absent after restoration")

    database_path = sqlite_home / db_name
    tables, rows, _ = compare_sqlite_databases(
        backup_dir / db_name,
        database_path,
        source_provider,
        source_provider,
    )
    backup_database_stat = (backup_dir / db_name).stat()
    restored_database_stat = database_path.stat()
    if stat.S_IMODE(backup_database_stat.st_mode) != stat.S_IMODE(
        restored_database_stat.st_mode
    ):
        raise MigrationError("SQLite mode differs after restoration")
    if (backup_database_stat.st_uid, backup_database_stat.st_gid) != (
        restored_database_stat.st_uid,
        restored_database_stat.st_gid,
    ):
        raise MigrationError("SQLite ownership differs after restoration")
    if backup_database_stat.st_mtime_ns != restored_database_stat.st_mtime_ns:
        raise MigrationError("SQLite mtime differs after restoration")

    history_descriptor = history_database_descriptor(manifest)
    history_database_restored = history_descriptor["present"]
    history_tables = 0
    history_rows = 0
    history_path = sqlite_home / HISTORY_DB_NAME
    if history_database_restored:
        history_tables, history_rows, changed_offsets = compare_history_databases(
            backup_dir / HISTORY_DB_NAME,
            history_path,
            backup_dir / db_name,
            backup_dir,
            source_provider,
            source_provider,
        )
        if changed_offsets:
            raise MigrationError("thread-history offsets differ after restoration")
        backup_history_stat = (backup_dir / HISTORY_DB_NAME).stat()
        restored_history_stat = history_path.stat()
        if stat.S_IMODE(backup_history_stat.st_mode) != stat.S_IMODE(
            restored_history_stat.st_mode
        ):
            raise MigrationError("thread-history SQLite mode differs after restoration")
        if (backup_history_stat.st_uid, backup_history_stat.st_gid) != (
            restored_history_stat.st_uid,
            restored_history_stat.st_gid,
        ):
            raise MigrationError(
                "thread-history SQLite ownership differs after restoration"
            )
        if backup_history_stat.st_mtime_ns != restored_history_stat.st_mtime_ns:
            raise MigrationError(
                "thread-history SQLite mtime differs after restoration"
            )
    elif history_path.exists() or history_path.is_symlink():
        raise MigrationError(
            "thread-history database should be absent after restoration"
        )
    return RestorationReport(
        rollout_files_restored=len(backup_paths),
        config_restored=config_restored,
        sqlite_tables_checked=tables,
        sqlite_rows_checked=rows,
        history_database_restored=history_database_restored,
        history_sqlite_tables_checked=history_tables,
        history_sqlite_rows_checked=history_rows,
    )


@progress.phase("Restore original state")
def restore_original_state(
    *,
    backup_dir: Path,
    codex_home: Path,
    sqlite_home: Path,
    manifest: dict[str, Any],
) -> RestorationReport:
    ensure_supported_storage(codex_home, sqlite_home)
    ensure_supported_storage(backup_dir, backup_dir)
    verify_recoverable_state(
        backup_dir=backup_dir,
        codex_home=codex_home,
        sqlite_home=sqlite_home,
        manifest=manifest,
    )
    backup_paths = {
        relative_rollout_path(backup_dir, path): path
        for path in rollout_paths(backup_dir)
    }
    current_paths = {
        relative_rollout_path(codex_home, path): path
        for path in rollout_paths(codex_home)
    }
    if backup_paths.keys() != current_paths.keys():
        raise MigrationError(
            "refusing restoration because the rollout file set has changed"
        )

    config_current = codex_home / "config.toml"
    config_expected = manifest["artifacts"]["config"]["present"]
    if config_current.is_symlink() or (
        config_current.exists() and not config_current.is_file()
    ):
        raise MigrationError(
            f"refusing unsafe config restore destination: {config_current}"
        )
    if not config_expected and config_current.exists():
        raise MigrationError(
            "refusing restoration because config.toml was absent from the backup"
        )

    history_descriptor = history_database_descriptor(manifest)
    history_path = sqlite_home / HISTORY_DB_NAME
    history_expected = history_descriptor["present"]
    if history_path.is_symlink() or (
        history_path.exists() and not history_path.is_file()
    ):
        raise MigrationError(
            f"refusing unsafe thread-history restore destination: {history_path}"
        )
    if not history_expected and history_path.exists():
        raise MigrationError(
            "refusing restoration because the thread-history database was absent "
            "from the backup"
        )

    db_name = manifest["state_db_name"]
    database_path = sqlite_home / db_name
    validate_state_db_path(database_path)
    database_is_original = False
    try:
        compare_sqlite_databases(
            backup_dir / db_name,
            database_path,
            manifest["source_provider"],
            manifest["source_provider"],
        )
        expected_database_metadata = {
            key: manifest["artifacts"]["database"][key]
            for key in ("mode", "uid", "gid", "mtime_ns")
        }
        database_is_original = (
            capture_file_metadata(database_path) == expected_database_metadata
        )
    except (MigrationError, sqlite3.Error):
        pass

    history_is_original = not history_expected
    if history_expected:
        try:
            _, _, changed_offsets = compare_history_databases(
                backup_dir / HISTORY_DB_NAME,
                history_path,
                backup_dir / db_name,
                backup_dir,
                manifest["source_provider"],
                manifest["source_provider"],
            )
            expected_history_metadata = {
                key: history_descriptor[key]
                for key in ("mode", "uid", "gid", "mtime_ns")
            }
            history_is_original = (
                changed_offsets == 0
                and capture_file_metadata(history_path) == expected_history_metadata
            )
        except (MigrationError, sqlite3.Error):
            history_is_original = False

    for relative, source in backup_paths.items():
        restore_file_from_backup(source, current_paths[relative])

    config_backup = backup_dir / "config.toml"
    if config_expected:
        restore_file_from_backup(config_backup, config_current)

    if not database_is_original:
        restore_database_from_backup(
            backup_dir / db_name,
            database_path,
        )
    if history_expected and not history_is_original:
        restore_database_from_backup(
            backup_dir / HISTORY_DB_NAME,
            history_path,
        )
    return verify_restored_state(
        backup_dir=backup_dir,
        codex_home=codex_home,
        sqlite_home=sqlite_home,
        manifest=manifest,
    )


def restore_from_backup(
    *,
    backup_dir: Path,
    codex_home: Path | None = None,
    sqlite_home: Path | None = None,
    confirm_stopped: bool,
) -> RestorationReport:
    if not confirm_stopped:
        raise MigrationError("--confirm-codex-stopped is required for restoration")
    backup_dir = backup_dir.resolve()
    manifest = load_backup_manifest(backup_dir)
    status = manifest.get("status")
    if status not in {"complete", "prepared", "restoring"}:
        raise MigrationError("backup manifest is not in a restorable state")
    codex_home = (codex_home or Path(manifest["codex_home"])).resolve()
    sqlite_home = (sqlite_home or Path(manifest["sqlite_home"])).resolve()
    if not codex_home.is_dir() or not sqlite_home.is_dir():
        raise MigrationError("recorded Codex or SQLite state directory is missing")
    for state_root in {codex_home, sqlite_home}:
        if (
            backup_dir == state_root
            or backup_dir.is_relative_to(state_root)
            or state_root.is_relative_to(backup_dir)
        ):
            raise MigrationError("backup directory must be outside Codex state")
    open_processes = find_processes_with_open_state(codex_home, sqlite_home)
    if open_processes:
        rendered = ", ".join(str(pid) for pid in open_processes)
        raise MigrationError(
            f"processes still have Codex state open (PIDs: {rendered}); stop them first"
        )
    if status == "complete":
        # Before the first undo attempt, require the exact completed migration
        # so later Codex activity cannot be overwritten.
        verify_against_backup(
            backup_dir=backup_dir,
            codex_home=codex_home,
            sqlite_home=sqlite_home,
        )
    else:
        # A hard process interruption can leave a prepared migration or a
        # restore half complete. Resume only if each artifact is one of the two
        # exact states already proven by this backup.
        verify_recoverable_state(
            backup_dir=backup_dir,
            codex_home=codex_home,
            sqlite_home=sqlite_home,
            manifest=manifest,
        )

    manifest["status"] = "restoring"
    manifest["restoration_started_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    try:
        write_json_atomic(backup_dir / MANIFEST_NAME, manifest)
    except (OSError, ValueError) as exc:
        raise MigrationError(
            "could not mark the backup as restoring; no restoration was attempted"
        ) from exc

    report = restore_original_state(
        backup_dir=backup_dir,
        codex_home=codex_home,
        sqlite_home=sqlite_home,
        manifest=manifest,
    )
    manifest["status"] = "restored"
    manifest["restored_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    manifest["restoration_report"] = dataclasses.asdict(report)
    try:
        write_json_atomic(backup_dir / MANIFEST_NAME, manifest)
    except (OSError, ValueError) as exc:
        raise MigrationError(
            "restoration was verified, but the backup manifest could not be updated"
        ) from exc
    return report


def apply_migration(
    *,
    codex_home: Path,
    sqlite_home: Path,
    backup_dir: Path,
    source_provider: str,
    target_provider: str,
    migrate_config: bool,
    confirm_stopped: bool,
) -> VerificationReport:
    if not confirm_stopped:
        raise MigrationError("--confirm-codex-stopped is required with --apply")
    resolved_backup = backup_dir.resolve()
    for state_root in {codex_home.resolve(), sqlite_home.resolve()}:
        if resolved_backup == state_root or resolved_backup.is_relative_to(state_root):
            raise MigrationError(
                f"backup directory must be outside Codex state: {backup_dir}"
            )
    utility_root = Path(__file__).resolve().parent
    if resolved_backup == utility_root or resolved_backup.is_relative_to(utility_root):
        raise MigrationError(
            "backup directory must be outside the migration utility checkout: "
            f"{backup_dir}"
        )
    open_processes = find_processes_with_open_state(codex_home, sqlite_home)
    if open_processes:
        rendered = ", ".join(str(pid) for pid in open_processes)
        raise MigrationError(
            f"processes still have Codex state open (PIDs: {rendered}); stop them first"
        )

    ensure_supported_storage(codex_home, sqlite_home)
    db_path = sqlite_home / STATE_DB_NAME
    history_db_path = sqlite_home / HISTORY_DB_NAME
    config_path = codex_home / "config.toml"
    config_before = config_path.read_bytes() if config_path.is_file() else None
    config_after = None
    if migrate_config:
        if config_before is None:
            raise MigrationError("config.toml is missing")
        config_after = plan_config_migration(
            codex_home, config_before, source_provider, target_provider
        )

    require_writable_database(db_path)
    rollout_analysis, rollout_migration = analyze_migratable_rollouts(
        codex_home,
        db_path,
        source_provider,
        target_provider,
        allow_paginated=history_db_path.is_file(),
    )
    database_analysis = analyze_database(db_path, source_provider)
    history_database_analysis, history_offset_updates = inspect_history_database(
        history_db_path,
        db_path,
        codex_home,
        source_provider,
        target_provider,
        rollout_migration=rollout_migration,
    )
    if history_database_analysis.present:
        require_writable_database(
            history_db_path,
            label="thread-history database",
        )

    manifest = create_backup(
        codex_home=codex_home,
        sqlite_home=sqlite_home,
        db_path=db_path,
        history_db_path=history_db_path,
        backup_dir=backup_dir,
        source_provider=source_provider,
        target_provider=target_provider,
        rollout_analysis=rollout_analysis,
        database_analysis=database_analysis,
        history_database_analysis=history_database_analysis,
        migrate_config=config_after is not None,
    )

    with progress.phase(
        "Recheck source after backup",
        total_files=len(rollout_migration.files),
        total_bytes=sum(f.size for f in rollout_migration.files.values()),
    ) as status:
        current_paths = {
            relative_rollout_path(codex_home, path): path
            for path in rollout_paths(codex_home)
        }
        if current_paths.keys() != rollout_migration.files.keys():
            raise MigrationError(
                "rollout state changed after backup; no migration was applied"
            )

        def recheck_rollout(item):
            relative, path = item
            if (
                sha256_file(path) != rollout_analysis.file_hashes[relative]
                or capture_file_metadata(path)
                != rollout_analysis.file_metadata[relative]
            ):
                raise MigrationError(
                    "rollout state changed after backup; no migration was applied"
                )
            workers.advance(rollout_migration.files[relative].size, files=1)

        workers.for_each(recheck_rollout, current_paths.items(), status)

    current_rollout_migration = rollout_migration
    if config_before is None:
        if config_path.exists() or config_path.is_symlink():
            raise MigrationError(
                "config.toml appeared after backup; no migration was applied"
            )
    elif config_path.read_bytes() != config_before:
        raise MigrationError(
            "config.toml changed after backup; no migration was applied"
        )
    elif capture_file_metadata(config_path) != {
        key: manifest["artifacts"]["config"][key]
        for key in ("mode", "uid", "gid", "mtime_ns")
    }:
        raise MigrationError(
            "config.toml metadata changed after backup; no migration was applied"
        )
    if analyze_database(db_path, source_provider) != database_analysis:
        raise MigrationError(
            "SQLite state changed after backup; no migration was applied"
        )
    compare_sqlite_databases(
        backup_dir / db_path.name,
        db_path,
        source_provider,
        source_provider,
    )
    if capture_file_metadata(db_path) != {
        key: manifest["artifacts"]["database"][key]
        for key in ("mode", "uid", "gid", "mtime_ns")
    }:
        raise MigrationError(
            "SQLite metadata changed after backup; no migration was applied"
        )
    current_history_analysis, current_history_updates = inspect_history_database(
        history_db_path,
        db_path,
        codex_home,
        source_provider,
        target_provider,
        rollout_migration=current_rollout_migration,
    )
    if (
        current_history_analysis != history_database_analysis
        or current_history_updates != history_offset_updates
    ):
        raise MigrationError(
            "thread-history state changed after backup; no migration was applied"
        )
    if history_database_analysis.present:
        compare_history_databases(
            backup_dir / HISTORY_DB_NAME,
            history_db_path,
            backup_dir / db_path.name,
            backup_dir,
            source_provider,
            source_provider,
            rollout_migration=dataclasses.replace(
                rollout_migration,
                files={
                    name: dataclasses.replace(file, edits=())
                    for name, file in rollout_migration.files.items()
                },
            ),
        )
        if capture_file_metadata(history_db_path) != {
            key: manifest["artifacts"]["history_database"][key]
            for key in ("mode", "uid", "gid", "mtime_ns")
        }:
            raise MigrationError(
                "thread-history SQLite metadata changed after backup; "
                "no migration was applied"
            )

    if config_after is not None:
        # Profiles can be created or changed while the backup is being taken.
        validate_config_profiles(codex_home, config_before, config_after)

    # Roll back on ordinary failures and operator interruption alike. Once the
    # first atomic replacement starts, returning without attempting recovery
    # would leave the cross-file migration in an unknown partial state.
    try:
        changed_files = 0
        with progress.phase(
            "Write migrated rollouts",
            total_files=len(rollout_migration.files),
            total_bytes=sum(f.size for f in rollout_migration.files.values()),
        ) as status:
            for relative, file in rollout_migration.files.items():
                path = codex_home / relative
                if file.edits:
                    atomic_write(path, file.chunks(path), preserve_mtime=True)
                    changed_files += 1
                elif sha256_file(path) != file.digest:
                    raise MigrationError(f"rollout changed since preflight: {path}")
                status.advance(file.size, files=1)
        if changed_files != rollout_analysis.files_requiring_changes:
            raise MigrationError("changed rollout file count does not match preflight")

        connection = sqlite3.connect(db_path)
        try:
            connection.execute("PRAGMA busy_timeout = 5000")
            connection.execute("BEGIN IMMEDIATE")
            current_rows = connection.execute(
                "SELECT count(*) FROM threads WHERE model_provider = ?",
                (source_provider,),
            ).fetchone()[0]
            if current_rows != database_analysis.rows_from_provider:
                connection.rollback()
                raise MigrationError("SQLite provider row count changed after backup")
            updated_rows = connection.execute(
                "UPDATE threads SET model_provider = ? WHERE model_provider = ?",
                (target_provider, source_provider),
            ).rowcount
            if updated_rows != database_analysis.rows_from_provider:
                connection.rollback()
                raise MigrationError("SQLite update count does not match preflight")
            connection.commit()
        finally:
            connection.close()
        os.chmod(db_path, manifest["artifacts"]["database"]["mode"])

        history_offsets_changed = 0
        if history_database_analysis.present:
            history_offsets_changed = apply_history_offset_updates(
                history_db_path,
                history_offset_updates,
            )
            if (
                history_offsets_changed
                != history_database_analysis.offset_fields_to_update
            ):
                raise MigrationError(
                    "thread-history offset update count does not match preflight"
                )
            os.chmod(
                history_db_path,
                manifest["artifacts"]["history_database"]["mode"],
            )

        if config_after is not None:
            atomic_write(config_path, config_after, preserve_mtime=False)
            validate_config_profiles(codex_home, config_before, config_after)

        report = verify_against_backup(
            backup_dir=backup_dir,
            codex_home=codex_home,
            sqlite_home=sqlite_home,
        )
        manifest["status"] = "complete"
        manifest["completed_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        manifest["verification_report"] = dataclasses.asdict(report)
        write_json_atomic(backup_dir / MANIFEST_NAME, manifest)
        return report
    except BaseException as migration_error:
        try:
            rollback_report = restore_original_state(
                backup_dir=backup_dir,
                codex_home=codex_home,
                sqlite_home=sqlite_home,
                manifest=manifest,
            )
        except BaseException as rollback_error:
            raise MigrationError(
                "migration failed and automatic rollback also failed; "
                f"migration error: {progress.exception_message(migration_error)}; rollback error: {progress.exception_message(rollback_error)}"
            ) from rollback_error
        manifest["status"] = "rolled_back"
        manifest["rolled_back_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        manifest["rollback_report"] = dataclasses.asdict(rollback_report)
        try:
            write_json_atomic(backup_dir / MANIFEST_NAME, manifest)
        except BaseException as manifest_error:
            raise MigrationError(
                "migration failed and automatic rollback was verified, but the "
                "backup manifest could not be updated; "
                f"migration error: {progress.exception_message(migration_error)}"
            ) from manifest_error
        raise


def default_codex_home() -> Path:
    configured = os.environ.get("CODEX_HOME")
    return Path(configured) if configured else Path.home() / ".codex"


def configured_sqlite_path(value: Any, config_path: Path) -> Path:
    if not isinstance(value, str) or not value.strip() or "\x00" in value:
        raise MigrationError(
            f"invalid sqlite_home in {config_path}; pass --sqlite-home explicitly"
        )
    path = Path(value)
    # Codex expands ~ and ~/..., but not shell-style ~other-user paths.
    if (
        value == "~"
        or value.startswith("~/")
        or (os.name == "nt" and value.startswith("~\\"))
    ):
        path = path.expanduser()
    return (path if path.is_absolute() else config_path.parent / path).resolve()


def resolve_sqlite_home(
    codex_home: Path,
    *,
    explicit: Path | None = None,
    profile: str | None = None,
) -> tuple[Path, str]:
    if explicit is not None:
        return explicit.resolve(), "--sqlite-home"
    config_path = codex_home / "config.toml"
    base = read_config_file(config_path)
    if "profile" in base or "profiles" in base:
        raise MigrationError(
            "legacy profile configuration makes SQLite discovery ambiguous; pass --sqlite-home"
        )
    configured = os.environ.get("CODEX_SQLITE_HOME")
    if "sqlite_home" in base:
        selected = configured_sqlite_path(base["sqlite_home"], config_path)
        source = str(config_path)
    elif configured:
        selected = configured_sqlite_path(configured, Path.cwd() / "CODEX_SQLITE_HOME")
        source = "CODEX_SQLITE_HOME"
    else:
        selected = codex_home.resolve()
        source = "Codex home"

    # Managed settings can override user files. Do not guess their effective
    # precedence (or the contents of cloud/MDM policy) in this offline utility.
    if os.name == "nt":
        program_data = os.environ.get("ProgramData")
        system_root = Path(program_data) / "OpenAI" / "Codex" if program_data else None
    else:
        system_root = Path("/etc/codex")
    external_paths = [codex_home / "managed_config.toml"]
    if system_root is not None:
        external_paths.extend(
            system_root / name
            for name in ("config.toml", "requirements.toml", "managed_config.toml")
        )
    for path in external_paths:
        if "sqlite_home" in read_config_file(path):
            raise MigrationError(
                f"SQLite location also configured in {path}; pass --sqlite-home explicitly"
            )

    if profile is not None:
        if not re.fullmatch(r"[A-Za-z0-9_-]+", profile):
            raise MigrationError("--profile must be a bare profile name")
        path = codex_home / f"{profile}.config.toml"
        values = read_config_file(path, required=True)
        if "sqlite_home" in values:
            selected = configured_sqlite_path(values["sqlite_home"], path)
            source = str(path)
    else:
        for path, values in read_profile_configs(codex_home).items():
            if (
                "sqlite_home" in values
                and configured_sqlite_path(values["sqlite_home"], path) != selected
            ):
                raise MigrationError(
                    f"profile {path.name} uses a different SQLite directory; "
                    "select --profile or pass --sqlite-home explicitly"
                )
    return selected, source


def default_sqlite_home(codex_home: Path) -> Path:
    return resolve_sqlite_home(codex_home)[0]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", action="version", version=TOOL_VERSION)
    parser.add_argument(
        "--codex-home",
        type=Path,
        default=default_codex_home(),
        help="Codex state directory (default: CODEX_HOME or ~/.codex)",
    )
    parser.add_argument(
        "--sqlite-home",
        type=Path,
        help=(
            "directory containing state_5.sqlite "
            "(default: user/profile sqlite_home, CODEX_SQLITE_HOME, or Codex state)"
        ),
    )
    parser.add_argument(
        "--profile",
        help=(
            "Codex profile name for SQLite discovery (<name>.config.toml); "
            "profile files are not edited"
        ),
    )
    parser.add_argument(
        "--from-provider",
        required=True,
        help="custom model_provider ID currently stored in sessions",
    )
    parser.add_argument(
        "--to-provider",
        default="openai",
        help="replacement provider ID (default: openai)",
    )
    parser.add_argument(
        "--migrate-config",
        action="store_true",
        help="convert a compatible custom provider to openai_base_url; skip with a warning if already openai",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="write the migration; without this flag Codex records are unchanged",
    )
    parser.add_argument(
        "--backup-dir",
        type=Path,
        help="new private backup directory outside Codex state and this checkout",
    )
    parser.add_argument(
        "--confirm-codex-stopped",
        action="store_true",
        help="confirm all Codex CLI, extension, and app-server processes are stopped",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="emit machine-readable JSON",
    )
    progress.add_arguments(parser)
    return parser


def emit(value: dict[str, Any], as_json: bool) -> None:
    if as_json:
        print(json.dumps(value, indent=2, sort_keys=True))
        return
    for key, item in value.items():
        print(f"{key}: {item}")


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        with (
            progress.reporting(args),
            progress.phase("Apply migration" if args.apply else "Dry run") as status,
        ):
            result = run(args)
            status.outcome = "failed" if result else "completed"
            return result
    except (MigrationError, OSError, ValueError, sqlite3.Error, MemoryError) as exc:
        progress.error(progress.exception_message(exc))
        return 1


def run(args: argparse.Namespace) -> int:
    try:
        codex_home = args.codex_home.resolve()
        sqlite_home, sqlite_home_source = resolve_sqlite_home(
            codex_home, explicit=args.sqlite_home, profile=args.profile
        )
        if args.from_provider == args.to_provider:
            raise MigrationError("source and target provider are identical")
        if not codex_home.is_dir():
            raise MigrationError(f"Codex home does not exist: {codex_home}")
        ensure_supported_storage(codex_home, sqlite_home)
        db_path = sqlite_home / STATE_DB_NAME
        history_db_path = sqlite_home / HISTORY_DB_NAME

        if args.apply:
            if args.backup_dir is None:
                raise MigrationError("--backup-dir is required with --apply")
            report = apply_migration(
                codex_home=codex_home,
                sqlite_home=sqlite_home,
                backup_dir=args.backup_dir.resolve(),
                source_provider=args.from_provider,
                target_provider=args.to_provider,
                migrate_config=args.migrate_config,
                confirm_stopped=args.confirm_codex_stopped,
            )
            emit(
                {
                    "result": "migration and verification passed",
                    "backup_dir": str(args.backup_dir.resolve()),
                    "sqlite_home": str(sqlite_home),
                    "sqlite_home_source": sqlite_home_source,
                    **dataclasses.asdict(report),
                },
                args.json,
            )
            return 0

        config_status = "not requested"
        if args.migrate_config:
            config_path = codex_home / "config.toml"
            original = config_path.read_bytes()
            transformed = plan_config_migration(
                codex_home, original, args.from_provider, args.to_provider
            )
            config_status = (
                "eligible" if transformed is not None else "skipped; already openai"
            )

        rollout, rollout_plan = analyze_migratable_rollouts(
            codex_home,
            db_path,
            args.from_provider,
            args.to_provider,
            allow_paginated=history_db_path.is_file(),
        )
        database = analyze_database(db_path, args.from_provider)
        history_database, _ = inspect_history_database(
            history_db_path,
            db_path,
            codex_home,
            args.from_provider,
            args.to_provider,
            rollout_migration=rollout_plan,
        )
        emit(
            {
                "result": "dry run; no Codex records changed",
                "sqlite_note": (
                    "read-only WAL access may create SQLite coordination sidecars"
                ),
                "codex_home": str(codex_home),
                "sqlite_home": str(sqlite_home),
                "sqlite_home_source": sqlite_home_source,
                "from_provider": args.from_provider,
                "to_provider": args.to_provider,
                "config_migration": config_status,
                **{
                    key: value
                    for key, value in dataclasses.asdict(rollout).items()
                    if key not in {"file_hashes", "file_metadata"}
                },
                **dataclasses.asdict(database),
                **{
                    f"history_{key}": value
                    for key, value in dataclasses.asdict(history_database).items()
                },
            },
            args.json,
        )
        return 0
    except KeyboardInterrupt:
        progress.error("interrupted; active file workers have stopped")
        return 130
    except (MigrationError, OSError, ValueError, sqlite3.Error, MemoryError) as exc:
        progress.error(progress.exception_message(exc))
        if args.apply and args.backup_dir and args.backup_dir.exists():
            progress.error(
                f"backup/recovery data may be available at: {args.backup_dir.resolve()}"
            )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
