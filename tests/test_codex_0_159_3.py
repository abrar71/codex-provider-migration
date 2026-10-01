"""Offline migration against the SQL schemas shipped in Codex CLI 0.159.3."""

from __future__ import annotations

import io
import json
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock

from test_migration import MigrationFixture

import migrate
import restore
import verify


SCHEMAS = Path(__file__).parent / "fixtures" / "codex_0_159_3"
ERROR = {"message": "Synthetic interruption", "codex_error_info": "too_many_denials"}


class Codex1593Tests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="migration-codex-1593-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    def prepare(self, name):
        fixture = MigrationFixture(self.root / name)
        fixture.enable_history_base()
        config = fixture.codex_home / "config.toml"
        config.write_text(
            config.read_text() + '\n[auto_review]\ncircuit_break_action = "strict"\n'
        )

        # Use the release's full schemas instead of the minimal shared fixture.
        for db_name, schema, tables in (
            (migrate.STATE_DB_NAME, "state.sql", ("threads",)),
            (migrate.HISTORY_DB_NAME, "thread_history.sql", (
                "thread_history_projection_state", "thread_turns", "thread_items",
            )),
        ):
            path = fixture.sqlite_home / db_name
            db = sqlite3.connect(path)
            db.row_factory = sqlite3.Row
            original = {
                table: [dict(row) for row in db.execute(f'SELECT * FROM "{table}"')]
                for table in tables
            }
            db.close()
            path.unlink()
            with sqlite3.connect(path) as db:
                db.execute("PRAGMA foreign_keys = ON")
                db.executescript((SCHEMAS / schema).read_text())
                for table, rows in original.items():
                    for row in rows:
                        if table == "threads":
                            row.update(
                                created_at=1_700_000_000, updated_at=1_700_000_001,
                                source="cli", cwd="/example/project",
                                sandbox_policy='{"type":"read-only"}',
                                approval_mode="on-request", cli_version="0.159.3",
                                name="Preserved name", is_pinned=1,
                                creator_user_id="synthetic-user",
                                creator_account_id="synthetic-account",
                            )
                        columns = ", ".join(f'"{key}"' for key in row)
                        placeholders = ", ".join("?" for _ in row)
                        db.execute(
                            f'INSERT INTO "{table}" ({columns}) '
                            f'VALUES ({placeholders})',
                            tuple(row.values()),
                        )

        # Append release-era payloads after the referenced parent prefix. The
        # history_base boundary stays unchanged while projection checkpoints move.
        with sqlite3.connect(fixture.sqlite_home / migrate.HISTORY_DB_NAME) as db:
            for path in migrate.rollout_paths(fixture.codex_home):
                lines = path.read_bytes().splitlines(keepends=True)
                meta = json.loads(lines[0])["payload"]
                if meta.get("history_mode") != "paginated":
                    continue
                ordinal = json.loads(lines[-1])["ordinal"] + 1
                payloads = [
                    ("compacted", {
                        "message": "Preserved summary",
                        "guardian_history": [{
                            "type": "message", "role": "user",
                            "content": [
                                {"type": "input_text", "text": "Preserved evidence"}
                            ],
                            "guardian_metadata": {"client_authored": True},
                        }],
                    }),
                    ("event_msg", {
                        "type": "turn_aborted", "turn_id": "turn-1",
                        "reason": "interrupted", "error": ERROR,
                        "started_at": 1_700_000_000,
                        "completed_at": 1_700_000_001, "duration_ms": 1000,
                    }),
                ]
                for index, (kind, payload) in enumerate(payloads):
                    lines.append(MigrationFixture._line({
                        "timestamp": "2026-09-30T12:00:00Z", "ordinal": ordinal + index,
                        "type": kind, "payload": payload,
                    }))
                path.write_bytes(b"".join(lines))
                db.execute(
                    "UPDATE thread_history_projection_state "
                    "SET next_rollout_byte_offset=?, "
                    "next_rollout_ordinal=? WHERE thread_id=?",
                    (path.stat().st_size, ordinal + len(payloads), meta["id"]),
                )
                db.execute(
                    "UPDATE thread_turns SET status='interrupted', error_json=?, "
                    "rollout_end_ordinal=?, rollout_end_byte_offset=? "
                    "WHERE thread_id=?",
                    (json.dumps(ERROR), ordinal + 1, path.stat().st_size, meta["id"]),
                )
                db.execute(
                    "UPDATE thread_items SET updated_at_ordinal=rollout_ordinal, "
                    "started_at_ms=1700000000000, completed_at_ms=1700000001000 "
                    "WHERE thread_id=?", (meta["id"],),
                )
        return fixture

    def snapshot(self, fixture):
        result = {
            str(path.relative_to(fixture.codex_home)): path.read_bytes()
            for path in migrate.rollout_paths(fixture.codex_home)
        }
        result["config.toml"] = (fixture.codex_home / "config.toml").read_bytes()
        for name in (migrate.STATE_DB_NAME, migrate.HISTORY_DB_NAME):
            with sqlite3.connect(fixture.sqlite_home / name) as db:
                result[name] = tuple(db.iterdump())
        return result

    def run_cli(self, module, args, workers):
        stdout, stderr = io.StringIO(), io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            status = module.main([
                *args, "--workers", str(workers), "--progress", "off", "--json"
            ])
        self.assertEqual(status, 0, stderr.getvalue())
        return json.loads(stdout.getvalue())

    def test_release_schema_dry_run_apply_verify_restore(self):
        for workers in (1, 3):
            with self.subTest(workers=workers):
                fixture = self.prepare(str(workers))
                before = self.snapshot(fixture)
                args = [
                    "--codex-home", str(fixture.codex_home),
                    "--sqlite-home", str(fixture.sqlite_home),
                    "--from-provider", "proxy", "--migrate-config",
                ]
                dry_run = self.run_cli(migrate, args, workers)
                self.assertEqual(dry_run["rows_from_provider"], 2)
                self.assertEqual(self.snapshot(fixture), before)
                with mock.patch.object(
                    migrate, "find_processes_with_open_state", return_value=[]
                ):
                    applied = self.run_cli(migrate, [
                        *args, "--apply", "--confirm-codex-stopped",
                        "--backup-dir", str(fixture.backup_dir),
                    ], workers)
                    self.assertEqual(applied["sqlite_thread_rows_changed"], 2)
                    self.assertEqual(applied["history_base_offsets_changed"], 1)
                    self.assertGreater(applied["history_offset_fields_changed"], 0)
                    verified = self.run_cli(
                        verify, ["--backup-dir", str(fixture.backup_dir)], workers
                    )
                    self.assertEqual(
                        verified["history_offset_fields_changed"],
                        applied["history_offset_fields_changed"],
                    )
                    # New error details and Guardian evidence must stay byte-exact.
                    for path in migrate.rollout_paths(fixture.codex_home):
                        key = str(path.relative_to(fixture.codex_home))
                        self.assertEqual(
                            path.read_bytes().splitlines()[-2:],
                            before[key].splitlines()[-2:],
                        )
                    self.run_cli(restore, [
                        "--backup-dir", str(fixture.backup_dir),
                        "--confirm-codex-stopped",
                    ], workers)
                self.assertEqual(self.snapshot(fixture), before)

    def test_release_schema_rolls_back_when_history_update_fails(self):
        fixture = self.prepare("rollback")
        before = self.snapshot(fixture)
        with (
            mock.patch.object(
                migrate, "find_processes_with_open_state", return_value=[]
            ),
            mock.patch.object(
                migrate, "apply_history_offset_updates",
                side_effect=RuntimeError("injected failure"),
            ),
            self.assertRaisesRegex(RuntimeError, "injected failure"),
        ):
            migrate.apply_migration(
                codex_home=fixture.codex_home, sqlite_home=fixture.sqlite_home,
                backup_dir=fixture.backup_dir,
                source_provider="proxy", target_provider="openai",
                migrate_config=True, confirm_stopped=True,
            )
        self.assertEqual(self.snapshot(fixture), before)
        self.assertEqual(
            migrate.load_backup_manifest(fixture.backup_dir)["status"], "rolled_back"
        )
