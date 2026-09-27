from __future__ import annotations

import io
import json
import os
from pathlib import Path
import shutil
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest import mock

from test_migration import MigrationFixture
import migrate


PARENT = "11111111-1111-4111-8111-111111111111"
REVERT_ONE = "22222222-2222-4222-8222-222222222222"
REVERT_TWO = "33333333-3333-4333-8333-333333333333"
CHILD = "44444444-4444-4444-8444-444444444444"
LATER_CHILD = "55555555-5555-4555-8555-555555555555"


class CompatibilityTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory(prefix="migration-compatibility-")
        self.addCleanup(temporary.cleanup)
        self.fixture = MigrationFixture(Path(temporary.name))
        self.home = self.fixture.codex_home

    def apply(self, *, config: bool = True) -> migrate.VerificationReport:
        with mock.patch.object(migrate, "find_processes_with_open_state", return_value=[]):
            return migrate.apply_migration(
                codex_home=self.home,
                sqlite_home=self.fixture.sqlite_home,
                backup_dir=self.fixture.backup_dir,
                source_provider="proxy",
                target_provider="openai",
                migrate_config=config,
                confirm_stopped=True,
            )

    def restore(self) -> None:
        with mock.patch.object(migrate, "find_processes_with_open_state", return_value=[]):
            migrate.restore_from_backup(
                backup_dir=self.fixture.backup_dir, confirm_stopped=True
            )

    def prepare_history(self) -> None:
        self.fixture.enable_paginated_history()
        self.fixture.proxy_rollout.unlink()
        with sqlite3.connect(self.home / migrate.STATE_DB_NAME) as db:
            db.execute("DELETE FROM threads WHERE id = 'proxy-thread'")
        with sqlite3.connect(self.home / migrate.HISTORY_DB_NAME) as db:
            for table in ("thread_items", "thread_turns", "thread_history_projection_state"):
                db.execute(f"DELETE FROM {table}")

    def write_rollout(
        self,
        thread_id: str,
        rollout_id: str,
        *,
        base: dict | None = None,
        archived: bool = False,
    ) -> Path:
        initial = base["end_ordinal_exclusive"] if base else 0
        records = [
            {"type": "session_meta", "payload": {
                "id": thread_id, "session_id": thread_id,
                "model_provider": "proxy", "history_mode": "paginated",
                "history_base": base,
            }},
            {"type": "event_msg", "payload": {
                "type": "thread_settings_applied",
                "thread_settings": {"model_provider_id": "proxy", "model": "gpt-test"},
            }},
            {"type": "event_msg", "payload": {
                "type": "task_started", "turn_id": "turn-1", "model_context_window": 1000,
            }},
            {"type": "event_msg", "payload": {
                "type": "task_complete", "turn_id": "turn-1", "last_agent_message": "done",
            }},
        ]
        for index, record in enumerate(records):
            record["ordinal"] = initial + index
            record["timestamp"] = "2026-09-20T12:00:00Z"
        root = self.home / "archived_sessions" if archived else self.fixture.sessions
        root.mkdir(exist_ok=True)
        ids = thread_id if thread_id == rollout_id else f"{thread_id}_{rollout_id}"
        path = root / f"rollout-2026-09-20T12-00-00-{ids}.jsonl"
        lines = [MigrationFixture._line(record) for record in records]
        path.write_bytes(b"".join(lines))
        with sqlite3.connect(self.home / migrate.STATE_DB_NAME) as db:
            db.execute(
                "INSERT OR REPLACE INTO threads VALUES (?, 'proxy', 'Kept title', ?, 'paginated')",
                (thread_id, str(path)),
            )
        self.project(rollout_id, lines, initial + 2, initial + 3, initial + 4)
        return path

    def project(
        self, rollout_id: str, lines: list[bytes], start: int, end: int, next_: int
    ) -> None:
        offset = 0
        start_offset = end_offset = None
        for line in lines:
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                record = {}
            if record.get("ordinal") == start:
                start_offset = offset
            offset += len(line)
            if record.get("ordinal") == end:
                end_offset = offset
        with sqlite3.connect(self.home / migrate.HISTORY_DB_NAME) as db:
            db.execute(
                "INSERT OR REPLACE INTO thread_history_projection_state VALUES (?, ?, ?)",
                (rollout_id, offset, next_),
            )
            db.execute(
                "INSERT OR REPLACE INTO thread_turns(thread_id, turn_id, rollout_ordinal, status, "
                "rollout_byte_offset, rollout_end_ordinal, rollout_end_byte_offset) "
                "VALUES (?, 'turn-1', ?, 'completed', ?, ?, ?)",
                (rollout_id, start, start_offset, end, end_offset),
            )

    @staticmethod
    def reference(path: Path, rollout_id: str) -> dict:
        lines = path.read_bytes().splitlines(keepends=True)
        return {
            "thread_id": rollout_id,
            "end_ordinal_exclusive": json.loads(lines[-1])["ordinal"] + 1,
            "end_byte_offset": sum(map(len, lines)),
        }

    def analyze(self) -> migrate.HistoryDatabaseAnalysis:
        return migrate.analyze_history_database(
            self.home / migrate.HISTORY_DB_NAME, self.home / migrate.STATE_DB_NAME,
            self.home, "proxy", "openai",
        )

    def test_repeated_reverts_and_forks_use_physical_rollouts_and_restore(self) -> None:
        self.prepare_history()
        original = self.write_rollout(PARENT, PARENT, archived=True)
        early_child = self.write_rollout(CHILD, CHILD, base=self.reference(original, PARENT))
        first = self.write_rollout(PARENT, REVERT_ONE, base=self.reference(original, PARENT))
        later_child = self.write_rollout(
            LATER_CHILD, LATER_CHILD, base=self.reference(first, REVERT_ONE)
        )
        second = self.write_rollout(PARENT, REVERT_TWO, base=self.reference(first, REVERT_ONE))
        paths = [original, early_child, first, later_child, second]
        before = {path: path.read_bytes() for path in paths}
        report = self.apply()
        self.assertEqual(report.history_base_offsets_changed, 4)
        self.assertEqual(report.history_offset_fields_changed, 15)
        self.assertEqual(report, migrate.verify_against_backup(backup_dir=self.fixture.backup_dir))
        for child, source in [
            (early_child, original), (first, original),
            (later_child, first), (second, first),
        ]:
            base = json.loads(child.read_bytes().splitlines()[0])["payload"]["history_base"]
            self.assertEqual(base["end_byte_offset"], len(source.read_bytes()))
        with sqlite3.connect(self.home / migrate.HISTORY_DB_NAME) as db:
            for path, rollout_id in zip(
                paths, [PARENT, CHILD, REVERT_ONE, LATER_CHILD, REVERT_TWO]
            ):
                self.assertEqual(db.execute(
                    "SELECT next_rollout_byte_offset FROM thread_history_projection_state "
                    "WHERE thread_id=?",
                    (rollout_id,),
                ).fetchone()[0], len(path.read_bytes()))
        self.restore()
        self.assertEqual(before, {path: path.read_bytes() for path in paths})

    def test_retained_source_does_not_need_a_logical_state_row(self) -> None:
        self.prepare_history()
        original = self.write_rollout(PARENT, PARENT, archived=True)
        self.write_rollout(CHILD, CHILD, base=self.reference(original, PARENT))
        with sqlite3.connect(self.home / migrate.STATE_DB_NAME) as db:
            db.execute("DELETE FROM threads WHERE id=?", (PARENT,))
        self.assertEqual(self.apply().history_base_offsets_changed, 1)
        self.restore()

    def test_reverted_history_rolls_back_after_projection_write_failure(self) -> None:
        self.prepare_history()
        original = self.write_rollout(PARENT, PARENT)
        replacement = self.write_rollout(PARENT, REVERT_ONE, base=self.reference(original, PARENT))
        before = {path: path.read_bytes() for path in (original, replacement)}
        with mock.patch.object(
            migrate, "apply_history_offset_updates", side_effect=RuntimeError("injected")
        ):
            with self.assertRaisesRegex(RuntimeError, "injected"):
                self.apply()
        self.assertEqual(before, {path: path.read_bytes() for path in before})
        self.assertEqual(
            migrate.load_backup_manifest(self.fixture.backup_dir)["status"], "rolled_back"
        )
        self.analyze()

    def test_duplicate_rollout_identity_and_filename_mismatch_are_refused(self) -> None:
        self.prepare_history()
        original = self.write_rollout(PARENT, PARENT)
        archived = self.home / "archived_sessions"
        archived.mkdir()
        duplicate = archived / original.name
        shutil.copyfile(original, duplicate)
        with self.assertRaisesRegex(migrate.MigrationError, "duplicate rollout ID"):
            self.apply()
        self.assertFalse(self.fixture.backup_dir.exists())
        duplicate.unlink()
        original.rename(original.with_name(original.name.replace(PARENT, CHILD)))
        with self.assertRaisesRegex(migrate.MigrationError, "filename disagrees"):
            self.analyze()

    def test_explicit_ordinals_survive_gaps_skipped_lines_and_partial_tail(self) -> None:
        self.prepare_history()
        path = self.write_rollout(PARENT, PARENT)
        records = [json.loads(line) for line in path.read_bytes().splitlines()]
        for record, ordinal in zip(records, [0, 2, 7, 10]):
            record["ordinal"] = ordinal
        skipped = [
            b"\n", b"{broken\n",
            b'{"type":"future_record","ordinal":500,"payload":{}}\n',
            b'{"type":"event_msg","ordinal":500,"payload":{"type":"future_event"}}\n',
            b'{"type":"event_msg","payload":{"type":"token_count"}}\n',
            b'{"type":"event_msg","ordinal":2,"payload":{"type":"token_count"}}\n',
        ]
        lines = [MigrationFixture._line(record) for record in records[:2]] + skipped
        lines += [MigrationFixture._line(record) for record in records[2:]] + [b"\n"]
        tail = b'{"partial":'
        path.write_bytes(b"".join(lines) + tail)
        self.project(PARENT, lines, 7, 10, 11)
        before = path.read_bytes()
        self.assertEqual(self.apply().history_offset_fields_changed, 3)
        for line in skipped:
            self.assertIn(line, path.read_bytes())
        self.assertTrue(path.read_bytes().endswith(tail))
        with sqlite3.connect(self.home / migrate.HISTORY_DB_NAME) as db:
            self.assertEqual(db.execute(
                "SELECT next_rollout_byte_offset, next_rollout_ordinal FROM thread_history_projection_state"
            ).fetchone(), (len(path.read_bytes()) - len(tail), 11))
        self.restore()
        self.assertEqual(path.read_bytes(), before)

    def test_checkpoint_before_gap_keeps_its_exact_physical_boundary(self) -> None:
        self.prepare_history()
        path = self.write_rollout(PARENT, PARENT)
        lines = path.read_bytes().splitlines(keepends=True)
        lines.insert(2, b"\n")
        lines.insert(3, b"\n")
        path.write_bytes(b"".join(lines))
        self.project(PARENT, lines, 2, 3, 4)
        checkpoint = sum(map(len, lines[:3]))
        with sqlite3.connect(self.home / migrate.HISTORY_DB_NAME) as db:
            db.execute(
                "UPDATE thread_history_projection_state SET next_rollout_byte_offset=?, "
                "next_rollout_ordinal=2", (checkpoint,),
            )
        self.apply()
        with sqlite3.connect(self.home / migrate.HISTORY_DB_NAME) as db:
            self.assertEqual(
                db.execute(
                    "SELECT next_rollout_byte_offset FROM thread_history_projection_state"
                ).fetchone()[0], checkpoint + 2,
            )
        self.restore()

    def test_jsonl_offsets_use_lf_boundaries_and_preserve_crlf(self) -> None:
        self.prepare_history()
        path = self.write_rollout(PARENT, PARENT)
        lines = path.read_bytes().splitlines(keepends=True)
        lines = [line[:-1] + b"\r\n" for line in lines]
        # CR within JSON whitespace is not a physical JSONL boundary.
        lines[1] = lines[1].replace(b',"payload"', b',\r"payload"')
        original = b"".join(lines)
        path.write_bytes(original)
        self.project(PARENT, lines, 2, 3, 4)
        self.apply()
        self.restore()
        self.assertEqual(path.read_bytes(), original)

    def test_invalid_checkpoint_does_not_become_valid_by_line_count(self) -> None:
        self.prepare_history()
        path = self.write_rollout(PARENT, PARENT)
        with sqlite3.connect(self.home / migrate.HISTORY_DB_NAME) as db:
            db.execute("UPDATE thread_history_projection_state SET next_rollout_ordinal=99")
        before = path.read_bytes()
        with self.assertRaisesRegex(migrate.MigrationError, "recorded JSONL boundary"):
            self.apply()
        self.assertEqual(path.read_bytes(), before)
        self.assertFalse(self.fixture.backup_dir.exists())

    def test_history_base_uses_explicit_cutoff_across_skipped_lines(self) -> None:
        self.prepare_history()
        source = self.write_rollout(PARENT, PARENT)
        records = [json.loads(line) for line in source.read_bytes().splitlines()]
        for record, ordinal in zip(records, [0, 2, 7, 10]):
            record["ordinal"] = ordinal
        lines = [MigrationFixture._line(record) for record in records]
        lines.insert(2, b"\n")
        source.write_bytes(b"".join(lines))
        self.project(PARENT, lines, 7, 10, 11)
        # There are two physical boundaries with next ordinal 3. Keep the one
        # before the blank line, even though the later boundary has that ordinal.
        cutoff = sum(map(len, lines[:2]))
        child = self.write_rollout(CHILD, CHILD, base={
            "thread_id": PARENT, "end_ordinal_exclusive": 3, "end_byte_offset": cutoff,
        })
        self.apply()
        migrated_base = json.loads(child.read_bytes().splitlines()[0])["payload"]["history_base"]
        self.assertEqual(migrated_base["end_byte_offset"], cutoff + 2)
        self.assertEqual(migrated_base["end_ordinal_exclusive"], 3)
        self.restore()

    def test_profile_dependency_and_endpoint_changes_fail_before_backup(self) -> None:
        profile = self.home / "work.config.toml"
        for content, message in [
            ('model_provider = "proxy"\n', "depends on provider"),
            ('[model_providers.proxy]\nname = "Override"\n', "depends on provider"),
            ('model_provider = "openai"\n', "endpoint inherited"),
            ('openai_base_url = "https://other.example.test"\n', "endpoint inherited"),
        ]:
            with self.subTest(content=content):
                profile.write_text(content)
                config_before = (self.home / "config.toml").read_bytes()
                with self.assertRaisesRegex(migrate.MigrationError, message):
                    self.apply()
                self.assertEqual((self.home / "config.toml").read_bytes(), config_before)
                self.assertFalse(self.fixture.backup_dir.exists())

    def test_safe_profiles_remain_unchanged_through_apply_and_restore(self) -> None:
        profiles = {
            self.home / "inherited.config.toml": 'model = "gpt-test"\n',
            self.home / "direct.config.toml": (
                'model_provider = "openai"\n'
                'openai_base_url = "https://api.example.test"\n'
            ),
        }
        for path, content in profiles.items():
            path.write_text(content)
        self.apply()
        self.restore()
        for path, content in profiles.items():
            self.assertEqual(path.read_text(), content)

    def test_profile_created_during_backup_prevents_writes(self) -> None:
        create_backup = migrate.create_backup
        original = self.fixture.proxy_rollout.read_bytes()

        def add_profile(**kwargs):
            manifest = create_backup(**kwargs)
            (self.home / "late.config.toml").write_text('model_provider = "proxy"\n')
            return manifest

        with mock.patch.object(migrate, "create_backup", side_effect=add_profile):
            with self.assertRaisesRegex(migrate.MigrationError, "depends on provider"):
                self.apply()
        self.assertEqual(self.fixture.proxy_rollout.read_bytes(), original)

    def test_profile_conflict_during_writes_rolls_back(self) -> None:
        write = migrate.atomic_write
        config = self.home / "config.toml"
        original = config.read_bytes()
        profile = self.home / "late.config.toml"

        def add_profile(path, data, **kwargs):
            write(path, data, **kwargs)
            if path == config and b"openai_base_url" in data:
                profile.write_text('model_provider = "proxy"\n')

        with mock.patch.object(migrate, "atomic_write", side_effect=add_profile):
            with self.assertRaisesRegex(migrate.MigrationError, "depends on provider"):
                self.apply()
        self.assertEqual(config.read_bytes(), original)
        self.assertEqual(profile.read_text(), 'model_provider = "proxy"\n')
        self.assertEqual(
            migrate.load_backup_manifest(self.fixture.backup_dir)["status"], "rolled_back"
        )

    def test_unreadable_profiles_fail_closed_but_metadata_only_migration_works(self) -> None:
        profile = self.home / "broken.config.toml"
        profile.write_text("invalid = [")
        with self.assertRaisesRegex(migrate.MigrationError, "cannot parse config"):
            self.apply()
        profile.unlink()
        profile.symlink_to(self.home / "config.toml")
        with self.assertRaisesRegex(migrate.MigrationError, "symlinked config"):
            self.apply()
        self.apply(config=False)
        self.restore()

    def test_sqlite_config_precedence_profile_and_relative_paths(self) -> None:
        config = self.home / "config.toml"
        config.write_text('sqlite_home = "database"\n')
        with mock.patch.dict(os.environ, {"CODEX_SQLITE_HOME": str(self.home / "env-db")}):
            self.assertEqual(migrate.default_sqlite_home(self.home), self.home / "database")
            profile = self.home / "work.config.toml"
            profile.write_text('sqlite_home = "profile-db"\n')
            with self.assertRaisesRegex(migrate.MigrationError, "select --profile"):
                migrate.default_sqlite_home(self.home)
            resolved, source = migrate.resolve_sqlite_home(self.home, profile="work")
            self.assertEqual(resolved, self.home / "profile-db")
            self.assertEqual(source, str(profile))
            self.assertEqual(
                migrate.resolve_sqlite_home(self.home, explicit=self.home / "explicit")[0],
                self.home / "explicit",
            )

    def test_cli_uses_configured_database_even_when_stale_default_exists(self) -> None:
        separate = self.home / "separate"
        separate.mkdir()
        shutil.copyfile(self.home / migrate.STATE_DB_NAME, separate / migrate.STATE_DB_NAME)
        with sqlite3.connect(self.home / migrate.STATE_DB_NAME) as db:
            db.execute("UPDATE threads SET model_provider='stale'")
        config = self.home / "config.toml"
        config.write_text('sqlite_home = "separate"\n' + config.read_text())
        output = io.StringIO()
        with redirect_stdout(output):
            result = migrate.main([
                "--codex-home", str(self.home), "--from-provider", "proxy", "--json"
            ])
        self.assertEqual(result, 0)
        report = json.loads(output.getvalue())
        self.assertEqual(report["rows_from_provider"], 1)
        self.assertEqual(report["sqlite_home"], str(separate))
        self.assertEqual(report["sqlite_home_source"], str(config))

    def test_invalid_sqlite_setting_never_falls_back_to_stale_database(self) -> None:
        for setting in ['sqlite_home = ""', 'sqlite_home = 42', 'invalid = [']:
            with self.subTest(setting=setting):
                (self.home / "config.toml").write_text(setting)
                output = io.StringIO()
                with redirect_stderr(output):
                    status = migrate.main([
                        "--codex-home", str(self.home), "--from-provider", "proxy"
                    ])
                self.assertEqual(status, 1)
                self.assertIn("error:", output.getvalue())

    def test_cli_apply_uses_selected_profile_sqlite_and_preserves_stale_database(self) -> None:
        separate = self.home / "profile-db"
        separate.mkdir()
        shutil.copyfile(self.home / migrate.STATE_DB_NAME, separate / migrate.STATE_DB_NAME)
        (self.home / "work.config.toml").write_text('sqlite_home = "profile-db"\n')
        output = io.StringIO()
        with mock.patch.object(migrate, "find_processes_with_open_state", return_value=[]):
            with redirect_stdout(output):
                status = migrate.main([
                    "--codex-home", str(self.home), "--profile", "work",
                    "--from-provider", "proxy", "--migrate-config", "--apply",
                    "--confirm-codex-stopped", "--backup-dir",
                    str(self.fixture.backup_dir), "--json",
                ])
        self.assertEqual(status, 0)
        self.assertEqual(json.loads(output.getvalue())["sqlite_home"], str(separate))
        for directory, expected in [(self.home, "proxy"), (separate, "openai")]:
            with sqlite3.connect(directory / migrate.STATE_DB_NAME) as db:
                self.assertEqual(
                    db.execute(
                        "SELECT model_provider FROM threads WHERE id='proxy-thread'"
                    ).fetchone()[0], expected,
                )
        self.restore()

    def test_external_sqlite_configuration_requires_explicit_location(self) -> None:
        read_config = migrate.read_config_file

        def managed(path, **kwargs):
            if path == self.home / "managed_config.toml":
                return {"sqlite_home": "managed-db"}
            return read_config(path, **kwargs)

        with mock.patch.object(migrate, "read_config_file", side_effect=managed):
            with self.assertRaisesRegex(migrate.MigrationError, "pass --sqlite-home explicitly"):
                migrate.default_sqlite_home(self.home)
            self.assertEqual(
                migrate.resolve_sqlite_home(self.home, explicit=self.home)[0], self.home
            )


if __name__ == "__main__":
    unittest.main()
