from __future__ import annotations

import io
import json
import os
import re
import sqlite3
import stat
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stderr
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

import migrate  # noqa: E402
import restore as restore_cli  # noqa: E402
import verify as verify_cli  # noqa: E402


class MigrationFixture:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.codex_home = root / "codex-home"
        self.sqlite_home = self.codex_home
        self.sessions = self.codex_home / "sessions" / "2026" / "08" / "03"
        self.sessions.mkdir(parents=True)
        self.backup_dir = root / "backup"
        self.proxy_rollout = self.sessions / "rollout-proxy.jsonl"
        self.openai_rollout = self.sessions / "rollout-openai.jsonl"
        self._write_config()
        self._write_rollouts()
        self._write_database()

    def _write_config(self) -> None:
        (self.codex_home / "config.toml").write_text(
            """chatgpt_base_url = "https://chat.example.test"
model_provider = "proxy"
model = "gpt-test"

[model_providers.proxy]
base_url = "https://proxy.example.test/backend-api/codex"
name = "Test proxy"
requires_openai_auth = true
wire_api = "responses"

[projects."/example/project"]
trust_level = "trusted"
"""
        )

    @staticmethod
    def _line(record: object, *, compact: bool = True) -> bytes:
        separators = (",", ":") if compact else None
        return (json.dumps(record, separators=separators) + "\n").encode()

    def _write_rollouts(self) -> None:
        proxy_lines = [
            self._line(
                {
                    "type": "session_meta",
                    "payload": {
                        "id": "proxy-thread",
                        "model_provider": "proxy",
                        "history_mode": "legacy",
                        "history_base": None,
                    },
                }
            ),
            self._line(
                {
                    "type": "event_msg",
                    "payload": {
                        "type": "thread_settings_applied",
                        "thread_settings": {
                            "model_provider_id": "proxy",
                            "model": "gpt-test",
                        },
                    },
                },
                compact=False,
            ),
            self._line(
                {
                    "type": "event_msg",
                    "payload": {
                        "type": "user_message",
                        "message": 'literal text: "model_provider":"proxy"',
                    },
                }
            ),
            # Forked histories can contain additional session_meta records.
            self._line(
                {
                    "type": "session_meta",
                    "payload": {
                        "id": "copied-parent",
                        "model_provider": "proxy",
                        "history_mode": "legacy",
                        "history_base": None,
                    },
                }
            ),
            b'{"preexisting_invalid":\n',
        ]
        self.proxy_rollout.write_bytes(b"".join(proxy_lines))
        os.chmod(self.proxy_rollout, 0o640)
        os.utime(self.proxy_rollout, ns=(1_700_000_000_000_000_000,) * 2)

        self.openai_rollout.write_bytes(
            self._line(
                {
                    "type": "session_meta",
                    "payload": {
                        "id": "openai-thread",
                        "model_provider": "openai",
                        "history_mode": "legacy",
                        "history_base": None,
                    },
                }
            )
            + self._line(
                {
                    "type": "event_msg",
                    "payload": {"type": "user_message", "message": "unchanged"},
                }
            )
        )
        os.utime(self.openai_rollout, ns=(1_700_000_001_000_000_000,) * 2)

    def _write_database(self) -> None:
        connection = sqlite3.connect(self.codex_home / migrate.STATE_DB_NAME)
        try:
            connection.executescript(
                """
                CREATE TABLE threads (
                    id TEXT PRIMARY KEY,
                    model_provider TEXT NOT NULL,
                    title TEXT NOT NULL
                );
                CREATE INDEX idx_threads_provider ON threads(model_provider);
                CREATE TABLE unrelated (
                    id INTEGER PRIMARY KEY,
                    value TEXT NOT NULL
                );
                """
            )
            connection.executemany(
                "INSERT INTO threads(id, model_provider, title) VALUES (?, ?, ?)",
                [
                    ("proxy-thread", "proxy", "Proxy thread"),
                    ("openai-thread", "openai", "OpenAI thread"),
                ],
            )
            connection.execute(
                "INSERT INTO unrelated(id, value) VALUES (1, 'must not change')"
            )
            connection.commit()
        finally:
            connection.close()


class ProviderMigrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(prefix="provider-migration-test-")
        self.addCleanup(self.temporary.cleanup)
        self.fixture = MigrationFixture(Path(self.temporary.name))

    def test_dry_run_analysis_does_not_change_codex_records(self) -> None:
        before = {
            path: path.read_bytes()
            for path in migrate.rollout_paths(self.fixture.codex_home)
        }
        config_before = (self.fixture.codex_home / "config.toml").read_bytes()
        database_before = migrate.analyze_database(
            self.fixture.codex_home / migrate.STATE_DB_NAME, "proxy"
        )

        rollout = migrate.analyze_rollouts(self.fixture.codex_home, "proxy")
        transformed_config = migrate.transform_config(
            config_before, "proxy", "openai"
        )

        self.assertEqual(rollout.rollout_files, 2)
        self.assertEqual(rollout.rollout_heads_from_provider, 1)
        self.assertEqual(rollout.files_requiring_changes, 1)
        self.assertEqual(rollout.session_meta_values, 2)
        self.assertEqual(rollout.thread_settings_values, 1)
        self.assertEqual(rollout.malformed_lines, 1)
        self.assertNotEqual(transformed_config, config_before)
        self.assertEqual(database_before.rows_from_provider, 1)
        self.assertEqual(
            before,
            {
                path: path.read_bytes()
                for path in migrate.rollout_paths(self.fixture.codex_home)
            },
        )
        self.assertEqual(
            (self.fixture.codex_home / "config.toml").read_bytes(), config_before
        )

    def test_process_classifier_ignores_docker_codex_mount_arguments(self) -> None:
        cases = (
            (["/usr/local/bin/codex", "exec"], True),
            (["C:\\Tools\\codex.exe", "app-server"], True),
            (["node", "/usr/lib/node_modules/@openai/codex/bin/codex.js"], True),
            (["node", "codex.js", "resume"], True),
            (
                [
                    "docker",
                    "run",
                    "--pid=host",
                    "--mount",
                    "type=bind,src=/state,dst=/codex",
                    "codex-provider-migration:local",
                    "migrate",
                    "--codex-home",
                    "/codex",
                ],
                False,
            ),
            (
                [
                    "python3",
                    "/opt/codex-provider-migration/migrate.py",
                    "--codex-home",
                    "/codex",
                ],
                False,
            ),
            ([], False),
        )
        for command_args, expected in cases:
            with self.subTest(command_args=command_args):
                self.assertEqual(
                    migrate.command_looks_like_codex(command_args), expected
                )

    def test_wal_dry_run_preserves_main_database_and_limits_side_effects(self) -> None:
        database = self.fixture.sqlite_home / migrate.STATE_DB_NAME
        connection = sqlite3.connect(database)
        try:
            self.assertEqual(
                connection.execute("PRAGMA journal_mode = WAL").fetchone(),
                ("wal",),
            )
        finally:
            connection.close()

        before_bytes = database.read_bytes()
        before_metadata = migrate.capture_file_metadata(database)
        before_entries = {path.name for path in database.parent.iterdir()}
        migrate.analyze_database(database, "proxy")
        after_entries = {path.name for path in database.parent.iterdir()}

        self.assertEqual(database.read_bytes(), before_bytes)
        self.assertEqual(migrate.capture_file_metadata(database), before_metadata)
        self.assertLessEqual(
            after_entries - before_entries,
            {f"{database.name}-wal", f"{database.name}-shm"},
        )

    def test_apply_changes_only_intended_bytes_and_verifies_everything(self) -> None:
        proxy_before = self.fixture.proxy_rollout.read_bytes()
        openai_before = self.fixture.openai_rollout.read_bytes()
        proxy_stat_before = self.fixture.proxy_rollout.stat()
        openai_stat_before = self.fixture.openai_rollout.stat()

        with mock.patch.object(
            migrate, "find_processes_with_open_state", return_value=[]
        ):
            report = migrate.apply_migration(
                codex_home=self.fixture.codex_home,
                sqlite_home=self.fixture.sqlite_home,
                backup_dir=self.fixture.backup_dir,
                source_provider="proxy",
                target_provider="openai",
                migrate_config=True,
                confirm_stopped=True,
            )

        expected_proxy, count = migrate.replace_provider_bytes(
            proxy_before, "proxy", "openai"
        )
        self.assertEqual(count, 3)
        self.assertEqual(self.fixture.proxy_rollout.read_bytes(), expected_proxy)
        self.assertEqual(self.fixture.openai_rollout.read_bytes(), openai_before)
        self.assertEqual(
            stat.S_IMODE(self.fixture.proxy_rollout.stat().st_mode),
            stat.S_IMODE(proxy_stat_before.st_mode),
        )
        self.assertEqual(
            self.fixture.proxy_rollout.stat().st_mtime_ns,
            proxy_stat_before.st_mtime_ns,
        )
        self.assertEqual(
            self.fixture.openai_rollout.stat().st_mtime_ns,
            openai_stat_before.st_mtime_ns,
        )
        self.assertEqual(report.rollout_files_checked, 2)
        self.assertEqual(report.changed_rollout_files, 1)
        self.assertEqual(report.unchanged_rollout_files, 1)
        self.assertEqual(report.malformed_lines_preserved, 1)
        self.assertEqual(report.session_meta_values_changed, 2)
        self.assertEqual(report.thread_settings_values_changed, 1)
        self.assertEqual(report.sqlite_thread_rows_changed, 1)
        self.assertTrue(report.config_matches_expected)

        # Re-running the independent verifier must produce the same result.
        second_report = migrate.verify_against_backup(
            backup_dir=self.fixture.backup_dir,
            codex_home=self.fixture.codex_home,
            sqlite_home=self.fixture.sqlite_home,
        )
        self.assertEqual(second_report, report)

        config = (self.fixture.codex_home / "config.toml").read_text()
        self.assertIn("openai_base_url", config)
        self.assertNotIn('model_provider = "proxy"', config)
        self.assertNotIn("[model_providers.proxy]", config)
        self.assertIn("chatgpt_base_url", config)

        connection = sqlite3.connect(self.fixture.codex_home / migrate.STATE_DB_NAME)
        try:
            self.assertEqual(
                connection.execute(
                    "SELECT id, model_provider, title FROM threads ORDER BY id"
                ).fetchall(),
                [
                    ("openai-thread", "openai", "OpenAI thread"),
                    ("proxy-thread", "openai", "Proxy thread"),
                ],
            )
            self.assertEqual(
                connection.execute("SELECT * FROM unrelated").fetchall(),
                [(1, "must not change")],
            )
        finally:
            connection.close()

    def test_apply_requires_explicit_stopped_confirmation(self) -> None:
        with self.assertRaisesRegex(
            migrate.MigrationError, "confirm-codex-stopped"
        ):
            migrate.apply_migration(
                codex_home=self.fixture.codex_home,
                sqlite_home=self.fixture.sqlite_home,
                backup_dir=self.fixture.backup_dir,
                source_provider="proxy",
                target_provider="openai",
                migrate_config=False,
                confirm_stopped=False,
            )
        self.assertFalse(self.fixture.backup_dir.exists())

    def test_apply_rejects_a_readonly_database_before_rollout_writes(self) -> None:
        database = self.fixture.sqlite_home / migrate.STATE_DB_NAME
        rollout_before = self.fixture.proxy_rollout.read_bytes()
        original_mode = stat.S_IMODE(database.stat().st_mode)
        os.chmod(database, 0o400)
        self.addCleanup(os.chmod, database, original_mode)

        with (
            mock.patch.object(
                migrate, "find_processes_with_open_state", return_value=[]
            ),
            self.assertRaisesRegex(migrate.MigrationError, "not writable"),
        ):
            migrate.apply_migration(
                codex_home=self.fixture.codex_home,
                sqlite_home=self.fixture.sqlite_home,
                backup_dir=self.fixture.backup_dir,
                source_provider="proxy",
                target_provider="openai",
                migrate_config=False,
                confirm_stopped=True,
            )

        self.assertFalse(self.fixture.backup_dir.exists())
        self.assertEqual(self.fixture.proxy_rollout.read_bytes(), rollout_before)

    def test_apply_refuses_backup_inside_codex_state(self) -> None:
        with mock.patch.object(
            migrate, "find_processes_with_open_state", return_value=[]
        ):
            with self.assertRaisesRegex(migrate.MigrationError, "outside Codex state"):
                migrate.apply_migration(
                    codex_home=self.fixture.codex_home,
                    sqlite_home=self.fixture.sqlite_home,
                    backup_dir=self.fixture.codex_home / "backup",
                    source_provider="proxy",
                    target_provider="openai",
                    migrate_config=False,
                    confirm_stopped=True,
                )

    def test_refuses_paginated_rollout(self) -> None:
        record = {
            "type": "session_meta",
            "payload": {
                "id": "proxy-thread",
                "model_provider": "proxy",
                "history_mode": "paginated",
                "history_base": None,
            },
        }
        self.fixture.proxy_rollout.write_bytes(MigrationFixture._line(record))
        with self.assertRaisesRegex(migrate.MigrationError, "paginated"):
            migrate.analyze_rollouts(self.fixture.codex_home, "proxy")

    def test_refuses_compressed_rollout(self) -> None:
        compressed = self.fixture.sessions / "rollout.jsonl.zst"
        compressed.write_bytes(b"not relevant")
        with self.assertRaisesRegex(migrate.MigrationError, "compressed"):
            migrate.ensure_supported_storage(
                self.fixture.codex_home, self.fixture.sqlite_home
            )

    def test_refuses_thread_history_database(self) -> None:
        (self.fixture.sqlite_home / "thread_history_1.sqlite").write_bytes(b"")
        with self.assertRaisesRegex(migrate.MigrationError, "thread-history"):
            migrate.ensure_supported_storage(
                self.fixture.codex_home, self.fixture.sqlite_home
            )

    def test_refuses_unknown_provider_metadata_path(self) -> None:
        record = {
            "type": "event_msg",
            "payload": {
                "type": "future_event",
                "future": {"model_provider": "proxy"},
            },
        }
        self.fixture.proxy_rollout.write_bytes(MigrationFixture._line(record))
        with self.assertRaisesRegex(migrate.MigrationError, "unknown provider"):
            migrate.analyze_rollouts(self.fixture.codex_home, "proxy")

    def test_refuses_provider_match_inside_malformed_line(self) -> None:
        self.fixture.proxy_rollout.write_bytes(b'{"model_provider":"proxy"\n')
        with self.assertRaisesRegex(migrate.MigrationError, "unparseable"):
            migrate.analyze_rollouts(self.fixture.codex_home, "proxy")

    def test_config_migration_refuses_semantically_incompatible_provider(self) -> None:
        config_path = self.fixture.codex_home / "config.toml"
        config_path.write_text(
            config_path.read_text().replace(
                'wire_api = "responses"',
                'wire_api = "responses"\nenv_key = "CUSTOM_TOKEN"',
            )
        )
        with self.assertRaisesRegex(migrate.MigrationError, "cannot preserve"):
            migrate.transform_config(config_path.read_bytes(), "proxy", "openai")

    def test_preflight_rejects_existing_foreign_key_violations(self) -> None:
        database = self.fixture.sqlite_home / migrate.STATE_DB_NAME
        connection = sqlite3.connect(database)
        try:
            connection.executescript(
                """
                CREATE TABLE parents (id INTEGER PRIMARY KEY);
                CREATE TABLE children (
                    id INTEGER PRIMARY KEY,
                    parent_id INTEGER REFERENCES parents(id)
                );
                INSERT INTO children(id, parent_id) VALUES (1, 999);
                """
            )
            connection.commit()
        finally:
            connection.close()

        rollout_before = self.fixture.proxy_rollout.read_bytes()
        with (
            mock.patch.object(
                migrate, "find_processes_with_open_state", return_value=[]
            ),
            self.assertRaisesRegex(migrate.MigrationError, "foreign_key_check"),
        ):
            migrate.apply_migration(
                codex_home=self.fixture.codex_home,
                sqlite_home=self.fixture.sqlite_home,
                backup_dir=self.fixture.backup_dir,
                source_provider="proxy",
                target_provider="openai",
                migrate_config=False,
                confirm_stopped=True,
            )
        self.assertFalse(self.fixture.backup_dir.exists())
        self.assertEqual(self.fixture.proxy_rollout.read_bytes(), rollout_before)

    def test_post_backup_database_drift_is_rejected_before_migration(self) -> None:
        real_create_backup = migrate.create_backup

        def create_backup_then_drift(**kwargs: object) -> dict[str, object]:
            manifest = real_create_backup(**kwargs)
            connection = sqlite3.connect(kwargs["db_path"])
            try:
                connection.execute(
                    "INSERT INTO unrelated(id, value) VALUES (2, 'newer state')"
                )
                connection.commit()
            finally:
                connection.close()
            return manifest

        rollout_before = self.fixture.proxy_rollout.read_bytes()
        with (
            mock.patch.object(
                migrate, "find_processes_with_open_state", return_value=[]
            ),
            mock.patch.object(
                migrate, "create_backup", side_effect=create_backup_then_drift
            ),
            self.assertRaisesRegex(migrate.MigrationError, "unexpected SQLite change"),
        ):
            migrate.apply_migration(
                codex_home=self.fixture.codex_home,
                sqlite_home=self.fixture.sqlite_home,
                backup_dir=self.fixture.backup_dir,
                source_provider="proxy",
                target_provider="openai",
                migrate_config=False,
                confirm_stopped=True,
            )

        self.assertEqual(self.fixture.proxy_rollout.read_bytes(), rollout_before)
        connection = sqlite3.connect(
            self.fixture.sqlite_home / migrate.STATE_DB_NAME
        )
        try:
            self.assertEqual(
                connection.execute(
                    "SELECT value FROM unrelated WHERE id = 2"
                ).fetchone(),
                ("newer state",),
            )
        finally:
            connection.close()

    def test_verifier_detects_unexpected_rollout_change(self) -> None:
        with mock.patch.object(
            migrate, "find_processes_with_open_state", return_value=[]
        ):
            migrate.apply_migration(
                codex_home=self.fixture.codex_home,
                sqlite_home=self.fixture.sqlite_home,
                backup_dir=self.fixture.backup_dir,
                source_provider="proxy",
                target_provider="openai",
                migrate_config=True,
                confirm_stopped=True,
            )
        with self.fixture.proxy_rollout.open("ab") as stream:
            stream.write(b'{"unexpected":true}\n')
        with self.assertRaisesRegex(migrate.MigrationError, "unexpected rollout"):
            migrate.verify_against_backup(
                backup_dir=self.fixture.backup_dir,
                codex_home=self.fixture.codex_home,
                sqlite_home=self.fixture.sqlite_home,
            )

    def test_apply_refuses_backup_inside_utility_checkout(self) -> None:
        unsafe_backup = REPO_ROOT / "private-test-backup"
        self.assertFalse(unsafe_backup.exists())
        with self.assertRaisesRegex(
            migrate.MigrationError, "outside the migration utility checkout"
        ):
            migrate.apply_migration(
                codex_home=self.fixture.codex_home,
                sqlite_home=self.fixture.sqlite_home,
                backup_dir=unsafe_backup,
                source_provider="proxy",
                target_provider="openai",
                migrate_config=False,
                confirm_stopped=True,
            )
        self.assertFalse(unsafe_backup.exists())

    def test_refuses_symlinked_session_root(self) -> None:
        if not hasattr(os, "symlink"):
            self.skipTest("symlinks are unavailable")
        outside = Path(self.temporary.name) / "outside-sessions"
        outside.mkdir()
        sessions = self.fixture.codex_home / "sessions"
        sessions.rename(self.fixture.codex_home / "sessions-original")
        try:
            sessions.symlink_to(outside, target_is_directory=True)
        except (OSError, NotImplementedError) as exc:
            self.skipTest(f"cannot create directory symlink: {exc}")
        with self.assertRaisesRegex(migrate.MigrationError, "symlinked session root"):
            migrate.ensure_supported_storage(
                self.fixture.codex_home, self.fixture.sqlite_home
            )

    def test_verifier_rejects_incomplete_manifest_cleanly(self) -> None:
        self.fixture.backup_dir.mkdir()
        (self.fixture.backup_dir / migrate.MANIFEST_NAME).write_text("{}\n")
        with self.assertRaisesRegex(
            migrate.MigrationError, "invalid backup manifest: missing"
        ):
            migrate.verify_against_backup(backup_dir=self.fixture.backup_dir)

    def test_verifier_rejects_invalid_utf8_and_nul_manifest_paths(self) -> None:
        self.fixture.backup_dir.mkdir()
        manifest_path = self.fixture.backup_dir / migrate.MANIFEST_NAME
        manifest_path.write_bytes(b"\xff")
        with self.assertRaisesRegex(migrate.MigrationError, "cannot read"):
            migrate.verify_against_backup(backup_dir=self.fixture.backup_dir)

        manifest_path.write_text(
            json.dumps(
                {
                    "manifest_version": migrate.MANIFEST_VERSION,
                    "migrate_config": False,
                    "codex_home": "invalid\x00path",
                    "sqlite_home": str(self.fixture.sqlite_home),
                    "state_db_name": migrate.STATE_DB_NAME,
                    "source_provider": "proxy",
                    "target_provider": "openai",
                }
            )
        )
        with self.assertRaisesRegex(
            migrate.MigrationError, "invalid backup manifest fields: codex_home"
        ):
            migrate.verify_against_backup(backup_dir=self.fixture.backup_dir)

        manifest_path.write_text(
            json.dumps(
                {
                    "manifest_version": migrate.MANIFEST_VERSION,
                    "migrate_config": False,
                    "codex_home": "relative-codex-state",
                    "sqlite_home": str(self.fixture.sqlite_home),
                    "state_db_name": migrate.STATE_DB_NAME,
                    "source_provider": "proxy",
                    "target_provider": "openai",
                }
            )
        )
        with self.assertRaisesRegex(
            migrate.MigrationError, "backup manifest paths must be absolute"
        ):
            migrate.verify_against_backup(backup_dir=self.fixture.backup_dir)

    def test_archived_sessions_are_analyzed(self) -> None:
        archived = (
            self.fixture.codex_home
            / "archived_sessions"
            / "rollout-archived.jsonl"
        )
        archived.parent.mkdir()
        archived.write_bytes(
            MigrationFixture._line(
                {
                    "type": "session_meta",
                    "payload": {
                        "id": "archived-thread",
                        "model_provider": "proxy",
                        "history_mode": "legacy",
                        "history_base": None,
                    },
                }
            )
        )
        analysis = migrate.analyze_rollouts(self.fixture.codex_home, "proxy")
        self.assertEqual(analysis.rollout_files, 3)
        self.assertEqual(analysis.rollout_heads_from_provider, 2)
        self.assertEqual(analysis.session_meta_values, 3)

    def test_default_state_paths_honor_environment(self) -> None:
        with mock.patch.dict(
            os.environ,
            {
                "CODEX_HOME": str(self.fixture.codex_home),
                "CODEX_SQLITE_HOME": str(self.fixture.sqlite_home),
            },
            clear=False,
        ):
            self.assertEqual(migrate.default_codex_home(), self.fixture.codex_home)
            self.assertEqual(
                migrate.default_sqlite_home(Path("unused")),
                self.fixture.sqlite_home,
            )

    def test_cli_json_dry_run_supports_separate_sqlite_home(self) -> None:
        separate_sqlite_home = Path(self.temporary.name) / "sqlite-home"
        separate_sqlite_home.mkdir()
        database = self.fixture.codex_home / migrate.STATE_DB_NAME
        database.rename(separate_sqlite_home / migrate.STATE_DB_NAME)
        output = io.StringIO()
        with redirect_stdout(output):
            status = migrate.main(
                [
                    "--codex-home",
                    str(self.fixture.codex_home),
                    "--sqlite-home",
                    str(separate_sqlite_home),
                    "--from-provider",
                    "proxy",
                    "--json",
                ]
            )
        self.assertEqual(status, 0)
        result = json.loads(output.getvalue())
        self.assertEqual(result["result"], "dry run; no Codex records changed")
        self.assertEqual(result["rows_from_provider"], 1)

    def test_failed_final_verification_rolls_back_wal_state(self) -> None:
        rollout_before = self.fixture.proxy_rollout.read_bytes()
        config_path = self.fixture.codex_home / "config.toml"
        config_before = config_path.read_bytes()
        database = self.fixture.sqlite_home / migrate.STATE_DB_NAME
        connection = sqlite3.connect(database)
        try:
            self.assertEqual(
                connection.execute("PRAGMA journal_mode = WAL").fetchone(),
                ("wal",),
            )
        finally:
            connection.close()

        def fail_verification(**_kwargs: object) -> None:
            raise migrate.MigrationError("injected final verification failure")

        with (
            mock.patch.object(
                migrate, "find_processes_with_open_state", return_value=[]
            ),
            mock.patch.object(
                migrate, "verify_against_backup", side_effect=fail_verification
            ),
            self.assertRaisesRegex(
                migrate.MigrationError, "injected final verification failure"
            ),
        ):
            migrate.apply_migration(
                codex_home=self.fixture.codex_home,
                sqlite_home=self.fixture.sqlite_home,
                backup_dir=self.fixture.backup_dir,
                source_provider="proxy",
                target_provider="openai",
                migrate_config=True,
                confirm_stopped=True,
            )

        self.assertEqual(self.fixture.proxy_rollout.read_bytes(), rollout_before)
        self.assertEqual(config_path.read_bytes(), config_before)
        connection = sqlite3.connect(database)
        try:
            self.assertEqual(
                connection.execute(
                    "SELECT model_provider FROM threads WHERE id = 'proxy-thread'"
                ).fetchone(),
                ("proxy",),
            )
        finally:
            connection.close()
        manifest = migrate.load_backup_manifest(self.fixture.backup_dir)
        self.assertEqual(manifest["status"], "rolled_back")
        self.assertIn("rollback_report", manifest)

    def test_restore_from_completed_backup_reverts_and_verifies(self) -> None:
        with mock.patch.object(
            migrate, "find_processes_with_open_state", return_value=[]
        ):
            migrate.apply_migration(
                codex_home=self.fixture.codex_home,
                sqlite_home=self.fixture.sqlite_home,
                backup_dir=self.fixture.backup_dir,
                source_provider="proxy",
                target_provider="openai",
                migrate_config=True,
                confirm_stopped=True,
            )
            report = migrate.restore_from_backup(
                backup_dir=self.fixture.backup_dir,
                codex_home=self.fixture.codex_home,
                sqlite_home=self.fixture.sqlite_home,
                confirm_stopped=True,
            )
        self.assertEqual(report.rollout_files_restored, 2)
        self.assertTrue(report.config_restored)
        connection = sqlite3.connect(
            self.fixture.sqlite_home / migrate.STATE_DB_NAME
        )
        try:
            self.assertEqual(
                connection.execute(
                    "SELECT model_provider FROM threads WHERE id = 'proxy-thread'"
                ).fetchone(),
                ("proxy",),
            )
        finally:
            connection.close()
        manifest = migrate.load_backup_manifest(self.fixture.backup_dir)
        self.assertEqual(manifest["status"], "restored")

    def test_interrupted_restore_is_safely_resumable(self) -> None:
        real_restore_file = migrate.restore_file_from_backup
        restore_calls = 0

        def interrupt_after_second_file(source: Path, destination: Path) -> None:
            nonlocal restore_calls
            real_restore_file(source, destination)
            restore_calls += 1
            if restore_calls == 2:
                raise OSError("injected restore interruption")

        with mock.patch.object(
            migrate, "find_processes_with_open_state", return_value=[]
        ):
            migrate.apply_migration(
                codex_home=self.fixture.codex_home,
                sqlite_home=self.fixture.sqlite_home,
                backup_dir=self.fixture.backup_dir,
                source_provider="proxy",
                target_provider="openai",
                migrate_config=True,
                confirm_stopped=True,
            )
            with (
                mock.patch.object(
                    migrate,
                    "restore_file_from_backup",
                    side_effect=interrupt_after_second_file,
                ),
                self.assertRaisesRegex(OSError, "injected restore interruption"),
            ):
                migrate.restore_from_backup(
                    backup_dir=self.fixture.backup_dir,
                    codex_home=self.fixture.codex_home,
                    sqlite_home=self.fixture.sqlite_home,
                    confirm_stopped=True,
                )

            interrupted_manifest = migrate.load_backup_manifest(
                self.fixture.backup_dir
            )
            self.assertEqual(interrupted_manifest["status"], "restoring")

            report = migrate.restore_from_backup(
                backup_dir=self.fixture.backup_dir,
                codex_home=self.fixture.codex_home,
                sqlite_home=self.fixture.sqlite_home,
                confirm_stopped=True,
            )

        self.assertEqual(report.rollout_files_restored, 2)
        self.assertEqual(
            migrate.load_backup_manifest(self.fixture.backup_dir)["status"],
            "restored",
        )
        connection = sqlite3.connect(
            self.fixture.sqlite_home / migrate.STATE_DB_NAME
        )
        try:
            self.assertEqual(
                connection.execute(
                    "SELECT model_provider FROM threads WHERE id = 'proxy-thread'"
                ).fetchone(),
                ("proxy",),
            )
        finally:
            connection.close()

    def test_prepared_backup_can_recover_an_interrupted_migration(self) -> None:
        with mock.patch.object(
            migrate, "find_processes_with_open_state", return_value=[]
        ):
            migrate.apply_migration(
                codex_home=self.fixture.codex_home,
                sqlite_home=self.fixture.sqlite_home,
                backup_dir=self.fixture.backup_dir,
                source_provider="proxy",
                target_provider="openai",
                migrate_config=True,
                confirm_stopped=True,
            )

            manifest = migrate.load_backup_manifest(self.fixture.backup_dir)
            manifest["status"] = "prepared"
            manifest.pop("completed_at")
            manifest.pop("verification_report")
            migrate.write_json_atomic(
                self.fixture.backup_dir / migrate.MANIFEST_NAME, manifest
            )

            migrate.restore_from_backup(
                backup_dir=self.fixture.backup_dir,
                codex_home=self.fixture.codex_home,
                sqlite_home=self.fixture.sqlite_home,
                confirm_stopped=True,
            )

        self.assertEqual(
            migrate.load_backup_manifest(self.fixture.backup_dir)["status"],
            "restored",
        )

    def test_automatic_rollback_rejects_a_tampered_backup_before_writing(self) -> None:
        original_rollout = self.fixture.proxy_rollout.read_bytes()

        def tamper_and_fail(**_kwargs: object) -> None:
            backup_rollout = (
                self.fixture.backup_dir
                / migrate.relative_rollout_path(
                    self.fixture.codex_home, self.fixture.proxy_rollout
                )
            )
            backup_rollout.write_bytes(backup_rollout.read_bytes() + b"\n")
            raise migrate.MigrationError("injected migration failure")

        with (
            mock.patch.object(
                migrate, "find_processes_with_open_state", return_value=[]
            ),
            mock.patch.object(
                migrate, "verify_against_backup", side_effect=tamper_and_fail
            ),
            self.assertRaisesRegex(
                migrate.MigrationError, "automatic rollback also failed"
            ),
        ):
            migrate.apply_migration(
                codex_home=self.fixture.codex_home,
                sqlite_home=self.fixture.sqlite_home,
                backup_dir=self.fixture.backup_dir,
                source_provider="proxy",
                target_provider="openai",
                migrate_config=True,
                confirm_stopped=True,
            )

        migrated_rollout, _ = migrate.replace_provider_bytes(
            original_rollout, "proxy", "openai"
        )
        self.assertEqual(self.fixture.proxy_rollout.read_bytes(), migrated_rollout)

    def test_restore_refuses_newer_live_state_without_writing(self) -> None:
        with mock.patch.object(
            migrate, "find_processes_with_open_state", return_value=[]
        ):
            migrate.apply_migration(
                codex_home=self.fixture.codex_home,
                sqlite_home=self.fixture.sqlite_home,
                backup_dir=self.fixture.backup_dir,
                source_provider="proxy",
                target_provider="openai",
                migrate_config=True,
                confirm_stopped=True,
            )

        with self.fixture.proxy_rollout.open("ab") as stream:
            stream.write(MigrationFixture._line({"post_migration": True}))
        rollout_after_new_activity = self.fixture.proxy_rollout.read_bytes()
        config_after_migration = (
            self.fixture.codex_home / "config.toml"
        ).read_bytes()

        with (
            mock.patch.object(
                migrate, "find_processes_with_open_state", return_value=[]
            ),
            self.assertRaisesRegex(migrate.MigrationError, "unexpected rollout"),
        ):
            migrate.restore_from_backup(
                backup_dir=self.fixture.backup_dir,
                codex_home=self.fixture.codex_home,
                sqlite_home=self.fixture.sqlite_home,
                confirm_stopped=True,
            )

        self.assertEqual(
            self.fixture.proxy_rollout.read_bytes(), rollout_after_new_activity
        )
        self.assertEqual(
            (self.fixture.codex_home / "config.toml").read_bytes(),
            config_after_migration,
        )
        connection = sqlite3.connect(
            self.fixture.sqlite_home / migrate.STATE_DB_NAME
        )
        try:
            self.assertEqual(
                connection.execute(
                    "SELECT model_provider FROM threads WHERE id = 'proxy-thread'"
                ).fetchone(),
                ("openai",),
            )
        finally:
            connection.close()

    def test_automatic_rollback_refuses_concurrent_live_changes(self) -> None:
        def change_live_state_and_fail(**_kwargs: object) -> None:
            connection = sqlite3.connect(
                self.fixture.sqlite_home / migrate.STATE_DB_NAME
            )
            try:
                connection.execute(
                    "INSERT INTO unrelated(id, value) VALUES (2, 'concurrent')"
                )
                connection.commit()
            finally:
                connection.close()
            raise migrate.MigrationError("injected final verification failure")

        with (
            mock.patch.object(
                migrate, "find_processes_with_open_state", return_value=[]
            ),
            mock.patch.object(
                migrate,
                "verify_against_backup",
                side_effect=change_live_state_and_fail,
            ),
            self.assertRaisesRegex(
                migrate.MigrationError, "automatic rollback also failed"
            ),
        ):
            migrate.apply_migration(
                codex_home=self.fixture.codex_home,
                sqlite_home=self.fixture.sqlite_home,
                backup_dir=self.fixture.backup_dir,
                source_provider="proxy",
                target_provider="openai",
                migrate_config=True,
                confirm_stopped=True,
            )

        self.assertIn(
            b'"model_provider":"openai"',
            self.fixture.proxy_rollout.read_bytes(),
        )
        connection = sqlite3.connect(
            self.fixture.sqlite_home / migrate.STATE_DB_NAME
        )
        try:
            self.assertEqual(
                connection.execute(
                    "SELECT model_provider FROM threads WHERE id = 'proxy-thread'"
                ).fetchone(),
                ("openai",),
            )
            self.assertEqual(
                connection.execute(
                    "SELECT value FROM unrelated WHERE id = 2"
                ).fetchone(),
                ("concurrent",),
            )
        finally:
            connection.close()

    def test_restore_refuses_persistent_sqlite_setting_changes(self) -> None:
        with mock.patch.object(
            migrate, "find_processes_with_open_state", return_value=[]
        ):
            migrate.apply_migration(
                codex_home=self.fixture.codex_home,
                sqlite_home=self.fixture.sqlite_home,
                backup_dir=self.fixture.backup_dir,
                source_provider="proxy",
                target_provider="openai",
                migrate_config=True,
                confirm_stopped=True,
            )

        database = self.fixture.sqlite_home / migrate.STATE_DB_NAME
        connection = sqlite3.connect(database)
        try:
            connection.execute("PRAGMA user_version = 987")
        finally:
            connection.close()

        with (
            mock.patch.object(
                migrate, "find_processes_with_open_state", return_value=[]
            ),
            self.assertRaisesRegex(
                migrate.MigrationError,
                "persistent setting changed: user_version",
            ),
        ):
            migrate.restore_from_backup(
                backup_dir=self.fixture.backup_dir,
                codex_home=self.fixture.codex_home,
                sqlite_home=self.fixture.sqlite_home,
                confirm_stopped=True,
            )

        connection = sqlite3.connect(database)
        try:
            self.assertEqual(
                connection.execute("PRAGMA user_version").fetchone(), (987,)
            )
        finally:
            connection.close()

    def test_restore_refuses_live_sqlite_mode_changes(self) -> None:
        with mock.patch.object(
            migrate, "find_processes_with_open_state", return_value=[]
        ):
            migrate.apply_migration(
                codex_home=self.fixture.codex_home,
                sqlite_home=self.fixture.sqlite_home,
                backup_dir=self.fixture.backup_dir,
                source_provider="proxy",
                target_provider="openai",
                migrate_config=True,
                confirm_stopped=True,
            )

        database = self.fixture.sqlite_home / migrate.STATE_DB_NAME
        original_mode = stat.S_IMODE(database.stat().st_mode)
        changed_mode = 0o600 if original_mode != 0o600 else 0o640
        os.chmod(database, changed_mode)
        with (
            mock.patch.object(
                migrate, "find_processes_with_open_state", return_value=[]
            ),
            self.assertRaisesRegex(migrate.MigrationError, "SQLite mode changed"),
        ):
            migrate.restore_from_backup(
                backup_dir=self.fixture.backup_dir,
                codex_home=self.fixture.codex_home,
                sqlite_home=self.fixture.sqlite_home,
                confirm_stopped=True,
            )
        self.assertEqual(stat.S_IMODE(database.stat().st_mode), changed_mode)

    def test_restore_rejects_tampered_backup_before_writing(self) -> None:
        with mock.patch.object(
            migrate, "find_processes_with_open_state", return_value=[]
        ):
            migrate.apply_migration(
                codex_home=self.fixture.codex_home,
                sqlite_home=self.fixture.sqlite_home,
                backup_dir=self.fixture.backup_dir,
                source_provider="proxy",
                target_provider="openai",
                migrate_config=True,
                confirm_stopped=True,
            )

        live_rollout = self.fixture.proxy_rollout.read_bytes()
        backup_rollout = self.fixture.backup_dir / migrate.relative_rollout_path(
            self.fixture.codex_home, self.fixture.proxy_rollout
        )
        backup_rollout.write_bytes(backup_rollout.read_bytes() + b"\n")

        with (
            mock.patch.object(
                migrate, "find_processes_with_open_state", return_value=[]
            ),
            self.assertRaisesRegex(migrate.MigrationError, "digest mismatch"),
        ):
            migrate.restore_from_backup(
                backup_dir=self.fixture.backup_dir,
                codex_home=self.fixture.codex_home,
                sqlite_home=self.fixture.sqlite_home,
                confirm_stopped=True,
            )

        self.assertEqual(self.fixture.proxy_rollout.read_bytes(), live_rollout)

    def test_restore_preserves_config_created_after_configless_migration(self) -> None:
        config_path = self.fixture.codex_home / "config.toml"
        config_path.unlink()
        with mock.patch.object(
            migrate, "find_processes_with_open_state", return_value=[]
        ):
            migrate.apply_migration(
                codex_home=self.fixture.codex_home,
                sqlite_home=self.fixture.sqlite_home,
                backup_dir=self.fixture.backup_dir,
                source_provider="proxy",
                target_provider="openai",
                migrate_config=False,
                confirm_stopped=True,
            )

        new_config = b'model = "newer-user-config"\n'
        config_path.write_bytes(new_config)
        with (
            mock.patch.object(
                migrate, "find_processes_with_open_state", return_value=[]
            ),
            self.assertRaisesRegex(migrate.MigrationError, "appeared after the backup"),
        ):
            migrate.restore_from_backup(
                backup_dir=self.fixture.backup_dir,
                codex_home=self.fixture.codex_home,
                sqlite_home=self.fixture.sqlite_home,
                confirm_stopped=True,
            )
        self.assertEqual(config_path.read_bytes(), new_config)

    def test_restore_from_readonly_wal_backup_leaves_no_sidecars(self) -> None:
        source = self.fixture.sqlite_home / migrate.STATE_DB_NAME
        connection = sqlite3.connect(source)
        try:
            self.assertEqual(
                connection.execute("PRAGMA journal_mode = WAL").fetchone(),
                ("wal",),
            )
        finally:
            connection.close()

        readonly_dir = Path(self.temporary.name) / "readonly-backup"
        readonly_dir.mkdir()
        backup = readonly_dir / migrate.STATE_DB_NAME
        migrate.copy_database_backup(source, backup)
        destination_dir = Path(self.temporary.name) / "restored-state"
        destination_dir.mkdir()
        destination = destination_dir / migrate.STATE_DB_NAME

        os.chmod(backup, 0o400)
        os.chmod(readonly_dir, 0o500)
        try:
            migrate.restore_database_from_backup(backup, destination)
        finally:
            os.chmod(readonly_dir, 0o700)

        for database in (backup, destination):
            for suffix in ("-wal", "-shm", "-journal"):
                self.assertFalse(Path(f"{database}{suffix}").exists())
        self.assertEqual(
            list(destination_dir.glob(".*.provider-restore-*")),
            [],
        )
        connection = sqlite3.connect(
            migrate.sqlite_read_only_uri(destination, immutable=True), uri=True
        )
        try:
            self.assertEqual(
                connection.execute("PRAGMA integrity_check").fetchall(), [("ok",)]
            )
        finally:
            connection.close()

    def test_special_rollout_mode_survives_backup_apply_and_restore(self) -> None:
        special_mode = 0o6750
        os.chmod(self.fixture.proxy_rollout, special_mode)
        if stat.S_IMODE(self.fixture.proxy_rollout.stat().st_mode) != special_mode:
            self.skipTest("filesystem does not retain setuid/setgid mode bits")

        with mock.patch.object(
            migrate, "find_processes_with_open_state", return_value=[]
        ):
            migrate.apply_migration(
                codex_home=self.fixture.codex_home,
                sqlite_home=self.fixture.sqlite_home,
                backup_dir=self.fixture.backup_dir,
                source_provider="proxy",
                target_provider="openai",
                migrate_config=False,
                confirm_stopped=True,
            )
            self.assertEqual(
                stat.S_IMODE(self.fixture.proxy_rollout.stat().st_mode),
                special_mode,
            )
            migrate.restore_from_backup(
                backup_dir=self.fixture.backup_dir,
                codex_home=self.fixture.codex_home,
                sqlite_home=self.fixture.sqlite_home,
                confirm_stopped=True,
            )
        self.assertEqual(
            stat.S_IMODE(self.fixture.proxy_rollout.stat().st_mode),
            special_mode,
        )

    def test_all_cli_parsers_expose_version(self) -> None:
        for module in (migrate, verify_cli, restore_cli):
            output = io.StringIO()
            with self.assertRaises(SystemExit) as raised, redirect_stdout(output):
                module.build_parser().parse_args(["--version"])
            self.assertEqual(raised.exception.code, 0)
            self.assertEqual(output.getvalue().strip(), migrate.TOOL_VERSION)

    def test_container_entrypoint_dispatches_all_commands(self) -> None:
        environment = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
        entrypoint = REPO_ROOT / "docker_entrypoint.py"
        for command in ("migrate", "verify", "restore"):
            completed = subprocess.run(
                [sys.executable, "-B", str(entrypoint), command, "--version"],
                cwd=REPO_ROOT,
                env=environment,
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertEqual(completed.stdout.strip(), migrate.TOOL_VERSION)

        rejected = subprocess.run(
            [sys.executable, "-B", str(entrypoint), "unknown"],
            cwd=REPO_ROOT,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(rejected.returncode, 2)
        self.assertIn("unknown container command", rejected.stderr)
        self.assertNotIn("Traceback", rejected.stderr)

    def test_verifier_cli_reports_corrupt_manifest_without_traceback(self) -> None:
        self.fixture.backup_dir.mkdir()
        (self.fixture.backup_dir / migrate.MANIFEST_NAME).write_bytes(b"\xff")
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            status = verify_cli.main(
                ["--backup-dir", str(self.fixture.backup_dir)]
            )
        self.assertEqual(status, 1)
        self.assertIn("error: cannot read backup manifest", stderr.getvalue())
        self.assertNotIn("Traceback", stderr.getvalue())

    def test_public_sources_contain_no_literal_user_home_paths(self) -> None:
        private_home_patterns = (
            re.compile("/" + r"home/[^/\s]+/"),
            re.compile("/" + r"Users/[^/\s]+/"),
            re.compile(r"[A-Za-z]:\\" + r"Users\\[^\\\s]+\\"),
        )
        public_files = [
            REPO_ROOT / ".dockerignore",
            REPO_ROOT / ".gitignore",
            REPO_ROOT / "Dockerfile",
            REPO_ROOT / "Makefile",
            REPO_ROOT / "README.md",
            REPO_ROOT / "docker_entrypoint.py",
            REPO_ROOT / "migrate.py",
            REPO_ROOT / "restore.py",
            REPO_ROOT / "verify.py",
            *sorted((REPO_ROOT / "tests").glob("*.py")),
        ]
        for path in public_files:
            text = path.read_text(encoding="utf-8")
            for pattern in private_home_patterns:
                self.assertIsNone(
                    pattern.search(text),
                    f"literal private home path found in {path.relative_to(REPO_ROOT)}",
                )


if __name__ == "__main__":
    unittest.main()
