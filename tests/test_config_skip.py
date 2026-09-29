"""An already-selected target must not block migration of saved sessions."""

from __future__ import annotations

import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock

from test_migration import MigrationFixture

import migrate


class ConfigSkipTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="migration-config-skip-")
        self.addCleanup(temporary.cleanup)
        self.fixture = MigrationFixture(Path(temporary.name))
        self.config = self.fixture.codex_home / "config.toml"

    def cli(self, *extra):
        stdout, stderr = io.StringIO(), io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            status = migrate.main(
                [
                    "--codex-home",
                    str(self.fixture.codex_home),
                    "--from-provider",
                    "proxy",
                    "--to-provider",
                    "openai",
                    "--migrate-config",
                    "--workers",
                    "1",
                    "--json",
                    *extra,
                ]
            )
        return status, stdout.getvalue(), stderr.getvalue()

    def test_dry_run_warns_and_reports_skipped_for_explicit_or_default_openai(self):
        for selector in ('model_provider = "openai"\n', ""):
            with self.subTest(selector=selector):
                original = (selector + '# User settings\nmodel = "gpt-test"\n').encode()
                self.config.write_bytes(original)
                metadata = migrate.capture_file_metadata(self.config)
                status, stdout, stderr = self.cli("--progress", "json")
                self.assertEqual(status, 0, stderr)
                report = json.loads(stdout)
                self.assertEqual(report["config_migration"], "skipped; already openai")
                self.assertGreater(report["files_requiring_changes"], 0)
                events = [json.loads(line) for line in stderr.splitlines()]
                warnings = [event for event in events if event["event"] == "warning"]
                self.assertEqual(len(warnings), 1)
                self.assertIn("skipping config migration", warnings[0]["message"])
                self.assertEqual(events[-1]["event"], "completed")
                self.assertEqual(self.config.read_bytes(), original)
                self.assertEqual(migrate.capture_file_metadata(self.config), metadata)
                self.assertFalse(self.fixture.backup_dir.exists())

    def test_warning_is_visible_when_progress_is_off(self):
        self.config.write_text('model = "gpt-test"\n')
        status, stdout, stderr = self.cli("--progress", "off")
        self.assertEqual(status, 0, stderr)
        self.assertEqual(
            json.loads(stdout)["config_migration"], "skipped; already openai"
        )
        self.assertTrue(stderr.startswith("warning: config.toml already uses"))
        self.assertEqual(len(stderr.splitlines()), 1)

    def test_apply_verify_restore_preserve_config_and_stale_provider_definitions(self):
        fixture = self.fixture
        # Unused definitions and profiles need no conversion when config is skipped.
        original = self.config.read_bytes().replace(b'model_provider = "proxy"\n', b"")
        self.config.write_bytes(original)
        profile = fixture.codex_home / "work.config.toml"
        profile.write_text('model_provider = "proxy"\n')
        metadata = migrate.capture_file_metadata(self.config)
        rollout = fixture.proxy_rollout.read_bytes()
        with mock.patch.object(
            migrate, "find_processes_with_open_state", return_value=[]
        ):
            status, stdout, stderr = self.cli(
                "--apply",
                "--confirm-codex-stopped",
                "--backup-dir",
                str(fixture.backup_dir),
                "--progress",
                "json",
            )
            self.assertEqual(status, 0, stderr)
            self.assertGreater(json.loads(stdout)["changed_rollout_files"], 0)
            self.assertFalse(
                migrate.load_backup_manifest(fixture.backup_dir)["migrate_config"]
            )
            self.assertEqual(self.config.read_bytes(), original)
            self.assertEqual(migrate.capture_file_metadata(self.config), metadata)
            self.assertTrue(
                migrate.verify_against_backup(
                    backup_dir=fixture.backup_dir
                ).config_matches_expected
            )
            migrate.restore_from_backup(
                backup_dir=fixture.backup_dir, confirm_stopped=True
            )
        self.assertEqual(self.config.read_bytes(), original)
        self.assertEqual(migrate.capture_file_metadata(self.config), metadata)
        self.assertEqual(fixture.proxy_rollout.read_bytes(), rollout)
        self.assertEqual(profile.read_text(), 'model_provider = "proxy"\n')

    def test_skipped_config_still_gets_strict_mtime_verification(self):
        fixture = self.fixture
        self.config.write_text('model_provider = "openai"\n')
        with mock.patch.object(
            migrate, "find_processes_with_open_state", return_value=[]
        ):
            status, _, stderr = self.cli(
                "--apply",
                "--confirm-codex-stopped",
                "--backup-dir",
                str(fixture.backup_dir),
                "--progress",
                "off",
            )
        self.assertEqual(status, 0, stderr)
        metadata = self.config.stat()
        os.utime(
            self.config, ns=(metadata.st_atime_ns, metadata.st_mtime_ns + 1_000_000_000)
        )
        with self.assertRaisesRegex(
            migrate.MigrationError, "config.toml mtime changed"
        ):
            migrate.verify_against_backup(backup_dir=fixture.backup_dir)

    def test_unrelated_provider_and_invalid_configs_still_fail_before_backup(self):
        fixture = self.fixture
        original_rollout = fixture.proxy_rollout.read_bytes()
        for config in (
            'model_provider = "another-provider"\n',
            "model_provider = 123\n",
            'model_provider = "openai"\nbroken = [',
            'model_provider = "proxy"\n',
        ):
            with (
                self.subTest(config=config),
                mock.patch.object(
                    migrate, "find_processes_with_open_state", return_value=[]
                ),
            ):
                self.config.write_text(config)
                status, stdout, stderr = self.cli(
                    "--apply",
                    "--confirm-codex-stopped",
                    "--backup-dir",
                    str(fixture.backup_dir),
                    "--progress",
                    "off",
                )
                self.assertEqual(status, 1)
                self.assertEqual(stdout, "")
                self.assertIn("error:", stderr)
                self.assertNotIn("warning:", stderr)
                self.assertFalse(fixture.backup_dir.exists())
                self.assertEqual(fixture.proxy_rollout.read_bytes(), original_rollout)

    def test_real_config_conversion_remains_eligible(self):
        status, stdout, stderr = self.cli("--progress", "json")
        self.assertEqual(status, 0, stderr)
        self.assertEqual(json.loads(stdout)["config_migration"], "eligible")
        self.assertFalse(
            any(json.loads(line)["event"] == "warning" for line in stderr.splitlines())
        )

    def test_invalid_config_is_rejected_before_scanning_sessions(self):
        self.config.write_text('model_provider = "proxy"\n')
        for apply in (False, True):
            with (
                self.subTest(apply=apply),
                mock.patch.object(
                    migrate, "find_processes_with_open_state", return_value=[]
                ),
                mock.patch.object(migrate, "analyze_migratable_rollouts") as scan,
            ):
                args = ["--progress", "off"]
                if apply:
                    args += [
                        "--apply",
                        "--confirm-codex-stopped",
                        "--backup-dir",
                        str(self.fixture.backup_dir),
                    ]
                status, _, stderr = self.cli(*args)
                self.assertEqual(status, 1)
                self.assertIn("has no [model_providers.proxy] table", stderr)
                scan.assert_not_called()
                self.assertFalse(self.fixture.backup_dir.exists())


if __name__ == "__main__":
    unittest.main()
