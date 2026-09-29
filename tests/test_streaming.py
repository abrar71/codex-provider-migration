"""Regression coverage for bounded memory, exact comparisons, and CLI telemetry."""

from __future__ import annotations

import io
import json
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from test_migration import REPO_ROOT, MigrationFixture

import migrate
import migration_io
import progress
import restore as restore_cli
import verify as verify_cli


class StreamingTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="migration-streaming-")
        self.addCleanup(temporary.cleanup)
        self.fixture = MigrationFixture(Path(temporary.name))

    def test_apply_verify_restore_never_read_whole_rollouts(self):
        fixture = self.fixture
        fixture.enable_paginated_history()
        original_read = Path.read_bytes

        def guarded_read(path):
            if path.suffix == ".jsonl":
                raise AssertionError("rollout must be streamed")
            return original_read(path)

        with (
            mock.patch.object(Path, "read_bytes", guarded_read),
            mock.patch.object(
                migrate, "find_processes_with_open_state", return_value=[]
            ),
        ):
            report = migrate.apply_migration(
                codex_home=fixture.codex_home,
                sqlite_home=fixture.sqlite_home,
                backup_dir=fixture.backup_dir,
                source_provider="proxy",
                target_provider="openai",
                migrate_config=True,
                confirm_stopped=True,
            )
            verified = migrate.verify_against_backup(backup_dir=fixture.backup_dir)
            self.assertEqual(report, verified)
            migrate.restore_from_backup(
                backup_dir=fixture.backup_dir, confirm_stopped=True
            )

    def test_source_drift_cannot_commit_partial_streamed_output(self):
        fixture = self.fixture
        _, plan = migrate.analyze_migratable_rollouts(
            fixture.codex_home,
            fixture.codex_home / migrate.STATE_DB_NAME,
            "proxy",
            "openai",
            allow_paginated=False,
        )
        relative = fixture.proxy_rollout.relative_to(fixture.codex_home).as_posix()
        changed = fixture.proxy_rollout.read_bytes().replace(
            b"literal text", b"changed text"
        )
        fixture.proxy_rollout.write_bytes(changed)
        with self.assertRaisesRegex(migrate.MigrationError, "changed since preflight"):
            migrate.atomic_write(
                fixture.proxy_rollout,
                plan.files[relative].chunks(fixture.proxy_rollout),
                preserve_mtime=True,
            )
        self.assertEqual(fixture.proxy_rollout.read_bytes(), changed)
        self.assertEqual(list(fixture.sessions.glob("*.provider-migration-*")), [])

    def test_record_limit_fails_before_backup(self):
        with (
            mock.patch.object(migration_io, "MAX_RECORD_BYTES", 32),
            mock.patch.object(
                migrate, "find_processes_with_open_state", return_value=[]
            ),
        ):
            with self.assertRaisesRegex(migrate.MigrationError, "JSONL record exceeds"):
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

    def test_sqlite_comparison_preserves_multiset_and_binary_collation(self):
        first = self.fixture.root / "a.sqlite"
        second = self.fixture.root / "b.sqlite"
        rows = [("a", b"x"), ("A", b"x"), ("a", b"x"), (None, b"y"), ("b", None)]
        for path, values in [(first, rows), (second, list(reversed(rows)))]:
            with sqlite3.connect(path) as db:
                db.execute("CREATE TABLE records (text TEXT COLLATE NOCASE, data BLOB)")
                db.executemany("INSERT INTO records VALUES (?, ?)", values)
        self.assertEqual(
            migrate.compare_sqlite_databases(first, second, "proxy", "openai"),
            (1, 5, 0),
        )
        with sqlite3.connect(second) as db:
            db.execute("UPDATE records SET data = 'tampered' WHERE rowid=1")
        with self.assertRaisesRegex(migrate.MigrationError, "unexpected SQLite change"):
            migrate.compare_sqlite_databases(first, second, "proxy", "openai")

    def test_json_progress_is_separate_and_reports_success_and_failure(self):
        stdout, stderr = io.StringIO(), io.StringIO()
        args = [
            "--codex-home",
            str(self.fixture.codex_home),
            "--from-provider",
            "proxy",
            "--to-provider",
            "openai",
            "--json",
            "--progress",
            "json",
        ]
        with redirect_stdout(stdout), redirect_stderr(stderr):
            status = migrate.main(args)
        self.assertEqual(status, 0)
        self.assertEqual(
            json.loads(stdout.getvalue())["result"], "dry run; no Codex records changed"
        )
        events = [json.loads(line) for line in stderr.getvalue().splitlines()]
        self.assertEqual(events[0]["event"], "started")
        self.assertEqual(events[-1]["event"], "completed")
        scans = [
            e
            for e in events
            if e["phase"] == "Scan and validate rollouts" and e["event"] == "completed"
        ]
        self.assertEqual(scans[0]["files_processed"], 2)
        self.assertEqual(scans[0]["bytes_processed"], scans[0]["bytes_total"])
        self.fixture.proxy_rollout.write_bytes(b'{"model_provider":"proxy", bad}\n')
        stdout, stderr = io.StringIO(), io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            status = migrate.main(args)
        self.assertEqual(status, 1)
        self.assertEqual(stdout.getvalue(), "")
        events = [json.loads(line) for line in stderr.getvalue().splitlines()]
        self.assertEqual(events[-1]["event"], "failed")
        self.assertTrue(any(e["event"] == "error" for e in events))

    def test_progress_heartbeat_during_blocking_work(self):
        output = io.StringIO()
        args = SimpleNamespace(
            progress="json", progress_interval=0.01, max_record_mib=64
        )
        with (
            redirect_stderr(output),
            progress.reporting(args),
            progress.phase("SQLite work"),
        ):
            threading.Event().wait(0.08)
        events = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertTrue(any(e["event"] == "progress" for e in events))
        self.assertEqual(events[-1]["event"], "completed")

    @unittest.skipUnless(
        sys.platform.startswith("linux"), "Linux address-space limit regression"
    )
    def test_archive_larger_than_memory_limit_completes_all_operations(self):
        completed = subprocess.run(
            [
                sys.executable,
                "-B",
                "-c",
                "import sys; sys.path.insert(0, 'tests'); from test_streaming import bounded_worker; bounded_worker(sys.argv[1])",
                str(self.fixture.root / "bounded"),
            ],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=180,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr[-5000:])
        result = json.loads(completed.stdout)
        self.assertGreater(result["archive_bytes"], result["address_space_limit"])
        self.assertLess(result["peak_memory_bytes"], result["address_space_limit"])
        self.assertEqual(
            result["operations"], ["dry-run", "apply", "verify", "restore"]
        )


def bounded_worker(root: str, worker_count: int = 1, limit_mib: int = 256) -> None:
    """Exercise the CLI and real rollback/verification paths under a hard RAM bound."""
    import resource

    limit = limit_mib * 1024**2
    resource.setrlimit(resource.RLIMIT_AS, (limit, limit))
    fixture = MigrationFixture(Path(root))
    original = fixture.proxy_rollout.read_bytes()
    record = MigrationFixture._line(
        {"type": "response_item", "payload": {"text": "x" * 65536}}
    )
    parts = [fixture.proxy_rollout]
    if worker_count > 1:
        for index in range(worker_count - 1):
            part = fixture.sessions / f"rollout-extra-{index}.jsonl"
            part.write_bytes(original)
            parts.append(part)
    record_count = limit // len(record) + 512
    for index, part in enumerate(parts):
        with part.open("ab") as stream:
            for _ in range(index, record_count, len(parts)):
                stream.write(record)
    original_digest = migrate.sha256_file(fixture.proxy_rollout)
    archive_size = sum(path.stat().st_size for path in parts)
    with sqlite3.connect(fixture.sqlite_home / migrate.STATE_DB_NAME) as db:
        db.execute("CREATE TABLE payloads (value BLOB)")
        db.executemany(
            "INSERT INTO payloads VALUES (?)", ((b"z" * 65536,) for _ in range(512))
        )
    base = [
        "--codex-home",
        str(fixture.codex_home),
        "--from-provider",
        "proxy",
        "--to-provider",
        "openai",
        "--migrate-config",
        "--progress",
        "json",
        "--json",
    ]
    operations = []
    peak_memory = 0
    for name, command, args in [
        ("dry-run", migrate.main, base),
        (
            "apply",
            migrate.main,
            [
                *base,
                "--apply",
                "--confirm-codex-stopped",
                "--backup-dir",
                str(fixture.backup_dir),
            ],
        ),
        (
            "verify",
            verify_cli.main,
            ["--backup-dir", str(fixture.backup_dir), "--progress", "json", "--json"],
        ),
        (
            "restore",
            restore_cli.main,
            [
                "--backup-dir",
                str(fixture.backup_dir),
                "--confirm-codex-stopped",
                "--progress",
                "json",
                "--json",
            ],
        ),
    ]:
        stdout, stderr = io.StringIO(), io.StringIO()
        with (
            mock.patch.object(
                migrate, "find_processes_with_open_state", return_value=[]
            ),
            redirect_stdout(stdout),
            redirect_stderr(stderr),
        ):
            result = command([*args, "--workers", str(worker_count)])
        if result:
            raise AssertionError(stderr.getvalue())
        json.loads(stdout.getvalue())
        events = [json.loads(line) for line in stderr.getvalue().splitlines()]
        peak_memory = max(
            peak_memory, *(event.get("peak_memory_bytes") or 0 for event in events)
        )
        assert events[-1]["event"] == "completed"
        operations.append(name)
    assert migrate.sha256_file(fixture.proxy_rollout) == original_digest
    assert fixture.proxy_rollout.open("rb").read(len(original)) == original
    print(
        json.dumps(
            {
                "archive_bytes": archive_size,
                "address_space_limit": limit,
                "peak_memory_bytes": max(
                    peak_memory, progress.peak_memory_bytes() or 0
                ),
                "operations": operations,
            }
        )
    )
