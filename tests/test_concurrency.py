"""Concurrency must preserve exact plans, recovery, and bounded scheduling."""

from __future__ import annotations

import io
import json
import os
import signal
import stat
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import test_migration
from test_migration import REPO_ROOT, MigrationFixture

import migrate
import migration_io
import migration_workers as workers
import progress
import restore
import verify


def process_job(item):
    """A real worker, also used by subprocess cancellation checks."""
    action, marker = item
    if action == "crash":
        os._exit(17)
    if marker:
        Path(marker).write_text(str(os.getpid()))
    if action == "wait":
        try:
            while True:
                workers.advance(1)
                time.sleep(0.01)
        finally:
            Path(marker + ".stopped").touch()
    if action == "slow":
        time.sleep(0.05)
    workers.advance(17, files=1)
    return os.getpid()


class ConcurrencyTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="migration-concurrency-")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.fixture = MigrationFixture(self.root)

    def test_default_worker_count_and_cli_overrides(self):
        for cpus, expected in ((1, 1), (2, 1), (4, 3), (8, 7), (16, 14), (64, 57)):
            with mock.patch.object(workers, "available_cpus", return_value=cpus):
                self.assertEqual(workers.resolve_workers("auto"), expected)
                self.assertEqual(workers.resolve_workers(0), cpus)
                self.assertEqual(workers.resolve_workers(2), 2)
        for module in (migrate, verify, restore):
            args = [] if module is migrate else ["--backup-dir", "/tmp/example"]
            if module is migrate:
                args = ["--from-provider", "proxy", "--to-provider", "openai"]
            parser = module.build_parser()
            self.assertEqual(parser.parse_args(args).workers, "auto")
            self.assertEqual(parser.parse_args([*args, "--workers", "1"]).workers, 1)
            self.assertEqual(parser.parse_args([*args, "--workers", "0"]).workers, 0)
            for invalid in ("-1", "1.5", "many"):
                with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                    parser.parse_args([*args, "--workers", invalid])

    @unittest.skipUnless(hasattr(os, "sched_getaffinity"), "Linux CPU limits")
    def test_auto_respects_affinity_and_inherited_cpu_quota(self):
        values = {
            "/proc/self/cgroup": "0::/parent/child\n",
            "/sys/fs/cgroup/parent/child/cpu.max": "max 100000",
            "/sys/fs/cgroup/parent/cpu.max": "400000 100000",
        }

        def read(path, *args, **kwargs):
            if str(path) not in values:
                raise FileNotFoundError(path)
            return values[str(path)]

        with (
            mock.patch.object(os, "cpu_count", return_value=64),
            mock.patch.object(os, "sched_getaffinity", return_value=set(range(8))),
            mock.patch.object(Path, "read_text", read),
        ):
            self.assertEqual(workers.available_cpus(), 4)
            self.assertEqual(workers.resolve_workers("auto"), 3)
            self.assertEqual(workers.resolve_workers(0), 4)

    def test_parallel_plan_equals_sequential_including_history_references(self):
        fixture = self.fixture
        fixture.enable_history_base()
        arguments = (
            fixture.codex_home,
            fixture.sqlite_home / migrate.STATE_DB_NAME,
            "proxy",
            "openai",
        )
        sequential = migrate.build_rollout_migration_plan(*arguments)
        with workers.configuration(3):
            parallel = migrate.build_rollout_migration_plan(*arguments)
        self.assertEqual(parallel, sequential)
        self.assertEqual(list(parallel.files), list(sequential.files))
        self.assertGreater(parallel.history_base_offsets_changed, 0)

    @unittest.skipUnless(sys.platform.startswith("linux"), "Linux memory limits")
    def test_concurrent_archive_larger_than_memory_limit_completes_round_trip(self):
        result = subprocess.run(
            [
                sys.executable,
                "-B",
                "-c",
                "import sys; sys.path.insert(0, 'tests'); from test_streaming import bounded_worker; bounded_worker(sys.argv[1], 3, 512)",
                str(self.root / "bounded"),
            ],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            timeout=180,
            # Bound glibc's per-thread virtual reservations as well as real RAM.
            env={**os.environ, "MALLOC_ARENA_MAX": "2"},
        )
        self.assertEqual(result.returncode, 0, result.stderr[-5000:])
        report = json.loads(result.stdout)
        self.assertGreater(report["archive_bytes"], report["address_space_limit"])
        self.assertLess(report["peak_memory_bytes"], report["address_space_limit"])
        self.assertEqual(
            report["operations"], ["dry-run", "apply", "verify", "restore"]
        )

    def test_parallel_chained_history_and_cycle_checks(self):
        # Exercise the established multi-generation and cycle fixtures in processes.
        for name in (
            "test_history_base_translates_chained_source_ordinals",
            "test_history_base_refuses_reference_cycles",
        ):
            case = test_migration.ProviderMigrationTests(name)
            with workers.configuration(3):
                result = unittest.TestResult()
                case.run(result)
            self.assertTrue(result.wasSuccessful(), result.errors + result.failures)

    def test_duplicate_ids_across_workers_and_record_limit_are_rejected(self):
        fixture = self.fixture
        fixture.enable_paginated_history()
        original = fixture.proxy_rollout.read_bytes()
        duplicate = fixture.sessions / "rollout-duplicate.jsonl"
        duplicate.write_bytes(original)
        with (
            workers.configuration(3),
            self.assertRaisesRegex(migrate.MigrationError, "duplicate"),
        ):
            migrate.build_rollout_migration_plan(
                fixture.codex_home,
                fixture.sqlite_home / migrate.STATE_DB_NAME,
                "proxy",
                "openai",
            )
        duplicate.unlink()
        with (
            mock.patch.object(migration_io, "MAX_RECORD_BYTES", 32),
            workers.configuration(3),
        ):
            with self.assertRaisesRegex(migrate.MigrationError, "record exceeds 32"):
                migrate.scan_rollouts(
                    fixture.codex_home, "proxy", "openai", allow_paginated=True
                )
        self.assertFalse(fixture.backup_dir.exists())
        self.assertEqual(fixture.proxy_rollout.read_bytes(), original)

    def test_parallel_apply_verify_restore_matches_original_bytes_and_metadata(self):
        fixture = self.fixture
        fixture.enable_history_base()
        paths = migrate.rollout_paths(fixture.codex_home)
        before = {p: (p.read_bytes(), migrate.capture_file_metadata(p)) for p in paths}
        with (
            workers.configuration(3),
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
            self.assertEqual(
                report, migrate.verify_against_backup(backup_dir=fixture.backup_dir)
            )
            migrate.restore_from_backup(
                backup_dir=fixture.backup_dir, confirm_stopped=True
            )
        self.assertEqual(
            before,
            {p: (p.read_bytes(), migrate.capture_file_metadata(p)) for p in paths},
        )

    def test_parallel_backup_preserves_readonly_directory_permissions(self):
        fixture = self.fixture
        db = fixture.sqlite_home / migrate.STATE_DB_NAME
        history = fixture.sqlite_home / migrate.HISTORY_DB_NAME
        analysis, plan = migrate.analyze_migratable_rollouts(
            fixture.codex_home, db, "proxy", "openai", allow_paginated=False
        )
        database = migrate.analyze_database(db, "proxy")
        history_analysis, _ = migrate.inspect_history_database(
            history, db, fixture.codex_home, "proxy", "openai", rollout_migration=plan
        )
        fixture.sessions.chmod(0o555)
        destination = fixture.backup_dir / fixture.sessions.relative_to(
            fixture.codex_home
        )
        try:
            with workers.configuration(3):
                migrate.create_backup(
                    codex_home=fixture.codex_home,
                    sqlite_home=fixture.sqlite_home,
                    db_path=db,
                    history_db_path=history,
                    backup_dir=fixture.backup_dir,
                    source_provider="proxy",
                    target_provider="openai",
                    rollout_analysis=analysis,
                    database_analysis=database,
                    history_database_analysis=history_analysis,
                    migrate_config=False,
                )
            self.assertEqual(stat.S_IMODE(destination.stat().st_mode), 0o555)
            self.assertEqual(
                destination.stat().st_mtime_ns, fixture.sessions.stat().st_mtime_ns
            )
            self.assertEqual(
                (destination / fixture.proxy_rollout.name).read_bytes(),
                fixture.proxy_rollout.read_bytes(),
            )
        finally:
            fixture.sessions.chmod(0o755)
            if destination.is_dir():
                destination.chmod(0o755)

    def test_parallel_verification_failure_rolls_back_after_workers_finish(self):
        fixture = self.fixture
        fixture.enable_history_base()
        original = fixture.proxy_rollout.read_bytes()
        compare = migrate.matches_chunks
        injected = threading.Event()

        def fail_live(path, expected):
            if path == fixture.proxy_rollout and not injected.is_set():
                injected.set()
                expected.close()
                raise migrate.MigrationError("injected comparison failure")
            return compare(path, expected)

        with (
            workers.configuration(3),
            mock.patch.object(
                migrate, "find_processes_with_open_state", return_value=[]
            ),
            mock.patch.object(migrate, "matches_chunks", fail_live),
        ):
            with self.assertRaisesRegex(
                migrate.MigrationError, "injected comparison failure"
            ):
                migrate.apply_migration(
                    codex_home=fixture.codex_home,
                    sqlite_home=fixture.sqlite_home,
                    backup_dir=fixture.backup_dir,
                    source_provider="proxy",
                    target_provider="openai",
                    migrate_config=False,
                    confirm_stopped=True,
                )
        self.assertEqual(fixture.proxy_rollout.read_bytes(), original)
        self.assertEqual(
            migrate.load_backup_manifest(fixture.backup_dir)["status"], "rolled_back"
        )

    def test_thread_work_overlaps_is_bounded_and_drains_on_consumer_error(self):
        entered = threading.Barrier(2, timeout=5)
        producer_count = 0
        completed = []
        stopped = []

        def items():
            nonlocal producer_count
            for i in range(100):
                producer_count += 1
                yield i

        def task(i):
            try:
                entered.wait()
                workers.advance(10, files=1)
                completed.append(i)
                return i
            finally:
                stopped.append(i)

        status = progress.Stage("bounded threads")
        with workers.configuration(2):
            with (
                self.assertRaisesRegex(RuntimeError, "consumer failed"),
                workers.map_files(task, items(), status) as results,
            ):
                next(results)
                self.assertEqual(producer_count, 2)
                raise RuntimeError("consumer failed")
        self.assertEqual(sorted(stopped), [0, 1])
        self.assertGreaterEqual(len(completed), 1)
        self.assertEqual(status.completed_files, len(completed))
        self.assertEqual(status.completed_bytes, 10 * len(completed))
        self.assertEqual(status.active_workers, 0)

    def test_cli_interrupt_after_write_restores_state_and_returns_130(self):
        fixture = self.fixture
        fixture.enable_history_base()
        before = {
            path: path.read_bytes()
            for path in migrate.rollout_paths(fixture.codex_home)
        }
        write = migrate.atomic_write
        interrupted = False

        def interrupt_once(path, *args, **kwargs):
            nonlocal interrupted
            result = write(path, *args, **kwargs)
            if path.suffix == ".jsonl" and not interrupted:
                interrupted = True
                raise KeyboardInterrupt
            return result

        output = io.StringIO()
        with (
            redirect_stderr(output),
            mock.patch.object(
                migrate, "find_processes_with_open_state", return_value=[]
            ),
            mock.patch.object(migrate, "atomic_write", interrupt_once),
        ):
            result = migrate.main(
                [
                    "--codex-home",
                    str(fixture.codex_home),
                    "--from-provider",
                    "proxy",
                    "--to-provider",
                    "openai",
                    "--apply",
                    "--confirm-codex-stopped",
                    "--backup-dir",
                    str(fixture.backup_dir),
                    "--workers",
                    "3",
                    "--json",
                    "--progress",
                    "json",
                ]
            )
        self.assertEqual(result, 130)
        self.assertTrue(interrupted)
        self.assertEqual(before, {path: path.read_bytes() for path in before})
        self.assertEqual(
            migrate.load_backup_manifest(fixture.backup_dir)["status"], "rolled_back"
        )
        events = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual(events[-1]["event"], "failed")

    def test_process_failure_drains_and_pool_can_be_recreated(self):
        with workers.configuration(2):
            with (
                self.assertRaisesRegex(migrate.MigrationError, "worker exited"),
                workers.map_files(
                    process_job,
                    [("crash", None), ("ok", None)],
                    progress.Stage("failure"),
                    processes=True,
                ) as results,
            ):
                list(results)
            with workers.map_files(
                process_job, [("ok", None)] * 2, progress.Stage("retry"), processes=True
            ) as results:
                self.assertTrue(all(pid != os.getpid() for pid in results))

    @unittest.skipUnless(os.name == "posix", "POSIX SIGINT")
    def test_sigint_stops_process_work_before_returning(self):
        marker = str(self.root / "worker-started")
        script = """
import sys
sys.path.insert(0, 'tests')
from test_concurrency import process_job
import migration_workers as workers
import progress
try:
    with workers.configuration(2), workers.map_files(process_job, [('wait', sys.argv[1])], progress.Stage('wait'), processes=True) as results:
        list(results)
except KeyboardInterrupt:
    print('interrupted after workers stopped')
"""
        child = subprocess.Popen(
            [sys.executable, "-B", "-c", script, marker],
            cwd=REPO_ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        try:
            deadline = time.monotonic() + 10
            while (
                not Path(marker).exists()
                and time.monotonic() < deadline
                and child.poll() is None
            ):
                time.sleep(0.01)
            self.assertTrue(Path(marker).exists())
            child.send_signal(signal.SIGINT)
            stdout, stderr = child.communicate(timeout=10)
            self.assertEqual(child.returncode, 0, stderr)
            self.assertIn("interrupted after workers stopped", stdout)
            self.assertTrue(Path(marker + ".stopped").exists())
        finally:
            if child.poll() is None:
                child.kill()
                child.communicate()

    def test_progress_includes_all_process_memory_and_exact_counts(self):
        output = io.StringIO()
        args = SimpleNamespace(
            progress="json", progress_interval=0.01, max_record_mib=64, workers=3
        )
        with (
            redirect_stderr(output),
            progress.reporting(args),
            progress.phase("process counts", total_files=8, total_bytes=136) as status,
        ):
            with workers.map_files(
                process_job, [("slow", None)] * 8, status, processes=True
            ) as results:
                self.assertGreaterEqual(len(set(results)), 2)
        events = [json.loads(line) for line in output.getvalue().splitlines()]
        final = events[-1]
        self.assertEqual(final["workers"], 3)
        self.assertEqual(final["files_processed"], 8)
        self.assertEqual(final["bytes_processed"], 136)
        self.assertEqual(final["active_workers"], 0)
        self.assertGreater(final["worker_peak_memory_bytes"], 0)
        self.assertGreater(
            final["peak_memory_bytes"], final["worker_peak_memory_bytes"]
        )


if __name__ == "__main__":
    unittest.main()
