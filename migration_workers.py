"""Bounded file workers, cooperative cancellation, and parent-owned progress."""

from __future__ import annotations

import argparse
import contextlib
import itertools
import math
import multiprocessing
import os
import signal
import threading
from concurrent.futures import (
    FIRST_COMPLETED,
    CancelledError,
    ProcessPoolExecutor,
    ThreadPoolExecutor,
    wait,
)
from concurrent.futures.process import BrokenProcessPool
from pathlib import Path

_local = threading.local()
_process_counters = None
_process_stop = None
_runtime = None
_STRIDE = 4  # bytes, files, PID, peak RSS


class ProcessStop:
    """One coordinator writes one shared byte; workers only read it.

    A lock-based Event can stay locked if a worker dies while polling it.
    This flag also avoids a shared lock on every JSONL record.
    """

    def __init__(self, context):
        self.flag = context.RawValue("b", 0)

    def is_set(self):
        return bool(self.flag.value)

    def set(self):
        self.flag.value = 1

    def clear(self):
        self.flag.value = 0


def available_cpus() -> int:
    """Logical CPUs available through affinity and, on Linux, cgroup quotas."""
    count = os.cpu_count() or 1
    if hasattr(os, "sched_getaffinity"):
        count = min(count, len(os.sched_getaffinity(0)))
    # Check all ancestors: containers may inherit a quota from a parent cgroup.
    try:
        entries = Path("/proc/self/cgroup").read_text().splitlines()
        for entry in entries:
            _, controllers, relative = entry.split(":", 2)
            if controllers and "cpu" not in controllers.split(","):
                continue
            root = Path("/sys/fs/cgroup")
            if controllers:
                root /= "cpu"
            group = root / relative.lstrip("/")
            for directory in (group, *group.parents):
                if directory != root and root not in directory.parents:
                    break
                try:
                    if controllers:
                        quota = int((directory / "cpu.cfs_quota_us").read_text())
                        period = int((directory / "cpu.cfs_period_us").read_text())
                    else:
                        quota_text, period_text = (
                            (directory / "cpu.max").read_text().split()
                        )
                        if quota_text == "max":
                            continue
                        quota, period = int(quota_text), int(period_text)
                    if quota > 0 and period > 0:
                        count = min(count, max(1, math.floor(quota / period)))
                except (OSError, ValueError):
                    continue
    except (OSError, ValueError):
        pass
    return max(1, count)


def parse_workers(value: str) -> int | str:
    if value == "auto":
        return value
    try:
        number = int(value)
    except ValueError:
        number = -1
    if number < 0:
        raise argparse.ArgumentTypeError(
            "workers must be 'auto' or a nonnegative integer (0 uses all available CPUs)"
        )
    return number


def resolve_workers(value: int | str) -> int:
    count = max(1, available_cpus() * 9 // 10) if value == "auto" else int(value)
    if count == 0:
        count = available_cpus()
    if count < 1:
        raise ValueError("workers must be nonnegative")
    return count


def check_cancelled() -> None:
    stop = getattr(_local, "stop", None)
    if stop is not None and stop.is_set():
        raise CancelledError("file work cancelled")


def advance(size: int = 0, files: int = 0) -> None:
    """Report bounded counters; workers never print or return file contents."""
    check_cancelled()
    status = getattr(_local, "status", None)
    if status is not None:
        status.advance(size, files)
    counters = getattr(_local, "counters", None)
    if counters is not None:
        index = _local.slot * _STRIDE
        counters[index] += size
        counters[index + 1] += files
        if _local.process:
            from progress import peak_memory_bytes

            counters[index + 3] = peak_memory_bytes() or 0


def _initialize_process(counters, stop, record_limit):
    import migration_io

    global _process_counters, _process_stop
    _process_counters, _process_stop = counters, stop
    migration_io.MAX_RECORD_BYTES = record_limit
    # The coordinator handles Ctrl+C, drains workers, then starts any rollback.
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def _invoke(function, item, slot, counters=None, stop=None):
    process = counters is None
    counters = _process_counters if process else counters
    stop = _process_stop if process else stop
    _local.counters, _local.stop, _local.slot, _local.process = (
        counters,
        stop,
        slot,
        process,
    )
    counters[slot * _STRIDE + 2] = os.getpid()
    try:
        advance()
        return function(item)
    finally:
        # Capture the final peak even when the task raises.
        if process:
            from progress import peak_memory_bytes

            counters[slot * _STRIDE + 3] = peak_memory_bytes() or 0
        _local.__dict__.clear()


@contextlib.contextmanager
def _defer_interrupts():
    previous = None
    if threading.current_thread() is threading.main_thread():
        previous = signal.signal(signal.SIGINT, signal.SIG_IGN)
    try:
        yield
    finally:
        if previous is not None:
            signal.signal(signal.SIGINT, previous)


class Runtime:
    def __init__(self, count: int):
        self.count = count
        self.processes = self.threads = None
        self.process_counters = self.process_stop = None

    def close(self):
        with _defer_interrupts():
            for executor in (self.processes, self.threads):
                if executor is not None:
                    executor.shutdown(wait=True, cancel_futures=True)

    def pool(self, processes: bool):
        if processes:
            if self.processes is None:
                import migration_io

                context = multiprocessing.get_context("spawn")
                self.process_counters = context.RawArray("q", self.count * _STRIDE)
                self.process_stop = ProcessStop(context)
                self.processes = ProcessPoolExecutor(
                    max_workers=self.count,
                    mp_context=context,
                    initializer=_initialize_process,
                    initargs=(
                        self.process_counters,
                        self.process_stop,
                        migration_io.MAX_RECORD_BYTES,
                    ),
                )
            return self.processes, self.process_counters, self.process_stop
        if self.threads is None:
            self.threads = ThreadPoolExecutor(
                max_workers=self.count, thread_name_prefix="migration-file"
            )
        return self.threads, [0] * (self.count * _STRIDE), threading.Event()

    @contextlib.contextmanager
    def map(self, function, items, status, *, processes=False):
        iterator = iter(items)
        try:
            first = next(iterator)
        except StopIteration:
            yield iter(())
            return
        iterator = itertools.chain((first,), iterator)
        if self.count == 1:

            def sequential():
                for item in iterator:
                    _local.status = status
                    status.active_workers = 1
                    try:
                        result = function(item)
                    finally:
                        _local.__dict__.clear()
                        status.active_workers = 0
                    yield result

            results = sequential()
            try:
                yield results
            finally:
                results.close()
            return

        executor, counters, stop = self.pool(processes)
        for i in range(len(counters)):
            counters[i] = 0
        stop.clear()
        pending = {}
        previous_bytes = previous_files = 0

        def collect():
            nonlocal previous_bytes, previous_files
            size = sum(counters[i] for i in range(0, len(counters), _STRIDE))
            files = sum(counters[i + 1] for i in range(0, len(counters), _STRIDE))
            status.advance(size - previous_bytes, files - previous_files)
            previous_bytes, previous_files = size, files
            status.active_workers = len(pending)
            if processes:
                import progress

                for i in range(0, len(counters), _STRIDE):
                    if counters[i + 2]:
                        progress.record_worker_memory(counters[i + 2], counters[i + 3])

        def submit(slot):
            try:
                item = next(iterator)
            except StopIteration:
                return
            args = () if processes else (counters, stop)
            pending[executor.submit(_invoke, function, item, slot, *args)] = slot

        def completed():
            for slot in range(self.count):
                submit(slot)
            while pending:
                done, _ = wait(pending, timeout=0.1, return_when=FIRST_COMPLETED)
                collect()
                # Observe every completed failure before submitting more work.
                for future in done:
                    if future.exception() is not None:
                        future.result()
                for future in done:
                    slot = pending.pop(future)
                    yield future.result()
                    submit(slot)
            collect()

        results = completed()
        broken = False
        try:
            yield results
        except BrokenProcessPool as exc:
            from migration_io import MigrationError

            broken = True
            raise MigrationError(
                "a scan worker exited unexpectedly; retry with fewer --workers"
            ) from exc
        finally:
            # No worker can outlive this phase, including an early consumer error.
            with _defer_interrupts():
                stop.set()
                for future in pending:
                    future.cancel()
                # A cancelled queued future cannot start. A broken executor may
                # never mark it CANCELLED_AND_NOTIFIED for futures.wait().
                running = [future for future in pending if not future.cancelled()]
                if running:
                    wait(running)
                results.close()
                collect()
                status.active_workers = 0
                stop.clear()
                if broken:
                    executor.shutdown(wait=True, cancel_futures=True)
                    self.processes = None


@contextlib.contextmanager
def configuration(workers: int | str):
    global _runtime
    previous = _runtime
    runtime = Runtime(resolve_workers(workers))
    _runtime = runtime
    try:
        yield runtime
    finally:
        runtime.close()
        _runtime = previous


@contextlib.contextmanager
def map_files(function, items, status, *, processes=False):
    """At most N tasks/results in flight; consume results inside the context."""
    runtime = _runtime or Runtime(1)
    with runtime.map(function, items, status, processes=processes) as results:
        yield results


def for_each(function, items, status):
    with map_files(function, items, status) as results:
        for _ in results:
            pass
