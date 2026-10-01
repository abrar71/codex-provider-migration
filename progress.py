"""Local progress on stderr; final command results stay on stdout."""

from __future__ import annotations

import contextlib
import dataclasses
import json
import math
import sys
import threading
import time

import migration_workers


def peak_memory_bytes() -> int | None:
    try:
        import resource

        value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return int(value if sys.platform == "darwin" else value * 1024)
    except (ImportError, OSError):
        return None


@dataclasses.dataclass
class Stage:
    name: str
    total_bytes: int | None = None
    total_files: int | None = None
    completed_bytes: int = 0
    completed_files: int = 0
    outcome: str = "completed"
    started: float = dataclasses.field(default_factory=time.monotonic)
    active_workers: int = 0

    def advance(self, size: int = 0, files: int = 0) -> None:
        self.completed_bytes += size
        self.completed_files += files


class Reporter:
    def __init__(self, mode: str, interval: float, workers: int = 1) -> None:
        self.mode = "text" if mode == "auto" else mode
        self.interval = interval
        self.started = time.monotonic()
        self.stage: Stage | None = None
        self.workers = workers
        self.worker_peaks: dict[int, int] = {}
        self.lock = threading.RLock()
        self.stop = threading.Event()
        self.worker = threading.Thread(target=self._heartbeat, daemon=True)
        self.worker.start()

    def _heartbeat(self) -> None:
        while not self.stop.wait(self.interval):
            self.emit("progress")

    def emit(self, event: str, stage: Stage | None = None) -> None:
        if self.mode == "off":
            return
        with self.lock:
            stage = stage or self.stage
            if stage is None:
                return
            elapsed = max(0.000001, time.monotonic() - stage.started)
            rate = stage.completed_bytes / elapsed
            eta = (
                max(0, stage.total_bytes - stage.completed_bytes) / rate
                if stage.total_bytes is not None and rate > 0
                else None
            )
            parent_peak = peak_memory_bytes()
            worker_peak = sum(self.worker_peaks.values())
            values = {
                "event": event,
                "phase": stage.name,
                "elapsed_seconds": round(time.monotonic() - self.started, 3),
                "phase_elapsed_seconds": round(elapsed, 3),
                "bytes_processed": stage.completed_bytes,
                "bytes_total": stage.total_bytes,
                "files_processed": stage.completed_files,
                "files_total": stage.total_files,
                "bytes_per_second": round(rate),
                "eta_seconds": round(eta, 1) if eta is not None else None,
                "workers": self.workers,
                "active_workers": stage.active_workers,
                "worker_peak_memory_bytes": worker_peak
                if parent_peak is not None
                else None,
                "peak_memory_bytes": (
                    parent_peak + worker_peak if parent_peak is not None else None
                ),
            }
            if self.mode == "json":
                line = json.dumps(values, separators=(",", ":"))
            else:
                parts = [f"[{values['elapsed_seconds']:.1f}s] {stage.name}: {event}"]
                parts.append(f"workers {stage.active_workers}/{self.workers}")
                if stage.total_files is not None:
                    parts.append(f"{stage.completed_files}/{stage.total_files} files")
                if stage.total_bytes is not None or stage.completed_bytes:
                    total = (
                        f"/{stage.total_bytes / 1048576:.1f}"
                        if stage.total_bytes is not None
                        else ""
                    )
                    parts.append(f"{stage.completed_bytes / 1048576:.1f}{total} MiB")
                    parts.append(f"{rate / 1048576:.1f} MiB/s")
                if eta is not None:
                    parts.append(f"ETA {eta:.0f}s")
                if values["peak_memory_bytes"] is not None:
                    parts.append(
                        f"peak RAM {values['peak_memory_bytes'] / 1048576:.1f} MiB"
                    )
                line = " | ".join(parts)
            try:
                print(line, file=sys.stderr, flush=True)
            except (OSError, ValueError):
                # Closed Python streams raise ValueError.
                # Telemetry must never interrupt a migration or its rollback.
                self.mode = "off"

    def close(self) -> None:
        self.stop.set()
        self.worker.join()


_reporter: Reporter | None = None


def record_worker_memory(pid: int, peak: int) -> None:
    if _reporter is not None:
        with _reporter.lock:
            _reporter.worker_peaks[pid] = max(_reporter.worker_peaks.get(pid, 0), peak)


def exception_message(error: BaseException) -> str:
    if isinstance(error, MemoryError):
        return "out of memory; retry with fewer --workers (use --workers 1 for minimum memory)"
    return str(error) or type(error).__name__


def error(message: str) -> None:
    if _reporter is not None and _reporter.mode == "json":
        with _reporter.lock:
            print(
                json.dumps({"event": "error", "message": message}),
                file=sys.stderr,
                flush=True,
            )
    else:
        print(f"error: {message}", file=sys.stderr)


def warning(message: str) -> None:
    if _reporter is not None and _reporter.mode == "json":
        with _reporter.lock:
            print(
                json.dumps({"event": "warning", "message": message}),
                file=sys.stderr,
                flush=True,
            )
    else:
        print(f"warning: {message}", file=sys.stderr)


def positive_interval(value: str) -> float:
    import argparse

    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError(
            "progress interval must be finite and greater than zero"
        )
    return number


def add_arguments(parser) -> None:
    parser.add_argument(
        "--workers",
        type=migration_workers.parse_workers,
        default="auto",
        metavar="auto|N",
        help="file workers (0: all available logical CPUs; default auto: 90%%, rounded down; minimum 1)",
    )
    parser.add_argument(
        "--progress",
        choices=("auto", "text", "json", "off"),
        default="auto",
        help="progress on stderr (default: readable text; json emits JSON Lines)",
    )
    parser.add_argument(
        "--progress-interval",
        type=positive_interval,
        default=1.0,
        help="seconds between progress updates (default: 1)",
    )
    parser.add_argument(
        "--max-record-mib",
        type=int,
        default=64,
        help="maximum JSONL record size in MiB (default: 64)",
    )


@contextlib.contextmanager
def reporting(args):
    import migration_io

    global _reporter
    if args.max_record_mib <= 0:
        raise migration_io.MigrationError("--max-record-mib must be greater than zero")
    previous = _reporter
    old_limit = migration_io.MAX_RECORD_BYTES
    migration_io.MAX_RECORD_BYTES = args.max_record_mib * 1048576
    with migration_workers.configuration(getattr(args, "workers", "auto")) as runtime:
        reporter = Reporter(args.progress, args.progress_interval, runtime.count)
        _reporter = reporter
        try:
            yield reporter
        finally:
            reporter.close()
            _reporter = previous
            migration_io.MAX_RECORD_BYTES = old_limit


@contextlib.contextmanager
def phase(name: str, *, total_bytes: int | None = None, total_files: int | None = None):
    stage = Stage(name, total_bytes, total_files)
    reporter = _reporter
    previous = reporter.stage if reporter else None
    if reporter:
        reporter.stage = stage
        reporter.emit("started")
    try:
        yield stage
    except BaseException:
        if reporter:
            reporter.emit("failed", stage)
        raise
    else:
        if reporter:
            reporter.emit(stage.outcome, stage)
    finally:
        if reporter:
            reporter.stage = previous
