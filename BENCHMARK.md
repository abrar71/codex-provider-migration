# Version 1.5 concurrency validation

Measured on September 29, 2026, with CPython 3.12 on a Linux host with four
available logical CPUs and approximately 8 GB RAM. The default selects three
workers: `max(1, floor(0.9 * available_cpus))`.

Each command ran in a fresh process against the same private snapshot:
**1,994 rollout files, 3,383,495,286 JSONL bytes
(3.15 GiB)**, and the state and thread-history databases.
No Codex authentication or configuration files were copied. The snapshot and
its migration backup were removed after verification and restoration passed.

| Operation | Wall time | Reported peak memory |
| --- | ---: | ---: |
| 1.4 dry-run (sequential) | 57.79 s | 60.33 MiB |
| 1.5 dry-run (`--workers 1`) | 57.87 s | 62.62 MiB |
| 1.5 dry-run (default, 3 workers) | 27.37 s | 192.39 MiB |
| 1.5 apply (default) | 137.38 s | 267.14 MiB |
| 1.5 independent verify (default) | 47.36 s | 245.48 MiB |
| 1.5 restore (default) | 207.15 s | 260.66 MiB |

The default dry run was **2.11× as fast** as version 1.4 in this
comparison. All three dry-run reports were identical. Apply and independent
verification also returned identical verification counts. Restore verified the
original file bytes, metadata, and database contents.

The 1.5 memory figure sums the coordinator's peak RSS and each scan process's
reported peak RSS. This is a conservative estimate: peaks need not occur at the
same time, and shared pages can be counted repeatedly. The 1.4 measurement covers
its single process. These runs used the normal allocator without an address-space
limit. The table records one run per mode on a shared host; cache, disk throughput,
and other activity affect timings. Apply includes backup and verification;
restore includes its existing recoverability checks and final verification.

## Checks

- 89 tests passed, including concurrent scans, cross-file history dependencies,
  duplicate IDs, record-size limits in workers, exact restored bytes and metadata,
  read-only backup directories, bounded task submission, worker crashes, Ctrl+C,
  rollback after an interrupted write, and combined progress counters.
- The sequential memory regression completed all four commands with an archive
  larger than its 256 MiB address-space limit. The concurrent regression used
  three workers and an archive larger than 512 MiB, with 512 MiB address-space
  limits per process and a combined reported peak below 512 MiB. That test sets
  `MALLOC_ARENA_MAX=2` to bound glibc's per-thread virtual reservations.
- Ruff's E4, E7, E9, F, I, and B checks passed for the implementation and new tests.
- Docker image `codex-provider-migration:1.5.0-workspace` passed dry-run, apply,
  independent verification, and restore with three workers and networking disabled.
  A separate container check confirmed a two-CPU quota selects one default worker.

---

# Version 1.4.0 validation

Measured on September 29, 2026, using CPython 3.12 on an approximately 8 GB
Linux host. Each operation ran in a fresh process with a 512 MiB address-space
limit. Peak memory is resident process memory reported by the OS.

The input was a private snapshot containing **1,994 rollout files and 3.15 GiB
of JSONL**, plus the state and thread-history SQLite databases. Live sessions
were not modified. The private snapshot and its backup were removed after the
checks completed.

| Operation | Wall time | Peak RAM |
| --- | ---: | ---: |
| dry-run | 51.98 s | 60.33 MiB |
| apply | 199.03 s | 105.02 MiB |
| verify | 83.40 s | 79.80 MiB |
| restore | 361.47 s | 87.45 MiB |

Apply updated 1,921 rollout files,
1,918 state rows, and
6,207 history byte-offset fields. Independent
verification returned exactly the same verification counts as apply. Restoration
verified the original rollout bytes, metadata, and database contents.

These are measurements from one run on a shared host; filesystem cache and load
influence timing. Apply includes backup creation and verification. Restore
includes validation of the migrated state, recoverability checks, and verification
of the restored state.

## Automated checks

- 75 tests passed, including the existing rollback and compatibility tests.
- A Linux regression test completed dry-run, apply, verify, and restore on a
  generated archive larger than its 256 MiB address-space limit.
- Additional coverage checks source changes during streaming writes, JSONL record
  limits, duplicate SQLite rows and collations, separate JSON progress output,
  failure events, and heartbeat updates during blocking work.
- Ruff's E4/E7/E9/F/I/B checks passed for the changed implementation modules and
  the new test module.
- Docker image `codex-provider-migration:1.4.0-workspace` built successfully.
  Its migrate, verify, and restore entrypoints all reported version 1.4.0.

The previous implementation was killed near 6.1 GiB of resident memory while
validating the live archive. Version 1.4.0 completed every operation under the
512 MiB address-space limit above.
