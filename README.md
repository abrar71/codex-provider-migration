# Codex model-provider migration

This standalone, standard-library-only Python utility relabels Codex CLI
sessions from a custom provider ID (for example, `proxy`) to the built-in
`openai` provider. It preserves existing thread UUIDs and SQLite-only metadata
such as names and pins.

The command does not change Codex records by default. Applying a migration
requires an explicit backup directory and `--confirm-codex-stopped`, and
automatically performs an exhaustive backup-versus-result verification before
reporting success. If any write or final verification fails, it restores and
verifies the original state from the backup before returning the error.

## Version 1.5: concurrent file processing

The commands in this README use `--workers 0` to run with **all available
logical CPUs**. CPU affinity and Linux cgroup CPU quotas restrict the available
count. This applies to dry runs, apply, verification, and restoration, including
the Docker commands.

If you omit `--workers`, the CLI uses `auto`: **90% of available logical CPUs,
rounded down, with a minimum of one worker**. For example, 4 available CPU
threads select 3 workers; 16 select 14. `--workers 1` runs sequentially; other
positive values select that many workers. These options set the worker count;
they do not throttle CPU usage to a fixed percentage.

Scan workers run in separate processes so JSON parsing can use multiple cores.
Each worker streams one file at a time. Paginated boundary checks also run in
processes; backup copies, hash checks, and byte comparisons use threads. The
coordinator combines file results in a stable order and resolves cross-file
history references after validation. SQLite operations, live file replacements,
and restoration writes stay sequential. A failed or interrupted phase stops
and drains its workers before recovery begins.

```bash
# Use all available CPU threads, with progress enabled.
python3 migrate.py --from-provider proxy --to-provider openai --workers 0

# Other options: automatic sizing, a fixed count, or sequential processing.
python3 migrate.py --from-provider proxy --to-provider openai --workers auto
python3 migrate.py --from-provider proxy --to-provider openai --workers 4
python3 migrate.py --from-provider proxy --to-provider openai --workers 1
```

The scheduler keeps at most one task per worker in flight. More workers increase
memory use because each process parses its own records and maintains its Python
runtime. A busy or slow disk can also limit the benefit. Use `--workers 1` for
tight memory limits; the memory regression test exercises that mode under a
256 MiB address-space limit. All three commands and Docker subcommands accept
`--workers auto|N`.

## Streaming and progress

Version 1.4 fixes the archive-wide memory loading that could exhaust RAM before
backup creation. It scans JSONL one record at a time and keeps a sparse plan of
the exact bytes to replace. Apply, verification, and restoration stream file
contents; SQLite comparisons stream sorted rows instead of loading whole tables.
Preflight reuses its validated plan, and the check after backup compares hashes
and metadata without parsing the archive again. Unchanged rollouts are not rewritten.

Memory depends on the worker count, largest JSONL record or SQLite row, changed
fields, referenced history boundaries, and SQLite caches. It no longer includes
complete original and transformed copies of the archive. The default maximum
record size is 64 MiB;
`--max-record-mib N` changes that limit for unusually large imported records.
Raising it also raises the possible memory use. SQLite may use temporary disk
space to sort tables during verification.

Every command now reports progress on **stderr**, including dry runs, apply,
verification, and restoration. Reports include the current phase, elapsed time,
file and byte counts where available, MiB/s, an estimate of time remaining for
that phase, active/configured workers, and estimated peak memory including scan
processes. A heartbeat continues during database checks and other operations
whose total work is not known. Nothing is sent to
a telemetry service, and progress does not include conversation text.

```bash
# Human-readable progress is enabled by default.
python3 migrate.py --from-provider proxy --to-provider openai --workers 0

# Final result on stdout; progress and operational errors as JSON Lines on stderr.
python3 migrate.py --from-provider proxy --to-provider openai \
  --workers 0 --json --progress json >result.json 2>progress.jsonl

# Less frequent updates, or no progress output.
python3 migrate.py --from-provider proxy --to-provider openai --workers 0 --progress-interval 5
python3 migrate.py --from-provider proxy --to-provider openai --workers 0 --progress off
```

`--workers auto|N`, `--progress {auto,text,json,off}`, `--progress-interval SECONDS`, and
`--max-record-mib N` also work with `verify.py`, `restore.py`, and the Docker
subcommands. `--progress auto` selects readable text for both terminals and logs.
The JSON progress fields are `event`, `phase`, `elapsed_seconds`,
`phase_elapsed_seconds`, `bytes_processed`, `bytes_total`, `files_processed`,
`files_total`, `bytes_per_second`, `eta_seconds`, `workers`, `active_workers`,
`worker_peak_memory_bytes`, and `peak_memory_bytes`.
Unknown totals, estimates, or unsupported memory measurements are `null`.
Warnings have `event: "warning"` and a `message`. Operational errors have
`event: "error"` and a `message`; invalid CLI arguments
are handled by argparse before progress starts. Rates and totals describe each
phase, so rescanning or verifying a file counts as separate work.

Memory reporting sums the coordinator's peak RSS and the highest reported RSS
of each scan process. It is a conservative estimate: processes can peak at
different times, and shared pages can be counted more than once. It is not a
hard memory limit. Worker memory is sampled as chunks and tasks complete.
`active_workers` counts tasks in flight; database and sequential write phases
report zero. Very short phases may finish before a heartbeat update.

If your configuration already uses `openai`, you can omit `--migrate-config`.
With that flag, an explicit `model_provider = "openai"` or an omitted selector
produces a warning and skips config changes. Session migration continues, and
the config's bytes and modification time are preserved. Dry-run JSON reports
`config_migration: "skipped; already openai"`. The backup manifest records that
no config migration occurred, so verification and restore check the original
config exactly. Warnings remain visible with `--progress off`.

A selector for an unrelated provider, an invalid config, or incompatible settings
in a provider that needs conversion still produce an error. Config conversion
is checked before scanning the archive, so these errors fail early. Stop Codex
before a consistent dry run and always before apply. A file changing during
either scan is reported as an error.

> [!CAUTION]
> Codex state can contain prompts, responses, repository paths, and account
> metadata. Treat both the live state and migration backup as private data. Do
> not commit, upload, or attach either one to a public issue.

Measured results for a 3.15 GiB archive are in [BENCHMARK.md](BENCHMARK.md).

## Compatibility and supported scope

The utility was developed against the legacy rollout format in Codex CLI
0.146.0 and the paginated format in Codex CLI 0.149.0. Version 1.3.0 also checks
the SQLite schemas from 0.157.1 and upstream commit `18344a972d` (2026-09-27).
It supports:

- plain JSONL rollouts in `sessions` and `archived_sessions`;
- `state_5.sqlite` as the metadata database;
- mixed legacy and paginated rollouts;
- the `thread_history_1.sqlite` projection schemas used by Codex CLI 0.149.0
  and 0.157.1, including their rollout byte offsets;
- paginated `history_base` references, including chained source histories,
  repeated reverts, forks of reverted threads, and archived ancestors;
- explicit rollout ordinals, including forward gaps and recoverable skipped
  records, while preserving the exact byte boundary of each stored offset;
- provider metadata at these two rollout paths only:
  - `session_meta.payload.model_provider`;
  - `event_msg.payload.thread_settings.model_provider_id` when the event type is
    `thread_settings_applied`.

It deliberately refuses to write if it finds:

- compressed rollouts;
- unknown history modes or ambiguous duplicate rollout IDs;
- paginated history without its thread-history database;
- an unknown `history_base` schema, missing source thread, reference cycle, or
  cutoff that is not on the recorded source JSONL boundary;
- an unknown thread-history schema, an unrecognized byte-offset column, or an
  offset/ordinal pair that is not on the recorded JSONL boundary;
- provider values at an unknown JSON path;
- a provider-looking match inside malformed JSON;
- a custom provider configuration that cannot be represented safely by
  `openai_base_url`;
- a profile that would retain a removed provider or inherit a changed endpoint
  during config conversion;
- ambiguous automatic SQLite discovery.

For a supported paginated history, the utility validates every stored byte
offset against the original JSONL line boundary and shifts it by the exact
cumulative replacement delta before that boundary. For `history_base`, it
resolves the source rollout dependency first, preserves the referenced ordinal,
and rewrites the embedded source byte offset. It then verifies every rollout
and the entire thread-history database against the backup, allowing changes
only to those calculated fields. A refusal means the detected format has not
been proven safe; do not work around it by manually replacing text.

## What changes

For the requested source and target providers, `migrate.py` changes only:

1. The two validated provider fields in active and archived rollout JSONL.
2. `threads.model_provider` in `state_5.sqlite`.
3. For paginated child histories, `history_base.end_byte_offset` when its source
   rollout changes length.
4. For paginated threads, the three recognized rollout byte-offset columns in
   `thread_history_1.sqlite` when their values need to shift.
5. With `--migrate-config`, the active `config.toml`:
   - removes the root custom-provider selector;
   - removes that provider's table;
   - copies its exact `base_url` value to root `openai_base_url`.

The config conversion is allowed only when the custom provider uses the
Responses API, requires OpenAI authentication, and has no custom authentication,
headers, query parameters, or other settings that the built-in provider would
lose.

Before config conversion, the tool inspects every `<name>.config.toml` profile
in the Codex home. It refuses if a profile selects or overrides the provider
being removed, or would inherit a different OpenAI endpoint. Resolve the named
profile conflict first, or omit `--migrate-config` and manage the configuration
separately. Profiles are checked again before writes and before reporting
success; the tool never edits them. Invalid or symlinked profiles also prevent
automatic config conversion.

Preserving the endpoint and auth settings does not establish runtime proxy
compatibility: built-in OpenAI can enable different transports and capabilities.

Removing the provider table also removes comments and blank lines inside that
table. Comments and values outside the removed selector and table are retained;
the semantic verifier rejects changes to unrelated TOML values.

The utility does not alter `history.jsonl`, `auth.json`, model caches, logs,
goals, memories, other SQLite databases, thread-history items or turn data,
message text, thread IDs, rollout ordinals, rollout timestamps, file modes, or
ownership. Rewriting `config.toml` normally updates that file's modification
time, as any intentional config edit would.

## Requirements

- Python 3.11 or newer (for `tomllib`).
- Enough free space for a complete copy of the session directories and
  consistent backups of both SQLite databases when paginated history exists.
- A writable SQLite state directory. Opening a database whose persistent
  journal mode is WAL can make SQLite create standard empty `-wal` and `-shm`
  coordination files even though the connection is read-only.
- All Codex CLI, desktop app, IDE extension, and app-server processes stopped
  before apply, including the background daemon.

The default Codex state directory is `CODEX_HOME` when that environment variable
is set, otherwise `~/.codex`. SQLite discovery uses this order:

1. Explicit `--sqlite-home`.
2. `sqlite_home` in a selected `--profile <name>` file.
3. `sqlite_home` in the user `config.toml`.
4. `CODEX_SQLITE_HOME`.
5. The Codex state directory.

Relative TOML paths resolve against the directory containing that config file.
If unselected profiles specify different SQLite directories, discovery stops
and asks for `--profile` or `--sqlite-home`. The dry-run and apply reports include
`sqlite_home_source`, so the selected location is visible before use. `--profile`
selects configuration for database discovery; it does not enable profile edits.

Use `--sqlite-home` for locations supplied by project config, Codex `--config`
overrides, cloud policy, or MDM. This offline tool does not reproduce Codex's full
configuration stack. Locally detected system/managed `sqlite_home` settings
require an explicit path. Inside Docker, use the mounted container path as
shown below.

### Stop Codex before apply or restore

Stop every process that can write to the state being migrated. Exit Codex CLI
sessions, quit the desktop app, and disable the IDE extension or close its host
editor. Close these clients first because they can restart app-server processes.

With the current CLI, stop the managed background app-server from a separate
terminal, using the same `CODEX_HOME` as the state being migrated:

```bash
codex app-server daemon stop
```

Stop separately launched `codex app-server` processes too. On Linux, inspect
remaining processes with:

```bash
pgrep -af codex
```

Exit identified processes normally or send them SIGTERM. `codex app-server`
normally uses the same executable as the CLI, so `pkill -x codex` generally
targets both; it also affects other matching Codex sessions. Recheck that the
processes have exited and have not restarted before applying the migration.

Keep Codex stopped throughout migration and verification. If restoring later,
repeat these shutdown steps before running `restore.py`.

On Linux, apply and restore also check `/proc` for Codex processes and open
state files. These checks do not stop processes for you. On systems without
`/proc`, `--confirm-codex-stopped` remains an explicit promise from the operator
rather than a process-level guarantee. Run the migrator outside Codex after
closing the session that helped prepare it.

## Docker

### Build Image

Build the local image from the repository checkout:

```bash
docker build -t codex-provider-migration:local .
# Equivalent: make docker-build
```

The image has three subcommands: `migrate`, `verify`, and `restore`. The
container makes no network requests at runtime, and the commands below disable
container networking explicitly. Its build context is restricted to the seven
Python modules and `Dockerfile`, so local Codex state and backups cannot be
copied into the image accidentally. Run `make docker-check` to build the image
and smoke-test all three subcommands without network access.

### Dry Run

Set the host state path, then perform a dry run with the state mounted at
`/codex`:

```bash
image="codex-provider-migration:local"
codex_state_dir="${CODEX_HOME:-${HOME}/.codex}"

docker run --rm \
  --network none \
  --user "$(id -u):$(id -g)" \
  --mount "type=bind,src=${codex_state_dir},dst=/codex" \
  "$image" migrate \
  --workers 0 \
  --codex-home /codex \
  --sqlite-home /codex \
  --from-provider proxy \
  --to-provider openai \
  --migrate-config
```

The state mount is intentionally writable because SQLite may create WAL
coordination sidecars during otherwise read-only validation. On systems where
Docker does not support host numeric user IDs, omit `--user`; be aware that a
root container can leave the new backup owned by root.

### Run Migration

To apply, mount a backup *parent* directory and pass a child path that does not
exist yet. Stop every Codex process first:

```bash
backup_parent="${HOME}/codex-provider-backups"
backup_name="codex-provider-backup-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$backup_parent"

docker run --rm \
  --network none \
  --pid=host \
  --user "$(id -u):$(id -g)" \
  --mount "type=bind,src=${codex_state_dir},dst=/codex" \
  --mount "type=bind,src=${backup_parent},dst=/backups" \
  "$image" migrate \
  --workers 0 \
  --codex-home /codex \
  --sqlite-home /codex \
  --from-provider proxy \
  --to-provider openai \
  --migrate-config \
  --apply \
  --confirm-codex-stopped \
  --backup-dir "/backups/${backup_name}"
```

`--pid=host` improves host-process detection on Linux; it is not available with
every Docker runtime and does not replace the requirement to stop Codex. If the
SQLite state is separate, add another bind mount at `/sqlite` and use
`--sqlite-home /sqlite`.

### Verify Migration

Re-run verification with the same state and backup mounts:

```bash
docker run --rm \
  --network none \
  --user "$(id -u):$(id -g)" \
  --mount "type=bind,src=${codex_state_dir},dst=/codex" \
  --mount "type=bind,src=${backup_parent},dst=/backups,readonly" \
  "$image" verify \
  --workers 0 \
  --backup-dir "/backups/${backup_name}" \
  --codex-home /codex \
  --sqlite-home /codex
```

### Restore Backup

To undo a completed migration, stop Codex and use a writable backup mount so
the manifest can record recovery progress:

```bash
docker run --rm \
  --network none \
  --pid=host \
  --user "$(id -u):$(id -g)" \
  --mount "type=bind,src=${codex_state_dir},dst=/codex" \
  --mount "type=bind,src=${backup_parent},dst=/backups" \
  "$image" restore \
  --workers 0 \
  --backup-dir "/backups/${backup_name}" \
  --codex-home /codex \
  --sqlite-home /codex \
  --confirm-codex-stopped
```

## 1. Dry run

Clone the public repository, then run the dry-run validation:

```bash
git clone https://github.com/abrar71/codex-provider-migration.git
cd codex-provider-migration

python3 migrate.py \
  --workers 0 \
  --from-provider proxy \
  --to-provider openai \
  --migrate-config
```

This reads and validates every rollout, validates SQLite, and checks whether the
config conversion preserves all supported values. It does not change Codex
records. As noted above, SQLite itself may create WAL coordination sidecars
during the read. Add `--json` for machine-readable output.

If the state is not in the default location, add, for example,
`--codex-home /path/to/codex-state`. If SQLite is stored separately, also add
`--sqlite-home /path/to/sqlite-state`.

Omit `--migrate-config` if you only want to relabel saved sessions and the
SQLite index. In that case, update or remove the custom provider configuration
yourself after the migration.

## 2. Apply

After the dry run succeeds, stop every Codex process. Choose a private backup
location outside the Codex state directory and outside the repository checkout:

```bash
migration_backup_dir="${HOME}/codex-provider-backup-$(date -u +%Y%m%dT%H%M%SZ)"

python3 migrate.py \
  --workers 0 \
  --from-provider proxy \
  --to-provider openai \
  --migrate-config \
  --apply \
  --confirm-codex-stopped \
  --backup-dir "$migration_backup_dir"
```

Do not restart Codex until the command reports `migration and verification
passed`.

The backup contains:

- the original active and archived rollout trees;
- the original `config.toml`, when present;
- consistent backups of `state_5.sqlite` and, when present,
  `thread_history_1.sqlite`, created through SQLite's backup API;
- `migration-manifest.json`, including artifact digests and metadata,
  preflight counts, recovery status, and the final verification report.

Keep this backup until the migrated chats have been inspected successfully.

## 3. Verify again

Before restarting Codex, verification can be repeated independently:

```bash
python3 verify.py --workers 0 --backup-dir "$migration_backup_dir"
```

The verifier compares every rollout byte-for-byte against the exact expected
replacement, checks every JSONL record, preserves malformed lines verbatim,
compares every table and row in both SQLite databases, verifies their schemas
and integrity, validates the exact paginated-offset shifts, and checks the
expected config transformation.

By default, the verifier reads the original state paths recorded in the private
backup manifest. Use `--codex-home` or `--sqlite-home` to verify state restored
or moved to another location.

Verification expects Codex to remain stopped. New sessions created after the
migration correctly cause the original file-set comparison to fail.

## 4. Open the picker

Once verification passes:

```bash
codex resume --all
```

`--all` removes the working-directory filter. It does not remove provider
filtering; the migration works because all migrated sessions now carry the
effective default provider ID, `openai`.

## Failure and recovery

If apply fails after writes begin, it automatically restores every rollout,
`config.toml`, and both SQLite databases from the backup and verifies the
restored state. SQLite restoration uses its transactional backup API so
committed WAL state is replaced consistently. The manifest status becomes
`rolled_back` when that succeeds.

If you want to undo a completed migration, keep Codex stopped and run:

```bash
python3 restore.py \
  --workers 0 \
  --backup-dir "$migration_backup_dir" \
  --confirm-codex-stopped
```

The restore command defaults to the state paths in the private manifest. It
accepts `--codex-home` and `--sqlite-home` overrides when the state has moved.
Before its first write, it requires the live rollouts, config, and SQLite data
to match the exact completed migration. It refuses to overwrite new sessions,
edits, or database rows created after migration.

The command marks the backup `restoring` before it changes live state. If the
process is interrupted, run the same command again while Codex remains stopped.
Recovery from a `prepared` or `restoring` backup proceeds only when every live
artifact exactly matches either its original form or its expected migrated
form. This permits a provably partial operation to resume without providing a
force option that could erase later activity.

If the migration reports that automatic rollback also failed, do not restart
Codex or rerun `migrate.py`. Keep the backup intact and inspect the two reported
errors. `restore.py` can resume a `prepared` backup, but only when its strict
original-or-migrated checks pass; otherwise it exits before recovery writes.

## Security properties

- The utility makes no network requests.
- It rejects symlinks in state that it would migrate or back up.
- It uses SQLite's backup API instead of copying a potentially live database.
- It records and revalidates backup file digests and metadata before recovery
  writes. These checks detect accidental backup changes; the manifest is not a
  cryptographic signature against an attacker who can alter the whole backup.
- Automatic rollback and explicit restoration use SQLite's transactional
  backup API to replace the database without a main-file/sidecar swap window.
- It performs bounded, field-specific transformations rather than general text
  replacement.
- It preserves rollout permissions, ownership, and modification times and
  verifies them against the backup.
- Unknown or ambiguous state fails closed.

The migration manifest intentionally records the local state paths required for
independent verification. It belongs in the private backup and must not be
published.

## Tests

```bash
make check
```

Or, without Make:

```bash
python3 -B -m unittest discover -s tests -v
```

The tests cover dry-run record immutability, exact byte changes, malformed-line
preservation, special file modes and timestamps, SQLite-only change
enforcement, config conversion, paginated offset migration and restoration,
`history_base` chains and cycles, repeated reverts, physical rollout IDs,
explicit ordinals and skipped records, profile dependencies, configured SQLite
locations, independent re-verification, and refusal of unknown history schemas
or misaligned offsets. They also cover backup
tampering, newer live activity, config presence changes, WAL sidecars,
automatic rollback, and resuming an interrupted restore. Test data uses
reserved example domains and temporary directories; it does not require real
Codex state.

Streaming regressions also check source changes during a rewrite, exact SQLite
row comparisons with duplicates and collations, record-size limits, and progress
output. On Linux, a subprocess exercises dry-run, apply, verify, and restore on
a generated archive larger than its 256 MiB address-space limit with
`--workers 1`. A second test runs those operations with three workers on an
archive larger than 512 MiB, with a 512 MiB limit per process and a check that
the reported combined peak stays below 512 MiB. That test sets
`MALLOC_ARENA_MAX=2` to bound glibc's virtual memory reservations for threads.
Both tests remove their data.

Concurrency tests check identical plans and restored bytes, chained history
references, duplicate IDs across workers, inherited record-size limits, bounded
task submission, worker failures, Ctrl+C, rollback, and aggregated progress.

## Project status

This is an independent migration utility, not an official OpenAI product. Codex
state formats can change. Review the dry-run report and keep the verified backup
even when using a Codex version listed above.
