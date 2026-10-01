# Codex CLI 0.159.3 schema fixtures

These schemas are derived from OpenAI Codex release
[`rust-v0.159.3`](https://github.com/openai/codex/tree/rust-v0.159.3), commit
`01fc69f4026735edfdf6789820549727a4867b11`.
Upstream SQL is covered by the accompanying `LICENSE` and `NOTICE`.

- `state.sql`: the result of applying all 58 files in
  [`codex-rs/state/migrations`](https://github.com/openai/codex/tree/01fc69f4026735edfdf6789820549727a4867b11/codex-rs/state/migrations).
  The last migration adds archive-listing indexes.
- `thread_history.sql`: the result of applying all seven files in
  [`codex-rs/state/thread_history_migrations`](https://github.com/openai/codex/tree/01fc69f4026735edfdf6789820549727a4867b11/codex-rs/state/thread_history_migrations).
  The last migration adds item lifecycle timestamps.

Tables, indexes, and triggers are retained. Bootstrap rows, SQLite internal
bookkeeping, and SQLx's migration ledger are excluded. Tests populate only
synthetic data; these files contain no user state.

## Compatibility review

Compared with the previous audited source, `18344a972d`, this release keeps
the relevant database schemas, provider field locations, history reference
format, and rollout byte-offset semantics unchanged. The additional state
migration since 0.157.1 only creates indexes.

The review also checked:

- `protocol/src/protocol.rs`: `SessionMeta`, `HistoryPosition`,
  `ThreadSettingsSnapshot`, `EventMsg`, and `TurnAbortedEvent`.
- `history/src/rollout_payload.rs` and `history/src/guardian_history.rs`:
  rollout envelopes and preserved Guardian metadata.
- `rollout/src/ordinal.rs`, `rollout/src/rollout_file_name.rs`, and
  `state/src/sqlite.rs`: history positions, physical rollout IDs, and database names.
- `core/src/config/mod.rs`, `config/src/loader/mod.rs`, and
  `model-provider-info/src/lib.rs`: SQLite discovery, separate profile files,
  the default `openai` provider, and `openai_base_url`.

The release tests exercise the existing migration code against the complete
schemas with legacy and paginated sessions, inherited history, interrupted-turn
error details, Guardian evidence, names, pins, creator fields, and item timestamps.
They run dry-run/apply/verify/restore with one and three workers, and inject a
history update failure to verify rollback. These are offline format tests; they
do not launch Codex, contact a provider, or establish proxy transport compatibility.

## Regenerating the schemas

Export the pinned release into a temporary directory. For each migration
directory above, run its `.sql` files in filename order against a fresh SQLite
database with Python's `sqlite3.Connection.executescript`. Export using
`Connection.iterdump()`, omitting statements beginning with `INSERT INTO ` or
`DELETE FROM ` to remove bootstrap rows and the internal `sqlite_sequence`
cleanup. Retain the provenance headers and validate each export by loading it
into another fresh database.
