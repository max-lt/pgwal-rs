# pgwal-rs

Continuous archiving of the Postgres WAL to object storage.

The receiver connects to Postgres over physical replication. It writes the
WAL to local disk and uploads it to a sink every few seconds. The recovery
point is the upload interval, not one 16 MiB segment.

## Crates

- `pgwal`: the WAL archive format, `pg_control`, the replay plan and the
  segment writer. It uses `std` only.
- `pgwal-receiver`: the replication stream, the local store, the uploader,
  salvage and compaction. A sink implements the `WalSink` trait.

## Requirements

- A role with the `REPLICATION` attribute and a physical replication slot.
- Postgres 16, 17 or 18.

## Tests

The tests in `crates/pgwal-receiver/tests/restore.rs` start Postgres servers. Set
`PGWAL_PG_BIN` to the directory of the Postgres binaries. Without them, the
tests skip.

## License

MIT.
