# pgwal-rs

Continuous archiving of the Postgres WAL to object storage.

The receiver connects to Postgres over physical replication. It writes the
WAL to local disk and uploads it to a sink every few seconds. The recovery
point is the upload interval, not one 16 MiB segment.

## Crates

- `pgwal`: the WAL archive format, `pg_control`, the replay plan and the
  segment writer. It uses `std` only.
- `pgwal-receiver`: the replication stream, the local store, the uploader,
  salvage and compaction. A sink implements the `WalSink` trait. Without
  the default `runtime` feature, only the protocol module `wire` builds,
  for hosts without tokio such as wasm32.
- `pgwal-cli`: the `pgwal` command.
- `pgwal-worker`: a receiver in a Cloudflare Durable Object, for one
  database. It is not published on crates.io. See its
  [README](crates/pgwal-worker/README.md).

## Usage

```sh
PGPASSWORD=... pgwal receive --host db.example --user replicator \
  --dir /var/lib/pgwal/store --archive /var/lib/pgwal/archive
```

- `--host`, `--port` and `--user` also read `PGHOST`, `PGPORT` and `PGUSER`.
  A host that starts with `/` is the directory of a Unix socket.
- The password comes from `PGPASSWORD` only. The server can use trust, peer
  or SCRAM-SHA-256.
- `--sslmode` (or `PGSSLMODE`) sets TLS on TCP:
  - `disable` (default): no TLS. Use it on a Unix socket, `localhost` or an
    encrypted private network only. Without TLS, the WAL, and so every row
    the server writes, crosses the network in clear text.
  - `require`: TLS, with no check of the server certificate.
  - `verify-full`: TLS, with a check of the chain and of the host name.
    `--sslrootcert` (or `PGSSLROOTCERT`) gives a PEM file of the CAs to trust.
    Without it, the command trusts the Mozilla roots.
- With `require` or `verify-full`, the command never falls back to clear
  text. A server that refuses TLS stops it.
- The slot is `pgwal`. The command creates it on first use.
- The command reconnects after an error. It stops on Ctrl-C, after it uploads
  the WAL it received.

### Restore

A restore needs a copy of the data directory, taken while `pgwal receive`
archives the WAL: a base backup or a snapshot.

```sh
# While the server runs, once or on a schedule:
pg_basebackup -h db.example -U replicator -D /backups/2026-11-06 -X none

# At restore time:
cp -a /backups/2026-11-06 /restore/data
pgwal restore --archive /var/lib/pgwal/archive --pgdata /restore/data
pg_ctl -D /restore/data start
```

- `-X none` leaves the WAL of the copy out of the backup: the archive holds
  it.
- `pgwal restore` writes the archived WAL that follows the copy into its
  `pg_wal`. Postgres replays it at its next start.
- The replay starts at `START WAL LOCATION` of `backup_label`, or at the
  checkpoint of `pg_control` without a label.
- A base backup replays as an archive recovery, then promotes to a new
  timeline. The command follows the timelines of the archive.
- The command refuses an archive with a gap after the start. With
  `--accept-gap`, it restores up to the gap.
- The command refuses a data directory with a `postmaster.pid`.
- Postgres accepts connections once the replay ends, not when `pg_ctl start`
  returns.
- The replay goes to the end of the archive. There is no target time.
- The command prints the LSN where the replay ends.
- With `--standby`, Postgres replays the archive as a standby and does not
  promote. A copy of the data directory after a clean stop is a new image,
  on the timeline of the archive. Its replay starts at the last checkpoint
  of the primary in the archive.

## Requirements

- A role with the `REPLICATION` attribute, and a `pg_hba.conf` line for the
  `replication` database.
- Postgres 16, 17 or 18.

## Tests

Some tests start Postgres servers. Set `PGWAL_PG_BIN` to the directory of the
Postgres binaries. Without them, these tests skip.

## License

MIT.
