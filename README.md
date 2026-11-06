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

## Requirements

- A role with the `REPLICATION` attribute, and a `pg_hba.conf` line for the
  `replication` database.
- Postgres 16, 17 or 18.

## Tests

Some tests start Postgres servers. Set `PGWAL_PG_BIN` to the directory of the
Postgres binaries. Without them, these tests skip.

## License

MIT.
