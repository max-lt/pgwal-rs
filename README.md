# pgwal-rs

Continuous archiving of the Postgres WAL to object storage.

The receiver connects to Postgres over physical replication. It writes the
WAL to local disk and uploads it to a sink every few seconds. The recovery
point is the upload interval, not one 16 MiB segment.

## Crates

- `pgwal`: the WAL archive format, `pg_control`, the replay plan and the
  segment writer. It uses `std` only.
- `pgwal-receiver`: the replication stream, the local store, the uploader,
  salvage, compaction and base backups. A sink implements the `WalSink`
  trait. Without the default `runtime` feature, only the protocol module
  `wire` builds, for hosts without tokio such as wasm32.
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

### S3 archives

`--archive` takes a directory or `s3://bucket/prefix/`, in `receive`,
`restore` and `rebase`.

```sh
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...
pgwal receive --host db.example --user replicator --dir /var/lib/pgwal/store \
  --archive s3://wal/db1/ --s3-endpoint https://<account>.r2.cloudflarestorage.com --s3-region auto
```

- The keys come from `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` only.
- `--s3-endpoint` (or `AWS_ENDPOINT_URL`) is the store. Without it, the
  command uses AWS in `--s3-region` (or `AWS_REGION`, default `us-east-1`).
- `--s3-ca-file` (or `AWS_CA_BUNDLE`) adds a PEM file of CAs to trust, for a
  store with a private CA.
- Paths are in the path style: `/bucket/key`.
- Every request signs the SHA-256 of its body, so the store refuses a body
  that changed on the way.
- Every upload carries `If-None-Match: *`, so no object is overwritten. A key
  names one LSN range of one timeline of one system. When the key exists, the
  command reads the object back: the same bytes count as uploaded, other
  bytes are an error.
- A restore and a rebase fetch only the objects that the copy needs, 64 at a
  time at most.
- The S3 client is the `s3` feature of `pgwal-receiver`, on in the `pgwal`
  command.

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
  promote. A copy of the data directory after a clean stop is a new base backup,
  on the timeline of the archive. Its replay starts at the last checkpoint
  of the primary in the archive.

### Base backups in the archive

`pgwal_receiver::base_backup` takes a base backup over the replication
protocol, without its WAL, and writes it to a `WalSink`. The `pgwal` command
has no subcommand for it.

- The backup starts with a fast checkpoint on the server.
- The objects go under `{system_id}/base/{start}/`, with the start LSN in 16
  hex digits.
- Each file goes in parts of at most 16 MiB: `base.tar.000000`,
  `base.tar.000001`, and so on. A tablespace is `<oid>.tar`. The manifest is
  `backup_manifest`.
- The `done` object goes last. It gives the start and end LSN, the timeline,
  the part count of each file, and the tablespaces. A backup without `done`
  is incomplete.
- A restore needs the archived WAL from the start to the end. Take the backup
  while `receive` streams the slot: the slot keeps that WAL on the server
  until the archive has it.
- To make a data directory, join the parts of each file in order, extract
  `base.tar` into an empty directory with mode 700, and put `backup_manifest`
  in it. `pg_verifybackup -n` checks the files. Then `pgwal restore` writes
  the WAL.

### Rebase

`pgwal rebase` moves a base backup forward to the end of the archive,
without a new `pg_basebackup` of the primary.

```sh
pgwal rebase --archive /var/lib/pgwal/archive --pgdata /backups/2026-11-06 \
  --pg-bin /usr/lib/postgresql/18/bin
```

- The command writes the WAL into the copy, starts a Postgres on it as a
  standby on a private socket, waits for the replay to reach the end of the
  archive, and stops it. The copy is then a base backup at the last
  checkpoint of the primary in the archive. The command prints its redo
  point.
- The binaries must be of the major version of the copy, on the same
  platform as the primary. `--pg-bin` (or `PGWAL_PG_BIN`) gives their
  directory. Without it, they come from `PATH`.
- `--user` is a superuser of the copy, for the local connection that
  watches the replay. Its default is `postgres`.
- Then the command runs `pgwal verify` on the copy, and fails if the check
  fails.
- The command runs as the owner of the data directory, not as root.

`pgwal verify` checks a stopped copy:

- `pg_checksums --check`, when the copy has page checksums.
- `pg_amcheck --all --heapallindexed` on the heap and the B-tree indexes of
  every database. It runs on a scratch copy next to the copy, because a
  server on the copy itself would end its recovery and write WAL of its own.
  The scratch copy uses file clones where the file system has them, and goes
  away at the end.

`pgwal plan` reads archive keys on stdin and prints the ones that a restore
or a rebase of a copy reads. Fetch only these from object storage:

```sh
aws s3 ls --recursive s3://bucket/wal/ | awk '{print $4}' \
  | pgwal plan --pgdata /backups/2026-11-06 --prefix wal/
```

## Requirements

- A role with the `REPLICATION` attribute, and a `pg_hba.conf` line for the
  `replication` database.
- Postgres 16, 17 or 18.

## Tests

Some tests start Postgres servers. Set `PGWAL_PG_BIN` to the directory of the
Postgres binaries. Without them, these tests skip.

The S3 tests run against a real store, under a prefix of their own that they
delete:

```sh
PGWAL_TEST_S3_ENDPOINT=https://s3.example PGWAL_TEST_S3_BUCKET=pgwal-test \
AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
  cargo test -p pgwal-receiver --features s3 --test s3
```

## License

MIT.
