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
  trait: `DirSink` keeps a directory, `S3Sink` an S3 bucket. Without the
  default `runtime` feature, only the protocol module `wire` builds, for
  hosts without tokio such as wasm32.
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
  - `require`: TLS, with no check of the server certificate. With
    `--sslrootcert`, the command refuses to start.
  - `verify-full`: TLS, with a check of the chain and of the host name.
    `--sslrootcert` (or `PGSSLROOTCERT`) gives a PEM file of the CAs to trust.
    Without it, or with `system` as in libpq, the command trusts the Mozilla
    roots. The server certificate needs the host name in its subjectAltName,
    and cannot be a CA certificate: a self-signed certificate made with
    `openssl req -x509` fails.
  - The other values of libpq, `allow`, `prefer` and `verify-ca`, are an
    error.
- With `require` or `verify-full`, the command never falls back to clear
  text. A server that refuses TLS stops it, and so does a certificate that
  fails the check for good, such as a missing host name or an unknown CA.
  An expired certificate, or one not valid yet, gives a retry: a renewal or
  a clock fixes it.
- `--slot` (default `pgwal`) names the slot. The command creates it on first
  use. `--application-name` (default `pgwal`) is the name that
  `synchronous_standby_names` matches.
- `--upload-every` (default 2 s) sets the upload interval. `--idle-timeout`
  (default 60 s) is the longest silence accepted from the server.
- The command reconnects after an error of the network or of the server.
  It stops on a wrong setting, on a store of another cluster, on WAL that
  the server no longer has, and on a server that refuses TLS.
- It stops on Ctrl-C or SIGTERM, after it uploads the WAL it received. When
  that upload fails, the exit status is not 0, and the WAL stays in the
  store.
- Every command runs with umask 077: the archive and the store hold every
  row of the server, and only the user of the command reads them. `fetch`
  keeps the modes that the backup gives its files.

### S3 archives

`--archive` takes a directory or `s3://bucket/prefix/`, in every command
that reads or writes an archive.

```sh
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...
pgwal receive --host db.example --user replicator --dir /var/lib/pgwal/store \
  --archive s3://wal/db1/ --s3-region auto \
  --s3-endpoint https://<account>.r2.cloudflarestorage.com
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
- The archive must hold the WAL from the start of the segment that holds
  `START WAL LOCATION` of `backup_label`, or the redo point of `pg_control`
  without a label. Postgres reads that segment from its first page. A copy
  whose `pg_wal` holds that segment needs the WAL from the LSN itself.
- A timeline needs its WAL up to its switch point only. The command reads the
  switch points in the newest history file of the archive.
- A base backup, a copy after a clean stop, or a copy of a standby replays
  as an archive recovery, then promotes to a new timeline. A snapshot of a
  running primary gets crash recovery and keeps its timeline, unless the
  archive goes on to a later timeline or has a gap. The command follows the
  timelines of the archive.
- The command refuses an archive with a gap after the start, on any
  timeline. With `--accept-gap`, it restores up to the gap.
- The command refuses an archive that holds no WAL of the copy, such as a
  wrong `--archive`, and a data directory with a `postmaster.pid`.
- An archive recovery accepts read-only connections once the copy is
  consistent, which is when `pg_ctl start -w` returns. Writes wait for the
  promotion at the end of the replay: wait for `pg_is_in_recovery()` to be
  false. A crash recovery accepts no connection until its end.
- The replay goes to the end of the archive. There is no target time.
- The command prints the LSN where the replay ends.
- With `--standby`, Postgres replays the archive as a standby and does not
  promote. It connects to no primary: the command clears `primary_conninfo`
  and `primary_slot_name` of the copy. A copy of the data directory after a
  clean stop is a new base backup, on the timeline of the archive. Its
  replay starts at the redo point of the last checkpoint of the primary in
  the archive. Its `postgresql.auto.conf` keeps `primary_conninfo = ''` and
  `primary_slot_name = ''`, which win over `postgresql.conf`: remove them to
  make a streaming standby of it.

### Base backups in the archive

`pgwal backup` takes a base backup over the replication protocol, without
its WAL, and writes it to the archive. `pgwal fetch` writes the latest one
into an empty data directory.

```sh
# While pgwal receive runs, once or on a schedule:
pgwal backup --host db.example --user replicator --archive s3://wal/db1/

# At restore time:
pgwal fetch --archive s3://wal/db1/ --pgdata /restore/data
pgwal restore --archive s3://wal/db1/ --pgdata /restore/data
pg_ctl -D /restore/data start
```

- The backup starts with a fast checkpoint on the server. `pgwal backup`
  prints the start and end LSN.
- A restore needs the archived WAL from the start to the end. Run
  `pgwal backup` while `pgwal receive` streams the slot: the slot keeps that
  WAL on the server until the archive has it. `--slot` names the slot
  (default `pgwal`). Without it, or with a slot that holds no WAL, the
  backup does not start.
- The objects go under `{system_id}/base/{start}/`, with the start LSN in 16
  hex digits. Each file goes in parts of at most 16 MiB: `base.tar.000000`,
  `base.tar.000001`, and so on. A tablespace is `<oid>.tar`. The manifest is
  `backup_manifest`.
- The `done` object goes last. It gives the start and end LSN, the timeline,
  the part count of each file, and the tablespaces. A backup without `done`
  is incomplete.
- `pgwal fetch` takes the latest backup whose WAL the archive holds up to
  its end, and prints its start LSN. With backups of more than one cluster
  in the archive, `--system-id` selects one.
- `pgwal fetch` unpacks `base.tar` as the parts arrive, and puts
  `backup_manifest` in the directory. `pg_verifybackup -n` checks the files.
- `pgwal fetch` refuses a backup with tablespaces.
- The library function is `pgwal_receiver::base_backup`.

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
- The command refuses an archive with a gap, and an archive that ends before
  the copy is consistent: the end of the backup in `backup_manifest`, or the
  minimum recovery point of a rebased copy. A standby that waits for WAL
  before it is consistent is an error too.
- The replay can stand still before the end of the archive when the last
  object ends inside a WAL record. After `--stall-seconds` (default 10) of
  no progress, the command takes the replay position as the end once
  Postgres waits for more WAL. A stop more than one segment before the end is
  an error: Postgres takes an unreadable record for the end of the WAL.
- The binaries must be of the major version of the copy, on the same
  platform as the primary. `--pg-bin` (or `PGWAL_PG_BIN`) gives their
  directory. Without it, they come from `PATH`.
- `--user` is a superuser of the copy, for the local connections. Its
  default is `postgres`. It does not read `PGUSER`, which names the
  replication role of the other commands.
- Then the command runs `pgwal verify` on the copy, and fails if the check
  fails.
- The server listens on a socket in a directory that only the user of the
  command can enter, and trusts local connections there. The `pg_hba.conf`
  of the copy stays as it is. The server archives nothing, cleans no archive,
  connects to no primary, waits for no synchronous standby, delays no replay,
  and runs without TLS, whatever the settings of the copy say.
- The settings that a standby needs at least as high as on the primary, such
  as `max_connections`, come from `pg_control`. When the primary raised one
  after the copy, the replay pauses, and the command starts the server again
  with the new value. A pause for another reason is an error.
- Once the WAL is in the copy, Ctrl-C, SIGTERM or SIGHUP stops the running
  tool and the server, and removes their files. The tools run in process
  groups of their own, so a Ctrl-C in the terminal does not stop
  `pg_ctl stop`. During the download, a signal stops the command at once. A
  signal that the shell ignores, as `nohup` ignores SIGHUP, stays ignored.
- After the rebase, the copy has no `backup_label.old` and no
  `backup_manifest`: they describe the copy before the rebase. Its
  `postgresql.auto.conf` keeps `restore_command = 'false'`,
  `primary_conninfo = ''` and `primary_slot_name = ''`.
- A copy without `postgresql.conf`, such as a copy of a Debian primary,
  starts with the defaults and the settings of `pg_control`.
- The command runs as the owner of the data directory, not as root.

`pgwal verify` checks a copy after a clean stop, and refuses another one:

- `pg_checksums --check`, when the copy has page checksums.
- `pg_amcheck --all --heapallindexed --install-missing` on the heap and the
  B-tree indexes of every database. It runs on a scratch copy next to the
  copy, because a server on the copy itself would end its recovery and write
  WAL of its own. The scratch copy holds the targets of the links of the
  copy, uses file clones where the file system has them, and goes away at
  the end. It archives nothing, loads no `shared_preload_libraries` and
  starts no logical replication worker.

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
- For `pgwal verify` and `pgwal rebase`: the `amcheck` extension, in the
  contrib package of some distributions, such as `postgresql18-contrib`.

## Tests

Some tests start Postgres servers. They take the Postgres binaries from
`PGWAL_PG_BIN`, else from `PATH`. Without them, these tests skip.

The S3 tests run against a real store, under a prefix of their own that they
delete:

```sh
PGWAL_TEST_S3_ENDPOINT=https://s3.example PGWAL_TEST_S3_BUCKET=pgwal-test \
AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
  cargo test -p pgwal-receiver --features s3 --test s3
```

## License

MIT.
