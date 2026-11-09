# Changelog

## 0.5.0

### Breaking

- `pgwal`: `ArchiveKey` parses and writes every archive key. It replaces
  `history_key`, `parse_history_key`, `backup_part_key`, `backup_done_key`
  and `parse_backup_done_key`.
- `pgwal`: `request_recovery(dir, AfterReplay)` replaces
  `request_archive_recovery` and `request_standby`.
- `pgwal`: `plan` takes a `ReplayStart`, read from `backup_label` or
  `pg_control`.
- `pgwal-receiver`: `ConnConfig` holds the connection settings.
  `StreamConfig` and `base_backup` take it. `StreamConfig::new`,
  `BackupConfig` and `Retry` are removed: build the configs as struct
  literals.
- `pgwal-receiver`: `StreamConfig` has an `on_gap` field.
- `pgwal-receiver`: `forget_slot` takes a `ConnConfig` and a slot name.

### Archive and store

- The archive keys do not change: 0.5 reads an archive of 0.4.
- The upload watermark of the store has a new format. The first run of 0.5
  on a store of 0.4 uploads the local WAL again. The archive accepts the
  same bytes.

### pgwal

- `ControlFile` gives the segment size, the cluster state and the redo
  point. A copy without `backup_label` starts at its redo point.
- `plan` chains each timeline up to its switch point only, and reports a
  gap when a later timeline does not meet the chain.
- `chain_end`, `timeline_switches` and `is_segment_size`.

### pgwal-receiver

- `OnGap`: a start that leaves a gap in the sink fails with `WalGone`,
  unless the config accepts the gap. A sink with no WAL of the system has
  no gap.
- After a failover, the receiver streams the old timeline to its switch
  point, uploads it, and archives every history file.
- An empty store starts after the newest WAL of the sink.
- Every start reads the slot: Postgres 16 and 17 stream from an
  invalidated slot. A slot with no WAL, and no WAL of the system in the
  store or the sink, is made again with its WAL reserved.
- The store flushes the tokio file before each sync, so a write error is
  not lost.
- The receiver flushes at once when it has all the WAL of the server, and
  within 100 ms otherwise. A catch-up goes from 72 to 1326 MiB/s on Linux.
- The upload watermark is written in place, so it works on a full disk.
  The disk is free as the backlog goes up.
- The finished segments of a dead run go up at the next start.
- Only a segment that went up in more than one object is compacted.
- `base_backup` of a standby gives the backup that the sink holds with the
  same start. A failed backup removes its parts.
- `DirSink`: a sink in a local directory. Its errors give the path.
- The WARN of a failed upload repeats each 60 s, with the lag in bytes.

### pgwal-cli

- `receive --confirm uploaded|flushed`, default `uploaded`: the slot keeps
  the WAL until the archive has it.
- `receive --accept-gap`, for one run after a WAL gone error.
- `receive` stops on SIGTERM and SIGHUP, and uploads the store on every
  stop. A failed final upload gives a non-zero exit status.
- TLS: `disable` by default, `verify-full` with `--sslrootcert`. A
  certificate that fails the check stops the command; an expired one gives
  a retry.
- Every command runs with umask 077. The commands refuse to run as root,
  and refuse a directory of another user.
- `restore` and `plan` refuse an archive that holds no WAL of the copy.
  `restore` reads a segment that `receive` compacts during the restore.
- `restore --standby` clears `primary_conninfo` and `primary_slot_name`.
- `rebase` runs its server in a private directory, accepts a setting that
  the primary raised, and stops on a signal.
- No ANSI colors when stderr is not a terminal.

## 0.4.0

### Breaking

- `StreamConfig` has a `confirm` field. Set `Confirm::Flushed` to keep the
  behavior of 0.3.
- `StreamConfig`, `Error` and `Confirm` are `#[non_exhaustive]`. Build a
  config with `StreamConfig::new`, then set the other fields. A `match` on
  `Error` needs a `_` arm.

### pgwal

- `ArchivedBackup`, `backup_part_key`, `backup_done_key` and
  `parse_backup_done_key`: the layout of a base backup in an archive.
- `BackupLabel`: reads `backup_label`, and gives the WAL segment size.
- `request_standby`.

### pgwal-receiver

- `base_backup` and `BackupConfig`: a base backup over the replication
  protocol, into a `WalSink`. It refuses to start without the slot of the
  stream.
- `Confirm::Uploaded`: the receiver confirms the flush once the WAL is in
  the sink.
- The `s3` feature: `S3Sink`, `S3Config` and `Retry`.
- A stop ends at the WAL end that the server gave before it answered.

### pgwal-cli

- `pgwal backup` and `pgwal fetch`.
- `pgwal restore`, with `--standby`.
- `pgwal plan`, `pgwal rebase` and `pgwal verify`.
- `s3://` archives, with the default `s3` feature.
