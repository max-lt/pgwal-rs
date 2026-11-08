# Changelog

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
