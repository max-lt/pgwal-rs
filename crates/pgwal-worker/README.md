# pgwal-worker

A pgwal receiver in a Cloudflare Durable Object. It streams the WAL of a
Postgres server into R2, under the keys of `pgwal receive`.

## Design

- One `alarm()` runs one replication session of `SESSION_SECONDS`. The
  runtime stops an alarm handler after 15 minutes.
- The WAL goes to the SQLite storage of the object. The flush goes to
  Postgres once `storage.sync()` resolves.
- Every `UPLOAD_SECONDS`, the stored WAL goes to R2, one object per
  contiguous LSN range inside a segment.
- On a timeline above 1, the session uploads the `.history` file and starts
  at the switch point.

`connect()` sends no `SSLRequest` and no ALPN, so Postgres cannot accept
its TLS. Put the server behind a proxy that terminates TLS, such as a tunnel.

## Deploy

```sh
wrangler secret put PGPASSWORD
wrangler secret put ADMIN_TOKEN
wrangler deploy
```

`wrangler.toml` holds the other settings and the R2 bucket.

## Control

Each request needs `Authorization: Bearer <ADMIN_TOKEN>`.

| Request            | Effect                                                                            |
| ------------------ | --------------------------------------------------------------------------------- |
| `POST /start`      | Starts the sessions.                                                              |
| `POST /stop`       | Ends the session. No other session starts.                                        |
| `GET /status`      | Positions, counters, last error, current step.                                    |
| `GET /list`        | Keys under `PREFIX`.                                                              |
| `GET /object?key=` | One object.                                                                       |
| `POST /reset`      | Deletes the objects under `PREFIX` and the state. Refused with an empty `PREFIX`. |

## Limits

- No restore. Restore with `pgwal`.
- The object is billed for its duration at 128 MB while a session runs.
