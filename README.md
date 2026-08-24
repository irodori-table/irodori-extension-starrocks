# irodori-extension-starrocks

Native [Irodori Table](https://github.com/irodori-table/irodori-table) connector
extension for [StarRocks](https://github.com/StarRocks/starrocks).

Not installed by default. Install it from **Settings ▸ Extensions**, or from the
command palette with **Open Extensions**, and the `starrocks` engine appears in
the connection dialog.

## What it connects to

The StarRocks frontend serves the **MySQL wire protocol on its query port,
9030** — not the HTTP port on 8030, which answers a different API. This
connector is a MySQL client pointed at the FE query port.

| Field | Default |
| --- | --- |
| Host | `127.0.0.1` |
| Port | `9030` |
| User | — |
| Database | — (optional; catalogs and databases are browsable without one) |

Authentication is user/password, a connection URL, or none — which is what a
fresh `root` on a local cluster needs. TLS is off unless the profile asks for
it; a CA certificate is given as a **path**, because connector options persist
to the workspace in the clear and key material must not.

## What it reads

- **Query results**, with cell types resolved from the column type rather than
  from the bytes on the wire — MySQL's text protocol sends every value as
  bytes, so an `INT` reaches the grid as a JSON number and not as `"1"`.
- **Catalogs.** `SHOW CATALOGS` lists the internal catalog and every external
  catalog the frontend has attached, which is the reason to point Irodori Table
  at StarRocks rather than at the object store directly.
- **Metadata** for the object browser, from `information_schema.tables` and
  `information_schema.columns`: databases, tables, views, materialized views,
  and their columns. StarRocks' own `_statistics_` database is filtered out.

## Local development

```sh
make check           # fmt, clippy, unit tests
make build           # release cdylib
make package         # dist/irodori-extension-starrocks.tar.gz
```

### Connection check

`cargo test` alone never touches the network. The live check is opt-in:

```sh
podman run -d --name starrocks -p 9030:9030 -p 8030:8030 \
  --memory 8g docker.io/starrocks/allin1-ubuntu:latest

IRODORI_LIVE_STARROCKS=127.0.0.1:9030 make live

podman rm -f starrocks
```

It connects, runs a typed `SELECT`, lists catalogs, creates and reads back a
table, reads the object-browser metadata, and closes. Last run against
**StarRocks 4.1.4**.

## A note on `prefer_socket`

The Rust MySQL client defaults `prefer_socket` to true, and on a loopback
address it then reads `@@socket` to try to upgrade the TCP connection to a unix
socket. StarRocks has no such variable and rejects the statement, so a frontend
on `localhost` fails to connect while the same build against a remote host
succeeds. A StarRocks frontend never serves a unix socket, so the driver turns
the preference off on both the host/port and the URL path — a DSN can otherwise
re-enable it.

## Licence

MIT OR 0BSD. See [LICENSE-MIT](LICENSE-MIT) and [LICENSE-0BSD](LICENSE-0BSD).
