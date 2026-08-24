//! StarRocks connector behavior.
//!
//! The StarRocks frontend speaks the MySQL wire protocol on its query port
//! (9030 by default), so this driver is a MySQL client that knows where
//! StarRocks differs: `current_version()` names the server, catalogs sit
//! above databases, and `information_schema` carries the internal catalog
//! plus whatever external catalogs the frontend has attached.

use irodori_connector_abi::{collect_url_auth, option_bool, option_string, push_sensitive};
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};

use mysql::consts::ColumnType;
use mysql::prelude::*;
use mysql::{Opts, OptsBuilder, Pool, SslOpts, Value as MyValue};
use serde_json::{json, Map, Value};

use crate::abi::{self, IrodoriConnectorBuffer};
use crate::{ABI_VERSION, CONFIG_JSON, DRIVER_LINKED, ENGINE, MANIFEST_JSON};

/// Statements that name the server, most specific first. StarRocks answers
/// `current_version()`; the MySQL-compatible `version()` is the fallback for
/// a frontend too old to have it.
const VERSION_QUERIES: &[&str] = &["SELECT current_version()", "SELECT version()"];
const PRODUCT_MATCH: &str = "starrocks";
const PRODUCT: &str = "StarRocks";
const DEFAULT_PORT: u16 = 9030;

/// Schemas the object browser should not show. `_statistics_` is StarRocks'
/// own statistics database, not user data.
const SYSTEM_SCHEMAS: &[&str] = &[
    "information_schema",
    "sys",
    "mysql",
    "performance_schema",
    "_statistics_",
];

static CONNECTIONS: OnceLock<Mutex<HashMap<String, Connection>>> = OnceLock::new();

#[derive(Clone)]
struct Connection {
    pool: Pool,
    config: Config,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Config {
    url: Option<String>,
    host: String,
    port: u16,
    user: Option<String>,
    password: Option<String>,
    database: Option<String>,
    tls: TlsConfig,
    redaction_values: Vec<String>,
}

/// Transport security as `connector.config.json` declares it.
///
/// A path, never key material: connector options persist to the workspace in
/// the clear, so the profile carries a path and the driver reads the file at
/// connect time.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct TlsConfig {
    enabled: bool,
    root_cert_path: Option<String>,
    accept_invalid_certs: bool,
}

#[derive(Default)]
struct ObjectMeta {
    name: String,
    kind: String,
    columns: Vec<Value>,
}

type QueryRows = Vec<Vec<Value>>;
type QueryOutput = (Vec<String>, QueryRows, bool);

fn connections() -> &'static Mutex<HashMap<String, Connection>> {
    CONNECTIONS.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn call_json(request: IrodoriConnectorBuffer) -> IrodoriConnectorBuffer {
    let request = match abi::parse_request(request) {
        Ok(request) => request,
        Err(response) => return response,
    };
    let method = match abi::request_method(request.as_ref()) {
        Ok(method) => method,
        Err(response) => return response,
    };

    match method {
        "health" | "ping" => abi::ok(Map::from_iter([
            ("engine".to_string(), Value::String(ENGINE.to_string())),
            ("abiVersion".to_string(), json!(ABI_VERSION)),
            ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
        ])),
        "describe" | "capabilities" => abi::ok(Map::from_iter([
            ("engine".to_string(), Value::String(ENGINE.to_string())),
            ("abiVersion".to_string(), json!(ABI_VERSION)),
            ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
            (
                "manifest".to_string(),
                serde_json::from_str(MANIFEST_JSON).unwrap_or(Value::Null),
            ),
            (
                "config".to_string(),
                serde_json::from_str(CONFIG_JSON).unwrap_or(Value::Null),
            ),
        ])),
        "manifest" => abi::owned_buffer(MANIFEST_JSON.to_string()),
        "config" => abi::owned_buffer(CONFIG_JSON.to_string()),
        "connect" => connect(request.as_ref().expect("connect has request")),
        "query" => query(request.as_ref().expect("query has request")),
        "metadata" => metadata(request.as_ref().expect("metadata has request")),
        "close" => close(request.as_ref().expect("close has request")),
        other => abi::error(
            "connector.unknownMethod",
            format!("unknown connector method: {other}"),
        ),
    }
}

fn connect(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let config = match Config::from_request(request) {
        Ok(config) => config,
        Err(err) => return abi::error("connector.invalidRequest", err),
    };
    let opts = match config.build_opts() {
        Ok(opts) => opts,
        Err(err) => return abi::error("connector.invalidRequest", config.redact(&err)),
    };
    let pool = match Pool::new(opts) {
        Ok(pool) => pool,
        Err(err) => return abi::error("connector.connectFailed", config.redact(&err.to_string())),
    };
    let connection = Connection { pool, config };
    let version = match load_version(&connection) {
        Ok(version) => version,
        Err(err) => return abi::error("connector.connectFailed", connection.config.redact(&err)),
    };
    let mut guard = match connections().lock() {
        Ok(guard) => guard,
        Err(_) => {
            return abi::error(
                "connector.statePoisoned",
                "Connector connection state is poisoned.",
            )
        }
    };
    let response = Map::from_iter([
        ("engine".to_string(), Value::String(ENGINE.to_string())),
        (
            "connectionId".to_string(),
            Value::String(connection_id.clone()),
        ),
        (
            "endpoint".to_string(),
            Value::String(connection.config.endpoint()),
        ),
        (
            "database".to_string(),
            connection
                .config
                .database
                .clone()
                .map(Value::String)
                .unwrap_or(Value::Null),
        ),
        ("serverVersion".to_string(), Value::String(version)),
        ("driverLinked".to_string(), Value::Bool(DRIVER_LINKED)),
    ]);
    guard.insert(connection_id, connection);
    abi::ok(response)
}

fn query(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let Some(sql) = abi::string_field(request, "sql")
        .or_else(|| abi::string_field(request, "query"))
        .or_else(|| abi::string_field(request, "statement"))
    else {
        return abi::error(
            "connector.invalidRequest",
            "query requires a string sql, query, or statement field.",
        );
    };
    let connection = match connection(&connection_id) {
        Ok(connection) => connection,
        Err(response) => return response,
    };
    match run_query(&connection, sql, abi::max_rows(request)) {
        Ok((columns, rows, truncated)) => abi::ok(Map::from_iter([
            ("connectionId".to_string(), Value::String(connection_id)),
            (
                "columns".to_string(),
                Value::Array(columns.into_iter().map(Value::String).collect()),
            ),
            (
                "rows".to_string(),
                Value::Array(rows.into_iter().map(Value::Array).collect()),
            ),
            ("truncated".to_string(), Value::Bool(truncated)),
        ])),
        Err(err) => abi::error("connector.queryFailed", connection.config.redact(&err)),
    }
}

fn metadata(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let connection = match connection(&connection_id) {
        Ok(connection) => connection,
        Err(response) => return response,
    };
    match load_metadata(&connection) {
        Ok(metadata) => abi::ok(Map::from_iter([
            ("connectionId".to_string(), Value::String(connection_id)),
            ("metadata".to_string(), metadata),
        ])),
        Err(err) => abi::error("connector.metadataFailed", connection.config.redact(&err)),
    }
}

fn close(request: &Value) -> IrodoriConnectorBuffer {
    let connection_id = abi::connection_id(Some(request));
    let mut guard = match connections().lock() {
        Ok(guard) => guard,
        Err(_) => {
            return abi::error(
                "connector.statePoisoned",
                "Connector connection state is poisoned.",
            )
        }
    };
    let existed = guard.remove(&connection_id).is_some();
    abi::ok(Map::from_iter([
        ("connectionId".to_string(), Value::String(connection_id)),
        ("closed".to_string(), Value::Bool(existed)),
    ]))
}

fn connection(connection_id: &str) -> Result<Connection, IrodoriConnectorBuffer> {
    let guard = connections().lock().map_err(|_| {
        abi::error(
            "connector.statePoisoned",
            "Connector connection state is poisoned.",
        )
    })?;
    guard.get(connection_id).cloned().ok_or_else(|| {
        abi::error(
            "connector.connectionNotFound",
            format!("no open connection: {connection_id}"),
        )
    })
}

impl Config {
    fn from_request(request: &Value) -> Result<Self, String> {
        let url = option_string(request, &["connectionString", "url", "dsn"]);
        let host =
            option_string(request, &["host", "endpoint"]).unwrap_or_else(|| "127.0.0.1".into());
        let port = option_string(request, &["port"])
            .and_then(|port| port.trim().parse::<u16>().ok())
            .unwrap_or(DEFAULT_PORT);
        let user = option_string(request, &["user", "username"]);
        let password = option_string(request, &["password"]);
        let database = option_string(request, &["database", "db"]);
        let tls = TlsConfig {
            enabled: option_bool(request, &["tls", "ssl", "secure"]).unwrap_or(false),
            root_cert_path: option_string(
                request,
                &[
                    "sslRootCert",
                    "sslrootcert",
                    "ssl-ca",
                    "caCert",
                    "caCertificate",
                ],
            ),
            accept_invalid_certs: option_bool(request, &["sslInsecure", "tlsInsecure"])
                .unwrap_or(false),
        };
        let mut redaction_values = Vec::new();
        push_sensitive(&mut redaction_values, password.as_deref());
        if let Some(url) = url.as_deref() {
            collect_url_auth(url, &mut redaction_values);
        }
        Ok(Self {
            url,
            host,
            port,
            user,
            password,
            database,
            tls,
            redaction_values,
        })
    }

    fn endpoint(&self) -> String {
        match self.url.as_deref() {
            Some(_) => format!("mysql://{}:{}", self.host, self.port),
            None => format!("mysql://{}:{}", self.host, self.port),
        }
    }

    fn build_opts(&self) -> Result<Opts, String> {
        let opts = if let Some(url) = self.url.as_deref() {
            Opts::from_url(url).map_err(|err| format!("invalid connection URL: {err}"))?
        } else {
            let mut builder = OptsBuilder::new()
                .ip_or_hostname(Some(self.host.clone()))
                .tcp_port(self.port)
                .user(self.user.clone())
                .pass(self.password.clone());
            if let Some(database) = self.database.as_deref() {
                builder = builder.db_name(Some(database.to_string()));
            }
            if let Some(ssl) = self.tls.build()? {
                builder = builder.ssl_opts(Some(ssl));
            }
            Opts::from(builder)
        };
        // prefer_socket defaults to true, and on a loopback address the MySQL
        // client then reads `@@socket` to try to upgrade the TCP connection to
        // a unix socket. StarRocks has no such variable and rejects the whole
        // statement, so a frontend on localhost fails to connect while the
        // same build against a remote host succeeds. The frontend never serves
        // a unix socket, so there is nothing to prefer: turn it off for both
        // the host/port and the URL path, since a DSN can re-enable it.
        Ok(Opts::from(
            OptsBuilder::from_opts(opts).prefer_socket(false),
        ))
    }

    fn redact(&self, message: &str) -> String {
        abi::redact_endpoint(
            message,
            &self.endpoint(),
            "<starrocks-endpoint>",
            &self.redaction_values,
        )
    }
}

impl TlsConfig {
    /// `None` leaves the connection in plaintext, which is how a frontend on
    /// a private network is normally reached.
    fn build(&self) -> Result<Option<SslOpts>, String> {
        if !self.enabled && self.root_cert_path.is_none() && !self.accept_invalid_certs {
            return Ok(None);
        }
        let mut opts =
            SslOpts::default().with_danger_accept_invalid_certs(self.accept_invalid_certs);
        if let Some(path) = self.root_cert_path.as_deref() {
            opts = opts.with_root_cert_path(Some(Cow::Owned(PathBuf::from(path))));
        }
        Ok(Some(opts))
    }
}

fn load_version(connection: &Connection) -> Result<String, String> {
    let mut last_error = String::from("no version query succeeded");
    for statement in VERSION_QUERIES {
        match run_query(connection, statement, 1) {
            Ok((_, rows, _)) => {
                if let Some(version) = rows
                    .first()
                    .and_then(|row| row.first())
                    .map(value_to_display)
                    .filter(|version| !version.is_empty())
                {
                    return Ok(label_version(&version));
                }
            }
            Err(err) => last_error = err,
        }
    }
    Err(last_error)
}

/// The server's own string when it already names the product, prefixed when
/// it does not. `@@version_comment` answers with the product name in it, so
/// prefixing unconditionally produced labels that said the name twice; a bare
/// `version()` answers with digits only and needs the prefix.
fn label_version(version: &str) -> String {
    if version.to_lowercase().contains(PRODUCT_MATCH) {
        version.to_string()
    } else {
        format!("{PRODUCT} {version}")
    }
}

fn run_query(connection: &Connection, sql: &str, cap: usize) -> Result<QueryOutput, String> {
    let mut conn = connection
        .pool
        .get_conn()
        .map_err(|err| format!("checkout connection failed: {err}"))?;
    let mut result = conn
        .query_iter(sql)
        .map_err(|err| format!("query failed: {err}"))?;

    let column_defs: Vec<(String, ColumnType)> = result
        .columns()
        .as_ref()
        .iter()
        .map(|column| (column.name_str().to_string(), column.column_type()))
        .collect();
    let names: Vec<String> = column_defs.iter().map(|(name, _)| name.clone()).collect();

    let mut rows: QueryRows = Vec::new();
    let mut truncated = false;
    for row in result.by_ref() {
        let row = row.map_err(|err| format!("read row failed: {err}"))?;
        if rows.len() >= cap {
            truncated = true;
            break;
        }
        let mut cells = Vec::with_capacity(column_defs.len());
        for (index, (_, column_type)) in column_defs.iter().enumerate() {
            let value = row.as_ref(index).cloned().unwrap_or(MyValue::NULL);
            cells.push(cell_to_json(&value, *column_type));
        }
        rows.push(cells);
    }
    Ok((names, rows, truncated))
}

/// MySQL's text protocol hands every cell back as bytes, so the column type
/// is what decides whether a value reaches the grid as a JSON number or a
/// string. Anything that does not parse stays a string rather than becoming
/// a wrong number.
fn cell_to_json(value: &MyValue, column_type: ColumnType) -> Value {
    match value {
        MyValue::NULL => Value::Null,
        MyValue::Int(number) => json!(number),
        MyValue::UInt(number) => json!(number),
        MyValue::Float(number) => json!(number),
        MyValue::Double(number) => json!(number),
        MyValue::Bytes(bytes) => {
            let text = String::from_utf8_lossy(bytes).to_string();
            match column_type {
                ColumnType::MYSQL_TYPE_TINY
                | ColumnType::MYSQL_TYPE_SHORT
                | ColumnType::MYSQL_TYPE_LONG
                | ColumnType::MYSQL_TYPE_LONGLONG
                | ColumnType::MYSQL_TYPE_INT24
                | ColumnType::MYSQL_TYPE_YEAR => text
                    .parse::<i64>()
                    .map(|number| json!(number))
                    .unwrap_or(Value::String(text)),
                ColumnType::MYSQL_TYPE_FLOAT
                | ColumnType::MYSQL_TYPE_DOUBLE
                | ColumnType::MYSQL_TYPE_DECIMAL
                | ColumnType::MYSQL_TYPE_NEWDECIMAL => text
                    .parse::<f64>()
                    .ok()
                    .and_then(serde_json::Number::from_f64)
                    .map(Value::Number)
                    .unwrap_or(Value::String(text)),
                _ => Value::String(text),
            }
        }
        MyValue::Date(year, month, day, hour, minute, second, micros) => Value::String(
            format_datetime(*year, *month, *day, *hour, *minute, *second, *micros),
        ),
        MyValue::Time(negative, days, hours, minutes, seconds, micros) => Value::String(
            format_time(*negative, *days, *hours, *minutes, *seconds, *micros),
        ),
    }
}

/// Rendered the way the server would have printed it, so a DATE and a
/// DATETIME read the same whether the row arrived over the text or the
/// binary protocol.
#[allow(clippy::too_many_arguments)]
fn format_datetime(
    year: u16,
    month: u8,
    day: u8,
    hour: u8,
    minute: u8,
    second: u8,
    micros: u32,
) -> String {
    let date = format!("{year:04}-{month:02}-{day:02}");
    if hour == 0 && minute == 0 && second == 0 && micros == 0 {
        return date;
    }
    let time = format!("{hour:02}:{minute:02}:{second:02}");
    if micros == 0 {
        format!("{date} {time}")
    } else {
        format!("{date} {time}.{micros:06}")
    }
}

fn format_time(
    negative: bool,
    days: u32,
    hours: u8,
    minutes: u8,
    seconds: u8,
    micros: u32,
) -> String {
    let sign = if negative { "-" } else { "" };
    let hours = u32::from(hours) + days * 24;
    if micros == 0 {
        format!("{sign}{hours:02}:{minutes:02}:{seconds:02}")
    } else {
        format!("{sign}{hours:02}:{minutes:02}:{seconds:02}.{micros:06}")
    }
}

fn value_to_display(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn load_metadata(connection: &Connection) -> Result<Value, String> {
    let kinds = load_table_kinds(connection).unwrap_or_default();
    let (columns, rows, _) = run_query(
        connection,
        "SELECT table_schema, table_name, column_name, data_type, ordinal_position, is_nullable \
         FROM information_schema.columns \
         ORDER BY table_schema, table_name, ordinal_position",
        200_000,
    )?;
    Ok(metadata_from_columns(&columns, rows, &kinds))
}

fn load_table_kinds(connection: &Connection) -> Result<HashMap<(String, String), String>, String> {
    let (columns, rows, _) = run_query(
        connection,
        "SELECT table_schema, table_name, table_type FROM information_schema.tables",
        200_000,
    )?;
    let schema_idx = index_of(&columns, "table_schema");
    let name_idx = index_of(&columns, "table_name");
    let type_idx = index_of(&columns, "table_type");
    let (Some(schema_idx), Some(name_idx), Some(type_idx)) = (schema_idx, name_idx, type_idx)
    else {
        return Ok(HashMap::new());
    };
    let mut kinds = HashMap::new();
    for row in rows {
        let schema = string_cell(&row, schema_idx);
        let name = string_cell(&row, name_idx);
        if schema.is_empty() || name.is_empty() {
            continue;
        }
        kinds.insert((schema, name), normalise_kind(&string_cell(&row, type_idx)));
    }
    Ok(kinds)
}

fn normalise_kind(table_type: &str) -> String {
    match table_type.to_ascii_uppercase().as_str() {
        "VIEW" | "SYSTEM VIEW" => "view".to_string(),
        "MATERIALIZED VIEW" => "materializedView".to_string(),
        _ => "table".to_string(),
    }
}

fn metadata_from_columns(
    columns: &[String],
    rows: QueryRows,
    kinds: &HashMap<(String, String), String>,
) -> Value {
    let schema_idx = index_of(columns, "table_schema");
    let table_idx = index_of(columns, "table_name");
    let name_idx = index_of(columns, "column_name");
    let type_idx = index_of(columns, "data_type");
    let position_idx = index_of(columns, "ordinal_position");
    let nullable_idx = index_of(columns, "is_nullable");
    let (Some(schema_idx), Some(table_idx), Some(name_idx), Some(type_idx)) =
        (schema_idx, table_idx, name_idx, type_idx)
    else {
        return json!({ "schemas": [] });
    };

    let mut schemas: BTreeMap<String, BTreeMap<String, ObjectMeta>> = BTreeMap::new();
    for row in rows {
        let schema = string_cell(&row, schema_idx);
        let table = string_cell(&row, table_idx);
        let name = string_cell(&row, name_idx);
        if schema.is_empty() || table.is_empty() || name.is_empty() {
            continue;
        }
        if SYSTEM_SCHEMAS
            .iter()
            .any(|system| system.eq_ignore_ascii_case(&schema))
        {
            continue;
        }
        let ordinal = position_idx
            .and_then(|idx| row.get(idx))
            .and_then(Value::as_i64)
            .unwrap_or(0);
        let nullable = nullable_idx
            .map(|idx| string_cell(&row, idx))
            .map(|value| !value.eq_ignore_ascii_case("NO"))
            .unwrap_or(true);
        let kind = kinds
            .get(&(schema.clone(), table.clone()))
            .cloned()
            .unwrap_or_else(|| "table".to_string());
        let object = schemas
            .entry(schema.clone())
            .or_default()
            .entry(table.clone())
            .or_insert_with(|| ObjectMeta {
                name: table,
                kind,
                columns: Vec::new(),
            });
        object.columns.push(json!({
            "name": name,
            "dataType": string_cell(&row, type_idx),
            "nullable": nullable,
            "ordinal": ordinal
        }));
    }

    json!({
        "schemas": schemas
            .into_iter()
            .map(|(schema, objects)| {
                json!({
                    "name": schema,
                    "objects": objects
                        .into_values()
                        .map(|object| json!({
                            "schema": schema,
                            "name": object.name,
                            "kind": object.kind,
                            "columns": object.columns,
                            "indexes": [],
                            "primaryKey": [],
                            "foreignKeys": []
                        }))
                        .collect::<Vec<_>>()
                })
            })
            .collect::<Vec<_>>()
    })
}

fn index_of(columns: &[String], name: &str) -> Option<usize> {
    columns
        .iter()
        .position(|column| column.eq_ignore_ascii_case(name))
}

fn string_cell(row: &[Value], index: usize) -> String {
    row.get(index).map(value_to_display).unwrap_or_default()
}
