//! Live connection check against a running StarRocks frontend.
//!
//! Skipped unless IRODORI_LIVE_STARROCKS is set to host:port, so `cargo test`
//! stays green on a machine with no server. CI runs the unit tests only; this
//! is the check to run by hand, or against a compose service, before cutting
//! a release.

use irodori_extension_starrocks::{
    irodori_connector_call_json, irodori_connector_free_buffer, IrodoriConnectorBuffer,
};
use serde_json::Value;

fn call(request: &str) -> Value {
    let buffer = IrodoriConnectorBuffer {
        ptr: request.as_ptr(),
        len: request.len(),
    };
    let response = irodori_connector_call_json(buffer);
    let bytes = unsafe { std::slice::from_raw_parts(response.ptr, response.len) };
    let value: Value = serde_json::from_slice(bytes).expect("response is json");
    irodori_connector_free_buffer(response);
    value
}

#[test]
fn connects_queries_and_reads_metadata() {
    let Ok(endpoint) = std::env::var("IRODORI_LIVE_STARROCKS") else {
        eprintln!("skipped: IRODORI_LIVE_STARROCKS not set");
        return;
    };
    let (host, port) = endpoint
        .split_once(':')
        .unwrap_or((endpoint.as_str(), "9030"));
    let user = std::env::var("IRODORI_LIVE_STARROCKS_USER").unwrap_or_else(|_| "root".into());

    let connect = call(&format!(
        r#"{{"method":"connect","connectionId":"live","host":"{host}","port":"{port}","user":"{user}"}}"#
    ));
    assert_eq!(connect["ok"], true, "connect failed: {connect}");
    let version = connect["serverVersion"].as_str().unwrap_or_default();
    assert!(
        version.starts_with("StarRocks "),
        "unexpected server version: {version}"
    );
    eprintln!("connect -> {version}");

    // Scalars must reach the grid as JSON numbers, not as the strings the
    // MySQL text protocol actually puts on the wire.
    let query = call(
        r#"{"method":"query","connectionId":"live","sql":"SELECT 1 AS one, 2.5 AS ratio, 'irodori' AS who"}"#,
    );
    assert_eq!(query["ok"], true, "query failed: {query}");
    assert_eq!(query["columns"], serde_json::json!(["one", "ratio", "who"]));
    assert_eq!(query["rows"][0][0], serde_json::json!(1));
    assert_eq!(query["rows"][0][2], serde_json::json!("irodori"));
    eprintln!("query -> {}", query["rows"]);

    // Catalogs are the reason this connector exists: the internal one plus
    // whatever external catalogs the frontend has attached.
    let catalogs = call(r#"{"method":"query","connectionId":"live","sql":"SHOW CATALOGS"}"#);
    assert_eq!(catalogs["ok"], true, "SHOW CATALOGS failed: {catalogs}");
    eprintln!("catalogs -> {}", catalogs["rows"]);

    // Dropped first so a re-run against the same server starts clean;
    // otherwise the INSERT below accumulates and the row count drifts.
    let setup = [
        "CREATE DATABASE IF NOT EXISTS irodori_live",
        "DROP TABLE IF EXISTS irodori_live.readings",
        "CREATE TABLE IF NOT EXISTS irodori_live.readings (\
            id INT, label VARCHAR(32), value DOUBLE) \
            DISTRIBUTED BY HASH(id) PROPERTIES ('replication_num' = '1')",
        "INSERT INTO irodori_live.readings VALUES (1, 'a', 1.5), (2, 'b', 2.5)",
    ];
    for statement in setup {
        let response = call(&format!(
            r#"{{"method":"query","connectionId":"live","sql":{}}}"#,
            serde_json::to_string(statement).unwrap()
        ));
        assert_eq!(
            response["ok"], true,
            "setup failed on {statement}: {response}"
        );
    }

    let rows = call(
        r#"{"method":"query","connectionId":"live","sql":"SELECT id, label, value FROM irodori_live.readings ORDER BY id"}"#,
    );
    assert_eq!(rows["ok"], true, "read back failed: {rows}");
    assert_eq!(rows["rows"].as_array().map(Vec::len), Some(2));
    assert_eq!(rows["rows"][0][1], serde_json::json!("a"));
    eprintln!("readings -> {}", rows["rows"]);

    let metadata = call(r#"{"method":"metadata","connectionId":"live"}"#);
    assert_eq!(metadata["ok"], true, "metadata failed: {metadata}");
    let schemas = metadata["metadata"]["schemas"]
        .as_array()
        .expect("schemas array");
    let live = schemas
        .iter()
        .find(|schema| schema["name"] == "irodori_live")
        .expect("irodori_live schema is listed");
    let table = live["objects"]
        .as_array()
        .and_then(|objects| objects.iter().find(|object| object["name"] == "readings"))
        .expect("readings table is listed");
    assert_eq!(table["kind"], "table");
    assert_eq!(table["columns"].as_array().map(Vec::len), Some(3));
    eprintln!("metadata -> {} schemas", schemas.len());

    let close = call(r#"{"method":"close","connectionId":"live"}"#);
    assert_eq!(close["ok"], true, "close failed: {close}");
    assert_eq!(close["closed"], true);
}
