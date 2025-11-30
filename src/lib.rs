extern crate duckdb;
extern crate duckdb_loadable_macros;
extern crate libduckdb_sys;

use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    ffi,
    types::DuckString,
    vscalar::{ScalarFunctionSignature, VScalar},
    vtab::arrow::WritableVector,
    Connection, Result,
};
use duckdb_loadable_macros::duckdb_entrypoint_c_api;
use flate2::read::GzDecoder;
use libduckdb_sys::duckdb_string_t;
use std::error::Error;
use std::io::{BufReader, Read};
use warc::{WarcHeader, WarcReader};

/// Parsed WARC record with all required fields
struct ParsedRecord {
    warc_version: String,
    warc_headers: String,   // JSON map
    http_version: Option<String>,
    http_status: Option<i32>,
    http_headers: Option<String>, // JSON map
    http_body: Option<String>,
}

/// Convert WARC headers to a JSON-like map string
fn headers_to_json(record: &warc::Record<warc::BufferedBody>) -> String {
    let mut pairs = Vec::new();

    // Get standard headers
    if let Some(v) = record.header(WarcHeader::WarcType) {
        pairs.push(format!("\"WARC-Type\": \"{}\"", v.replace('"', "\\\"")));
    }
    if let Some(v) = record.header(WarcHeader::Date) {
        pairs.push(format!("\"WARC-Date\": \"{}\"", v.replace('"', "\\\"")));
    }
    if let Some(v) = record.header(WarcHeader::RecordID) {
        pairs.push(format!("\"WARC-Record-ID\": \"{}\"", v.replace('"', "\\\"")));
    }
    if let Some(v) = record.header(WarcHeader::TargetURI) {
        pairs.push(format!("\"WARC-Target-URI\": \"{}\"", v.replace('"', "\\\"")));
    }
    if let Some(v) = record.header(WarcHeader::IPAddress) {
        pairs.push(format!("\"WARC-IP-Address\": \"{}\"", v.replace('"', "\\\"")));
    }
    if let Some(v) = record.header(WarcHeader::ContentType) {
        pairs.push(format!("\"Content-Type\": \"{}\"", v.replace('"', "\\\"")));
    }
    pairs.push(format!("\"Content-Length\": {}", record.content_length()));
    if let Some(v) = record.header(WarcHeader::PayloadDigest) {
        pairs.push(format!("\"WARC-Payload-Digest\": \"{}\"", v.replace('"', "\\\"")));
    }
    if let Some(v) = record.header(WarcHeader::BlockDigest) {
        pairs.push(format!("\"WARC-Block-Digest\": \"{}\"", v.replace('"', "\\\"")));
    }
    if let Some(v) = record.header(WarcHeader::IdentifiedPayloadType) {
        pairs.push(format!("\"WARC-Identified-Payload-Type\": \"{}\"", v.replace('"', "\\\"")));
    }

    format!("{{{}}}", pairs.join(", "))
}

/// Parse HTTP response from WARC body
fn parse_http_response(body: &[u8]) -> (Option<String>, Option<i32>, Option<String>, Option<String>) {
    let content = String::from_utf8_lossy(body);
    let mut lines = content.lines();

    // Parse HTTP status line (e.g., "HTTP/1.1 200 OK")
    let (http_version, http_status) = if let Some(status_line) = lines.next() {
        if status_line.starts_with("HTTP/") {
            let parts: Vec<&str> = status_line.splitn(3, ' ').collect();
            let version = parts.first().map(|s| s.to_string());
            let status = parts.get(1).and_then(|s| s.parse::<i32>().ok());
            (version, status)
        } else {
            (None, None)
        }
    } else {
        (None, None)
    };

    // Parse HTTP headers
    let mut header_pairs = Vec::new();
    let mut body_start = false;
    let mut body_lines = Vec::new();

    for line in lines {
        if body_start {
            body_lines.push(line);
        } else if line.trim().is_empty() {
            body_start = true;
        } else if let Some((key, value)) = line.split_once(':') {
            let key = key.trim().replace('"', "\\\"");
            let value = value.trim().replace('"', "\\\"");
            header_pairs.push(format!("\"{}\": \"{}\"", key, value));
        }
    }

    let http_headers = if header_pairs.is_empty() {
        None
    } else {
        Some(format!("{{{}}}", header_pairs.join(", ")))
    };

    let http_body = if body_lines.is_empty() {
        None
    } else {
        Some(body_lines.join("\n"))
    };

    (http_version, http_status, http_headers, http_body)
}

/// Parse a WARC record from decompressed bytes using the warc library
fn parse_warc_record(data: &[u8]) -> Option<ParsedRecord> {
    let reader = BufReader::new(data);
    let mut warc_reader = WarcReader::new(reader);

    // Get the first record
    let record = warc_reader.iter_records().next()?.ok()?;

    // Get WARC version from the record
    let warc_version = record.warc_version().to_string();

    // Convert headers to JSON
    let warc_headers = headers_to_json(&record);

    // Check if this is a response record
    let warc_type = record.header(WarcHeader::WarcType)?;

    if warc_type == "response" {
        let body = record.body();
        let (http_version, http_status, http_headers, http_body) = parse_http_response(body);

        Some(ParsedRecord {
            warc_version,
            warc_headers,
            http_version,
            http_status,
            http_headers,
            http_body,
        })
    } else {
        // Non-response records don't have HTTP fields
        Some(ParsedRecord {
            warc_version,
            warc_headers,
            http_version: None,
            http_status: None,
            http_headers: None,
            http_body: None,
        })
    }
}

/// DuckDB scalar function to parse WARC records from gzip-compressed data
///
/// Returns a struct with:
/// - warc_version: VARCHAR
/// - warc_headers: VARCHAR (JSON map)
/// - http_version: VARCHAR
/// - http_status: INTEGER
/// - http_headers: VARCHAR (JSON map)
/// - http_body: VARCHAR
struct ParseWarc;

impl VScalar for ParseWarc {
    type State = ();

    unsafe fn invoke(
        _state: &Self::State,
        input: &mut DataChunkHandle,
        output: &mut dyn WritableVector,
    ) -> std::result::Result<(), Box<dyn Error>> {
        let size = input.len();
        let input_vector = input.flat_vector(0);
        let output_struct = output.struct_vector();

        // Get child vectors for each struct field
        let mut warc_version_vec = output_struct.child(0, size);
        let mut warc_headers_vec = output_struct.child(1, size);
        let mut http_version_vec = output_struct.child(2, size);
        let mut http_status_vec = output_struct.child(3, size);
        let mut http_headers_vec = output_struct.child(4, size);
        let mut http_body_vec = output_struct.child(5, size);

        // Get input as blob slice
        let blob_slice = input_vector.as_slice_with_len::<duckdb_string_t>(size);

        for i in 0..size {
            if input_vector.row_is_null(i as u64) {
                warc_version_vec.set_null(i);
                warc_headers_vec.set_null(i);
                http_version_vec.set_null(i);
                http_status_vec.set_null(i);
                http_headers_vec.set_null(i);
                http_body_vec.set_null(i);
                continue;
            }

            // Get data as blob
            let mut blob_data = blob_slice[i];
            let mut blob = DuckString::new(&mut blob_data);
            let raw_data = blob.as_bytes();

            // Try to decompress gzip data, fall back to raw data if it fails
            let data_to_parse = {
                let mut decoder = GzDecoder::new(raw_data);
                let mut decompressed = Vec::new();
                if decoder.read_to_end(&mut decompressed).is_ok() && !decompressed.is_empty() {
                    decompressed
                } else {
                    // Not gzip compressed, use raw data
                    raw_data.to_vec()
                }
            };

            // Parse the WARC record
            match parse_warc_record(&data_to_parse) {
                Some(record) => {
                    warc_version_vec.insert(i, record.warc_version.as_str());
                    warc_headers_vec.insert(i, record.warc_headers.as_str());

                    match &record.http_version {
                        Some(v) => http_version_vec.insert(i, v.as_str()),
                        None => http_version_vec.set_null(i),
                    }

                    match record.http_status {
                        Some(v) => {
                            let slice = http_status_vec.as_mut_slice::<i32>();
                            slice[i] = v;
                        }
                        None => http_status_vec.set_null(i),
                    }

                    match &record.http_headers {
                        Some(v) => http_headers_vec.insert(i, v.as_str()),
                        None => http_headers_vec.set_null(i),
                    }

                    match &record.http_body {
                        Some(v) => http_body_vec.insert(i, v.as_str()),
                        None => http_body_vec.set_null(i),
                    }
                }
                None => {
                    warc_version_vec.set_null(i);
                    warc_headers_vec.set_null(i);
                    http_version_vec.set_null(i);
                    http_status_vec.set_null(i);
                    http_headers_vec.set_null(i);
                    http_body_vec.set_null(i);
                }
            }
        }

        Ok(())
    }

    fn signatures() -> Vec<ScalarFunctionSignature> {
        // Helper to create struct type
        let make_struct_type = || {
            LogicalTypeHandle::struct_type(&[
                ("warc_version", LogicalTypeHandle::from(LogicalTypeId::Varchar)),
                ("warc_headers", LogicalTypeHandle::from(LogicalTypeId::Varchar)),
                ("http_version", LogicalTypeHandle::from(LogicalTypeId::Varchar)),
                ("http_status", LogicalTypeHandle::from(LogicalTypeId::Integer)),
                ("http_headers", LogicalTypeHandle::from(LogicalTypeId::Varchar)),
                ("http_body", LogicalTypeHandle::from(LogicalTypeId::Varchar)),
            ])
        };

        // Support both BLOB and VARCHAR inputs
        vec![
            ScalarFunctionSignature::exact(
                vec![LogicalTypeHandle::from(LogicalTypeId::Blob)],
                make_struct_type(),
            ),
            ScalarFunctionSignature::exact(
                vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)],
                make_struct_type(),
            ),
        ]
    }
}

#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_scalar_function::<ParseWarc>("parse_warc")?;
    Ok(())
}
