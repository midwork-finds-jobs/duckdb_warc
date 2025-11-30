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
    http_body: Option<Vec<u8>>,   // Binary body data
}

/// Sanitize header value for JSON output (escape quotes, remove null bytes)
fn sanitize_header(v: &std::borrow::Cow<str>) -> String {
    v.replace('"', "\\\"").replace('\0', "")
}

/// Convert WARC headers to a JSON-like map string
fn headers_to_json(record: &warc::Record<warc::BufferedBody>) -> String {
    let mut pairs = Vec::new();

    // Get standard headers
    if let Some(v) = record.header(WarcHeader::WarcType) {
        pairs.push(format!("\"WARC-Type\": \"{}\"", sanitize_header(&v)));
    }
    if let Some(v) = record.header(WarcHeader::Date) {
        pairs.push(format!("\"WARC-Date\": \"{}\"", sanitize_header(&v)));
    }
    if let Some(v) = record.header(WarcHeader::RecordID) {
        pairs.push(format!("\"WARC-Record-ID\": \"{}\"", sanitize_header(&v)));
    }
    if let Some(v) = record.header(WarcHeader::TargetURI) {
        pairs.push(format!("\"WARC-Target-URI\": \"{}\"", sanitize_header(&v)));
    }
    if let Some(v) = record.header(WarcHeader::IPAddress) {
        pairs.push(format!("\"WARC-IP-Address\": \"{}\"", sanitize_header(&v)));
    }
    if let Some(v) = record.header(WarcHeader::ContentType) {
        pairs.push(format!("\"Content-Type\": \"{}\"", sanitize_header(&v)));
    }
    pairs.push(format!("\"Content-Length\": {}", record.content_length()));
    if let Some(v) = record.header(WarcHeader::PayloadDigest) {
        pairs.push(format!("\"WARC-Payload-Digest\": \"{}\"", sanitize_header(&v)));
    }
    if let Some(v) = record.header(WarcHeader::BlockDigest) {
        pairs.push(format!("\"WARC-Block-Digest\": \"{}\"", sanitize_header(&v)));
    }
    if let Some(v) = record.header(WarcHeader::IdentifiedPayloadType) {
        pairs.push(format!("\"WARC-Identified-Payload-Type\": \"{}\"", sanitize_header(&v)));
    }

    format!("{{{}}}", pairs.join(", "))
}

/// Sanitize a string for C FFI - remove null bytes and any control chars
fn sanitize_for_ffi(s: &str) -> String {
    s.chars()
        .filter(|c| *c != '\0')
        .collect()
}

/// Parse HTTP response from WARC body
/// Returns (http_version, http_status, http_headers_json, http_body_bytes)
/// If skip_body is true, returns None for body (used for binary content)
fn parse_http_response(body: &[u8], skip_body: bool) -> (Option<String>, Option<i32>, Option<String>, Option<Vec<u8>>) {
    // Quick check: if body doesn't start with HTTP, return None
    if !body.starts_with(b"HTTP/") {
        return (None, None, None, None);
    }

    // Find the header/body separator (\r\n\r\n or \n\n)
    let separator_pos = body
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|p| (p, 4))
        .or_else(|| body.windows(2).position(|w| w == b"\n\n").map(|p| (p, 2)));

    let (header_bytes, body_bytes) = match separator_pos {
        Some((pos, sep_len)) => (&body[..pos], Some(&body[pos + sep_len..])),
        None => {
            // No separator found
            return (None, None, None, None);
        }
    };

    // Parse headers as text (headers are always ASCII-compatible)
    let header_text = String::from_utf8_lossy(header_bytes);
    let mut lines = header_text.lines();

    // Parse HTTP status line (e.g., "HTTP/1.1 200 OK")
    let (http_version, http_status) = if let Some(status_line) = lines.next() {
        let parts: Vec<&str> = status_line.splitn(3, ' ').collect();
        let version = parts.first().map(|s| sanitize_for_ffi(s));
        let status = parts.get(1).and_then(|s| s.parse::<i32>().ok());
        (version, status)
    } else {
        (None, None)
    };

    // Parse HTTP headers (sanitize and lowercase keys for consistent access)
    let mut header_pairs = Vec::new();
    for line in lines {
        if let Some((key, value)) = line.split_once(':') {
            let key = sanitize_for_ffi(key.trim()).to_lowercase().replace('"', "\\\"");
            let value = sanitize_for_ffi(value.trim()).replace('"', "\\\"");
            header_pairs.push(format!("\"{}\": \"{}\"", key, value));
        }
    }

    let http_headers = if header_pairs.is_empty() {
        None
    } else {
        Some(format!("{{{}}}", header_pairs.join(", ")))
    };

    // Return body only if not skipping and body exists
    let http_body = if skip_body {
        None
    } else {
        body_bytes.map(|b| b.to_vec())
    };

    (http_version, http_status, http_headers, http_body)
}

/// Parse a WARC record from decompressed bytes using the warc library
fn parse_warc_record(data: &[u8]) -> Option<ParsedRecord> {
    let reader = BufReader::new(data);
    let warc_reader = WarcReader::new(reader);

    // Get the first record
    let record = match warc_reader.iter_records().next() {
        Some(Ok(r)) => r,
        Some(Err(_)) => return None,
        None => return None,
    };

    // Get WARC version from the record (sanitize for C FFI)
    let warc_version = sanitize_for_ffi(&record.warc_version().to_string());

    // Convert headers to JSON (sanitize for C FFI)
    let warc_headers = sanitize_for_ffi(&headers_to_json(&record));

    // Check if this is a response record
    let warc_type = record.header(WarcHeader::WarcType)?;

    if warc_type == "response" {
        let body = record.body();

        // Check if body contains null bytes (binary content)
        // Parse HTTP headers but skip binary body
        let is_binary = body.contains(&0u8);
        if is_binary {
            let uri = record.header(WarcHeader::TargetURI).unwrap_or_default();
            let payload_type = record.header(WarcHeader::IdentifiedPayloadType).unwrap_or_default();
            eprintln!("parse_warc: binary content, omitting body uri={} type={}", uri, payload_type);
        }

        let (http_version, http_status, http_headers, http_body) = parse_http_response(body, is_binary);

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
        let _input_vector = input.flat_vector(0);

        let output_struct = output.struct_vector();
        let mut warc_version_vec = output_struct.child(0, size);
        let mut warc_headers_vec = output_struct.child(1, size);
        let mut http_version_vec = output_struct.child(2, size);
        let mut http_status_vec = output_struct.child(3, size);
        let mut http_headers_vec = output_struct.child(4, size);
        let mut http_body_vec = output_struct.child(5, size);

        let input_vector = _input_vector;

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
                        Some(v) => {
                            // Use explicit &[u8] type to ensure BLOB insertion (not string)
                            Inserter::<&[u8]>::insert(&http_body_vec, i, v.as_slice());
                        }
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
        // Helper to create struct return type (needed twice since LogicalTypeHandle doesn't impl Clone)
        let make_return_type = || {
            LogicalTypeHandle::struct_type(&[
                ("warc_version", LogicalTypeHandle::from(LogicalTypeId::Varchar)),
                ("warc_headers", LogicalTypeHandle::from(LogicalTypeId::Varchar)),
                ("http_version", LogicalTypeHandle::from(LogicalTypeId::Varchar)),
                ("http_status", LogicalTypeHandle::from(LogicalTypeId::Integer)),
                ("http_headers", LogicalTypeHandle::from(LogicalTypeId::Varchar)),
                ("http_body", LogicalTypeHandle::from(LogicalTypeId::Blob)),
            ])
        };

        // Support both BLOB and VARCHAR inputs
        vec![
            ScalarFunctionSignature::exact(
                vec![LogicalTypeHandle::from(LogicalTypeId::Blob)],
                make_return_type(),
            ),
            ScalarFunctionSignature::exact(
                vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)],
                make_return_type(),
            ),
        ]
    }
}

#[duckdb_entrypoint_c_api()]
pub unsafe fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn Error>> {
    con.register_scalar_function::<ParseWarc>("parse_warc")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn load_example_warc() -> Vec<u8> {
        fs::read("test-data/example.warc").expect("Failed to read test-data/example.warc")
    }

    #[test]
    fn test_parse_warc_record_basic() {
        let data = load_example_warc();
        let result = parse_warc_record(&data);
        assert!(result.is_some());

        let record = result.unwrap();
        assert_eq!(record.warc_version, "1.0");
        assert_eq!(record.http_status, Some(200));
        assert_eq!(record.http_version, Some("HTTP/1.1".to_string()));
        assert!(record.http_body.is_some());
        let body = String::from_utf8_lossy(record.http_body.as_ref().unwrap());
        assert!(body.contains("Example Domain"));
    }

    #[test]
    fn test_parse_warc_headers_json() {
        let data = load_example_warc();
        let result = parse_warc_record(&data).unwrap();

        // Check WARC headers contain expected fields
        assert!(result.warc_headers.contains("\"WARC-Type\": \"response\""));
        assert!(result.warc_headers.contains("\"WARC-Target-URI\": \"http://www.example.com/\""));
        assert!(result.warc_headers.contains("\"WARC-IP-Address\": \"2.18.67.69\""));
    }

    #[test]
    fn test_parse_http_headers_lowercase() {
        let data = load_example_warc();
        let result = parse_warc_record(&data).unwrap();
        let http_headers = result.http_headers.unwrap();

        // HTTP header keys should be lowercase
        assert!(http_headers.contains("\"content-type\": \"text/html\""));
        assert!(http_headers.contains("\"content-length\": \"513\""));
    }

    #[test]
    fn test_parse_http_response_basic() {
        let http_data = b"HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\n\r\nNot found";
        let (version, status, headers, body) = parse_http_response(http_data, false);

        assert_eq!(version, Some("HTTP/1.1".to_string()));
        assert_eq!(status, Some(404));
        assert!(headers.unwrap().contains("\"content-type\": \"text/plain\""));
        assert_eq!(body, Some(b"Not found".to_vec()));
    }

    #[test]
    fn test_parse_http_response_skip_body() {
        let http_data = b"HTTP/1.1 200 OK\r\nContent-Type: image/png\r\n\r\n\x89PNG\r\n\x1a\n";
        let (version, status, headers, body) = parse_http_response(http_data, true);

        assert_eq!(version, Some("HTTP/1.1".to_string()));
        assert_eq!(status, Some(200));
        assert!(headers.is_some());
        assert!(body.is_none()); // Body skipped
    }

    #[test]
    fn test_parse_http_response_not_http() {
        let data = b"Not HTTP data";
        let (version, status, headers, body) = parse_http_response(data, false);

        assert!(version.is_none());
        assert!(status.is_none());
        assert!(headers.is_none());
        assert!(body.is_none());
    }

    #[test]
    fn test_sanitize_for_ffi_removes_nulls() {
        let input = "hello\0world";
        let result = sanitize_for_ffi(input);
        assert_eq!(result, "helloworld");
    }

    #[test]
    fn test_parse_warc_invalid_data() {
        let invalid = b"This is not a WARC file";
        let result = parse_warc_record(invalid);
        assert!(result.is_none());
    }

    #[test]
    fn test_gzip_decompression() {
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Write;

        let data = load_example_warc();

        // Compress the data
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&data).unwrap();
        let compressed = encoder.finish().unwrap();

        // Decompress and parse
        let mut decoder = GzDecoder::new(compressed.as_slice());
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed).unwrap();

        let result = parse_warc_record(&decompressed);
        assert!(result.is_some());
        assert_eq!(result.unwrap().http_status, Some(200));
    }
}
