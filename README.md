# DuckDB WARC Extension

A DuckDB extension for parsing WARC (Web ARChive) records. Designed for use with Common Crawl's columnar index for efficient selective record fetching.

## Features

- `parse_warc(BLOB|VARCHAR)` scalar function to parse WARC records
- Returns structured data: WARC headers, HTTP headers, and body
- Auto-detects gzip compression
- Works with Common Crawl byte-range fetching workflow

## Installation

```sql
-- Load the extension
LOAD './build/release/extension/warc/warc.duckdb_extension';
```

## Building

```bash
make configure
make release
```

## Usage

### parse_warc() Function

Parses a WARC record and returns a struct with:

| Field | Type | Description |
|-------|------|-------------|
| `warc_version` | VARCHAR | WARC format version (e.g., "1.0") |
| `warc_headers` | VARCHAR | JSON object of WARC headers |
| `http_version` | VARCHAR | HTTP version (e.g., "HTTP/1.1") |
| `http_status` | INTEGER | HTTP status code (e.g., 200) |
| `http_headers` | VARCHAR | JSON object of HTTP headers |
| `http_body` | BLOB | Response body content (binary) |

### Examples

**Parse a local WARC file:**
```sql
SELECT parse_warc(content) FROM read_blob('record.warc.gz');
```

**Parse uncompressed WARC from text:**
```sql
SELECT parse_warc(content) FROM read_text('record.warc');
```

**Extract specific fields:**
```sql
SELECT
    (parse_warc(content)).http_status,
    (parse_warc(content)).http_body
FROM read_blob('record.warc.gz');
```

### Common Crawl Workflow

The recommended workflow for Common Crawl is:

1. **Query the columnar index** (Parquet) to find records
2. **Fetch only the specific byte ranges** you need
3. **Parse with this extension**

```sql
-- Step 1: Query Common Crawl index to find a URL
-- (Use their Parquet index at s3://commoncrawl/cc-index/...)

-- Step 2: Download specific byte range
-- curl -r OFFSET-END https://data.commoncrawl.org/FILENAME > record.warc.gz

-- Step 3: Parse the record
SELECT
    (parse_warc(content)).http_status,
    (parse_warc(content)).http_body
FROM read_blob('record.warc.gz');
```

**Example: Fetch example.com from Common Crawl**
```bash
# Download only 945 bytes instead of 1.1GB WARC file
curl -s -r"46376769-46377713" \
  "https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-47/segments/1762439342185.16/warc/CC-MAIN-20251106200718-20251106230718-00970.warc.gz" \
  > record.warc.gz
```

```sql
SELECT
    (parse_warc(content)).warc_version,
    (parse_warc(content)).http_status,
    (parse_warc(content)).http_body
FROM read_blob('record.warc.gz');
```

Output:
```
┌──────────────┬─────────────┬─────────────────────────────────────────────────┐
│ warc_version │ http_status │ http_body                                       │
├──────────────┼─────────────┼─────────────────────────────────────────────────┤
│ 1.0          │ 200         │ <!doctype html><html lang="en"><head><title>... │
└──────────────┴─────────────┴─────────────────────────────────────────────────┘
```

## Schema

```sql
parse_warc(BLOB) -> STRUCT(
    warc_version VARCHAR,
    warc_headers VARCHAR,    -- JSON: {"WARC-Type": "response", "WARC-Date": "...", ...}
    http_version VARCHAR,
    http_status INTEGER,
    http_headers VARCHAR,    -- JSON: {"content-type": "text/html", ...}
    http_body BLOB           -- Binary body (use decode(http_body) for text)
)
```

## Technical Details

- Built with Rust using the `warc` crate (v0.4.0)
- Uses `flate2` for gzip decompression
- Auto-detects compressed vs uncompressed input
- Compatible with DuckDB v1.4.2

## License

MIT
