# DuckDB WARC Extension

A DuckDB extension for reading WARC (Web ARChive) files, including compressed `.warc.gz` files commonly used by Common Crawl.

## Features

- Read WARC and WARC.gz files
- Support for concatenated gzip format (used by Common Crawl)
- Extract metadata and content from WARC records
- SQL-based querying of web archive data

## Building

```bash
make configure
make debug    # or make release
```

## Usage

### Loading the Extension

```sql
LOAD './build/release/extension/warc/warc.duckdb_extension';
```

### Reading WARC Files

The extension provides a `read_warc()` table function that returns the following columns:

- `warc_record_id` (VARCHAR) - Unique record identifier
- `warc_type` (VARCHAR) - Record type (request, response, metadata, warcinfo)
- `warc_date` (VARCHAR) - Timestamp of the record
- `target_uri` (VARCHAR) - Target URI (for request/response records)
- `content_length` (UBIGINT) - Length of the content
- `content_type` (VARCHAR) - Content type header
- `content` (BLOB) - Raw content data

### Quick Start Examples

For complete examples, see:
- `examples/create_table.sh` - Creating persistent tables for fast querying
- `examples/analyze_commoncrawl.sql` - Common Crawl analysis queries

#### Count records in a WARC file

```sql
SELECT COUNT(*) as record_count
FROM read_warc('./test-data/sample.warc.gz');
```

#### Group by record type

```sql
SELECT warc_type, COUNT(*) as count
FROM read_warc('./test-data/sample.warc.gz')
GROUP BY warc_type
ORDER BY count DESC;
```

#### Query response records

```sql
SELECT
    warc_type,
    warc_date,
    target_uri,
    content_length,
    octet_length(content) as actual_content_length
FROM read_warc('./test-data/sample.warc.gz')
WHERE warc_type = 'response'
LIMIT 5;
```

#### Extract text content from responses

```sql
SELECT
    target_uri,
    content_type,
    CAST(content AS VARCHAR) as text_content
FROM read_warc('./test-data/sample.warc.gz')
WHERE warc_type = 'response'
  AND content_type LIKE '%text/html%'
LIMIT 10;
```

### Using with Common Crawl

To use with Common Crawl data, you can either:

1. **Download the file first:**
```bash
wget https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-43/segments/1759648358356.7/warc/CC-MAIN-20251009035013-20251009065013-00795.warc.gz
```

Then query it:
```sql
SELECT * FROM read_warc('./CC-MAIN-20251009035013-20251009065013-00795.warc.gz') LIMIT 10;
```

2. **Use httpfs extension (for streaming):**
```sql
-- Note: Direct HTTP support is not yet implemented in the WARC extension
-- You'll need to download files locally first
```

## Schema

```
┌─────────────────┬──────────┬─────────┐
│  Column Name    │   Type   │ Nullable│
├─────────────────┼──────────┼─────────┤
│ warc_record_id  │ VARCHAR  │   No    │
│ warc_type       │ VARCHAR  │   No    │
│ warc_date       │ VARCHAR  │   No    │
│ target_uri      │ VARCHAR  │   Yes   │
│ content_length  │ UBIGINT  │   No    │
│ content_type    │ VARCHAR  │   Yes   │
│ content         │ BLOB     │   No    │
└─────────────────┴──────────┴─────────┘
```

## Performance Characteristics

**Important:** The current implementation loads all WARC records into memory during the initial query phase.

**Quick Summary:**
- First query: ~13 seconds (1.1GB file, 78k records) - regardless of LIMIT
- Subsequent queries: instant (data cached in memory)
- **Recommended:** Create persistent table for 1,300x faster queries!

**Best Practice:**
```sql
-- One-time load (13-30 seconds)
CREATE TABLE warc_data AS SELECT * FROM read_warc('file.warc.gz');

-- All subsequent queries (0.02 seconds)
SELECT * FROM warc_data WHERE warc_type = 'response' LIMIT 10;
```

**When to use this extension:**
- ✅ Exploratory analysis with multiple queries
- ✅ Files with up to a few hundred thousand records
- ✅ Creating persistent DuckDB tables from WARC data
- ❌ Streaming massive files (10GB+)
- ❌ One-off queries on large files

See **[PERFORMANCE.md](PERFORMANCE.md)** for detailed analysis, benchmarks, and future improvement options.

## Technical Details

- Built with Rust using the `warc` crate (v0.4.0)
- Supports both compressed (.warc.gz) and uncompressed (.warc) files
- Uses `MultiGzDecoder` to handle concatenated gzip format (used by Common Crawl)
- Compatible with DuckDB v1.4.2
- Records are loaded eagerly during query initialization and stored in memory

## Example Output

```
┌───────────┬─────────────────────────┬───────────────────────────────────┬────────────────┐
│ warc_type │        warc_date        │            target_uri             │ content_length │
├───────────┼─────────────────────────┼───────────────────────────────────┼────────────────┤
│ response  │ 2025-10-09 04:22:18 UTC │ http://021ka.com/gupiaogo...      │          21529 │
│ response  │ 2025-10-09 05:13:35 UTC │ http://0u0.cn/?id=130             │          27056 │
│ response  │ 2025-10-09 04:48:14 UTC │ http://0u0.cn/?id=581             │          25239 │
└───────────┴─────────────────────────┴───────────────────────────────────┴────────────────┘
```

## License

This extension is built using the DuckDB Rust extension template and follows the DuckDB ecosystem licensing practices.
