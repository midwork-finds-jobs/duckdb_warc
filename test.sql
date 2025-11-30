-- Test parse_warc scalar function
-- Load the extension
LOAD './build/release/extension/warc/warc.duckdb_extension';

-- Test 1: Parse uncompressed WARC file
SELECT 'Test 1: Uncompressed WARC' as test;
SELECT
    (parse_warc(content)).warc_version,
    (parse_warc(content)).http_status,
    (parse_warc(content)).http_body
FROM read_blob('test-data/example.warc');

-- Test 2: Parse gzip compressed WARC from Common Crawl
-- Download: curl -s -r"46376769-46377713" "https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-47/segments/1762439342185.16/warc/CC-MAIN-20251106200718-20251106230718-00970.warc.gz" > /tmp/test.warc.gz
SELECT 'Test 2: Gzip compressed WARC' as test;
SELECT
    (parse_warc(content)).warc_version,
    (parse_warc(content)).http_status,
    (parse_warc(content)).http_body
FROM read_blob('/tmp/test_warc.gz');

-- Test 3: Full struct output
SELECT 'Test 3: Full struct' as test;
SELECT parse_warc(content) FROM read_blob('test-data/example.warc');
