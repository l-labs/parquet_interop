# parquet_interop

Apache Parquet reader/writer/streamer for L, as a shared library loaded via
`2:`. The Parquet format work (all encodings; zstd/snappy/lz4/gzip/brotli
decompression) is done by the arrow-rs `parquet` crate; writes use zstd
compression, dictionary encoding, and 1M-row row groups. Row groups decode
and encode in parallel with bulk buffer copies into/out of K vectors:
measured on a 10M-row × 4-column zstd table, `pq_read` ~91M rows/s
(~2.5 GB/s of column data) and `pq_write` ~24M rows/s (~660 MB/s).

## Quickstart

```sh
cargo build --release
# macOS: `2:` appends .so, cargo emits .dylib — give it the name it wants
cp target/release/libl_parquet.dylib target/release/libl_parquet.so
```

```q
.pq.read:  `:target/release/libl_parquet 2: (`pq_read; 1)
.pq.write: `:target/release/libl_parquet 2: (`pq_write; 1)
.pq.stream:`:target/release/libl_parquet 2: (`pq_stream; 1)

.pq.write[([]sym:`AAPL`GOOG;price:150.5 175.3); `:/tmp/out.parquet]
t:.pq.read `:/tmp/out.parquet
n:.pq.stream[`:/tmp/huge.parquet; `:/tmp/db/t]   / splay, 1 row group DRAM
```

## Type mapping (verified by the test suite)

| L type | Parquet/Arrow type | Read | Write | Notes |
|--------|--------------------|------|-------|-------|
| KB bool | Boolean | y | y | null reads as 0b |
| KG byte | UInt8 (Int8 accepted) | y | y | |
| KH short / KI int / KJ long | Int16/32/64 | y | y | 0Ni/0Nj ↔ null |
| KE real / KF float | Float32/64 | y | y | 0n ↔ null (f64) |
| KS symbol | Utf8 (dictionary-encoded on disk) | y | y | plain AND dict-encoded read; null ↔ ` |
| KD date | Date32[day] | y | y | epoch shift 2000 ↔ 1970 |
| KP timestamp | Timestamp[s/ms/us/ns → ns] | y | y | epoch shift, null-preserving |
| KN timespan | Duration[ns] | y | y | via embedded Arrow schema |
| KZ datetime | Timestamp[ns] | — | y | write-only; reads back as KP |

Nested columns (List/Struct/Map) raise `'nyi`. `pq_stream` writes native
splay files (32-byte header + raw payload; 0xFF01 form for symbols) chunk
by chunk and patches counts at the end — `get`/`\l` load the result directly.

## Tests

```sh
uv run --with pyarrow tests/make_fixtures.py   # interop fixtures (13-16)
l tests/test_parquet.q                         # 22 assertions, repo root
uv run --with pyarrow tests/check_l_written.py # pyarrow reads L's output

L_BIN=/path/to/l sh tests/run_all.sh           # the whole deep suite
```

`tests/run_all.sh` drives the full suite (~6 min): the baseline above,
then a seeded randomized round-trip matrix (`tests/matrix.py`: 16
type-kinds × lengths 0/1/7/4096/1M × null densities 0/5%/50%/100% ×
row-group sizes × none/snappy/zstd/lz4 — ~970 L-side exact-value
assertions, then pyarrow re-reads every file L wrote back and compares
~15M values bitwise), Parquet-format corners (data page v1+v2,
DELTA_BINARY_PACKED / DELTA_BYTE_ARRAY / DELTA_LENGTH_BYTE_ARRAY,
boolean RLE, INT96, gzip/brotli, ragged row groups, statistics off,
`'nyi` rejects for time64/decimal/binary/nested), hostile edges
(sentinel collisions, ±inf/-0.0/NaN, pre-1970 and post-2262 instants,
unicode + 10KB symbols, 5000-column tables), an adversarial harness
(`tests/adversarial.py`: 39 corrupted/truncated/non-Parquet files, each
in a fresh L subprocess — a SIGSEGV anywhere is a failure), three
seeded "shake" passes over the multi-row-group 1M cases (parallel
decode races), an L-side edge suite, and a 500-iteration
read/write/stream/error leak loop with an RSS growth bound.
`L_STRESS=1` expands the matrix and adds a >2^31-row rejection case.

## Caveats (by design, asserted by the suite)

- **Sentinel collision**: a Parquet file can hold VALID values equal to
  L's null bit patterns — i64 `-2^63`, i32 `-2^31`, i16 `-2^15`, f64
  NaN payloads.  `pq_read` cannot distinguish them from nulls: they
  arrive as `0Nj`/`0Ni`/`0Nh`/`0n`, and a subsequent `pq_write` emits
  i64/i32/f64 ones as Parquet nulls (i16 keeps the bit pattern).
- **Unrepresentable instants become null, never wrong values**:
  ns-timestamps before ~1707-09 (below `i64::MIN + NS2000`), non-ns
  timestamps whose ns form overflows i64, KP after ~2262 and KD past
  `i32::MAX - 10957` on write, and out-of-range KZ datetimes.
- Boolean/byte/real/symbol columns have no null concept at this
  boundary: Parquet nulls read as `0b`/`0x00`/`0Ne`(NaN)/empty symbol,
  and write back as valid `false`/`0`/NaN/`""` (see the type table).
- Int8 columns read into KG as raw two's-complement bits (echoed back
  as UInt8); empty string and null string both map to the empty symbol.
- Unsupported logical types (List/Struct/Map, Time64, Decimal, Binary,
  FixedSizeBinary) reject with `'nyi`; gzip/brotli/lz4/snappy/zstd all
  decode.  Files claiming more than 2^31 rows are refused by both
  `pq_read` and `pq_stream` rather than truncated.
