# parquet_interop

Apache Parquet reader/writer/streamer for L, as a shared library loaded via
`2:`. The Parquet format work (all encodings; zstd/snappy/lz4/gzip/brotli
compression) is done by the arrow-rs `parquet` crate. Reads decode row
groups in parallel with bulk buffer copies into K vectors. Writes are a
work queue over `row group × column` chunks across all cores, with **no
block codec by default** — light column encodings (dictionary, DELTA,
RLE) carry the compression, which is both smaller and far cheaper than
zstd-ing plain pages. Measured on the box (EPYC 9454, 48 cores,
100M rows × 8 columns of NYSE TAQ): `pq_write` 2.3 s for a 2.6 GB
uncompressed file and 2.2 s for a 2.3 GB zstd-1 one, against 15.7 s for
the previous writer — and the writer is disk-bound at that point
(`dd` tops out at 1.8 GB/s on the same volume).

## Quickstart

```sh
cargo build --release
# macOS: `2:` appends .so, cargo emits .dylib — give it the name it wants
cp target/release/libl_parquet.dylib target/release/libl_parquet.so
```

```q
.pq.meta:  `:target/release/libl_parquet 2: (`pq_meta;   1)
.pq.read:  `:target/release/libl_parquet 2: (`pq_read;   1)
.pq.rg:    `:target/release/libl_parquet 2: (`pq_rg;     1)
.pq.write: `:target/release/libl_parquet 2: (`pq_write;  1)
.pq.stream:`:target/release/libl_parquet 2: (`pq_stream; 1)

.pq.write (([]sym:`AAPL`GOOG;price:150.5 175.3); `:/tmp/out.parquet)
t:.pq.read `:/tmp/out.parquet
n:.pq.stream (`:/tmp/huge.parquet; `:/tmp/db/t)  / splay, 1 row group DRAM
```

Every export takes ONE argument, so a multi-argument call passes a list
(`.pq.write (t;path)`, never `.pq.write[t;path]`).

## Reading: projection, row-group windows, many files

```q
f:`:/tmp/taq.parquet
files:`:/tmp/db/part00.parquet`:/tmp/db/part01.parquet

.pq.read f                     / whole file, columns in FILE order
.pq.read (f;())                / same — () or ` means every column
.pq.read (f;`sym`size)         / only these chunks are read and decoded,
                               / result columns in the REQUESTED order
.pq.read (files;())            / many files -> one table, one work pool
.pq.rg   (files;`sym`size;0;8) / only global row groups [0,8)
.pq.meta files                 / footers only: no page is decoded
```

`.pq.meta` answers a dict — `` `files`cols`types`rows`rg`bytes`stats`ubytes`enc ``:

| key | value |
|-----|-------|
| `files` | the file symbols, echoed verbatim |
| `cols` | column names, file order |
| `types` | L type chars, as `meta`'s `t` column |
| `rows` | total rows (long atom) |
| `rg` | per file, a long vector of row-group row counts |
| `bytes` | per file, per row group, per column compressed size |
| `stats` | per column, `` `min`max`null!(...) `` indexed by GLOBAL row group |
| `ubytes` | same shape as `bytes`, uncompressed size — the pair a streaming caller budgets from |
| `enc` | per column, 1b when EVERY row group carries a dictionary page for it |

The key order is APPEND-ONLY: `ubytes` and `enc` came after the first
seven, and anything added later goes on the end too, so a caller that
indexes positionally keeps working.

GLOBAL row-group numbering is the concatenation order: file 0's groups,
then file 1's, and so on — the same numbering `.pq.rg` windows into, so
`.pq.meta` plans the loop and `.pq.rg` executes each step.

Files read together must agree POSITIONALLY: same column names in the
same order, each landing on the same L type (`Timestamp(us)` and
`Timestamp(ns)` are both `KP`, so they agree — and each file is scaled
by its OWN unit).  A disagreement raises `pq_read: schema <file>`.

`stats` is reported only where the footer flags a bound EXACT, and a
missing bound reads back as the column's null — a caller can only ever
skip work it is entitled to skip.  One caveat for pruning: writers old
enough to predate the `is_min_value_exact` fields set neither, and the
parquet crate then reports a present bound as exact.  Numeric and
temporal bounds are safe (nothing truncates them); a **symbol min/max
is ADVISORY** unless the file's `created_by` is known to be recent,
because such a writer may have truncated it.

One q wrinkle worth knowing: q collapses a pair of symbol ATOMS into a
symbol VECTOR, so `` (`:f;`c) `` and `` `:a`:b `` are the SAME value and
only one reading can be honoured.  The rule:

> a 2-element symbol vector is `(file; cols)` when its second element
> cannot be a companion path — it is the empty symbol, or the first
> element is an hsym and the second is not.  Anything else is a list of
> files.

So `` (`:f;`c) `` is a one-column read, `` (`:f;`) `` is every column,
and `` `:a`:b `` is two files.  A one-column read of a PLAIN (non-hsym)
path has to spell the general list: `` (f; enlist `c) `` — which is what
a generated caller should always emit anyway.

A forged footer is refused rather than decoded: a file whose own row
count disagrees with the sum of its row groups' is rejected when the
footer is read, and a row group that delivers a different number of
rows than it claimed fails the read (an inflated claim would otherwise
leave uninitialized values — and NULL symbol pointers — in the result).

Worker count is `available_parallelism()`; it does not see the host's
`-s` setting.

## Write options

`pq_write` takes `(table; path)` or `(table; path; opts)`, where `opts` is
a dict of any of:

| key | type | default | meaning |
|-----|------|---------|---------|
| `` `codec `` | symbol | `` `none `` | `none`, `zstd`, `snappy`, `lz4` (LZ4_RAW), `gzip` |
| `` `level `` | long | zstd 1, gzip 6 | codec level; **silently ignored** by `none`, `snappy` and `lz4`, which have no levels (out of range for `zstd`/`gzip` is an error) |
| `` `rg `` | long | 1048576 | rows per row group |
| `` `dict `` | boolean | `1b` | dictionary encoding on. **`0b` also changes the integer rule**: a column that would have been dictionary-encoded takes DELTA instead, and symbols fall back to PLAIN |
| `` `stats `` | boolean | `1b` | chunk min/max/null_count |

An unknown key, a wrongly typed value, or an out-of-range level is an
error naming the key. A failed write leaves nothing behind: the file is
built beside the target as `<path>.<pid>.<n>.tmp` and renamed only once its
footer is on disk, so a reader sees either the previous file or the
finished one — never a prefix of this one. An existing target is
UNLINKED just before the rename: renaming onto a live inode makes ext4
flush the whole new file synchronously inside `rename(2)` (measured on
the box: 2.4 s becomes 4.4 s on a 2.8 GB overwrite), and the
microsecond in between is a window in which a reader sees no file,
never half of one — a crash inside it leaves nothing at `path`, the
finished data still under its `.tmp` name.

```q
.pq.write (t; `:/tmp/out.parquet; (`codec`level)!(`zstd;3))
.pq.write (t; `:/tmp/out.parquet; `rg`stats!(250000;0b))
```

### Encoding policy

The default file has **no block codec**; every column carries a light
Parquet encoding instead, and the result is ordinary Parquet that
pyarrow, duckdb, polars and Spark all read:

| L column | encoding |
|----------|----------|
| symbol (KS) | dictionary + RLE_DICTIONARY, PLAIN once the dictionary outgrows its page |
| int/long/short/byte + date/time/timestamp/timespan | sorted → DELTA_BINARY_PACKED; estimated cardinality < 2^16 → dictionary; otherwise PLAIN |
| float/real (KF/KE) | PLAIN |
| boolean (KB) | RLE |

BYTE_STREAM_SPLIT was measured on the TAQ set and rejected: it takes
zstd-1 from 2.33 GB to 2.25 GB, but zstd-3 from 1.45 GB to 2.25 GB and
gzip from 1.47 GB to 2.22 GB — splitting the byte planes destroys the
whole-value matches a stronger compressor lives on.

The integral choice is made from a strided sample of 8192 values per
column, and the cardinality comes from the sample's COLLISION count
(`C ≈ m² / 2(m − distinct)`), which separates a thousand distinct
values from a million with 8192 probes. On the TAQ set that puts the
sorted `ts` on DELTA (100M distinct), `size` on a dictionary (999
distinct) and `price` on PLAIN (1,000,183 distinct).

## Type mapping (verified by the test suite)

| L type | Parquet/Arrow type | Read | Write | Notes |
|--------|--------------------|------|-------|-------|
| KB bool | Boolean | y | y | null reads as 0b |
| KG byte | UInt8 (Int8 accepted) | y | y | |
| KH short / KI int / KJ long | Int16/32/64 | y | y | 0Ni/0Nj ↔ null |
| KE real / KF float | Float32/64 | y | y | 0n ↔ null (f64) |
| KS symbol | Utf8 (dictionary-encoded on disk) | y | y | read PRESERVES the dictionary (below); written from a pointer-keyed dictionary, never per-row string hashing; null ↔ ` |
| KD date | Date32[day] | y | y | epoch shift 2000 ↔ 1970 |
| KP timestamp | Timestamp[s/ms/us/ns → ns] | y | y | epoch shift, null-preserving; every unit takes the zero-copy path |
| KN timespan | Duration[ns] | y | y | via embedded Arrow schema |
| KZ datetime | Timestamp[ns] | — | y | write-only; reads back as KP |

Nested columns (List/Struct/Map) raise `'nyi` — but only when they are
actually requested: a projection that names only flat columns reads a
file whose other columns are nested (and reads the right leaf: a nested
column ahead of a flat one shifts every column chunk after it).
`pq_stream` writes native splay files (32-byte header + raw payload;
0xFF01 form for symbols) chunk by chunk and patches counts at the end —
`get`/`\l` load the result directly. The directory is built beside
`dst` and renamed over it: a failed stream leaves the previous splay
untouched, and a successful one REPLACES it whole, so no column file
from an earlier schema survives into the new table.

## Symbols keep their dictionary

arrow-rs infers `Utf8` for a byte-array column, which inflates the
on-disk dictionary into one heap string per ROW.  The reader instead
supplies a schema hint re-declaring every string field as
`Dictionary(Int32, Utf8)`, so a symbol column arrives as (dictionary
page, index vector) and each DISTINCT value is interned once — the
intern table is touched O(cardinality) times, not O(rows), and no row
is hashed at all.  Columns genuinely written without dictionary pages,
and files whose schema the hint does not fit, fall back to a plain
decode through a direct-mapped symbol cache; the values are identical
either way (asserted by `tests/test_w1.q`).

## Symbols as (dictionary; codes)

A dictionary-encoded Parquet string column already IS a dictionary and a
vector of indices. `pq_read` and `pq_rg` take an OPTIONAL trailing opts
dict that asks for it in that shape instead of as 100M interned
pointers:

```q
.pq.read (f; ();          (enlist`codes)!enlist 1b)   / whole file
.pq.read (f; `sym`size;   (enlist`codes)!enlist 1b)   / projection
.pq.rg   (f; (); 0; 8;    (enlist`codes)!enlist 1b)   / row-group window
```

| key | type | default | meaning |
|-----|------|---------|---------|
| `` `codes `` (alias `` `sym ``) | boolean | `0b` | symbol columns come back as `(D;C)` |

Unknown key, wrong type, or a non-dict is an error naming the key. Every
older argument shape keeps its old meaning.

**The answer is a column DICT, not a table**, whenever `codes` is set —
a table cannot hold a column that is a 2-list. `flip` it if nothing
paired.

For a symbol column that is dictionary-encoded in EVERY row group of the
window, its value is a 2-list `(D; C)`:

- **D** — the union of the row groups' dictionaries as a KS vector,
  interned once per distinct value, in FIRST-SEEN order (row groups in
  window order, each dictionary in its own order). A dictionary entry
  that no row references stays in D: Parquet dictionaries may carry
  unused entries, and nothing here scans the codes to find out. The
  empty symbol — which is what both a Parquet null and an empty string
  read as — is appended at the end if it is not already present and the
  window's statistics say a null can occur (or do not say).
- **C** — one unsigned index per row into D, as the narrowest L type
  that can INDEX it: KG up to 256 entries, KH up to 32768, else KI. The
  bound is what `D[C]` must stay legal for, not what the bits could
  hold. Codes decode straight into the K payload; a row group's own
  dictionary is remapped to union ids through a lookup table built once
  per dictionary, so no string is touched per row and no pointer is
  written.

`D[C]` is the plain symbol read, exactly — the suite asserts it on every
symbol fixture. A column that is PLAIN in any row group of the window,
or that meets a value its union never saw (a chunk that fell back to
PLAIN mid-way), comes back as an ordinary KS vector instead; `pq_meta`'s
`enc` says in advance which columns can pair.

## Zero-copy columns

A column whose Parquet PHYSICAL type is already the machine type L
wants is decoded by the page reader STRAIGHT into the K vector — no
Arrow array, no allocation, no copy:

| L type | on disk | path |
|--------|---------|------|
| KI, KJ, KE, KF | INT32 / INT64 / FLOAT / DOUBLE | straight into K |
| KD, KT | INT32 (Date32, Time32[ms]) | straight into K, epoch shift fused |
| KP, KN | INT64 (Timestamp[ns], Duration[ns]) | straight into K, epoch shift fused |
| KP of another unit, INT96 | INT64 / INT96 | Arrow (the unit conversion IS a copy) |
| KS | BYTE_ARRAY | Arrow (dictionary page -> codes -> gather) |
| KB, KG, KH | BOOLEAN / INT32 | Arrow (bit-vs-byte, narrowing) |

`read_records` writes only non-null values, packed; one backwards pass
then spreads them onto their definition levels, fills nulls and applies
the epoch shift. The values buffer is a `Vec` over the K range with
capacity exactly the batch size, which is what makes it safe: with no
repetition levels parquet-rs reads at most `min(remaining_records,
remaining_levels)` values per page, so the buffer can never grow, never
reallocate, and never free host memory — a page header that claims more
values than the row group has rows is clamped, and one that claims fewer
is caught by the delivery check (both are in `tests/adversarial.py`).

## Threads

Every parallel phase — footer opens, row-group decode, column encode —
runs on ONE process-wide pool sized `available_parallelism()`, built in
the background after the first call so a one-shot process never waits
for it. Repeat calls are 1.2-2.3x faster than the per-call thread spawn
it replaced (`pq_rg` of one row group: 63.5 -> 16.3 ms; a two-column
read: 54.6 -> 28.5 ms). A forked child (`peach`) and a nested call both
fall back to fresh threads, so neither can wait on a pool that is not
theirs.

Footers are cached by (path, device, inode, size, mtime), so a caller
that walks a file row group by row group parses it once: `pq_meta` over
the same file went from 1.74 ms to 0.084 ms per call.

## Tests

```sh
uv run --with pyarrow tests/make_fixtures.py   # interop fixtures (13-16)
l tests/test_parquet.q                         # 22 assertions, repo root
uv run --with pyarrow tests/check_l_written.py # pyarrow reads L's output
l tests/test_write.q                           # writer options + encodings
uv run --with pyarrow --with duckdb tests/check_write.py

l tests/test_w1.q                              # 72 read-surface asserts
L_BIN=/path/to/l sh tests/run_all.sh           # the whole deep suite
PQ_TMP=/tmp/mine L_BIN=... sh tests/run_all.sh # two suites, one machine
```

`tests/run_all.sh` drives the full suite (~8 min): the baseline above,
then the writer's own suite (`tests/test_write.q` + `tests/check_write.py`:
every option key and its errors, every codec round-tripped through
`pq_read`, pyarrow AND duckdb, the encoding policy read back out of the
file metadata, row-group boundaries at 0/1/1M/2M+1 rows and 5000
columns), then a seeded randomized round-trip matrix (`tests/matrix.py`: 16
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

Source layout is hand-maintained house style — code in columns 1-80,
`//` at column 81 — so the repo ships a `rustfmt.toml` that turns
formatting off outright: `cargo fmt` is a no-op here, and so is an
editor's format-on-save.

## Errors

Every error carries the entry point that raised it as its prefix —
`pq_read: …`, `pq_write: …` — so a trap sees which door failed. The
ones a caller can provoke by argument rather than by file are: an
option error naming the key (see the tables above), `expected symbol
path` / `expected column symbol` for a wrong argument shape, `symbol
utf8` for a symbol whose bytes are not UTF-8, and `nyi: column type …`
for a column with no L type. The rest name the file.

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
