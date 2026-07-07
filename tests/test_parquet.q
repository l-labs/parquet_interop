/ test_parquet.q — Parquet round-trip test suite
/ Run from the repo root after `cargo build --release`:
/   macOS: cp target/release/libl_parquet.dylib target/release/libl_parquet.so
/   l tests/test_parquet.q
/ Interop tests 13-16 need fixtures: uv run --with pyarrow tests/make_fixtures.py

p:"target/release/libl_parquet"
pr:hsym[`$p] 2: (`pq_read; 1)
pw:hsym[`$p] 2: (`pq_write; 1)
ps:hsym[`$p] 2: (`pq_stream; 1)

pass:0; fail:0
T:{[nm;ok] $[ok;pass+:1;fail+:1]; show $[ok;"  PASS ";"  FAIL "],nm}

show "=== Parquet Round-Trip Test Suite ==="
show ""

/ ── 1. Basic types ──
show "-- Basic types --"
t:([]i:1 2 3 4 5i;j:10 20 30 40 50j;f:1.5 2.5 3.5 4.5 5.5;s:`a`b`c`d`e;b:10110b)
pw (t;`:/tmp/pq_basic.parquet); t2:pr `:/tmp/pq_basic.parquet
T["int+long+float+sym+bool";t~t2]

/ ── 2. Short + Byte + Real ──
t:([]h:1 2 3 4 5h;g:0x0102030405;e:1.5 2.5 3.5 4.5 5.5e)
pw (t;`:/tmp/pq_hge.parquet); t2:pr `:/tmp/pq_hge.parquet
T["short+byte+real";t~t2]

/ ── 3. Date ──
d:.z.d - 0 1 2 3 4
t:([]d;v:1 2 3 4 5i)
pw (t;`:/tmp/pq_date.parquet); t2:pr `:/tmp/pq_date.parquet
T["date round-trip";t~t2]

/ ── 4. Nulls (int) ──
show "-- Nulls --"
x:1 2 3 4 5i; x[1]:0N; x[3]:0N
t:([]x;v:1 2 3 4 5i)
pw (t;`:/tmp/pq_nulli.parquet); t2:pr `:/tmp/pq_nulli.parquet
T["int nulls";t~t2]

/ ── 5. Nulls (float) ──
y:1 2 3 4 5.0; y[1]:0n; y[3]:0n
t:([]y;v:1 2 3 4 5i)
pw (t;`:/tmp/pq_nullf.parquet); t2:pr `:/tmp/pq_nullf.parquet
T["float nulls";t~t2]

/ ── 6. All-null int column ──
x:5#0N; t:([]x;v:1 2 3 4 5i)
pw (t;`:/tmp/pq_allnull.parquet); t2:pr `:/tmp/pq_allnull.parquet
T["all-null int column";t~t2]

/ ── 7. Empty table ──
show "-- Edge cases --"
te:([]x:`int$();y:`float$())
pw (te;`:/tmp/pq_empty.parquet); t2:pr `:/tmp/pq_empty.parquet
T["empty table";te~t2]

/ ── 8. Single row ──
ts:([]x:enlist 42i;y:enlist 3.14)
pw (ts;`:/tmp/pq_single.parquet); t2:pr `:/tmp/pq_single.parquet
T["single row";ts~t2]

/ ── 9. Wide table (20 columns) ──
tw:([]c0:5?100i;c1:5?100i;c2:5?100i;c3:5?100i;c4:5?100i;c5:5?100i;c6:5?100i;c7:5?100i;c8:5?100i;c9:5?100i;c10:5?100i;c11:5?100i;c12:5?100i;c13:5?100i;c14:5?100i;c15:5?100i;c16:5?100i;c17:5?100i;c18:5?100i;c19:5?100i)
pw (tw;`:/tmp/pq_wide.parquet); t2:pr `:/tmp/pq_wide.parquet
T["wide (20 cols)";tw~t2]

/ ── 10. Symbols incl. empty symbol (dictionary-encoded on disk) ──
show "-- Symbols --"
t:([]sym:1000?`AAPL`GOOG`MSFT`AMZN`META`TSLA`NVDA`NFLX;v:1000?100.0)
pw (t;`:/tmp/pq_sym.parquet); t2:pr `:/tmp/pq_sym.parquet
T["1K symbols";t~t2]
t:([]s:`a``c;v:1 2 3i)
pw (t;`:/tmp/pq_esym.parquet); t2:pr `:/tmp/pq_esym.parquet
T["empty symbol";t~t2]

/ ── 11. Large table (1M) ──
show "-- Large --"
t:([]sym:1000000?`AAPL`GOOG`MSFT;p:1000000?100.0;v:1000000?1000i)
pw (t;`:/tmp/pq_1m.parquet); t2:pr `:/tmp/pq_1m.parquet
T["1M round-trip";t~t2]

/ ── 12. Timestamp (KP) + Timespan (KN) — Timestamp[ns]/Duration[ns] ──
show "-- Temporal (KP/KN) --"
tp:2000.01.01D00:00:00.0+1000000000j*til 5; tp[2]:"p"$0N
tn:"n"$86400000000000j*1 2 3 4 5; tn[1]:"n"$0N
t:([]tp;tn;v:1 2 3 4 5i)
pw (t;`:/tmp/pq_tsn.parquet); t2:pr `:/tmp/pq_tsn.parquet
T["timestamp+timespan round-trip";t~t2]
T["timestamp stays KP (12)";12=type t2`tp]
T["timespan stays KN (16)";16=type t2`tn]

/ ── 13. Read pyarrow plain (no dictionary, uncompressed) ──
show "-- Interop (pyarrow fixtures) --"
pts:2020.01.01D00:00:00.0+86400000000000j*til 3; pts[2]:"p"$0N
tref:([]a:1 2 3i;b:1.5 2.5 3.5;s:`x`y`z;bl:101b;ts:pts;ni:1 0N 3i;nf:1.5 0n 3.5)
t2:@[pr;`:/tmp/pq_py_plain.parquet;{`err}]
T["pyarrow plain values";tref~t2]

/ ── 14. Read pyarrow dictionary-encoded ──
t2:@[pr;`:/tmp/pq_py_dict.parquet;{`err}]
T["pyarrow dict-encoded values";([]s:`a`b`a`c`b`a;v:1 2 3 4 5 6j)~t2]

/ ── 15. Read pyarrow zstd-compressed ──
t2:@[pr;`:/tmp/pq_py_zstd.parquet;{`err}]
T["pyarrow zstd values";([]a:"j"$til 1000;f:0.5+til 1000)~t2]

/ ── 16. Nested column rejects with 'nyi ──
e:@[pr;`:/tmp/pq_py_nested.parquet;{x}]
T["nested column raises nyi";"nyi"~3#e]

/ ── 17. Streaming: multi-row-group → splayed table dir ──
show "-- Streaming --"
n:2500000
t:([]sym:n?`AAPL`GOOG`MSFT;p:n?100.0;v:n?1000i)
pw (t;`:/tmp/pq_stream_src.parquet)
c:ps (`:/tmp/pq_stream_src.parquet;`:/tmp/pq_stream_out)
T["stream row count";c=n]
t2:get `:/tmp/pq_stream_out
T["stream splay read-back";t~t2]

/ ── 18. Write the pyarrow cross-check input (verified by python) ──
lp:2020.01.01D00:00:00.0+86400000000000j*til 3; lp[2]:"p"$0N
ln:"n"$1000000000j*1 2 3; ln[1]:"n"$0N
t:([]a:1 0N 3i;b:1.5 0n 3.5;s:`x`y`z;bl:101b;ts:lp;dn:ln)
pw (t;`:/tmp/pq_l_written.parquet)
T["wrote python cross-check file";1b]

show ""
show "=== Results: ",string[pass]," passed, ",string[fail]," failed ==="
show (-45)!0
\\
