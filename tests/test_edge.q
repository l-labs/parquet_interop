/ test_edge.q — L-side edge cases: argument validation, error-message
/ shape, sentinel round-trips, overflow-to-null policy, wide tables,
/ KZ write-only mapping, path handling, streaming corners.
/ Run from the repo root: l tests/test_edge.q

p:"target/release/libl_parquet"
pr:hsym[`$p] 2: (`pq_read; 1)
pw:hsym[`$p] 2: (`pq_write; 1)
ps:hsym[`$p] 2: (`pq_stream; 1)

pass:0; fail:0
T:{[nm;ok] $[ok;pass+:1;fail+:1]; show $[ok;"  PASS ";"  FAIL "],nm}
E:{[f;x] @[f;x;{x}]}

/ ── argument validation: every wrong shape must krr, prefixed ──
e:E[pw;42]; T["pw atom arg";(10=type e)and"pq_write"~8#e]
e:E[pw;(1 2 3;`:/tmp/pq_edge.parquet)]
T["pw non-table";(10=type e)and"pq_write"~8#e]
t0:([]a:1 2 3i;b:1.5 2.5 3.5)
e:E[pw;(t0;42)]; T["pw non-sym path";(10=type e)and"pq_write"~8#e]
e:E[pw;(t0;`:/tmp/pq_edge.parquet;`extra)]
T["pw 3-list";(10=type e)and"pq_write"~8#e]
e:E[pr;42]; T["pr atom arg";(10=type e)and"pq_read"~7#e]
e:E[pr;`:/tmp/no_such_file_pq.parquet]
T["pr missing file";(10=type e)and"pq_read"~7#e]
e:E[pw;(t0;`$":/no/such/dir/pq_edge.parquet")]
T["pw unwritable dir";(10=type e)and"pq_write"~8#e]
e:E[ps;42]; T["ps atom arg";(10=type e)and"pq_stream"~9#e]
e:E[ps;(`:/tmp/a.parquet;`:/tmp/b;`:/tmp/c)]
T["ps 3-list";(10=type e)and"pq_stream"~9#e]
e:E[ps;(`:/tmp/no_such_file_pq.parquet;`:/tmp/pq_edge_out)]
T["ps missing src";(10=type e)and"pq_stream"~9#e]
kt:@[{1!x};t0;{`nokey}]
$[`nokey~kt;
  T["keyed table (host has no 1!)";1b];
  [e:E[pw;(kt;`:/tmp/pq_edge.parquet)];
   T["pw keyed table rejects";(10=type e)and"pq_write"~8#e]]]

/ ── sentinel + null round-trip identities (write then read) ──
th:([]h:0N 1 -32767 32767 0h)
pw (th;`:/tmp/pq_edge_h.parquet)
T["short null/extremes identity";th~pr `:/tmp/pq_edge_h.parquet]
te:([]e:"e"$0n 1.5 -1.5 0.0)
pw (te;`:/tmp/pq_edge_e.parquet)
T["real null(NaN) identity";te~pr `:/tmp/pq_edge_e.parquet]
tg:([]g:0x00ff7f80)
pw (tg;`:/tmp/pq_edge_g.parquet)
T["byte identity";tg~pr `:/tmp/pq_edge_g.parquet]
tb:([]b:10b)
pw (tb;`:/tmp/pq_edge_b.parquet)
T["bool identity";tb~pr `:/tmp/pq_edge_b.parquet]

/ ── overflow-to-null policy: KP past 2262 / KD past i32 range ──
tp:([]tp:("p"$8300000000000000000j),"p"$1j)
pw (tp;`:/tmp/pq_edge_p.parquet); t2:pr `:/tmp/pq_edge_p.parquet
T["KP >2262 writes null";(first null t2`tp)and not last null t2`tp]
T["KP in-range survives";("p"$1j)~last t2`tp]
td:([]d:("d"$2147480000i),"d"$1i)
pw (td;`:/tmp/pq_edge_d.parquet); t2:pr `:/tmp/pq_edge_d.parquet
T["KD overflow writes null";(first null t2`d)and not last null t2`d]

/ ── KZ datetime is write-only: emits Timestamp[ns], reads as KP ──
tz:([]z:"z"$2020.01.01 2020.01.02 2020.06.15)
pw (tz;`:/tmp/pq_edge_z.parquet); t2:pr `:/tmp/pq_edge_z.parquet
T["KZ reads back as KP";12=type t2`z]
T["KZ whole-day instants exact";("p"$tz`z)~t2`z]

/ ── wide table: 5000 columns, both directions ──
tw:flip (`$"c",'string til 5000)!5000#enlist 1 2 3i
pw (tw;`:/tmp/pq_edge_wide.parquet)
t2:pr `:/tmp/pq_edge_wide.parquet
T["5000-col identity";tw~t2]

/ ── zero-column table: host cannot build one; a no-column WRITE is
/ untestable from q, so assert the host constructor itself refuses ──
zt:@[{flip x!()};0#`;{`nozero}]
T["zero-col table unrepresentable";`nozero~zt]

/ ── paths: spaces, unicode, overwrite, extensionless ──
tsp:([]a:1 2 3i)
pw (tsp;`$":/tmp/pq edge space.parquet")
T["path with space";tsp~pr `$":/tmp/pq edge space.parquet"]
pw (tsp;`$":/tmp/pq_edge_ü.parquet")
T["unicode path";tsp~pr `$":/tmp/pq_edge_ü.parquet"]
pw (t0;`:/tmp/pq_edge_ow.parquet)
pw (tsp;`:/tmp/pq_edge_ow.parquet)
T["overwrite same path";tsp~pr `:/tmp/pq_edge_ow.parquet]
pw (tsp;`:/tmp/pq_edge_noext)
T["extensionless path";tsp~pr `:/tmp/pq_edge_noext]

/ ── column names: unicode + 200 chars ──
tn:flip (`$("日本語";200#"n"))!(1 2 3i;1.5 2.5 3.5)
pw (tn;`:/tmp/pq_edge_names.parquet)
T["unicode+long col names";tn~pr `:/tmp/pq_edge_names.parquet]

/ ── streaming corners ──
te0:([]x:"j"$();y:"f"$();s:0#`)
pw (te0;`:/tmp/pq_edge_e0.parquet)
c:ps (`:/tmp/pq_edge_e0.parquet;`:/tmp/pq_edge_splay0)
T["stream empty table rows";0=c]
tu:([]s:(`$"héllo";`$"世界";`$"🚀");v:1 2 3i)
pw (tu;`:/tmp/pq_edge_u.parquet)
c:ps (`:/tmp/pq_edge_u.parquet;`:/tmp/pq_edge_splayu)
T["stream unicode syms rows";3=c]
T["stream unicode splay read-back";tu~get `:/tmp/pq_edge_splayu]
`:/tmp/pq_edge_afile 0: enlist "occupied"
e:E[ps;(`:/tmp/pq_edge_u.parquet;`:/tmp/pq_edge_afile)]
T["stream dst is a file";(10=type e)and"pq_stream"~9#e]

show "EDGE: ",string[pass]," passed, ",string[fail]," failed"
\\
