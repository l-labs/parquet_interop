/ test_w1.q — the read-side surface added in W1: column projection,
/ pq_meta, pq_rg row-group windows, multi-file reads, and dictionary
/ vs plain symbol decoding.  Needs the pyarrow fixtures:
/   uv run --with pyarrow tests/make_fixtures.py
/ Run from the repo root: l tests/test_w1.q

p:"target/release/libl_parquet"
/ 2: arity: kdb writes `1`, some L builds want `1i` — try both so the
/ suite runs unchanged on either host.
LD:{[f] .[{hsym[`$y] 2: (x;1)};(f;p);{[f;e] hsym[`$p] 2: (f;1i)}[f]]}
pr:LD`pq_read
pw:LD`pq_write
pm:LD`pq_meta
pg:LD`pq_rg

pass:0; fail:0
T:{[nm;ok] $[ok;pass+:1;fail+:1]; show $[ok;"  PASS ";"  FAIL "],nm}
E:{[f;x] @[f;x;{x}]}

f:`:/tmp/pq_py_mixed.parquet     / 20000 rows, 4 row groups, 4 columns
t:pr f

/ ── pq_meta: shape, footer facts, statistics ──
show "-- pq_meta --"
m:pm f
T["meta keys";(`files`cols`types`rows`rg`bytes`stats`ubytes`enc)~key m]
T["meta echoes the file symbol";(enlist f)~m`files]
T["meta cols";`sd`sp`sf`v~m`cols]
T["meta types";"sssj"~m`types]
T["meta types = host meta";(exec t from meta t)~m`types]
T["meta rows";20000j~m`rows]
T["meta rg";(enlist 5000 5000 5000 5000j)~m`rg]
T["meta rg sums to rows";(m`rows)=sum raze m`rg]
T["meta bytes rows x cols";(4 4)~(count first m`bytes;count first first m`bytes)]
T["meta bytes all positive";all 0<raze raze m`bytes]
T["meta ubytes rows x cols";(4 4)~(count first m`ubytes;count first first m`ubytes)]
T["meta ubytes at least bytes";all (raze raze m`ubytes)>=raze raze m`bytes]
T["meta enc marks the dictionary columns";1010b~m`enc]
T["meta stats keys";(`min`max`null)~key first m`stats]
jst:(m[`stats])[3]
T["stats long min";(0 5000 10000 15000j)~jst`min]
T["stats long max";(4999 9999 14999 19999j)~jst`max]
T["stats null counts";(4#0j)~jst`null]
sst:(m[`stats])[0]
T["stats symbol min";(4#`s0)~sst`min]
T["stats symbol max";(4#`s9)~sst`max]

/ ── projection ──
show "-- projection --"
T["projection keeps requested order";`sf`v~cols pr (f;`sf`v)]
T["projection reversed";`v`sd~cols pr (f;`v`sd)]
T["projection single column";(enlist `sd)~cols pr (f;enlist `sd)]
T["projected values match full read";(t`sd)~(pr (f;enlist `sd))`sd]
T["projected long column";(t`v)~(pr (f;enlist `v))`v]
T["empty list = all columns";t~pr (f;())]
T["backtick = all columns";t~pr (f;`)]
T["empty symbol vector = all";t~pr (f;0#`)]
T["bare symbol atom = whole file";t~pr f]
e:E[pr;(f;`sd`nope)]
T["unknown column names it";(10=type e)and e like "*no column nope*"]
e:E[pr;(f;`sd`sd)]
T["duplicate column rejects";(10=type e)and e like "*duplicate column*"]
T["(hsym;plain sym) is one column";(enlist `sd)~cols pr (f;`sd)]
T["(hsym;plain sym) values";(t`sd)~(pr (f;`sd))`sd]

/ ── pq_rg: global row-group windows ──
show "-- pq_rg --"
T["rg first group";(5000#t)~pg (f;();0;1)]
T["rg last group";(-5000#t)~pg (f;();3;4)]
T["rg middle group";(5000#5000_t)~pg (f;();1;2)]
T["rg two-group window";(10000#5000_t)~pg (f;();1;3)]
T["rg whole file = read";t~pg (f;();0;4)]
T["rg empty window";0=count pg (f;();2;2)]
T["rg empty window keeps columns";(cols t)~cols pg (f;();2;2)]
T["rg with projection";`v`sd~cols pg (f;`v`sd;0;1)]
T["rg projected values";(5000#t`v)~(pg (f;enlist `v;0;1))`v]
e:E[pg;(f;();0;5)]; T["rg hi past end";(10=type e)and"pq_rg"~5#e]
e:E[pg;(f;();-1;2)]
T["rg negative lo";(10=type e)and e like "*-1,*"]
e:E[pg;(f;();-1i;2)]
T["rg negative int lo keeps its sign";(10=type e)and e like "*-1,*"]
e:E[pg;(f;();-1h;2)]
T["rg negative short lo keeps its sign";(10=type e)and e like "*-1,*"]
e:E[pg;(f;();3;1)]; T["rg hi below lo";10=type e]
e:E[pg;(f;();0)];   T["rg wrong arity";10=type e]

/ ── multi-file ──
show "-- multi-file --"
f0:`:/tmp/pq_multi/part0.parquet
f1:`:/tmp/pq_multi/part1.parquet
f2:`:/tmp/pq_multi/part2.parquet
mf:f0,f1,f2
a:pr f0; b:pr f1; c:pr f2
T["multi via symbol vector";(a,b,c)~pr mf]
T["two hsym files stay files";(a,b)~pr (f0;f1)]
T["multi via (files;())";(a,b,c)~pr (mf;())]
T["multi projection";((a,b,c)`v)~(pr (mf;enlist `v))`v]
T["multi projection order";`v`sym~cols pr (mf;`v`sym)]
mm:pm mf
T["multi meta echoes files";mf~mm`files]
T["multi meta rows";12000j~mm`rows]
T["multi meta rg per file";3=count mm`rg]
T["multi meta global rg count";12=count raze mm`rg]
T["multi meta bytes per file";3=count mm`bytes]
T["multi meta stats span all rg";12=count (first mm`stats)`min]
T["multi rg global 0";(1000#a)~pg (mf;();0;1)]
T["multi rg global 4";(1000#b)~pg (mf;();4;5)]
T["multi rg crosses a file";((-1000#a),1000#b)~pg (mf;();3;5)]
T["multi rg whole set = read";(a,b,c)~pg (mf;();0;12)]
e:E[pr;(mf,`:/tmp/pq_multi/bad.parquet;())]
T["schema mismatch rejects";(10=type e)and e like "*schema*"]
e:E[pm;mf,`:/tmp/pq_multi/bad.parquet]
T["schema mismatch names the file";(10=type e)and e like "*bad.parquet*"]
e:E[pr;(0#`;())]; T["zero files rejects";(10=type e)and e like "*no files*"]
e:E[pm;0#`];      T["meta zero files rejects";10=type e]
e:E[pr;(f;42)];   T["non-symbol column list rejects";10=type e]
e:E[pr;(42;())];  T["non-symbol file rejects";10=type e]

/ ── symbols: dictionary vs plain, and mixed pages in one file ──
show "-- symbols --"
d:pr `:/tmp/pq_py_same_dict.parquet
pl:pr `:/tmp/pq_py_same_plain.parquet
T["dictionary and plain files agree";d~pl]
T["dictionary symbols are symbols";11=type d`s]
T["dictionary values";(`$"k",/:string (til 5000) mod 13)~d`s]
T["dictionary-encoded column";(`$"s",/:string (til 20000) mod 50)~t`sd]
T["plain-encoded column";(`$"p",/:string (til 20000) mod 37)~t`sp]
T["dictionary-overflow column distinct";20000=count distinct t`sf]
T["dictionary-overflow first/last";(`u000000;`u019999)~(first t`sf;last t`sf)]
T["long column beside them";("j"$til 20000)~t`v]

/ ── nested columns: 'nyi only when actually requested ──
show "-- nested --"
nf:`:/tmp/pq_py_mixnest.parquet
T["projection reads past a nested column";(1 2 3j)~(pr (nf;enlist `v))`v]
e:E[pr;(nf;enlist `lst)]; T["nested column projected raises nyi";"nyi"~3#e]
e:E[pr;nf];               T["nested column all-columns raises nyi";"nyi"~3#e]
e:E[pm;nf];               T["pq_meta on a nested column raises nyi";"nyi"~3#e]
T["nyi names the column";e like "*lst*"]
/ A STRUCT is two column chunks, so every leaf after it is shifted by
/ one: `v` is root 1 but leaf 2.  Reading root indices as leaf indices
/ decoded `s.b` into `v` — silently, with no error anywhere.
sf1:`:/tmp/pq_py_structfirst.parquet
T["flat column past a struct";(7*"j"$til 5000)~(pr (sf1;enlist `v))`v]
T["symbol column past a struct";
  (`$"x",/:string (til 5000) mod 13)~(pr (sf1;enlist `w))`w]
cw:pr (sf1;(enlist `w);(enlist`codes)!enlist 1b)
T["codes past a struct";
  (`$"x",/:string (til 5000) mod 13)~(first cw`w)(last cw`w)]
T["two flat columns past a struct";
  (7*"j"$til 5000)~(pr (sf1;`v`w))`v]
e:E[pm;sf1];  T["pq_meta on a struct column raises nyi";"nyi"~3#e]
e:E[pr;sf1];  T["struct all-columns raises nyi";"nyi"~3#e]

/ ── two files, two timestamp UNITS, one KP column ──
/ Both files hold the SAME two instants; a reader that takes one
/ file's unit scale and applies it to the other reads 1970 for 2023.
show "-- mixed units --"
un:`:/tmp/pq_py_unit_ns.parquet
uu:`:/tmp/pq_py_unit_us.parquet
tn:pr un
tu:pr uu
T["us file alone matches the ns one";(tn`ts)~tu`ts]
T["ns then us";((tn`ts),tu`ts)~(pr (un;uu))`ts]
T["us then ns";((tu`ts),tn`ts)~(pr (uu;un))`ts]
T["mixed units keep the long column";(1 2 3 4j)~(pr (un;uu))`v]
/ pq_meta scales each file's bounds with ITS OWN unit: a set that took
/ file 0's scale for every file reported 1970 for the second file's
/ instants, which is a pruning guarantee quietly broken.
mn:{[a] ((pm a)`stats)[0]`min}
mx:{[a] ((pm a)`stats)[0]`max}
T["meta ns then us: both min bounds";(mn (un;uu))~2#first tn`ts]
T["meta ns then us: both max bounds";(mx (un;uu))~2#last tn`ts]
T["meta us then ns: both min bounds";(mn (uu;un))~2#first tn`ts]
T["meta us then ns: both max bounds";(mx (uu;un))~2#last tn`ts]
T["meta bounds bracket the rows";
  all (mn (un;uu))<=(mx (un;uu))]

/ ── forged footer row counts: garbage rows and NULL symbol pointers ──
show "-- forged row counts --"
lr:`:/tmp/pq_py_lie_rg.parquet
lb:`:/tmp/pq_py_lie_bal.parquet
e:E[pr;(lr;())]
T["file total vs row groups rejects";(10=type e)and e like "*row counts disagree*"]
e:E[pm;lr]
T["pq_meta rejects the same footer";(10=type e)and e like "*row counts disagree*"]
e:E[pr;(lb;enlist `v)]
T["forged row group rejects (long col)";(10=type e)and e like "*footer says*"]
e:E[pr;(lb;enlist `sd)]
T["forged row group rejects (symbol col)";(10=type e)and e like "*footer says*"]
e:E[pg;(lb;();0;1)]
T["forged row group rejects in a window";(10=type e)and e like "*footer says*"]

show "W1: ",string[pass]," passed, ",string[fail]," failed"
\\
