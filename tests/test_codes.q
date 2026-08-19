/ test_codes.q — symbol columns as (dictionary; codes).
/ `pq_read`/`pq_rg` with `codes:1b` answer a column DICT whose symbol
/ columns are the 2-list (D;C); the identity every case asserts is
/ D[C] ~ the plain KS read of the same file.
/ Run from the repo root: l tests/test_codes.q

p:"target/release/libl_parquet"
/ 2: arity: kdb writes `1`, some L builds want `1i` — try both so the
/ suite runs unchanged on either host.
LD:{[f] .[{hsym[`$y] 2: (x;1)};(f;p);{[f;e] hsym[`$p] 2: (f;1i)}[f]]}
pr:LD`pq_read
pw:LD`pq_write
pg:LD`pq_rg
pm:LD`pq_meta

pass:0; fail:0
T:{[nm;ok] $[ok;pass+:1;fail+:1]; show $[ok;"  PASS ";"  FAIL "],nm}
E:{[f;x] @[f;x;{x}]}
tmp:getenv`PQ_TMP
d:$[0=count tmp;"/tmp/pq_deep";tmp],"/codes/"
mk:system "mkdir -p ",d
F:{[n] `$":",d,n,".parquet"}
CO:(enlist`codes)!enlist 1b

/ pair[c] = D[C] for a paired column, or the column itself when the
/ read fell back to plain symbols.
pair:{[v;c] x:v c; $[(0=type x)and 2=count x; (first x) last x; x]}
paired:{[v;c] x:v c; (0=type x)and 2=count x}

/ ── 1. identity on every symbol shape ────────────────────────────────
n:3000
mk1:{[nm;t;o] z:$[0=count o;pw (t;F nm);pw (t;F nm;o)];
  u:pr F nm; v:pr (F nm;();CO);
  T[nm," dict result";99h=type v];
  T[nm," identity";all {[u;v;c] (u c)~pair[v;c]}[u;v] each cols[u] where 11h=type each value flip u];
  T[nm," non-symbols untouched";all {[u;v;c] (u c)~v c}[u;v] each cols[u] where not 11h=type each value flip u];
  v}
v:mk1["plain";([]s:n#`aa`bb`cc;j:n#1 2 3);()]
T["plain: paired";paired[v;`s]]
T["plain: codes are bytes";4h=type last v`s]
T["plain: D is symbols";11h=type first v`s]
v:mk1["nulls";([]s:n#`aa``cc`;j:til n);()]
T["nulls: empty symbol in D";(`)in first v`s]
v:mk1["empties";([]s:n#`aa`bb`;k:n#0 1 2);()]
T["empties: empty symbol in D";(`)in first v`s]
v:mk1["onesym";([]s:n#`only);()]
T["onesym: D has one entry";1=count first v`s]
v:mk1["rgs";([]s:n#`aa`bb`cc`dd;j:til n);(enlist`rg)!enlist 250]
T["rgs: still one union";4=count first v`s]
/ 300 distinct still fits a byte? no — 300 > 256, so the codes widen
v:mk1["w300";([]s:`$"s",/:string 300#til 300);()]
T["300 entries -> short codes";5h=type last v`s]
T["300 entries -> D of 300";300=count first v`s]

/ ── 2. fallback: a PLAIN symbol column comes back as symbols ─────────
t:([]s:n#`aa`bb`cc;j:til n)
z:pw (t;F"plainenc";(enlist`dict)!enlist 0b)
v:pr (F"plainenc";();CO)
T["dict=0b file falls back to KS";not paired[v;`s]]
T["dict=0b file still correct";t[`s]~v`s]
T["pq_meta enc says so";0b~first (pm F"plainenc")`enc]

/ ── 3. high cardinality: the writer's own dictionary overflows ───────
hc:([]s:`$"k",/:string til 200000)
z:pw (hc;F"hicard")
u:pr F"hicard"
v:pr (F"hicard";();CO)
T["200k distinct: identity";u[`s]~pair[v;`s]]

/ ── 4. multi-file, differing dictionaries ────────────────────────────
z:pw (([]s:n#`aa`bb;j:til n);F"m0")
z:pw (([]s:n#`cc`dd`aa;j:til n);F"m1")
mf:(F"m0";F"m1")
u:pr mf
v:pr (mf;();CO)
T["multi-file: dict result";99h=type v]
T["multi-file: identity";u[`s]~pair[v;`s]]
T["multi-file: union is first-seen";(first v`s)~`aa`bb`cc`dd]
/ one file dictionary-encoded, the other not: the window falls back
z:pw (([]s:n#`ee`ff;j:til n);F"m2";(enlist`dict)!enlist 0b)
v:pr ((F"m0";F"m2");();CO)
T["mixed dict/plain window falls back";not paired[v;`s]]
T["mixed window still correct";(pr (F"m0";F"m2"))[`s]~v`s]

/ ── 5. pq_rg takes the opts as a 5th argument ────────────────────────
z:pw (([]s:n#`aa`bb`cc;j:til n);F"win";(enlist`rg)!enlist 500)
u:pg (F"win";();1;3)
v:pg (F"win";();1;3;CO)
T["pq_rg window: dict result";99h=type v]
T["pq_rg window: identity";u[`s]~pair[v;`s]]
T["pq_rg window: rows";1000=count last v`s]
/ `sym is the same option under the name the core side uses
v:pg (F"win";();1;3;(enlist`sym)!enlist 1b)
T["`sym is `codes";u[`s]~pair[v;`s]]

/ ── 6. every older shape still means what it meant ───────────────────
T["1-arg read still a table";98h=type pr F"win"]
T["2-arg read still a table";98h=type pr (F"win";())]
T["projection still a table";98h=type pr (F"win";enlist`s)]
T["4-arg rg still a table";98h=type pg (F"win";();0;2)]
T["codes:0b is a table";98h=type pr (F"win";();(enlist`codes)!enlist 0b)]

/ ── 7. opts errors name the key ──────────────────────────────────────
e:E[pr;(F"win";();(enlist`nope)!enlist 1b)]
T["unknown read opt";(10=type e)and e like "*nope*"]
e:E[pr;(F"win";();(enlist`codes)!enlist 1)]
T["codes wants a boolean";(10=type e)and e like "*codes*"]
e:E[pr;(F"win";();`notadict)]
T["read opts must be a dict";(10=type e)and e like "*dict*"]
e:E[pg;(F"win";();0;2;42)]
T["rg opts must be a dict";(10=type e)and e like "*dict*"]

show "CODES: ",string[pass]," passed, ",string[fail]," failed"
\\
