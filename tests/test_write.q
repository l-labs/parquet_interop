/ test_write.q — writer options, codecs, encoding policy and row-group
/ boundaries.  Everything asserted here is L-side; tests/check_write.py
/ then re-reads the same files with pyarrow and duckdb.
/ Run from the repo root: l tests/test_write.q

p:"target/release/libl_parquet"
/ 2: arity: kdb writes `1`, some L builds want `1i` — try both so the
/ suite runs unchanged on either host.
LD:{[f] .[{hsym[`$y] 2: (x;1)};(f;p);{[f;e] hsym[`$p] 2: (f;1i)}[f]]}
pr:LD`pq_read
pw:LD`pq_write
pm:LD`pq_meta

pass:0; fail:0
T:{[nm;ok] $[ok;pass+:1;fail+:1]; show $[ok;"  PASS ";"  FAIL "],nm}
E:{[f;x] @[f;x;{x}]}
/ Scratch under PQ_TMP so two suites on one machine do not delete each
/ other's files (same override run_all.sh and matrix.py use).
tmp:getenv`PQ_TMP
d:$[0=count tmp;"/tmp/pq_deep";tmp],"/w1write/"
mk:system "mkdir -p ",d
F:{[nm] `$":",d,nm,".parquet"}

/ ── fixture: every exactly-round-tripping type, with nulls ───────────
/ This L build's parser rejects 0Nd/0Np/0Nn inside a vector literal, so
/ the temporal columns are cast from longs: 0N, then 2020.01.01 /
/ 1999.12.31, 2020.01.01D12:00:00.000000001 / 1970.01.01D0, and
/ 0D00:00:01.000000001 / -0D12:00:00.
n:1000
t:([]
  b:n#101b;
  g:"x"$n#0 1 255;
  h:n#0N 1 -32767 32767h;
  i:n#0N 1 -2147483647i;
  j:n#0N 1 -9223372036854775806;
  f:n#0n 1.5 -2.5 1e308;
  s:n#`aa`bb``ccddeeff;
  dt:"d"$n#0N 7305 -1;
  ts:"p"$n#0N 631195200000000001 -946684800000000000;
  sp:"n"$n#0N 1000000001 -43200000000000)

/ ── 1. codecs: round trip through pq_read, one file each ─────────────
cs:`none`zstd`snappy`lz4`gzip
{[c] o:(enlist`codec)!enlist c;
  f:F["codec_",string c];
  z:pw (t;f;o);
  T["codec ",string[c]," round trip";t~pr f]} each cs;

/ default (no opts at all) must equal the explicit `none codec
z:pw (t;F"codec_default")
T["default codec round trip";t~pr F"codec_default"]
T["default == none bytes";
  (hcount F"codec_default")=hcount F"codec_none"]

/ ── 2. levels ────────────────────────────────────────────────────────
z:pw (t;F"zstd22";(`codec`level)!(`zstd;22))
T["zstd level 22";t~pr F"zstd22"]
z:pw (t;F"gzip9";(`codec`level)!(`gzip;9))
T["gzip level 9";t~pr F"gzip9"]
T["zstd 22 <= zstd 1";(hcount F"zstd22")<=hcount F"codec_zstd"]
/ a level a codec cannot use is ignored, not an error
z:pw (t;F"snappy_lvl";(`codec`level)!(`snappy;7))
T["level ignored by snappy";t~pr F"snappy_lvl"]

/ ── 3. rg / dict / stats keys ────────────────────────────────────────
z:pw (t;F"rg100";(enlist`rg)!enlist 100)
T["rg=100 round trip";t~pr F"rg100"]
z:pw (t;F"nodict";(enlist`dict)!enlist 0b)
T["dict=0b round trip";t~pr F"nodict"]
z:pw (t;F"nostats";(enlist`stats)!enlist 0b)
T["stats=0b round trip";t~pr F"nostats"]
z:pw (t;F"allopts";`codec`level`rg`dict`stats!(`zstd;5;250;1b;1b))
T["all five opts";t~pr F"allopts"]

/ ── 4. opts errors: unknown key, bad types, bad values ───────────────
B:{[o] E[pw;(t;F"err";o)]}
e:B[(enlist`bogus)!enlist 1]
T["unknown key names it";(10=type e)and e like "*bogus*"]
e:B[(enlist`codec)!enlist 1]
T["codec wants a symbol";(10=type e)and e like "*codec*"]
e:B[(enlist`codec)!enlist "zstd"]
T["heap opt value names its type";(10=type e)and e like "*type 10*"]
e:B[(enlist`codec)!enlist`bz2]
T["unknown codec names it";(10=type e)and e like "*bz2*"]
e:B[(enlist`level)!enlist`x]
T["level wants a long";(10=type e)and e like "*level*"]
e:B[(`codec`level)!(`zstd;99)]
T["zstd level out of range";(10=type e)and e like "*level*"]
e:B[(`codec`level)!(`gzip;99)]
T["gzip level out of range";(10=type e)and e like "*level*"]
e:B[(`codec`level)!(`zstd;4294967297)]
T["level too wide for the codec";(10=type e)and e like "*level*"]
e:B[(`codec`level)!(`gzip;-1)]
T["negative level rejected";(10=type e)and e like "*level*"]
e:B[(enlist`rg)!enlist 0]
T["rg must be > 0";(10=type e)and e like "*rg*"]
e:B[(enlist`rg)!enlist`x]
T["rg wants a long";(10=type e)and e like "*rg*"]
e:B[(enlist`dict)!enlist 1]
T["dict wants a boolean";(10=type e)and e like "*dict*"]
e:B[(enlist`stats)!enlist 2.5]
T["stats wants a boolean";(10=type e)and e like "*stats*"]
e:B[`notadict]
T["opts must be a dict";(10=type e)and e like "*dict*"]
e:B[42]
T["opts atom rejected";(10=type e)and"pq_write"~8#e]
/ typed value vectors are a dict too: (`dict`stats)!01b
z:pw (t;F"typedvals";`dict`stats!01b)
T["typed boolean value vector";t~pr F"typedvals"]
z:pw (t;F"typedlongs";`rg`level!500 3)
T["typed long value vector";t~pr F"typedlongs"]
/ an empty dict is simply all-defaults
z:pw (t;F"emptyopts";(`$())!())
T["empty opts dict";t~pr F"emptyopts"]

/ ── 4b. a failed write leaves nothing behind ─────────────────────────
/ The writer builds beside the target and renames, so an error must
/ leave neither a truncated .parquet nor a .tmp: renaming onto a
/ DIRECTORY is the one failure that happens after the bytes are
/ written, which is exactly the path the scratch file exists for.
LIT:{[] (key `$":",-1_d) where (key `$":",-1_d) like "*.tmp*"}
mk:system "mkdir -p ",d,"isdir.parquet"
e:E[pw;(t;F"isdir")]
T["rename onto a directory errors";(10=type e)and"pq_write"~8#e]
T["failed write leaves no scratch";0=count LIT[]]
T["failed write leaves the directory";`isdir.parquet in key `$":",-1_d]
e:E[pw;(t;`$":/no/such/dir/pq_w1.parquet")]
T["unwritable dir errors";(10=type e)and"pq_write"~8#e]
T["unwritable dir leaves no scratch";0=count LIT[]]
/ a `nyi column is rejected before the target is touched, so an
/ existing good file at that path survives the failed write intact
z:pw (t;F"survive")
bad:([]a:1 2 3;c:("ab";"cd";"ef"))
e:E[pw;(bad;F"survive")]
T["nyi column errors";(10=type e)and e like "*nyi*"]
T["failed write keeps the old file";t~pr F"survive"]
T["nyi write leaves no scratch";0=count LIT[]]

/ ── 4c. overwriting an existing file ─────────────────────────────────
/ The target is unlinked before the rename (ext4 flushes the whole new
/ file inside rename(2) when it lands on an existing inode — 2.2 s
/ becomes 5+ s on a 2.8 GB overwrite), so this also proves the replace
/ still replaces.
ov1:([]x:til 1000)
ov2:([]x:1000+til 1000;y:1000#`p`q)
z:pw (ov1;F"over"); T["overwrite: first write";ov1~pr F"over"]
z:pw (ov2;F"over"); T["overwrite: second write replaces";ov2~pr F"over"]
z:pw (ov2;F"over";(enlist`codec)!enlist`zstd)
T["overwrite: third, different codec";ov2~pr F"over"]
T["overwrite leaves no scratch";0=count LIT[]]

/ ── 5. encoding policy fixtures (checked in check_write.py) ──────────
/ srt sorted -> DELTA; lo few values -> DICT; hi spread -> PLAIN;
/ sym symbols -> DICT; fl floats -> PLAIN; bo booleans -> RLE.
m:100000
enc:([]
  srt:til m;
  lo:m#0 3 7 2 9 1 5;
  hi:(7919*til m) mod 100003;
  sym:m#`aa`bb`cc;
  fl:0.5+til m;
  bo:m#110b)                                   / bool     -> RLE
z:pw (enc;F"enc")
T["encoding fixture round trip";enc~pr F"enc"]
z:pw (enc;F"enc_nodict";(enlist`dict)!enlist 0b)
T["encoding fixture, dict off";enc~pr F"enc_nodict"]

/ ── 5b. statistics reach a reader as EXACT bounds ────────────────────
/ W2's row-group pruning reads pq_meta`stats, which only reports a
/ bound the footer marks exact.  Every column type must therefore come
/ back non-null per row group with stats on, and null with stats off.
sn:30000
stt:([]j:til sn;
  tp:"p"$(til sn)*1000000;
  fl:0.5+til sn;
  sy:sn#`aa`bb`cc;
  ii:"i"$sn#0 3 7 2 9 1 5;
  dd:"d"$sn#0 7305 -1)
z:pw (stt;F"stats_on";(enlist`rg)!enlist 10000)
sm:(pm F"stats_on")`stats
T["stats: three row groups";3=count sm[0]`min]
T["stats: no null min";not any raze {null x`min} each sm]
T["stats: no null max";not any raze {null x`max} each sm]
T["stats: null counts are 0";0=sum raze {x`null} each sm]
T["stats: sorted long min/max exact";
  (0 10000 20000~sm[0]`min)and 9999 19999 29999~sm[0]`max]
T["stats: symbol min/max exact";(`aa`aa`aa~sm[3]`min)and`cc`cc`cc~sm[3]`max]
z:pw (stt;F"stats_off";(`stats`rg)!(0b;10000))
sf:(pm F"stats_off")`stats
T["stats=0b: min all null";all raze {null x`min} each sf]
T["stats=0b: null counts null";all raze {null x`null} each sf]

/ ── 6. row-group boundaries ──────────────────────────────────────────
b1:([]a:til 1)
z:pw (b1;F"rows1"); T["1 row";b1~pr F"rows1"]
b0:([]a:`long$())
z:pw (b0;F"rows0"); T["0 rows";b0~pr F"rows0"]
b1m:([]a:til 1048576;s:1048576#`p`q)
z:pw (b1m;F"rows1m"); T["exactly 1M rows";b1m~pr F"rows1m"]
b2m:([]a:til 2097153;s:2097153#`p`q)
z:pw (b2m;F"rows2m1"); T["2M+1 rows";b2m~pr F"rows2m1"]
/ a row-group size that does not divide the row count
z:pw (b2m;F"rows2m1_rg";(enlist`rg)!enlist 700000)
T["2M+1 rows, rg=700000";b2m~pr F"rows2m1_rg"]

/ ── 7. 5000 columns ──────────────────────────────────────────────────
nc:5000
w:flip (`$"c",/:string til nc)!(til nc)+\:til 4
z:pw (w;F"wide5000")
T["5000 columns";w~pr F"wide5000"]
z:pw (w;F"wide5000z";(`codec`rg)!(`zstd;2))
T["5000 columns, zstd, rg=2";w~pr F"wide5000z"]

/ ── 8. every codec on the wide/deep shapes readers care about ────────
{[c] f:F["big_",string c];
  z:pw (b1m;f;(enlist`codec)!enlist c);
  T["1M rows ",string[c]," round trip";b1m~pr f]} each cs;

T["no scratch file survived the suite";0=count LIT[]]

show "WRITE-OPTS: ",string[pass]," passed, ",string[fail]," failed"
\\
