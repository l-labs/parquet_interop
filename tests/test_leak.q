/ test_leak.q — leak loop: repeated read + write + stream + error-path
/ iterations with an RSS growth bound.  A wholesale per-iteration leak
/ of even one column (~200KB here) would blow the bound hundreds of
/ times over; allocator retention stays far under it.
/ Run from the repo root: l tests/test_leak.q   (L_STRESS=1 for 3x)

p:"target/release/libl_parquet"
pr:hsym[`$p] 2: (`pq_read; 1)
pw:hsym[`$p] 2: (`pq_write; 1)
ps:hsym[`$p] 2: (`pq_stream; 1)

pass:0; fail:0
T:{[nm;ok] $[ok;pass+:1;fail+:1]; show $[ok;"  PASS ";"  FAIL "],nm}
rss:{"J"$first system "ps -o rss= -p ",string .z.i}

/ fixtures: small (every iteration) + medium (every 25th) + a broken
/ file and a bad argument (error paths must not leak either)
sm:([]a:til 5000;f:5000?1.0;s:5000?`aa`bb`cc`dd;
  tp:2000.01.01D00:00:00.0+1000000000j*til 5000)
pw (sm;`:/tmp/pq_deep_leak_sm.parquet)
md:([]a:"j"$til 50000;f:50000?1.0;s:50000?`x`y`z`w)
pw (md;`:/tmp/pq_deep_leak_md.parquet)
`:/tmp/pq_deep_leak_bad.parquet 0: enlist "this is not a parquet file"

one:{[i]
  t:pr `:/tmp/pq_deep_leak_sm.parquet;
  pw (t;`:/tmp/pq_deep_leak_out.parquet);
  e:@[pr;`:/tmp/pq_deep_leak_bad.parquet;{x}];
  e2:@[pw;42;{x}];
  $[0=i mod 25;
    [tm:pr `:/tmp/pq_deep_leak_md.parquet;
     pw (tm;`:/tmp/pq_deep_leak_out2.parquet);
     n:ps (`:/tmp/pq_deep_leak_md.parquet;`:/tmp/pq_deep_leak_splay)];
    0];}

iters:$["1"~getenv`L_STRESS;1500;500]
t1:pr `:/tmp/pq_deep_leak_sm.parquet
T["leak fixture reads back";sm~t1]
i:0; while[i<60;one i;i+:1]          / warm allocator pools first
r0:rss[]
i:0; while[i<iters;one i;i+:1]
r1:rss[]
show "RSS after warmup: ",string[r0],"KB; after ",
  string[iters]," iterations: ",string[r1],"KB"
T["rss growth < 64MB over loop";65536>r1-r0]
T["values stable after loop";sm~pr `:/tmp/pq_deep_leak_sm.parquet]

show "LEAK: ",string[pass]," passed, ",string[fail]," failed"
\\
