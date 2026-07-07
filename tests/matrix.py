# matrix.py — seeded randomized round-trip matrix + Parquet-format
# corners + hostile edges, for pq_read / pq_write / pq_stream.
#
# Two phases sharing one deterministic case list (same --seed):
#   gen   — writes pyarrow fixtures under /tmp/pq_deep/ AND emits
#           /tmp/pq_deep/driver.q, a self-contained L script whose
#           assertions carry EXACT expected values (full literals for
#           small cases, seeded probe indices + null counts for large).
#           The driver also echo-writes every table L read back to
#           /tmp/pq_deep/echo_*.parquet via pq_write.
#   check — pyarrow re-reads every echo file and asserts schema + exact
#           values (bitwise for floats) under the documented null
#           policy, closing the L→pyarrow direction of the loop.
#
# Usage:  uv run --with pyarrow --with numpy tests/matrix.py gen   [opts]
#         uv run --with pyarrow --with numpy tests/matrix.py check [opts]
# Options: --seed N (default 20260706), --shake (large cases only, for
# repeated race-shaking runs).  L_STRESS=1 expands the case set.

import math
import os
import struct
import sys

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

DIR = "/tmp/pq_deep"
NS2000 = 946_684_800_000_000_000                # ns 1970-01-01 -> 2000-01-01
DAY2000 = 10_957                                # days 1970-01-01 -> 2000-01-01
NI, NJ, NH = -2**31, -2**63, -2**15             # L null sentinels i32/i64/i16
I64MAX = 2**63 - 1
STRESS = os.environ.get("L_STRESS") == "1"

# ── q literal emitters (expected L-side values; None = L null) ──────────


def q_f64(v):
    """One f64 as a q float token (L null and NaN are both 0n)."""
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return "0n"
    if v == math.inf:
        return "0w"
    if v == -math.inf:
        return "-0w"
    r = repr(float(v)).replace("+", "")
    return r if ("." in r or "e" in r or "n" in r) else r + "e0"


def q_ivec(vals, suf, null_scalar):
    """Integer-family vector literal: '1 0N 3i' style, enlist for n==1."""
    tok = ["0N" if v is None else str(v) for v in vals]
    if len(vals) == 0:
        return f'"{suf}"$()' if suf != "x" else "0#0x00"
    if len(vals) == 1:
        return "enlist " + (null_scalar if vals[0] is None else tok[0] + suf)
    return " ".join(tok) + suf


def q_str(s):
    """One string element for a `$(...) list; 1-char strings must be
    enlist'd (a bare char atom in a mixed list crashes the host parser)."""
    e = (s.replace("\\", "\\\\").replace('"', '\\"')
          .replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r"))
    one = len(s.encode()) == 1                   # q strings are BYTES
    return f'enlist "{e}"' if one else f'"{e}"'


def q_symvec(vals):
    """Symbol vector literal from python strings ('' = null symbol)."""
    if len(vals) == 0:
        return "0#`"
    if len(vals) == 1:
        return "enlist `$" + q_str(vals[0]).removeprefix("enlist ")
    return "`$(" + ";".join(q_str(v) for v in vals) + ")"

# ── type kinds: arrow gen + L-read transform + q literal + echo shape ───
#
# Each kind is (arrow_type, gen, lval, qlit, echo, echo_arrow_type):
#   gen(rng, n, dens)   -> python list of values (None = arrow null)
#   lval(v)             -> value L holds after pq_read (None = typed null)
#   qlit(lvals)         -> q literal expression equal to the read column
#   echo(v)             -> value pyarrow must see after L pq_write's it
#                          back ('NAN' marks a VALID NaN slot)
#   echo_arrow_type     -> expected echo file column type (string form)


def mask(rng, vals, dens):
    return [None if rng.random() < dens else v for v in vals]


def g_ints(lo, hi):
    def g(rng, n, dens):
        return mask(rng, rng.integers(lo, hi, n).tolist(), dens)
    return g


def g_f64(rng, n, dens):
    return mask(rng, (rng.standard_normal(n) * 1e6).tolist(), dens)


def g_f32(rng, n, dens):
    v = (rng.standard_normal(n) * 100).astype(np.float32)
    return mask(rng, [float(x) for x in v], dens)


def g_str(unique):
    ab = "abcdefghijklmnopqrstuvwxyzXYZ0189_.-"
    def g(rng, n, dens):
        out = []
        for i in range(n):
            k = int(rng.integers(0, 12))
            s = "".join(ab[j] for j in rng.integers(0, len(ab), k))
            out.append(s + (f"#{i}" if unique else ""))
        return mask(rng, out, dens)
    return g


def g_bool(rng, n, dens):
    return mask(rng, (rng.integers(0, 2, n) != 0).tolist(), dens)


def sym_q(vals):
    return q_symvec(["" if v is None else v for v in vals])


def ts_kind(unit, f):
    """Timestamp[unit] (f = ns per unit): KP with 1970->2000 shift.
    Instants before i64::MIN + NS2000 (~1707-09) cannot be represented
    in KP: the adapter must map them to 0Np, never wrap."""
    lo, hi = (-2**62) // f, (2**62) // f                # safely in ns range

    def rep(v):
        return (v is not None and abs(v * f) <= I64MAX
                and v * f - NS2000 > NJ)         # ns fits AND KP fits
    return (pa.timestamp(unit), g_ints(lo, hi),
            lambda v: v * f - NS2000 if rep(v) else None,
            lambda vs: '"p"$' + q_ivec(vs, "j", "0Nj"),
            lambda v: v * f if rep(v) else None, "timestamp[ns]")


def dur_kind(unit, f):
    """Duration[unit] (f = ns per unit): KN, no epoch shift."""
    lo, hi = (-2**62) // f, (2**62) // f
    return (pa.duration(unit), g_ints(lo, hi),
            lambda v: None if v is None else v * f,
            lambda vs: '"n"$' + q_ivec(vs, "j", "0Nj"),
            lambda v: None if v is None else v * f, "duration[ns]")


KINDS = {
    "bool": (pa.bool_(), g_bool,
             lambda v: bool(v) if v is not None else False,
             lambda vs: ("0#0b" if not vs else
                         ("enlist " if len(vs) == 1 else "")
                         + "".join("1" if v else "0" for v in vs) + "b"),
             lambda v: bool(v) if v is not None else False, "bool"),
    "u8":   (pa.uint8(), g_ints(0, 256),
             lambda v: 0 if v is None else v,
             lambda vs: ("0#0x00" if not vs else
                         ("enlist " if len(vs) == 1 else "")
                         + "0x" + "".join(f"{v:02x}" for v in vs)),
             lambda v: 0 if v is None else v, "uint8"),
    "i8":   (pa.int8(), g_ints(-128, 128),                # bits -> KG byte
             lambda v: 0 if v is None else v & 0xFF,
             lambda vs: ("0#0x00" if not vs else
                         ("enlist " if len(vs) == 1 else "")
                         + "0x" + "".join(f"{v:02x}" for v in vs)),
             lambda v: 0 if v is None else v & 0xFF, "uint8"),
    "i16":  (pa.int16(), g_ints(NH + 1, 2**15),
             lambda v: None if v in (None, NH) else v,     # -32768 IS 0Nh
             lambda vs: q_ivec(vs, "h", "0Nh"),
             lambda v: NH if v in (None, NH) else v,       # sentinel VALUE
             "int16"),
    "i32":  (pa.int32(), g_ints(NI + 1, 2**31),
             lambda v: None if v in (None, NI) else v,
             lambda vs: q_ivec(vs, "i", '"i"$0N'),
             lambda v: None if v in (None, NI) else v, "int32"),
    "i64":  (pa.int64(), g_ints(NJ + 1, 2**63),
             lambda v: None if v in (None, NJ) else v,
             lambda vs: q_ivec(vs, "j", "0Nj"),
             lambda v: None if v in (None, NJ) else v, "int64"),
    "f32":  (pa.float32(), g_f32,
             lambda v: None if v is None or math.isnan(v) else v,
             lambda vs: '"e"$' + (
                 '"f"$()' if not vs else
                 ("enlist " if len(vs) == 1 else "")
                 + " ".join(q_f64(v) for v in vs)),
             lambda v: "NAN" if v is None or math.isnan(v) else v, "float"),
    "f64":  (pa.float64(), g_f64,
             lambda v: None if v is None or math.isnan(v) else v,
             lambda vs: ('"f"$()' if not vs else
                         ("enlist " if len(vs) == 1 else "")
                         + " ".join(q_f64(v) for v in vs)),
             lambda v: (None if v is None or math.isnan(v) else v),
             "double"),
    "utf8": (pa.string(), g_str(True),
             lambda v: "" if v is None else v, sym_q,
             lambda v: "" if v is None else v, "string"),
    "utf8_dict": (pa.dictionary(pa.int32(), pa.string()), g_str(False),
                  lambda v: "" if v is None else v, sym_q,
                  lambda v: "" if v is None else v, "string"),
    "date32": (pa.date32(), g_ints(-40_000, 80_000),
               lambda v: None if v is None else v - DAY2000,
               lambda vs: '"d"$' + q_ivec(vs, "i", '"i"$0N'),
               lambda v: None if v is None else v, "date32[day]"),
    "time32_ms": (pa.time32("ms"), g_ints(0, 86_400_000),
                  lambda v: None if v is None else v,
                  lambda vs: '"t"$' + q_ivec(vs, "i", '"i"$0N'),
                  lambda v: None if v is None else v, "time32[ms]"),
    "ts_s": ts_kind("s", 10**9), "ts_ms": ts_kind("ms", 10**6),
    "ts_us": ts_kind("us", 10**3), "ts_ns": ts_kind("ns", 1),
    "dur_ns": dur_kind("ns", 1), "dur_us": dur_kind("us", 10**3),
}
MATRIX16 = ["bool", "u8", "i16", "i32", "i64", "f32", "f64", "utf8",
            "utf8_dict", "date32", "ts_s", "ts_ms", "ts_us", "ts_ns",
            "dur_ns", "dur_us"]
COMPS = [None, "snappy", "zstd", "lz4"]         # write-side compressions

# ── case model ──────────────────────────────────────────────────────────


class Case:
    """One fixture: columns of (name, kind, values), pyarrow write
    options, and what the driver must assert (values / error prefix)."""

    def __init__(self, name, cols, wkw=None, err=None, echo=True,
                 stream=False, reread=0, custom_q=None, arrays=None,
                 no_lit=False):
        self.name, self.cols, self.wkw = name, cols, wkw or {}
        self.err, self.echo, self.stream = err, echo and not err, stream
        self.reread = reread                     # extra reads (race shake)
        self.custom_q = custom_q or []           # extra raw driver lines
        self.arrays = arrays                     # override pa arrays
        self.no_lit = no_lit                     # custom_q replaces literal

    def path(self):
        return f"{DIR}/{self.name}.parquet"

    def echo_path(self):
        return f"{DIR}/echo_{self.name}.parquet"


def build_cases(seed, shake):
    """The deterministic case list both phases share."""
    rng = np.random.default_rng(seed)
    cases = []

    def add(name, kinds, n, dens, wkw=None, **kw):
        cols = [(f"c{i}", k, KINDS[k][1](rng, n, dens))
                for i, k in enumerate(kinds)]
        cases.append(Case(name, cols, wkw, **kw))

    # 1. matrix — small: every kind x lengths {0,1,7} x densities.
    if not shake:
        for i, k in enumerate(MATRIX16):
            cmp_ = {"compression": COMPS[i % 4]}
            add(f"sm_{k}_n0", [k], 0, 0.0, cmp_)
            add(f"sm_{k}_n1v", [k], 1, 0.0, cmp_)
            add(f"sm_{k}_n1n", [k], 1, 1.0, cmp_)
            add(f"sm_{k}_n7v", [k], 7, 0.0, cmp_)
            add(f"sm_{k}_n7m", [k], 7, 0.5, cmp_)
            add(f"sm_{k}_n7n", [k], 7, 1.0, cmp_)
        # matrix — medium: 4096 rows, tiny 100-row groups, 5% nulls.
        for i, k in enumerate(MATRIX16):
            add(f"md_{k}", [k], 4096, 0.05,
                {"compression": COMPS[(i + 1) % 4], "row_group_size": 100})
        if STRESS:                               # all-null mediums
            for k in MATRIX16:
                add(f"mn_{k}", [k], 4096, 1.0, {"row_group_size": 371})
        # matrix — mixed-column tables (multi-column write threads).
        add("mx_a", ["i32", "i64", "f64", "utf8", "bool", "date32",
                     "ts_us", "dur_ns"], 4096, 0.05,
            {"compression": "zstd", "row_group_size": 512}, stream=True)
        add("mx_b", ["u8", "i16", "f32", "utf8_dict", "ts_s", "ts_ms",
                     "time32_ms"], 4096, 0.05,
            {"data_page_version": "2.0", "compression": "snappy"})

    # 2. matrix — large: 1M rows, many row groups (parallel decode).
    big = MATRIX16 if STRESS else ["i32", "i64", "f64", "utf8",
                                   "utf8_dict", "ts_ns", "bool"]
    for i, k in enumerate(big):
        add(f"lg_{k}", [k], 1_000_000, 0.05,
            {"compression": ["zstd", "snappy"][i % 2],
             "row_group_size": 65_536}, reread=2)
    add("lg_manyrg", ["i64", "f64", "utf8_dict", "ts_ns"], 1_000_000,
        0.05, {"compression": "zstd", "row_group_size": 4096},
        reread=2, stream=True)
    add("lg_slice", ["i64", "f64", "utf8", "ts_ns"], 1_100_000, 0.05,
        {"compression": "zstd", "row_group_size": 65_536},
        stream=True)                             # echo spans 2 row groups
    if shake:
        return cases

    # 3. Parquet-format corners.
    add("cn_v1", ["i32", "i64", "f64", "utf8", "bool", "ts_us"], 3000,
        0.05, {"data_page_version": "1.0", "row_group_size": 500})
    add("cn_v2", ["i32", "i64", "f64", "utf8", "bool", "ts_us"], 3000,
        0.05, {"data_page_version": "2.0", "row_group_size": 500})
    add("cn_delta", ["i32", "i64"], 4000, 0.05,
        {"version": "2.6", "use_dictionary": False,
         "column_encoding": {"c0": "DELTA_BINARY_PACKED",
                             "c1": "DELTA_BINARY_PACKED"}})
    add("cn_delta_ba", ["utf8"], 2000, 0.05,
        {"version": "2.6", "use_dictionary": False,
         "column_encoding": {"c0": "DELTA_BYTE_ARRAY"}})
    add("cn_delta_len", ["utf8"], 2000, 0.05,
        {"version": "2.6", "use_dictionary": False,
         "column_encoding": {"c0": "DELTA_LENGTH_BYTE_ARRAY"}})
    add("cn_boolrle", ["bool"], 10_000, 0.02,
        {"version": "2.6", "use_dictionary": False,
         "data_page_version": "2.0", "column_encoding": {"c0": "RLE"}})
    add("cn_gzip", ["i64", "f64", "utf8"], 4096, 0.05,
        {"compression": "gzip"})
    add("cn_brotli", ["i64", "f64", "utf8"], 4096, 0.05,
        {"compression": "brotli"})
    add("cn_rg_rem", ["i64"], 2500, 0.05, {"row_group_size": 1000})
    add("cn_nostat", ["i64", "f64"], 1000, 0.05,
        {"write_statistics": False})
    add("cn_i8bits", ["i8"], 300, 0.05)          # i8 bits land in KG byte
    add("cn_time32", ["time32_ms"], 300, 0.05)

    # INT96 timestamps (deprecated spark form) — pyarrow writes us->INT96.
    i96 = [NS2000 // 1000, 0, -86_400_000_000, 1_600_000_000_000_000,
           None, 4_000_000_000_000_000]
    cases.append(Case("cn_int96", [("c0", "ts_us", i96)],
                      {"use_deprecated_int96_timestamps": True}))
    # tz-annotated timestamp: instants unchanged, tz dropped.
    k = KINDS["ts_ms"]
    tzv = k[1](rng, 100, 0.05)
    cases.append(Case("cn_tz", [("c0", "ts_ms", tzv)],
                      arrays=[pa.array(tzv,
                              pa.timestamp("ms", "America/New_York"))]))
    # unsupported logical types must reject with clean 'nyi.
    cases.append(Case("cn_time64", [("c0", None, None)], err="nyi",
                      arrays=[pa.array([1, 2, None], pa.time64("us"))]))
    cases.append(Case("cn_decimal", [("c0", None, None)], err="nyi",
                      arrays=[pa.array([1, None], pa.decimal128(10, 2))]))
    cases.append(Case("cn_binary", [("c0", None, None)], err="nyi",
                      arrays=[pa.array([b"\x00\xff", None],
                                       pa.binary())]))
    cases.append(Case("cn_fixedbin", [("c0", None, None)], err="nyi",
                      arrays=[pa.array([b"ab", b"cd"],
                                       pa.binary(2))]))

    # 4. hostile edges.
    # Denormals (idx 7) are asserted bit-exactly by the pyarrow echo
    # check only: the q-side comparison literal would pass through the
    # host's "e"$/float parse, which flushes denormals to zero under
    # x86 FTZ/DAZ — a host arithmetic mode, not an adapter path.
    fs = [0.0, -0.0, math.inf, -math.inf, math.nan, 1e308, -1e308,
          5e-324, 2.2250738585072014e-308, None, 1.7976931348623157e308]
    keep = [0, 1, 2, 3, 4, 5, 6, 8, 9, 10]
    lv = [KINDS["f64"][2](fs[i]) for i in keep]
    cases.append(Case(
        "hs_f64_special", [("c0", "f64", fs)], no_lit=True,
        custom_q=[f'T["hs_f64_special vals";({KINDS["f64"][3](lv)})'
                  f'~c[{" ".join(str(i) for i in keep)}]]']))
    f32s = [None if v is None else float(np.float32(v)) for v in
            [0.0, -0.0, math.inf, -math.inf, math.nan, 3.4e38,
             -3.4e38, 1.4e-45, None, 1.5]]
    keep = [0, 1, 2, 3, 4, 5, 6, 8, 9]
    lv = [KINDS["f32"][2](f32s[i]) for i in keep]
    cases.append(Case(
        "hs_f32_special", [("c0", "f32", f32s)], no_lit=True,
        custom_q=[f'T["hs_f32_special vals";({KINDS["f32"][3](lv)})'
                  f'~c[{" ".join(str(i) for i in keep)}]]']))
    # sentinel collisions: VALID values equal to L's null bit patterns.
    cases.append(Case("hs_sent_i64",
                      [("c0", "i64", [NJ, NJ + 1, -1, 0, I64MAX])]))
    cases.append(Case("hs_sent_i32",
                      [("c0", "i32", [NI, NI + 1, -1, 0, 2**31 - 1])]))
    qnan = struct.unpack("<d", struct.pack("<Q", 0x7FF8000000000000))[0]
    nnan = struct.unpack("<d", struct.pack("<Q", 0xFFF8000000000000))[0]
    snan = struct.unpack("<d", struct.pack("<Q", 0x7FF0000000000001))[0]
    cases.append(Case("hs_sent_f64nan",
                      [("c0", "f64", [qnan, nnan, snan, 1.5])]))
    # i16 sentinel: -32768 valid value IS 0Nh; both directions lossless
    # at the bit level (write emits it as a valid value again).
    cases.append(Case("hs_sent_i16",
                      [("c0", "i16", [NH, NH + 1, -1, 0, 2**15 - 1])]))
    # timestamp extremes (in ns range; pre-2000 / pre-1970 / far future).
    tse = [-9_000_000_000_000_000_000, -2_208_988_800_000_000_000,
           -1, 0, NS2000, 7_000_000_000_000_000_000, None]
    cases.append(Case("hs_ts_extreme", [("c0", "ts_ns", tse)]))
    # unit-conversion overflow: seconds whose ns form exceeds i64 must
    # surface as null (arrow safe cast), never as a wrapped instant.
    cases.append(Case("hs_ts_s_ovfl",
                      [("c0", "ts_s",
                        [0, 9_300_000_000, -9_300_000_000, 100, None])]))
    cases.append(Case("hs_dur_neg",
                      [("c0", "dur_ns",
                        [-9_000_000_000_000_000_000, -1, 0, 1,
                         9_000_000_000_000_000_000, None])]))
    cases.append(Case("hs_date_range",
                      [("c0", "date32",
                        [-719_162, -1, 0, 1, 2_932_896, None])]))
    uni = ["héllo", "世界", "🚀x", "é", "ß", "táb\there", "a·b"]
    cases.append(Case("hs_unicode", [("c0", "utf8", uni)]))
    cases.append(Case("hs_emptystr",
                      [("c0", "utf8", ["", None, "x", "", None])]))
    # long strings: q builds the expected symbols by expression (a
    # 10KB literal would be unreadable), so skip the generic literal.
    long_q = ('T["hs_longstr vals";'
              '(`$10000#"a";`$1000#"ab";`$"x")~t`c0]')
    cases.append(Case("hs_longstr",
                      [("c0", "utf8", ["a" * 10000, "ab" * 500, "x"])],
                      custom_q=[long_q], no_lit=True))
    return cases

# ── gen phase ───────────────────────────────────────────────────────────


def arrow_table(case):
    if case.arrays is not None:
        return pa.table({nm: a for (nm, _, _), a
                         in zip(case.cols, case.arrays)})
    cols = {}
    for nm, kind, vals in case.cols:
        cols[nm] = pa.array(vals, KINDS[kind][0])
    return pa.table(cols)


def probes(rng, n):
    k = min(8, n)
    return sorted(int(i) for i in rng.choice(n, size=k, replace=False))


def emit_case(case, rng, out):
    """Driver lines for one case: read, exact asserts, echo write."""
    nm, p = case.name, case.path()
    w = out.append
    if case.err:
        w(f'e:@[pr;`:{p};{{x}}]')                # raw message, no prefix
        w(f'T["{nm} rejects ({case.err})";'
          f'(10=type e)and"{case.err}"~{len(case.err)}#e]')
        return
    w(f't:@[pr;`:{p};{{"ERR: ",x}}]')
    n = len(case.cols[0][2])
    w(f'T["{nm} is table";98=type t]')
    w(f'T["{nm} rows";{n}=count t]')
    names = "`" + "`".join(nm2 for nm2, _, _ in case.cols)
    if len(case.cols) == 1:
        names = "enlist " + names                # 1-col: vector, not atom
    w(f'T["{nm} cols";({names})~cols t]')
    for cn, kind, vals in case.cols:
        _, _, lval, qlit, _, _ = KINDS[kind]
        lv = [lval(v) for v in vals]
        w(f'c:t`{cn}')
        if case.no_lit:
            pass                                 # custom_q asserts values
        elif n <= 8:
            w(f'T["{nm} {cn} vals";({qlit(lv)})~c]')
        else:
            idx = probes(rng, n)
            pv = [lv[i] for i in idx]
            ilit = " ".join(str(i) for i in idx)
            w(f'T["{nm} {cn} probes";({qlit(pv)})~c[{ilit}]]')
        if kind not in ("bool", "u8", "i8"):     # no null concept in L
            nn = sum(1 for v in lv if v is None or
                     (isinstance(v, float) and math.isnan(v)) or v == "")
            w(f'T["{nm} {cn} nullcount";{nn}=sum null c]')
    for line in case.custom_q:
        w(line)
    for r in range(case.reread):
        w(f'T["{nm} reread {r}";t~pr `:{p}]')    # parallel-decode shake
    if case.echo:
        w(f'r:@[pw;(t;`:{case.echo_path()});{{"ERRW: ",x}}]')
        w(f'T["{nm} echo write";-11=type r]')
    if case.stream:
        w(f'sd:`$":{DIR}/splay_{nm}"')
        w('n:@[ps;(`:' + p + ';sd);{"ERRS: ",x}]')
        w(f'T["{nm} stream rows";{n}=n]')
        w(f'T["{nm} splay read-back";t~get sd]')


def gen(cases, seed, shake):
    os.makedirs(DIR, exist_ok=True)
    rng = np.random.default_rng(seed + 1)        # probe-index stream
    out = [
        "/ driver.q — GENERATED by tests/matrix.py; do not edit.",
        'p:"target/release/libl_parquet"',
        "pr:hsym[`$p] 2: (`pq_read; 1)",
        "pw:hsym[`$p] 2: (`pq_write; 1)",
        "ps:hsym[`$p] 2: (`pq_stream; 1)",
        "pass:0; fail:0",
        'T:{[nm;ok] $[ok;pass+:1;fail+:1];'
        ' show $[ok;"  PASS ";"  FAIL "],nm}',
    ]
    for case in cases:
        pq.write_table(arrow_table(case), case.path(), **case.wkw)
        emit_case(case, rng, out)
    if STRESS and not shake:
        emit_bigrows(out)
    tag = "SHAKE" if shake else "MATRIX"
    out.append(f'show "{tag}: ",string[pass]," passed, ",'
               'string[fail]," failed"')
    out.append("\\\\")
    drv = f"{DIR}/{'driver_shake' if shake else 'driver'}.q"
    with open(drv, "w") as f:
        f.write("\n".join(out) + "\n")
    print(f"gen: {len(cases)} fixtures, driver {drv}")


def emit_bigrows(out):
    """L_STRESS only: a >2^31-row file must be REJECTED, not truncated.
    All-false bool column: RLE+zstd keeps the file small on disk."""
    path = f"{DIR}/big_rows.parquet"
    n_chunk, chunks = 1 << 25, 65                # 65*2^25 = 2^31 + 2^25
    schema = pa.schema([("b", pa.bool_())])
    w = pq.ParquetWriter(path, schema, compression="zstd")
    batch = pa.record_batch([pa.array(np.zeros(n_chunk, bool))],
                            schema=schema)
    for _ in range(chunks):
        w.write_batch(batch)
    w.close()
    out.append(f'e:@[pr;`:{path};{{x}}]')
    out.append('T["big_rows read rejects >2^31";"pq_read: >2^31"~14#e]')
    out.append(f'e:@[ps;(`:{path};`:{DIR}/splay_big);{{x}}]')
    out.append('T["big_rows stream rejects >2^31";'
               '"pq_stream: >2^31"~16#e]')

# ── check phase: pyarrow re-reads L's echo files ────────────────────────


def bits64(v):
    return struct.unpack("<Q", struct.pack("<d", v))[0]


def bits32(v):
    return struct.unpack("<I", struct.pack("<f", v))[0]


def col_values(tbl, i, kind):
    """Echo column -> comparable python list (ints for temporal)."""
    c = tbl.column(i)
    t = str(c.type)
    if t.startswith("timestamp") or t.startswith("duration"):
        c = c.cast(pa.int64())
    elif t.startswith("date32"):
        c = c.cast(pa.int32())
    elif t.startswith("time32"):
        c = c.cast(pa.int32())
    return c.to_pylist()


def cmp_val(kind, exp, got):
    if kind in ("f64",):
        if exp is None:
            return got is None
        return got is not None and bits64(exp) == bits64(got)
    if kind == "f32":
        if exp == "NAN":
            return got is not None and math.isnan(got)
        return got is not None and bits32(exp) == bits32(got)
    return exp == got


def check(cases):
    files = comps = bad = 0
    for case in cases:
        if not case.echo or not os.path.exists(case.echo_path()):
            if case.echo:
                print(f"CHECK MISSING: {case.echo_path()}")
                bad += 1
            continue
        t = pq.read_table(case.echo_path())
        files += 1
        for i, (cn, kind, vals) in enumerate(case.cols):
            et = KINDS[kind][5]
            at = str(t.schema.field(cn).type)
            if at != et and not (et == "string" and "string" in at):
                print(f"CHECK {case.name}.{cn}: type {at} != {et}")
                bad += 1
                continue
            echo = KINDS[kind][4]
            exp = [echo(v) for v in vals]
            got = col_values(t, i, kind)
            if len(exp) != len(got):
                print(f"CHECK {case.name}.{cn}: len {len(got)}"
                      f" != {len(exp)}")
                bad += 1
                continue
            for j, (e, g) in enumerate(zip(exp, got)):
                comps += 1
                if not cmp_val(kind, e, g):
                    print(f"CHECK {case.name}.{cn}[{j}]:"
                          f" exp {e!r} got {g!r}")
                    bad += 1
                    if bad > 40:
                        sys.exit("check: too many failures")
                    break
    print(f"CHECK: {files} echo files, {comps} value compares,"
          f" {bad} failed")
    sys.exit(1 if bad else 0)


def main():
    args = sys.argv[1:]
    if not args or args[0] not in ("gen", "check"):
        sys.exit("usage: matrix.py gen|check [--seed N] [--shake]")
    seed = 20260706
    if "--seed" in args:
        seed = int(args[args.index("--seed") + 1])
    shake = "--shake" in args
    cases = build_cases(seed, shake)
    if args[0] == "gen":
        gen(cases, seed, shake)
    else:
        check(cases)


if __name__ == "__main__":
    main()
