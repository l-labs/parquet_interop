//! meta — Parquet footers: open a set of files, agree ONE schema for
//! them, and hand the footer facts to L as `pq_meta`.
//!
//! Everything here is FOOTER-ONLY — not a single data page is touched.
//! A file is opened once into an `ArrowReaderMetadata` (footer bytes,
//! parsed thrift, arrow schema) that every decode worker then CLONES,
//! an Arc bump; that is why a 10-file / 1000-row-group read parses ten
//! footers rather than a thousand.  Footers of different files load in
//! PARALLEL: the cost is one seek + one small read each, and a 10-file
//! glob would otherwise serialize ten round trips.
//!
//! The second job done here is the SYMBOL DICTIONARY HINT.  arrow-rs
//! infers `Utf8` for a byte-array column, which inflates the on-disk
//! dictionary into one heap string per ROW before L ever sees it.
//! Re-declaring those fields `Dictionary(Int32, Utf8)` through
//! `ArrowReaderOptions::with_schema` makes the reader hand back the
//! dictionary page plus an index vector instead, so the read path
//! interns each DISTINCT value once (read.rs `fill_sym_dict`).  The
//! hint is all-or-nothing per file — arrow-rs validates the supplied
//! schema as a whole — so if any field is rejected we silently fall
//! back to the inferred schema and the plain string path, which is
//! correct either way, only slower.

use crate::ffi::*;
use crate::read::{l_type_of, ns_per};
use crate::Ctx;
use arrow::datatypes::*;
use parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use parquet::file::metadata::ColumnChunkMetaData;
use parquet::file::statistics::Statistics;
use std::fs::File;
use std::os::unix::fs::MetadataExt;
use std::sync::{Arc, Mutex};

/// One opened Parquet file: its parsed footer plus the two facts every
/// caller wants without re-walking the metadata.
pub struct Src {
    /// Filesystem path (the `:` hsym prefix already stripped).
    pub path: String,
    /// The symbol text the caller passed, echoed back by `pq_meta`.
    pub orig: String,
    /// Parsed footer + arrow schema; cloned (Arc bump) per worker.
    pub md: ArrowReaderMetadata,
    /// Row count of each row group, in file order.
    pub rg_rows: Vec<i64>,
    /// ROOT column index → the index of its one LEAF column, or None
    /// when the root has anything but exactly one (a Struct, a Map, a
    /// List).  Two numberings meet here and they are equal only while
    /// every column is flat: `names`, `lts` and `ProjectionMask::roots`
    /// speak ROOT indices, while `SchemaDescriptor::column`,
    /// `RowGroupMetaData::column` and `get_column_reader` all speak
    /// LEAF ones.  A Struct column ahead of a flat one shifts every
    /// leaf after it, and decoding the wrong leaf is silent.
    pub leaf: Vec<Option<usize>>,
}

impl Src {
    /// Chunk metadata of ROOT column `c` in row group `g`, or None when
    /// `c` is not a single-leaf column of this file.
    pub fn chunk(
        &self,
        g: usize,
        c: usize,
    ) -> Option<&ColumnChunkMetaData> {
        let l = self.leaf.get(c).copied().flatten()?;
        Some(self.md.metadata().row_group(g).column(l))
    }

    /// Does row group `g` carry a DICTIONARY page for root column `c`?
    pub fn chunk_dict(&self, g: usize, c: usize) -> bool {
        self.chunk(g, c)
            .is_some_and(|cc| cc.dictionary_page_offset().is_some())
    }
}

/// Root column index → its one leaf index (see `Src::leaf`).  Sized by
/// the ARROW schema, which is what every caller indexes with.
fn leaf_map(md: &ArrowReaderMetadata) -> Vec<Option<usize>> {
    let sd = md.parquet_schema();
    let mut v = vec![None; md.schema().fields().len()];
    let mut n = vec![0usize; v.len()];
    for l in 0..sd.num_columns() {
        let r = sd.get_column_root_idx(l);
        if let Some(k) = n.get_mut(r) {
            *k += 1;
            v[r] = (*k == 1).then_some(l);
        }
    }
    v
}

/// A set of files that all present the SAME table: same column names
/// in the same order, each mapping to the same L vector type.
pub struct Set {
    pub srcs: Vec<Src>,
    /// Column names in file order.
    pub names: Vec<String>,
    /// Per column: its L vector type, or the `'nyi` message explaining
    /// why it has none.  Kept as an error rather than raised at open
    /// time so a PROJECTION can read the mappable columns of a file
    /// that also holds, say, a list column.
    pub lts: Vec<Result<i16, String>>,
}

impl Set {
    /// Total rows across every file.
    pub fn rows(&self) -> i64 {
        self.srcs.iter().map(|s| s.rg_rows.iter().sum::<i64>()).sum()
    }
    /// Total row groups across every file — the GLOBAL numbering that
    /// `pq_meta`'s `rg` reports and `pq_rg`'s window indexes into, in
    /// file-argument order.
    pub fn n_rg(&self) -> usize {
        self.srcs.iter().map(|s| s.rg_rows.len()).sum()
    }
}

/// Open every `(orig, path)` pair, in parallel, and agree their schema.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub fn open(files: &[(String, String)], who: &str) -> Result<Set, String> {
    if files.is_empty() {
        return Err(format!("{who}: no files"));
    }
    // Footers differ wildly in size — a thousand row groups against
    // three — so `par_map` hands them out from one cursor rather than
    // cutting the list up in advance.
    let load1 = |i: usize| load(&files[i].0, &files[i].1, who);
    let out = crate::pool::par_map(files.len(), load1)
        .ok_or_else(|| format!("{who}: footer panic"))?;
    let mut srcs = Vec::with_capacity(out.len());
    for r in out {
        srcs.push(r?);
    }
    let s0 = srcs[0].md.schema().clone();
    let names: Vec<String> =
        s0.fields().iter().map(|f| f.name().clone()).collect();
    let lts: Vec<Result<i16, String>> = s0
        .fields()
        .iter()
        .map(|f| {
            l_type_of(f.data_type()).map_err(|_| {
                format!("nyi: column {} type {}", f.name(), f.data_type())
            })
        })
        .collect();
    // Files agree when their columns line up POSITIONALLY by name and
    // land on the same L type: Timestamp(us) and Timestamp(ns) both
    // normalize to KP, so they are the same table as far as L cares.
    for s in &srcs[1..] {
        let f = s.md.schema().fields();
        let same = f.len() == names.len()
            && f.iter().zip(&names).all(|(a, b)| a.name() == b)
            && f.iter().zip(&lts).all(|(a, b)| {
                l_type_of(a.data_type()).ok() == b.as_ref().ok().copied()
            });
        if !same {
            return Err(format!("{who}: schema {}", s.orig));
        }
    }
    Ok(Set { srcs, names, lts })
}

/// What the filesystem says this file IS.  A cached footer is reused
/// only for a file that still answers with the same identity AND the
/// same length and mtime, so an edit in place, a replacement, or a
/// different file that reused the inode all miss.
#[derive(PartialEq)]
struct Key {
    path: String,
    dev: u64,
    ino: u64,
    size: u64,
    mtime: (i64, i64),
}

/// Parsed footers, MRU first — a HIT rotates its entry to the front, so
/// the eviction at `FOOTERS_MAX` drops the least recently USED footer
/// and not merely the least recently inserted one.  Parsing one is
/// ~1.8 ms — a streaming caller that walks a file row group by row
/// group would otherwise pay it on every call, for the same file.
static FOOTERS: Mutex<Vec<(Key, ArrowReaderMetadata, Vec<i64>)>> =
    Mutex::new(Vec::new());

/// Cached footers kept.  A footer is tens of KB for a normal file and
/// a couple of MB for a thousand-row-group one; 32 bounds the worst
/// case at something a reader will not notice.
const FOOTERS_MAX: usize = 32;

/// Parse one file's footer, applying the dictionary hint if it takes.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn load(orig: &str, path: &str, who: &str) -> Result<Src, String> {
    let f = File::open(path).ctx(&format!("{who}: {path}"))?;
    let st = f.metadata().ctx(&format!("{who}: {path}"))?;
    let key = Key {
        path: path.to_string(),
        dev: st.dev(),
        ino: st.ino(),
        size: st.len(),
        mtime: (st.mtime(), st.mtime_nsec()),
    };
    let mk = |md: ArrowReaderMetadata, rg_rows: Vec<i64>| Src {
        path: path.to_string(),
        orig: orig.to_string(),
        leaf: leaf_map(&md),
        md,
        rg_rows,
    };
    if let Ok(mut c) = FOOTERS.lock() {
        if let Some(i) = c.iter().position(|(k, ..)| *k == key) {
            c[..=i].rotate_right(1);                                            // MRU first
            return Ok(mk(c[0].1.clone(), c[0].2.clone()));
        }
    }
    let base =
        ArrowReaderMetadata::load(&f, ArrowReaderOptions::new()).ctx(who)?;
    let md = match dict_hint(base.schema()) {
        Some(h) => ArrowReaderMetadata::try_new(
            base.metadata().clone(),
            ArrowReaderOptions::new().with_schema(h),
        )
        .unwrap_or(base),
        None => base,
    };
    let m = md.metadata().clone();
    let rg_rows: Vec<i64> =
        (0..m.num_row_groups()).map(|g| m.row_group(g).num_rows()).collect();
    // The two row counts a footer carries must agree: the decode window
    // is laid out from the per-row-group counts, and a file whose own
    // total disagrees with their sum is describing a table that does
    // not exist.  Refuse it here, before anything is sized from either.
    // A REJECTED footer is never cached: the next call re-reads the
    // file and answers from what is there then.
    let declared = m.file_metadata().num_rows();
    if declared != rg_rows.iter().sum::<i64>() {
        return Err(format!("{who}: {path}: row counts disagree"));
    }
    if let Ok(mut c) = FOOTERS.lock() {
        // Two threads can miss on one file and both parse it; the
        // second must not spend a second slot saying the same thing.
        if !c.iter().any(|(k, ..)| *k == key) {
            c.truncate(FOOTERS_MAX - 1);
            c.insert(0, (key, md.clone(), rg_rows.clone()));
        }
    }
    Ok(mk(md, rg_rows))
}

/// The inferred schema with every string field re-declared as a
/// dictionary, or None when the file holds no string column at all.
/// Name, nullability and field metadata are copied VERBATIM: arrow-rs
/// rejects a supplied schema that differs from the file in any of the
/// three, and only the data type is ours to change.
fn dict_hint(s: &SchemaRef) -> Option<SchemaRef> {
    let dict = DataType::Dictionary(
        Box::new(DataType::Int32),
        Box::new(DataType::Utf8),
    );
    let mut any = false;
    let fs: Vec<FieldRef> = s
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                any = true;
                Arc::new(
                    Field::new(f.name(), dict.clone(), f.is_nullable())
                        .with_metadata(f.metadata().clone()),
                )
            }
            _ => f.clone(),
        })
        .collect();
    any.then(|| {
        Arc::new(Schema::new_with_metadata(fs, s.metadata().clone()))
            as SchemaRef
    })
}

/// L type char for `meta`'s `t` column — the host's own table, indexed
/// by type number (data.c `tc`).
fn type_char(lt: i16) -> u8 {
    const T: &[u8] = b"*b**xhijefcspmdznuvt";
    *T.get(lt as usize).unwrap_or(&b'*')
}

/// `pq_meta` → `` `files`cols`types`rows`rg`bytes`stats!(...) ``.
///
/// Every column must map to an L type here: `pq_meta` describes the
/// WHOLE file, so an unmappable column is 'nyi rather than silently
/// missing from the answer.  Nothing below can fail, which is what
/// lets the K tree be built with no unwinding to undo.
pub unsafe fn dict(set: &Set) -> Result<K, String> {
    let mut lts = Vec::with_capacity(set.lts.len());
    for t in &set.lts {
        lts.push(t.clone()?);
    }
    let nc = lts.len();
    let files = ksyms(set.srcs.iter().map(|s| s.orig.as_bytes()));
    let cols = ksyms(set.names.iter().map(|s| s.as_bytes()));
    let tc: Vec<u8> = lts.iter().map(|t| type_char(*t)).collect();
    let types = kchars(&tc);
    let rows = kj(set.rows());
    let rg = klist(
        &set.srcs.iter().map(|s| kj_vec(&s.rg_rows)).collect::<Vec<K>>(),
    );
    let (bytes, ubytes) = sizes(set, nc);
    let stats = stats_of(set, &lts);
    let enc = enc_of(set, nc);
    // Key order is APPEND-ONLY: `ubytes` and `enc` arrived after the
    // first seven, and callers that index positionally must not break
    // when the eighth is added.
    let keys = ksyms(
        [
            &b"files"[..],
            b"cols",
            b"types",
            b"rows",
            b"rg",
            b"bytes",
            b"stats",
            b"ubytes",
            b"enc",
        ]
        .into_iter(),
    );
    Ok(xD(
        keys,
        klist(&[files, cols, types, rows, rg, bytes, stats, ubytes, enc]),
    ))
}

/// `(compressed, uncompressed)`, each size[file][row group] = the
/// per-column chunk sizes — the pair a streaming caller budgets from:
/// one says what it will read, the other what it will hold.  Both come
/// out of ONE walk of the row groups, because the walk is the cost and
/// the two answers sit in the same `ColumnChunkMetaData`.  The column
/// chunk order within a row group is the LEAF order, which is the
/// column order only while every column is flat — and `dict`, the one
/// caller, has already refused the file if any column is not.
unsafe fn sizes(set: &Set, nc: usize) -> (K, K) {
    let (mut cf, mut uf) = (Vec::new(), Vec::new());
    for s in &set.srcs {
        let m = s.md.metadata();
        let (mut cg, mut ug) = (Vec::new(), Vec::new());
        for g in 0..m.num_row_groups() {
            let r = m.row_group(g);
            let (cv, uv) =
                (ktn(KJ as i32, nc as i64), ktn(KJ as i32, nc as i64));
            for c in 0..nc {
                let cc = r.column(c);
                *v_j(cv).add(c) = cc.compressed_size();
                *v_j(uv).add(c) = cc.uncompressed_size();
            }
            cg.push(cv);
            ug.push(uv);
        }
        cf.push(klist(&cg));
        uf.push(klist(&ug));
    }
    (klist(&cf), klist(&uf))
}

/// Per column: 1b when EVERY row group of every file carries a
/// dictionary page for it, so a caller can predict up front whether the
/// read will take the codes-and-dictionary path or inflate to plain
/// values.  A set with no row groups answers 0b rather than the vacuous
/// 1b — there is no dictionary there to take.
unsafe fn enc_of(set: &Set, nc: usize) -> K {
    let r = ktn(KB as i32, nc as i64);
    let any = set.n_rg() > 0;
    for c in 0..nc {
        let mut all = any;
        for s in &set.srcs {
            for g in 0..s.rg_rows.len() {
                all &= s.chunk_dict(g, c);
            }
        }
        *v_g(r).add(c) = all as u8;
    }
    r
}

/// Long vector holding `v`.  Not `kjv_`: `kjv` is the ATOM accessor.
unsafe fn kj_vec(v: &[i64]) -> K {
    let r = ktn(KJ as i32, v.len() as i64);
    std::ptr::copy_nonoverlapping(v.as_ptr(), v_j(r), v.len());
    r
}

/// One `` `min`max`null!(...) `` dict per column, each vector indexed by
/// GLOBAL row group.  A row group whose footer carries no statistics —
/// or carries TRUNCATED ones, which is what `min_is_exact` reports for
/// long byte arrays — reads back as the column's null, so a caller can
/// only ever skip work it is entitled to skip.
///
/// One caveat a PRUNING caller must hold: `min_is_exact`/`max_is_exact`
/// come from the footer's `is_min_value_exact` flags, and a writer old
/// enough to predate those fields sets neither — the crate then reports
/// exact whenever a bound is present.  Numeric and temporal bounds are
/// safe (nothing truncates them); a KS bound from such a writer may be
/// a TRUNCATED prefix reported as exact, so treat symbol min/max as
/// ADVISORY unless the file's `created_by` is known to be recent.
unsafe fn stats_of(set: &Set, lts: &[i16]) -> K {
    let ng = set.n_rg() as i64;
    let mut out = Vec::with_capacity(lts.len());
    for (c, &lt) in lts.iter().enumerate() {
        let (mn, mx, nc) = (
            ktn(lt as i32, ng),
            ktn(lt as i32, ng),
            ktn(KJ as i32, ng),
        );
        let mut g = 0usize;
        for s in &set.srcs {
            // The unit is a property of the FILE, not of the set: two
            // files agree when they agree on the L TYPE, so a
            // Timestamp(us) column can sit beside a Timestamp(ns) one,
            // and a bound has to be normalized with the scale of the
            // file that wrote it.  Taking the set's — file 0's — was
            // reporting 1970 for the second file's instants.
            let dt = s.md.schema().field(c).data_type();
            let m = s.md.metadata();
            for r in 0..m.num_row_groups() {
                put_null(lt, mn, g);
                put_null(lt, mx, g);
                let st = m.row_group(r).column(c).statistics();
                *v_j(nc).add(g) = st
                    .and_then(|s| s.null_count_opt())
                    .map_or(NJ, |v| v as i64);
                if let Some(st) = st {
                    put_stat(lt, dt, mn, g, st, false);
                    put_stat(lt, dt, mx, g, st, true);
                }
                g += 1;
            }
        }
        let keys = ksyms([&b"min"[..], b"max", b"null"].into_iter());
        out.push(xD(keys, klist(&[mn, mx, nc])));
    }
    klist(&out)
}

/// Write element `g` of an L vector of type `lt` as that type's null.
unsafe fn put_null(lt: i16, col: K, g: usize) {
    match lt {
        KB | KG => *v_g(col).add(g) = 0,
        KH => *v_h(col).add(g) = NH,
        KI | KD | KT => *v_i(col).add(g) = NI,
        KJ | KP | KN => *v_j(col).add(g) = NJ,
        KE => *v_e(col).add(g) = f32::NAN,
        KF => *v_f(col).add(g) = f64::NAN,
        KS => *v_s(col).add(g) = intern(b""),
        _ => {}
    }
}

/// The exact min (hi=false) or max (hi=true) of a typed statistic.  A
/// macro, not a function: `ValueStatistics`'s accessors are bounded on
/// `ParquetValueType`, which the crate seals in a private module, so
/// there is no way to spell the generic — expanding per arm is.
macro_rules! pick {
    ($s:expr, $hi:expr) => {
        match $hi {
            true if $s.max_is_exact() => $s.max_opt(),
            false if $s.min_is_exact() => $s.min_opt(),
            _ => None,
        }
    };
}

/// Nanoseconds for a raw temporal statistic, or None on overflow — the
/// footer stores the column's OWN unit, the same normalization the
/// decode path applies to the values themselves.
fn to_ns(dt: &DataType, v: i64) -> Option<i64> {
    let u = match dt {
        DataType::Timestamp(u, _) => u,
        DataType::Duration(u) => u,
        _ => return None,
    };
    v.checked_mul(ns_per(u))
}

/// Write one footer extremum into element `g`, leaving the slot at its
/// pre-written null when the statistic is absent, inexact, of an
/// unexpected physical type (INT96 timestamps), or unrepresentable
/// after the epoch shift.
unsafe fn put_stat(
    lt: i16,
    dt: &DataType,
    col: K,
    g: usize,
    st: &Statistics,
    hi: bool,
) {
    // One arm: write `$w` when the bound is there and exact, and leave
    // the slot at its pre-written null when it is not.  Defined here,
    // inside the function, so `hi` is the parameter above.
    macro_rules! put {
        ($s:expr, $v:ident, $w:expr) => {
            if let Some($v) = pick!($s, hi) {
                $w
            }
        };
    }
    match (lt, st) {
        (KB, Statistics::Boolean(s)) => {
            put!(s, v, *v_g(col).add(g) = *v as u8)
        }
        (KG, Statistics::Int32(s)) => {
            put!(s, v, *v_g(col).add(g) = *v as u8)
        }
        (KH, Statistics::Int32(s)) => {
            put!(s, v, *v_h(col).add(g) = *v as i16)
        }
        (KI, Statistics::Int32(s)) | (KT, Statistics::Int32(s)) => {
            put!(s, v, *v_i(col).add(g) = *v)
        }
        (KJ, Statistics::Int64(s)) => put!(s, v, *v_j(col).add(g) = *v),
        (KE, Statistics::Float(s)) => put!(s, v, *v_e(col).add(g) = *v),
        (KF, Statistics::Double(s)) => put!(s, v, *v_f(col).add(g) = *v),
        (KS, Statistics::ByteArray(s)) => {
            put!(s, v, *v_s(col).add(g) = intern(v.data()))
        }
        (KD, Statistics::Int32(s)) => {
            if let Some(d) = pick!(s, hi).and_then(|v| v.checked_sub(DAY2000))
            {
                *v_i(col).add(g) = d;
            }
        }
        (KP, Statistics::Int64(s)) => {
            if let Some(t) = pick!(s, hi)
                .and_then(|v| to_ns(dt, *v))
                .and_then(|v| v.checked_sub(NS2000))
            {
                *v_j(col).add(g) = t;
            }
        }
        (KN, Statistics::Int64(s)) => {
            if let Some(t) = pick!(s, hi).and_then(|v| to_ns(dt, *v)) {
                *v_j(col).add(g) = t;
            }
        }
        _ => {}
    }
}
