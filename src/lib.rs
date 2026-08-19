//! l-parquet — Apache Parquet reader/writer for L, bound via `2:`.
//!
//! Exports (all arity 1 — a multi-argument call passes ONE L list):
//!   pq_meta(files)                       → footer dict (no page read)
//!   pq_read((files; cols))               → table  (cols () or ` = all)
//!   pq_rg((files; cols; lo; hi))         → table of global row groups
//!                                          [lo,hi) — the streaming door
//!   pq_write((table; `:file.parquet))    → path
//!   pq_stream((`:src.parquet; `:dstdir)) → rows written (splayed table)
//!
//! `files` is a symbol atom, a symbol vector, or a list of symbol
//! atoms; `pq_read` on a bare symbol atom is the original one-file,
//! all-columns call and keeps working unchanged.
//!
//! Every entry point wraps its body in catch_unwind: a Rust panic must
//! NEVER unwind across the C FFI boundary into the L interpreter —
//! panics become L errors via krr, exactly like ordinary failures.

/// Per-function profiling, compiled out unless `--features hotpath`:
/// `hp!("pq_read")` opens a measurement scope whose report prints when
/// the entry point returns, and the functions it measures are the ones
/// carrying `#[cfg_attr(feature = "hotpath", hotpath::measure)]`.
#[cfg(feature = "hotpath")]
macro_rules! hp {
    ($n:expr) => {
        let _hp = hotpath::HotpathGuardBuilder::new($n).build();
    };
}
#[cfg(not(feature = "hotpath"))]
macro_rules! hp {
    ($n:expr) => {};
}
pub(crate) use hp;

mod ffi;
mod meta;
mod pool;
mod read;
mod stream;
mod write;

use ffi::*;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

/// Stack size for spawned worker threads.  Rust's 2 MiB default
/// overflows on x86-64: the LTO'd parquet+zstd encode/decode frames
/// alone exceed it (SIGSEGV on the guard page).  8 MiB is lazily
/// committed, so idle reserve costs nothing.
pub(crate) const WORKER_STACK: usize = 8 << 20;

/// Prefix any error with the entry point that raised it — the one
/// error-plumbing shape every module shares.
pub(crate) trait Ctx<T> {
    fn ctx(self, who: &str) -> Result<T, String>;
}
impl<T, E: std::fmt::Display> Ctx<T> for Result<T, E> {
    fn ctx(self, who: &str) -> Result<T, String> {
        self.map_err(|e| format!("{who}: {e}"))
    }
}

/// A scratch path that must NOT survive a failure.  A Parquet file is
/// only a file once its footer is on disk and a splay is only a table
/// once its counts are patched, so a write that fails — an error return
/// OR a panic unwinding through it — has to take its half-written bytes
/// with it, or the next read over the directory picks them up.  Drop is
/// what removes them; `keep` is what a completed rename calls instead.
pub(crate) struct Scratch(Option<PathBuf>, bool);

impl Scratch {
    /// A unique scratch name beside `path`, and the guard that removes
    /// it.  The pid keeps two PROCESSES off each other's scratch and
    /// the counter keeps two THREADS of one process off it; the name
    /// does not end in `.parquet`, so a directory glob cannot pick it
    /// up mid-write.
    pub fn new(path: &str, dir: bool) -> (String, Scratch) {
        static N: AtomicUsize = AtomicUsize::new(0);
        let p = format!(
            "{}.{}.{}.tmp",
            path.trim_end_matches('/'),
            std::process::id(),
            N.fetch_add(1, Relaxed)
        );
        (p.clone(), Scratch(Some(p.into()), dir))
    }

    pub fn keep(&mut self) {
        self.0 = None;
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        if let Some(p) = self.0.take() {
            let _ = if self.1 {                                                 // nothing to do if it fails
                std::fs::remove_dir_all(p)
            } else {
                std::fs::remove_file(p)
            };
        }
    }
}

/// Run `f`, converting Err strings AND panics into L errors.  The
/// AssertUnwindSafe is sound because a failed closure's partial state
/// is abandoned wholesale — nothing observes it after the unwind.
fn guard(f: impl FnOnce() -> Result<K, String>) -> K {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(k)) => k,
        Ok(Err(m)) => err(&m),
        Err(_) => err("pq: internal panic"),
    }
}

/// Interned symbol pointer → owned string.  It decodes column and
/// option-key symbols as well as paths, so the error says `symbol`.
unsafe fn ptr_str(
    p: *const std::os::raw::c_char,
    who: &str,
) -> Result<String, String> {
    std::ffi::CStr::from_ptr(p)
        .to_str()
        .map(|s| s.to_string())
        .map_err(|_| format!("{who}: symbol utf8"))
}

/// A path symbol's text → a filesystem path: the hsym `:` drops.
fn path_of(s: &str) -> String {
    s.strip_prefix(':').unwrap_or(s).to_string()
}

/// Interned symbol pointer → owned path string, `:` prefix dropped.
unsafe fn ptr_path(
    p: *const std::os::raw::c_char,
    who: &str,
) -> Result<String, String> {
    Ok(path_of(&ptr_str(p, who)?))
}

/// Extract a file path from a symbol atom, dropping the `:` hsym prefix.
unsafe fn sym_path(x: K, who: &str) -> Result<String, String> {
    if kt(x) != -KS {
        return Err(format!("{who}: expected symbol path"));
    }
    ptr_path(ls(x), who)
}

/// The symbol texts of a symbol ARGUMENT, in the three shapes q can
/// hand one over in: a symbol ATOM is one, a symbol VECTOR is many, and
/// a general list must hold symbol atoms.  `what` names the thing in
/// the error, so one parser serves files, columns and stream paths.
unsafe fn syms_of(
    x: K,
    who: &str,
    what: &str,
) -> Result<Vec<String>, String> {
    let bad = || format!("{who}: expected {what}");
    match kt(x) {
        t if t == -KS => Ok(vec![ptr_str(ls(x), who)?]),
        t if t == KS => (0..kn(x))
            .map(|i| ptr_str(*v_s(x).add(i as usize), who))
            .collect(),
        0 => (0..kn(x))
            .map(|i| {
                let e = *v_k(x).add(i as usize);
                if kt(e) != -KS {
                    return Err(bad());
                }
                ptr_str(ls(e), who)
            })
            .collect(),
        _ => Err(bad()),
    }
}

/// A file argument → one `(symbol text, path)` per file.  The symbol
/// text is echoed back verbatim by `pq_meta`, so `` `:a.parquet `` comes
/// out the way it went in; the path is the same minus the hsym `:`.
unsafe fn files_of(
    x: K,
    who: &str,
) -> Result<Vec<(String, String)>, String> {
    Ok(syms_of(x, who, "symbol path")?
        .into_iter()
        .map(|s| {
            let p = path_of(&s);
            (s, p)
        })
        .collect())
}

/// Is this 2-element symbol vector a COLLAPSED (file; cols) pair rather
/// than two file names?  Only when the second symbol cannot be a
/// companion path: the empty symbol names no file, and a plain name
/// beside an hsym is a column far more often than it is a second file
/// spelled without its `:`.
unsafe fn collapsed_pair(x: K, who: &str) -> Result<bool, String> {
    let a = ptr_str(*v_s(x), who)?;
    let b = ptr_str(*v_s(x).add(1), who)?;
    Ok(b.is_empty() || (a.starts_with(':') && !b.starts_with(':')))
}

/// A column argument → the requested names, or None for "all columns
/// in file order".  `` ` ``, `()` and `` 0#` `` all mean all.
unsafe fn cols_of(
    x: K,
    who: &str,
) -> Result<Option<Vec<String>>, String> {
    let names = syms_of(x, who, "column symbol")?;
    // A lone ` is q's "no column named", i.e. every column.
    Ok(if names.iter().all(|n| n.is_empty()) {
        None
    } else {
        Some(names)
    })
}

/// A long/int/short atom → i64 (ffi's `atom_i64`, which owns the
/// narrow-atom sign rule), or an error naming the caller.
unsafe fn long_of(x: K, who: &str) -> Result<i64, String> {
    atom_i64(x).ok_or_else(|| format!("{who}: expected long"))
}

/// One decoded value out of a K DICT's value side — either a tagged
/// atom from a generic list or one slot of a typed vector.  `Bad`
/// carries the L type, so an error can name what the caller passed.
/// Both option parsers (`rd_opts` here, `parse_opts` in write.rs) read
/// their dict through this, so there is one answer to "what shape can
/// q hand a one-element option value in".
pub(crate) enum Val {
    Sym(*const std::os::raw::c_char),
    Int(i64),
    Bool(bool),
    Bad(i16),
}

/// Decode a tagged K ATOM.  `atom_i64` is the one place that knows the
/// narrow-atom sign rule (a raw read turns a negative int into a huge
/// positive), so the integral tags all go through it.
pub(crate) unsafe fn atom_val(x: K) -> Val {
    match vtag(x) {
        KS => Val::Sym(va(x) as *const std::os::raw::c_char),
        KB => Val::Bool(kjv(x) as u8 != 0),
        t @ (KJL | KJ | KI | KH) => {
            atom_i64(x).map_or(Val::Bad(-t), Val::Int)
        }
        // vtag 0 is a HEAP object (a string, a list, a nested dict);
        // its type is the header's subtype, not the tag.
        0 => Val::Bad(vt(x)),
        t => Val::Bad(-t),
    }
}

/// Decode element `i` of a dict's value side, which q may hand over as
/// a generic list of atoms OR as a typed vector (`` (`a;`b)!11b ``).
pub(crate) unsafe fn val_at(vals: K, i: usize) -> Val {
    match vt(vals) {
        0 => atom_val(*v_k(vals).add(i)),
        KS => Val::Sym(*v_s(vals).add(i)),
        KJ => Val::Int(*v_j(vals).add(i)),
        KI => Val::Int(*v_i(vals).add(i) as i64),
        KH => Val::Int(*v_h(vals).add(i) as i64),
        KB => Val::Bool(*v_g(vals).add(i) != 0),
        t => Val::Bad(t),
    }
}

/// The read options a trailing dict may carry.  One key today; the
/// shape is a dict so a second one costs a line rather than an arity.
struct RdOpts {
    /// `` `codes `` (or `` `sym ``): every symbol column that is
    /// dictionary-encoded in every row group of the window comes back
    /// as the PAIR (dictionary; codes) instead of a symbol vector.
    codes: bool,
}

/// Read the `opts` the argument list may carry (None = the defaults).
/// A dict is the only shape accepted, and an unknown key or a wrongly
/// typed value is an error naming the key — `pq_write`'s contract too.
unsafe fn rd_opts(x: Option<K>, who: &str) -> Result<RdOpts, String> {
    let mut o = RdOpts { codes: false };
    let Some(x) = x else { return Ok(o) };
    if kt(x) != XD {
        return Err(format!("{who}: opts must be a dict"));
    }
    let (keys, vals) = (*v_k(x), *v_k(x).add(1));
    if vn(keys) != 0 && vt(keys) != KS {
        return Err(format!("{who}: opts keys must be symbols"));
    }
    for i in 0..vn(keys) as usize {
        let k = ptr_str(*v_s(keys).add(i), who)?;
        let b = match val_at(vals, i) {
            Val::Bool(v) => Some(v),
            _ => None,
        };
        match (k.as_str(), b) {
            ("codes" | "sym", Some(v)) => o.codes = v,
            ("codes" | "sym", None) => {
                return Err(format!("{who}: opt `{k} wants a boolean"))
            }
            _ => return Err(format!("{who}: unknown opt `{k}")),
        }
    }
    Ok(o)
}

/// pq_meta(files) — footer-only description of one or more files.
#[no_mangle]
pub extern "C" fn pq_meta(x: K) -> K {
    hp!("pq_meta");
    guard(|| unsafe {
        let who = "pq_meta";
        let set = meta::open(&files_of(x, who)?, who)?;
        meta::dict(&set)
    })
}

/// pq_read(files) or pq_read((files; cols)) — Parquet → L table.
#[no_mangle]
pub extern "C" fn pq_read(x: K) -> K {
    hp!("pq_read");
    guard(|| unsafe {
        let who = "pq_read";
        // q collapses a pair of symbol ATOMS into a symbol VECTOR, so
        // (`:f;`c) and `:a`:b are the SAME K value and only one of them
        // can be honoured.  The rule (README, and it is a rule, not a
        // guess): a 2-element symbol vector is (file; cols) when the
        // second element cannot be a companion path — it is empty, or
        // the first is an hsym and the second is not.  Anything else is
        // a list of files.  A one-column read of a plain (non-hsym)
        // path must therefore spell the general list: (f; enlist `c).
        // A general list of 2 is (files;cols); of 3, (files;cols;opts).
        // Every older shape still means exactly what it meant.
        let gen = kt(x) == 0 && (kn(x) == 2 || kn(x) == 3);
        let opt = rd_opts((gen && kn(x) == 3).then(|| *v_k(x).add(2)), who)?;
        let (files, c) = if gen {
            (files_of(*v_k(x), who)?, cols_of(*v_k(x).add(1), who)?)
        } else if kt(x) == KS && kn(x) == 2 && collapsed_pair(x, who)? {
            let col = ptr_str(*v_s(x).add(1), who)?;
            let want = (!col.is_empty()).then_some(vec![col]);
            let p = *v_s(x);
            (vec![(ptr_str(p, who)?, ptr_path(p, who)?)], want)
        } else {
            (files_of(x, who)?, None)
        };
        let set = meta::open(&files, who)?;
        let sel = read::select(&set, c.as_deref(), who)?;
        read::read(&set, &sel, 0, set.n_rg(), opt.codes, who)
    })
}

/// pq_rg((files; cols; lo; hi)) — global row groups [lo,hi) only.
#[no_mangle]
pub extern "C" fn pq_rg(x: K) -> K {
    hp!("pq_rg");
    guard(|| unsafe {
        let who = "pq_rg";
        let n = kn(x);
        if kt(x) != 0 || (n != 4 && n != 5) {
            return Err(format!(
                "{who}: (files;cols;lo;hi) or (files;cols;lo;hi;opts) \
                 expected"
            ));
        }
        let opt = rd_opts((n == 5).then(|| *v_k(x).add(4)), who)?;
        let set = meta::open(&files_of(*v_k(x), who)?, who)?;
        let want = cols_of(*v_k(x).add(1), who)?;
        let sel = read::select(&set, want.as_deref(), who)?;
        let (lo, hi) = (
            long_of(*v_k(x).add(2), who)?,
            long_of(*v_k(x).add(3), who)?,
        );
        let ng = set.n_rg() as i64;
        if lo < 0 || hi < lo || hi > ng {
            return Err(format!("{who}: [{lo},{hi}) of {ng} row groups"));
        }
        read::read(&set, &sel, lo as usize, hi as usize, opt.codes, who)
    })
}

/// pq_write((table; path)) or pq_write((table; path; opts)) — L table →
/// Parquet file; returns the path.  `opts` is a dict, see write.rs.
#[no_mangle]
pub extern "C" fn pq_write(x: K) -> K {
    hp!("pq_write");
    guard(|| unsafe {
        let n = kn(x);
        if kt(x) != 0 || (n != 2 && n != 3) {
            return Err(
                "pq_write: (table;path) or (table;path;opts) expected"
                    .into(),
            );
        }
        let tbl = *v_k(x);
        let path = *v_k(x).add(1);
        let opts = (n == 3).then(|| *v_k(x).add(2));
        write::write_table(tbl, &sym_path(path, "pq_write")?, opts)?;
        // Return the caller's path atom, retained: the caller owns the
        // argument list and will release it after we return.
        Ok(r1(path))
    })
}

/// pq_stream((src; dst)) — Parquet → splayed table dir; returns rows.
#[no_mangle]
pub extern "C" fn pq_stream(x: K) -> K {
    hp!("pq_stream");
    guard(|| unsafe {
        let who = "pq_stream";
        let f = files_of(x, who)?;
        // (`a;`b) collapses to a symbol VECTOR in q, so files_of sees
        // the same two shapes either way; only the count is ours.
        let [src, dst] = &f[..] else {
            return Err(format!("{who}: (src;dst) expected"));
        };
        Ok(kj(stream::stream_table(&src.1, &dst.1)?))
    })
}
