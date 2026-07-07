//! l-parquet — Apache Parquet reader/writer for L, bound via `2:`.
//!
//! Exports (all arity 1, mirroring arrow_interop's surface):
//!   pq_read(`:file.parquet)          → table
//!   pq_write((table; `:file.parquet)) → path
//!   pq_stream((`:src.parquet; `:dstdir)) → rows written (splayed table)
//!
//! Every entry point wraps its body in catch_unwind: a Rust panic must
//! NEVER unwind across the C FFI boundary into the L interpreter —
//! panics become L errors via krr, exactly like ordinary failures.

mod ffi;
mod read;
mod stream;
mod write;

use ffi::*;
use std::panic::{catch_unwind, AssertUnwindSafe};

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

/// Interned symbol pointer → owned path string, `:` prefix dropped.
unsafe fn ptr_path(
    p: *const std::os::raw::c_char,
    who: &str,
) -> Result<String, String> {
    let s = std::ffi::CStr::from_ptr(p)
        .to_str()
        .map_err(|_| format!("{who}: path utf8"))?;
    Ok(s.strip_prefix(':').unwrap_or(s).to_string())
}

/// Extract a file path from a symbol atom, dropping the `:` hsym prefix.
unsafe fn sym_path(x: K, who: &str) -> Result<String, String> {
    if kt(x) != -KS {
        return Err(format!("{who}: expected symbol path"));
    }
    ptr_path(ls(x), who)
}

/// pq_read(path) — Parquet file → L table.
#[no_mangle]
pub extern "C" fn pq_read(path: K) -> K {
    guard(|| unsafe { read::read_table(&sym_path(path, "pq_read")?) })
}

/// pq_write((table; path)) — L table → Parquet file; returns the path.
#[no_mangle]
pub extern "C" fn pq_write(x: K) -> K {
    guard(|| unsafe {
        if kt(x) != 0 || kn(x) != 2 {
            return Err("pq_write: (table;path) expected".into());
        }
        let tbl = *v_k(x);
        let path = *v_k(x).add(1);
        write::write_table(tbl, &sym_path(path, "pq_write")?)?;
        // Return the caller's path atom, retained: the caller owns the
        // argument list and will release it after we return.
        Ok(r1(path))
    })
}

/// pq_stream((src; dst)) — Parquet → splayed table dir; returns rows.
#[no_mangle]
pub extern "C" fn pq_stream(x: K) -> K {
    guard(|| unsafe {
        // (`a;`b) collapses to a symbol VECTOR in q, so accept both a
        // 2-element KS vector and a 2-element generic list of symbols.
        let (src, dst) = if kt(x) == KS && kn(x) == 2 {
            (
                ptr_path(*v_s(x), "pq_stream")?,
                ptr_path(*v_s(x).add(1), "pq_stream")?,
            )
        } else if kt(x) == 0 && kn(x) == 2 {
            (
                sym_path(*v_k(x), "pq_stream")?,
                sym_path(*v_k(x).add(1), "pq_stream")?,
            )
        } else {
            return Err("pq_stream: (src;dst) expected".into());
        };
        Ok(kj(stream::stream_table(&src, &dst)?))
    })
}
