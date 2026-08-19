//! stream — Parquet file → L splayed table directory, one row group in
//! memory at a time (peak DRAM = one row group's columns, not the file).
//!
//! On-disk formats (the native splay layouts `get`/`\l` load directly):
//!
//!   fixed width   [u32 0][i16 type][u16 0][i64 count][16B 0] + payload
//!                 — a 32-byte header (type at offset 4, count at 8),
//!                 then the raw vector payload.
//!   symbols / .d  [0xFF 0x01][i16 type=KS][i32 count] + NUL-terminated
//!                 strings — interned pointers have no stable disk form,
//!                 so symbols serialize as their bytes.
//!
//! Both layouts put the count in a fixed slot, so streaming appends raw
//! chunk after raw chunk and PATCHES the count once at the end — no
//! rewrite, no buffering of more than one Arrow batch.
//!
//! The decode itself is read.rs's: the same footer (parsed once, with
//! the symbol dictionary hint applied), the same converters, the same
//! symbol cache — carried across row groups here, so a symbol seen in
//! group 0 is never re-interned in group 99.
//!
//! The directory is built BESIDE `dst` and renamed over it, the way
//! `pq_write` builds its file: a splay is only a table once its counts
//! are patched, and a stream that dies half way would otherwise leave a
//! `.d` naming every column beside column files whose headers still say
//! zero rows — which `get`/`\l` load, silently, as an empty table.  So
//! a failed stream leaves the PREVIOUS splay exactly as it was, and a
//! successful one REPLACES it whole: no column file from an earlier
//! schema survives into the new table.

use crate::ffi::*;
use crate::read::{fill_col, SymCache, BATCH_ROWS};
use crate::Ctx;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::ffi::CStr;
use std::fs::File;
use std::io::{Seek, SeekFrom, Write};

/// Start a fixed-width column file: 32-byte header, count 0 for now
/// (patched by `patch_count` when the total is known).
fn hdr_fixed(f: &mut File, lt: i16) -> Result<(), String> {
    let mut h = [0u8; 32];
    h[4..6].copy_from_slice(&lt.to_le_bytes());
    f.write_all(&h).ctx("pq_stream")
}

/// Start a symbol column (or .d) file: 0xFF01 header holding `n`.  A
/// column file passes 0 and patches the total in afterwards; `.d`
/// knows its count before a byte is written and passes it here.
fn hdr_syms(f: &mut File, n: i32) -> Result<(), String> {
    let mut h = [0u8; 8];
    h[0] = 0xFF;
    h[1] = 0x01;
    h[2..4].copy_from_slice(&KS.to_le_bytes());
    h[4..8].copy_from_slice(&n.to_le_bytes());
    f.write_all(&h).ctx("pq_stream")
}

/// Patch the element count into its fixed header slot: offset 8 as i64
/// for fixed-width files, offset 4 as i32 for 0xFF01 symbol files.
fn patch_count(f: &mut File, sym: bool, n: i64) -> Result<(), String> {
    if sym {
        f.seek(SeekFrom::Start(4)).ctx("pq_stream")?;
        f.write_all(&(n as i32).to_le_bytes()).ctx("pq_stream")
    } else {
        f.seek(SeekFrom::Start(8)).ctx("pq_stream")?;
        f.write_all(&n.to_le_bytes()).ctx("pq_stream")
    }
}

/// Append one L column chunk to its open file in the native layout.
unsafe fn append_chunk(
    f: &mut File,
    lt: i16,
    col: K,
    nr: usize,
) -> Result<(), String> {
    if lt == KS {
        // Symbol chunk: the interned strings' bytes, NUL included.
        let s = v_s(col);
        for i in 0..nr {
            let b = CStr::from_ptr(*s.add(i)).to_bytes_with_nul();
            f.write_all(b).ctx("pq_stream")?;
        }
        Ok(())
    } else {
        // Fixed width: raw payload bytes; nt() is the host's
        // authoritative storage width per type tag.
        let w = nt(lt as u32) as usize;
        let raw = std::slice::from_raw_parts(v_g(col), nr * w);
        f.write_all(raw).ctx("pq_stream")
    }
}

/// Stream Parquet `src` → splayed table directory `dst`; returns rows.
pub fn stream_table(src: &str, dst: &str) -> Result<i64, String> {
    let who = "pq_stream";
    let files = [(src.to_string(), src.to_string())];
    let set = crate::meta::open(&files, who)?;
    let s0 = &set.srcs[0];
    if s0.rg_rows.iter().sum::<i64>() > i32::MAX as i64 {
        // The 0xFF01 symbol header stores an i32 count and L vectors
        // are 2^31-bounded — refuse rather than write a corrupt splay.
        return Err(format!("{who}: >2^31 rows"));
    }
    let mut lts = Vec::with_capacity(set.lts.len());
    for t in &set.lts {
        // Validate the whole schema before touching the filesystem.
        lts.push(t.clone()?);
    }
    let nc = lts.len();
    // One spelling of dst from here on: a trailing `/` would make the
    // rename below name a path that does not exist yet, which POSIX
    // refuses (a trailing slash demands an existing directory).
    let dst = dst.trim_end_matches('/');
    if dst.is_empty() {
        return Err(format!("{who}: empty destination"));
    }
    // A dst that exists and is not a directory is refused BEFORE any
    // work: create_dir_all used to answer that, and the build-beside
    // -and-rename below would otherwise only find out at the very end.
    if std::fs::metadata(dst).is_ok_and(|m| !m.is_dir()) {
        return Err(format!("{who}: {dst}: not a directory"));
    }
    let (tmp, mut scratch) = crate::Scratch::new(dst, true);
    std::fs::create_dir_all(&tmp).ctx(&format!("{who}: {dst}"))?;
    // .d — the splay manifest: a symbol vector of column names.
    let mut df = File::create(format!("{tmp}/.d")).ctx("pq_stream: .d")?;
    hdr_syms(&mut df, nc as i32)?;
    for n in &set.names {
        df.write_all(n.as_bytes())
            .and_then(|_| df.write_all(&[0]))
            .ctx("pq_stream: .d")?;
    }
    // Column files: header now, count patched after the last chunk.
    let mut cfs = Vec::with_capacity(nc);
    for (c, n) in set.names.iter().enumerate() {
        let mut cf =
            File::create(format!("{tmp}/{n}")).ctx("pq_stream: col")?;
        if lts[c] == KS {
            hdr_syms(&mut cf, 0)?;
        } else {
            hdr_fixed(&mut cf, lts[c])?;
        }
        cfs.push(cf);
    }
    let mut sy = SymCache::new();
    let mut total = 0i64;
    for rg in 0..s0.rg_rows.len() {
        // A fresh reader per row group — with_row_groups is what bounds
        // memory to one group — but over the footer parsed at open.
        let f = File::open(&s0.path).ctx(&format!("{who}: {}", s0.path))?;
        let rdr =
            ParquetRecordBatchReaderBuilder::new_with_metadata(
                f,
                s0.md.clone(),
            )
            .with_row_groups(vec![rg])
            .with_batch_size(BATCH_ROWS)
            .build()
            .ctx(who)?;
        for b in rdr {
            let b = b.ctx(who)?;
            let nr = b.num_rows();
            unsafe {
                for c in 0..nc {
                    // Reuse the read-path converter for ONE batch: a
                    // transient L vector, spilled to disk, released.
                    let col = ktn(lts[c] as i32, nr as i64);
                    let r = fill_col(lts[c], col, 0, b.column(c), &mut sy)
                        .and_then(|_| {
                            append_chunk(&mut cfs[c], lts[c], col, nr)
                        });
                    r0(col);
                    r?;
                }
            }
            total += nr as i64;
        }
    }
    for c in 0..nc {
        patch_count(&mut cfs[c], lts[c] == KS, total)?;
    }
    drop(cfs);
    drop(df);
    // Every count is patched, so the directory is a table now: replace
    // dst with it.  Removing first, then renaming, is what `pq_write`
    // does for the same reason — rename(2) will not put a directory
    // onto a non-empty one.
    let _ = std::fs::remove_dir_all(dst);                                       // ENOENT is the normal case
    std::fs::rename(&tmp, dst).ctx(&format!("{who}: {dst}"))?;
    scratch.keep();
    Ok(total)
}
