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

use crate::ffi::*;
use crate::read::{l_type_of, BATCH_ROWS};
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

/// Start a symbol column (or .d) file: 0xFF01 header, count 0 for now.
fn hdr_syms(f: &mut File) -> Result<(), String> {
    let mut h = [0u8; 8];
    h[0] = 0xFF;
    h[1] = 0x01;
    h[2..4].copy_from_slice(&KS.to_le_bytes());
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
    let file = File::open(src).ctx(&format!("pq_stream: {src}"))?;
    let bld = ParquetRecordBatchReaderBuilder::try_new(
        file.try_clone().ctx("pq_stream")?,
    )
    .ctx("pq_stream")?;
    let schema = bld.schema().clone();
    let n_rg = bld.metadata().num_row_groups();
    let total_meta: i64 = (0..n_rg)
        .map(|g| bld.metadata().row_group(g).num_rows())
        .sum();
    if total_meta > i32::MAX as i64 {
        // The 0xFF01 symbol header stores an i32 count and L vectors
        // are 2^31-bounded — refuse rather than write a corrupt splay.
        return Err("pq_stream: >2^31 rows".into());
    }
    drop(bld);
    let nc = schema.fields().len();
    let mut lts = Vec::with_capacity(nc);
    for f in schema.fields() {
        // Validate the whole schema before touching the filesystem.
        lts.push(l_type_of(f.data_type())?);
    }
    std::fs::create_dir_all(dst).ctx(&format!("pq_stream: {dst}"))?;
    // .d — the splay manifest: a symbol vector of column names.
    let mut df = File::create(format!("{dst}/.d"))
        .ctx("pq_stream: .d")?;
    hdr_syms(&mut df)?;
    for f in schema.fields() {
        df.write_all(f.name().as_bytes())
            .and_then(|_| df.write_all(&[0]))
            .ctx("pq_stream: .d")?;
    }
    patch_count(&mut df, true, nc as i64)?;
    // Column files: header now, count patched after the last chunk.
    let mut cfs = Vec::with_capacity(nc);
    for (c, f) in schema.fields().iter().enumerate() {
        let mut cf = File::create(format!("{dst}/{}", f.name()))
            .ctx("pq_stream: col")?;
        if lts[c] == KS {
            hdr_syms(&mut cf)?;
        } else {
            hdr_fixed(&mut cf, lts[c])?;
        }
        cfs.push(cf);
    }
    let mut total = 0i64;
    for rg in 0..n_rg {
        // A fresh builder per row group: build() consumes it, and
        // with_row_groups is what bounds memory to one group.
        let rdr = ParquetRecordBatchReaderBuilder::try_new(
            file.try_clone().ctx("pq_stream")?,
        )
        .ctx("pq_stream")?
        .with_row_groups(vec![rg])
        .with_batch_size(BATCH_ROWS)
        .build()
        .ctx("pq_stream")?;
        for b in rdr {
            let b = b.ctx("pq_stream")?;
            let nr = b.num_rows();
            unsafe {
                for c in 0..nc {
                    // Reuse the read-path converter for ONE batch: a
                    // transient L vector, spilled to disk, released.
                    let col = ktn(lts[c] as i32, nr as i32);
                    let r = crate::read::fill_one(
                        lts[c],
                        col,
                        b.column(c),
                    )
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
    Ok(total)
}
