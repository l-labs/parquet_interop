//! read — Parquet file → L table.
//!
//! arrow-rs decodes the format (all physical encodings, all supported
//! compressions, dictionary pages); this module only converts Arrow
//! arrays into freshly allocated L vectors.  Flat schemas only: nested
//! and otherwise unmapped columns raise 'nyi before any allocation.
//!
//! Performance model: primitive columns are ONE bulk memcpy of the
//! Arrow values buffer into the K payload (the layouts are identical),
//! nulls are then patched by scanning only the ZERO bits of the null
//! bitmap, and temporal epoch shifts are a vectorized in-place add on
//! the copied buffer.  Dictionary-encoded strings intern each DICT
//! VALUE once and gather by key; plain strings dedupe through a
//! per-batch cache before hitting the global intern table.  Row groups
//! decode in PARALLEL: columns are pre-sized from file metadata and
//! each worker fills a disjoint row range, so no locks and no merge.
//!
//! Null policy mirrors arrow_interop: Int32→0Ni, Int64→0Nj, Float64→0n
//! (NaN), Float32→NaN, Int16→0Nh, Boolean/UInt8→0, Utf8→empty symbol.
//! Timestamps of any unit normalize to ns and shift epoch 1970→2000
//! (KP); Date32 shifts by 10957 days (KD); Duration keeps its raw ns
//! magnitude (KN, no epoch).  Plain Utf8 AND dictionary-encoded Utf8
//! both land as interned symbol columns — same choice as arrow_interop.

use crate::ffi::*;
use crate::Ctx;
use arrow::array::*;
use arrow::buffer::NullBuffer;
use arrow::compute::cast;
use arrow::datatypes::*;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashMap;
use std::fs::File;
use std::os::raw::c_char;

/// Rows decoded per Arrow batch: large enough to amortize per-batch
/// dispatch, small enough to keep peak transient memory reasonable.
pub const BATCH_ROWS: usize = 1 << 20;

/// Map an Arrow column type to its L vector type, or 'nyi.
pub fn l_type_of(dt: &DataType) -> Result<i16, String> {
    Ok(match dt {
        DataType::Boolean => KB,
        DataType::Int8 | DataType::UInt8 => KG,
        DataType::Int16 => KH,
        DataType::Int32 => KI,
        DataType::Int64 => KJ,
        DataType::Float32 => KE,
        DataType::Float64 => KF,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => KS,
        DataType::Dictionary(_, v) if l_type_of(v) == Ok(KS) => KS,
        DataType::Timestamp(_, _) => KP,
        DataType::Date32 => KD,
        DataType::Time32(TimeUnit::Millisecond) => KT,
        DataType::Duration(_) => KN,
        other => return Err(format!("nyi: column type {other}")),
    })
}

/// Call `f(i)` for every NULL row index: scan the validity bitmap a
/// word at a time and visit only the zero bits, so all-valid columns
/// cost one pass of 64-bit compares and nothing per element.
fn each_null(nb: &NullBuffer, mut f: impl FnMut(usize)) {
    let b = nb.inner();
    let bc = b.inner().bit_chunks(b.offset(), b.len());
    let mut base = 0usize;
    for w in bc.iter() {
        let mut inv = !w;
        while inv != 0 {
            f(base + inv.trailing_zeros() as usize);
            inv &= inv - 1;
        }
        base += 64;
    }
    let rl = bc.remainder_len();
    if rl > 0 {
        let mut inv = !bc.remainder_bits() & ((1u64 << rl) - 1);
        while inv != 0 {
            f(base + inv.trailing_zeros() as usize);
            inv &= inv - 1;
        }
    }
}

/// Bulk-copy a primitive Arrow column into the L payload at row `off`
/// (the layouts are identical), apply the vectorized in-place epoch
/// shift `sh` (0 = none), then patch nulls to `nullv` by visiting only
/// the null bitmap's zero bits.
unsafe fn fill_prim<A: ArrowPrimitiveType>(
    arr: &ArrayRef,
    dst: *mut A::Native,
    off: usize,
    nullv: A::Native,
    sh: A::Native,
) -> Result<(), String>
where
    A::Native: ArrowNativeTypeOp,
{
    let a = arr
        .as_any()
        .downcast_ref::<PrimitiveArray<A>>()
        .ok_or("pq_read: unexpected array layout")?;
    let n = a.len();
    let d = dst.add(off);
    std::ptr::copy_nonoverlapping(a.values().as_ptr(), d, n);
    if !sh.is_zero() {
        // Epoch shift must not wrap: an instant below the L epoch's
        // representable floor (ns timestamps before ~1707-09, dates
        // before i32::MIN+10957 days) becomes the L null, never a
        // wrapped-around wrong value.
        for v in std::slice::from_raw_parts_mut(d, n) {
            *v = v.sub_checked(sh).unwrap_or(nullv);
        }
    }
    if let Some(nb) = a.nulls() {
        each_null(nb, |i| unsafe { *d.add(i) = nullv });
    }
    Ok(())
}

/// Borrow `arr` as Arrow type `dt`: an O(1) clone of the Arc when it
/// already is, one Arrow cast otherwise (unit / tz / Utf8 flavors).
fn to_type(
    arr: &ArrayRef,
    dt: DataType,
    who: &str,
) -> Result<ArrayRef, String> {
    if arr.data_type() == &dt {
        Ok(arr.clone())
    } else {
        cast(arr, &dt).ctx(who)
    }
}

/// Dictionary-encoded strings → symbols: intern each DICTIONARY value
/// exactly once (the global intern table is touched O(cardinality),
/// not O(rows)), then gather interned pointers through the key array.
unsafe fn fill_sym_dict<T: ArrowDictionaryKeyType>(
    a: &DictionaryArray<T>,
    col: K,
    off: usize,
) -> Result<(), String> {
    let dv = to_type(a.values(), DataType::Utf8, "pq_read: string")?;
    let sa = dv
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("pq_read: string layout")?;
    let empty = intern(b"");
    let tbl: Vec<*mut c_char> = (0..sa.len())
        .map(|k| {
            if sa.is_null(k) {
                empty
            } else {
                intern(sa.value(k).as_bytes())
            }
        })
        .collect();
    let keys = a.keys();
    let d = v_s(col);
    for (i, k) in keys.values().iter().enumerate() {
        // get() guards null slots whose key bits may be out of range.
        *d.add(off + i) = *tbl.get(k.as_usize()).unwrap_or(&empty);
    }
    if let Some(nb) = keys.nulls() {
        each_null(nb, |i| unsafe { *d.add(off + i) = empty });
    }
    Ok(())
}

/// Plain strings → symbols: every distinct string is interned ONCE per
/// batch via a local cache — interning goes through a global table, so
/// skipping repeats is the whole ballgame on low-cardinality columns.
/// Null strings intern as the empty symbol (`), L's null.
unsafe fn fill_sym_plain(
    arr: &ArrayRef,
    col: K,
    off: usize,
) -> Result<(), String> {
    let dv = to_type(arr, DataType::Utf8, "pq_read: string")?;
    let a = dv
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("pq_read: string layout")?;
    let d = v_s(col);
    let empty = intern(b"");
    let mut seen: HashMap<&str, *mut c_char> =
        HashMap::with_capacity(1 << 12);
    // Probe the first PROBE rows through the cache; if they turn out
    // mostly UNIQUE the cache only doubles the hashing, so drop it and
    // intern directly for the rest of the batch.
    const PROBE: usize = 1 << 16;
    let mut cached = true;
    for i in 0..a.len() {
        *d.add(off + i) = if a.is_null(i) {
            empty
        } else {
            let s = a.value(i);
            if cached {
                *seen.entry(s).or_insert_with(|| intern(s.as_bytes()))
            } else {
                intern(s.as_bytes())
            }
        };
        if cached && i + 1 == PROBE && seen.len() * 4 > PROBE * 3 {
            cached = false;
            seen = HashMap::new();
        }
    }
    Ok(())
}

/// Copy one Arrow batch column into the pre-allocated L column `col`
/// starting at row `off`.  `lt` is the already-validated L target type.
unsafe fn fill_col(
    lt: i16,
    col: K,
    off: usize,
    arr: &ArrayRef,
) -> Result<(), String> {
    match lt {
        KB => {
            // Arrow packs bools 1 bit/value; L stores 1 byte/value.
            let a = arr
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or("pq_read: bool layout")?;
            let d = v_g(col);
            let vb = a.values();
            for i in 0..a.len() {
                *d.add(off + i) = vb.value(i) as u8;
            }
            if let Some(nb) = a.nulls() {
                each_null(nb, |i| unsafe { *d.add(off + i) = 0 });
            }
        }
        KG => match arr.data_type() {
            // Signed Int8 keeps its two's-complement BITS in the L byte
            // column (same as a C memcpy would) rather than saturating.
            DataType::Int8 => fill_prim::<Int8Type>(
                arr,
                v_g(col) as *mut i8,
                off,
                0,
                0,
            )?,
            _ => fill_prim::<UInt8Type>(arr, v_g(col), off, 0, 0)?,
        },
        KH => fill_prim::<Int16Type>(arr, v_h(col), off, NH, 0)?,
        KI => fill_prim::<Int32Type>(arr, v_i(col), off, NI, 0)?,
        KJ => fill_prim::<Int64Type>(arr, v_j(col), off, NJ, 0)?,
        KE => fill_prim::<Float32Type>(arr, v_e(col), off, f32::NAN, 0.)?,
        KF => fill_prim::<Float64Type>(arr, v_f(col), off, f64::NAN, 0.)?,
        KP => {
            // Normalize any unit/tz to naive ns (the tz annotation is
            // dropped — the values are the same instants), then bulk
            // copy + vector epoch shift 1970→2000.
            let ns = to_type(
                arr,
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                "pq_read: timestamp",
            )?;
            fill_prim::<TimestampNanosecondType>(
                &ns,
                v_j(col),
                off,
                NJ,
                NS2000,
            )?
        }
        KD => fill_prim::<Date32Type>(arr, v_i(col), off, NI, DAY2000)?,
        KT => {
            fill_prim::<Time32MillisecondType>(arr, v_i(col), off, NI, 0)?
        }
        KN => {
            // Duration has no epoch: only unit normalization to ns.
            let ns = to_type(
                arr,
                DataType::Duration(TimeUnit::Nanosecond),
                "pq_read: duration",
            )?;
            fill_prim::<DurationNanosecondType>(&ns, v_j(col), off, NJ, 0)?
        }
        KS => match arr.data_type() {
            DataType::Dictionary(_, _) => downcast_dictionary_array!(
                arr => fill_sym_dict(arr, col, off)?,
                _ => return Err("pq_read: dict layout".into())
            ),
            _ => fill_sym_plain(arr, col, off)?,
        },
        _ => return Err("pq_read: internal type dispatch".into()),
    }
    Ok(())
}

/// One-batch converter for the streaming path: fill a whole fresh L
/// column (offset 0) from a single Arrow batch column.
pub unsafe fn fill_one(
    lt: i16,
    col: K,
    arr: &ArrayRef,
) -> Result<(), String> {
    fill_col(lt, col, 0, arr)
}

/// Decode row groups `g0..g1` of `path` into the shared pre-allocated
/// columns `cols`, starting at absolute row `off0`.  Each worker owns a
/// disjoint row range, so plain pointer stores need no synchronization;
/// symbol interning is the host's own thread-safe global table.
fn fill_groups(
    path: &str,
    lts: &[i16],
    cols: &[K],
    g0: usize,
    g1: usize,
    off0: usize,
) -> Result<(), String> {
    let f = File::open(path).ctx(&format!("pq_read: {path}"))?;
    let rdr = ParquetRecordBatchReaderBuilder::try_new(f)
        .ctx("pq_read")?
        .with_row_groups((g0..g1).collect())
        .with_batch_size(BATCH_ROWS)
        .build()
        .ctx("pq_read")?;
    let mut off = off0;
    for b in rdr {
        let b = b.ctx("pq_read")?;
        for c in 0..cols.len() {
            unsafe { fill_col(lts[c], cols[c], off, b.column(c))? };
        }
        off += b.num_rows();
    }
    Ok(())
}

/// Read `path` (Parquet) → L table.  Columns are pre-sized from file
/// metadata, then row groups are decoded and bulk-copied in parallel,
/// one contiguous row-group span (and row range) per worker thread.
pub fn read_table(path: &str) -> Result<K, String> {
    let file = File::open(path).ctx(&format!("pq_read: {path}"))?;
    let bld =
        ParquetRecordBatchReaderBuilder::try_new(file).ctx("pq_read")?;
    let schema = bld.schema().clone();
    let mut lts = Vec::with_capacity(schema.fields().len());
    for f in schema.fields() {
        // Validate the WHOLE schema before allocating anything so a
        // nested column rejects cleanly with zero cleanup to do.
        lts.push(l_type_of(f.data_type())?);
    }
    let meta = bld.metadata().clone();
    drop(bld);
    let n_rg = meta.num_row_groups();
    let rg_rows: Vec<usize> =
        (0..n_rg).map(|g| meta.row_group(g).num_rows() as usize).collect();
    let total: usize = rg_rows.iter().sum();
    if total > i32::MAX as usize {
        // ktn takes an i32 count — refuse rather than truncate.
        return Err("pq_read: >2^31 rows".into());
    }
    let nc = schema.fields().len();
    unsafe {
        let names = ktn(KS as i32, nc as i32);
        let cols = ktn(0, nc as i32);
        let mut colv = Vec::with_capacity(nc);
        for (c, f) in schema.fields().iter().enumerate() {
            *v_s(names).add(c) = intern(f.name().as_bytes());
            // Insert each column into the list IMMEDIATELY so a later
            // error can free everything with one r0 of each container.
            let col = ktn(lts[c] as i32, total as i32);
            *v_k(cols).add(c) = col;
            colv.push(col);
        }
        if total > 0 && n_rg > 0 {
            let nthr = std::thread::available_parallelism()
                .map(|v| v.get())
                .unwrap_or(1)
                .min(n_rg);
            let res: Vec<Result<(), String>> = std::thread::scope(|s| {
                let mut hs = Vec::with_capacity(nthr);
                for t in 0..nthr {
                    // Contiguous row-group span per worker; its start
                    // row is the prefix sum of the groups before it.
                    let g0 = t * n_rg / nthr;
                    let g1 = (t + 1) * n_rg / nthr;
                    let off0: usize = rg_rows[..g0].iter().sum();
                    let (lts, colv) = (&lts, &colv);
                    let h = std::thread::Builder::new()
                        .stack_size(crate::WORKER_STACK)
                        .spawn_scoped(s, move || {
                            fill_groups(path, lts, colv, g0, g1, off0)
                        })
                        .expect("pq_read: spawn");
                    hs.push(h);
                }
                hs.into_iter()
                    .map(|h| {
                        h.join().unwrap_or_else(|_| {
                            Err("pq_read: worker panic".into())
                        })
                    })
                    .collect()
            });
            for r in res {
                if let Err(e) = r {
                    r0(names);
                    r0(cols);
                    return Err(e);
                }
            }
        }
        Ok(xT(xD(names, cols)))
    }
}
