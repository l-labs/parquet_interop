//! write — L table → Parquet file.
//!
//! Each L column becomes one Arrow array; the file is written with
//! zstd compression, dictionary encoding (the win for symbol columns)
//! and 1M-row row groups.  Within each row group every COLUMN is
//! encoded + compressed on its own thread (ArrowColumnWriter), then
//! the finished chunks are stitched into the file in schema order —
//! same output bytes as the serial ArrowWriter, minus the wall time.
//!
//! Performance model: numeric columns are built by ONE bulk copy of
//! the K payload into an Arrow values buffer (epoch shifts applied as
//! a vectorized map during the copy); the null bitmap comes from one
//! branchless sentinel-compare pass and is dropped entirely when no
//! sentinel occurs.  Symbol columns resolve each DISTINCT interned
//! pointer once (pointer-keyed cache) instead of strlen+utf8-checking
//! every row.
//!
//! Null policy mirrors arrow_interop's writer: 0Ni/0Nj/NaN(f64) become
//! Parquet nulls; short/byte/real/bool/symbol columns are written fully
//! valid (L gives those types no null concept at this boundary — the
//! empty symbol round-trips as the empty string, NOT as null).

use crate::ffi::*;
use crate::Ctx;
use arrow::array::*;
use arrow::buffer::{BooleanBuffer, NullBuffer, ScalarBuffer};
use arrow::datatypes::*;
use arrow::ipc::writer::{
    DictionaryTracker, IpcDataGenerator, IpcWriteOptions,
};
use parquet::arrow::arrow_writer::{
    compute_leaves, get_column_writers, ArrowColumnChunk,
};
use parquet::arrow::{ArrowSchemaConverter, ARROW_SCHEMA_META_KEY};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
use parquet::format::KeyValue;
use std::collections::HashMap;
use std::ffi::CStr;
use std::fs::File;
use std::sync::Arc;

/// Rows per Parquet row group: big enough for good compression and
/// column-chunk locality, small enough to bound reader memory.
const ROW_GROUP: usize = 1 << 20;

/// L payload → Arrow primitive array with no L null sentinel: one bulk
/// copy of `n` elements from the typed payload pointer, no bitmap.
unsafe fn col_raw<A: ArrowPrimitiveType>(
    src: *const A::Native,
    n: usize,
) -> ArrayRef {
    let v = std::slice::from_raw_parts(src, n).to_vec();
    Arc::new(PrimitiveArray::<A>::new(ScalarBuffer::from(v), None))
}

/// L payload → Arrow primitive array, bulk path: one vectorized copy
/// (with `sh` added per element for epoch shifts; 0 = plain copy) plus
/// one branchless `valid` pass over the SOURCE values for the null
/// bitmap; all-valid columns carry no bitmap at all.
unsafe fn col_prim<A: ArrowPrimitiveType>(
    src: *const A::Native,
    n: usize,
    sh: A::Native,
    valid: impl Fn(&A::Native) -> bool,
) -> ArrayRef
where
    A::Native: ArrowNativeTypeOp,
{
    let s = std::slice::from_raw_parts(src, n);
    let v: Vec<_> = if sh.is_zero() {
        s.to_vec()
    } else {
        s.iter().map(|&x| x.add_wrapping(sh)).collect()
    };
    let nb = NullBuffer::new(BooleanBuffer::collect_bool(n, |i| unsafe {
        valid(s.get_unchecked(i))
    }));
    let nulls = (nb.null_count() > 0).then_some(nb);
    Arc::new(PrimitiveArray::<A>::new(ScalarBuffer::from(v), nulls))
}

/// Convert one L column K vector → Arrow array (the Arrow type is read
/// back off the array itself when building the schema field).
unsafe fn col_to_arrow(col: K, n: usize) -> Result<ArrayRef, String> {
    let t = kt(col);
    Ok(match t {
        KB => {
            let s = v_g(col);
            let bb = BooleanBuffer::collect_bool(n, |i| unsafe {
                *s.add(i) != 0
            });
            Arc::new(BooleanArray::new(bb, None))
        }
        // KG/KH/KE have no null sentinel at this boundary — written
        // fully valid (f32 NaN is written as a VALUE, no null scan),
        // matching arrow_interop.
        KG => col_raw::<UInt8Type>(v_g(col), n),
        KH => col_raw::<Int16Type>(v_h(col), n),
        KE => col_raw::<Float32Type>(v_e(col), n),
        KI => col_prim::<Int32Type>(v_i(col), n, 0, |&x| x != NI),
        KJ => col_prim::<Int64Type>(v_j(col), n, 0, |&x| x != NJ),
        // f64: NaN IS the L float null → Parquet null.
        KF => col_prim::<Float64Type>(v_f(col), n, 0., |x| !x.is_nan()),
        KS => {
            // Interned symbols are NUL-terminated and pointer-unique,
            // so strlen + utf8 validation runs once per DISTINCT symbol
            // (pointer-keyed cache); dictionary encoding in the writer
            // properties de-duplicates them on disk.
            let s = v_s(col);
            let mut seen: HashMap<usize, Option<&str>> =
                HashMap::with_capacity(1 << 10);
            let a: StringArray = (0..n)
                .map(|i| {
                    let p = *s.add(i);
                    *seen.entry(p as usize).or_insert_with(|| {
                        CStr::from_ptr(p).to_str().ok()
                    })
                })
                .collect();
            Arc::new(a)
        }
        KD => {
            // Epoch shift 2000 → 1970, null-preserving; same for KP.
            // Values whose shifted form overflows the target width
            // (dates past i32::MAX-10957 days, timestamps past ~2262)
            // are unrepresentable in Parquet — written as null, never
            // as a wrapped-around wrong value.
            col_prim::<Date32Type>(v_i(col), n, DAY2000, |&x| {
                x != NI && x <= i32::MAX - DAY2000
            })
        }
        KT => col_prim::<Time32MillisecondType>(
            v_i(col),
            n,
            0,
            |&x| x != NI,
        ),
        KP => col_prim::<TimestampNanosecondType>(
            v_j(col),
            n,
            NS2000,
            |&x| x != NJ && x <= i64::MAX - NS2000,
        ),
        // Duration keeps raw ns.  Parquet has no duration logical type;
        // arrow-rs stores Int64 plus the embedded Arrow schema, so
        // arrow readers (incl. this crate and pyarrow) restore
        // Duration[ns] and L reads it back as KN.
        KN => col_prim::<DurationNanosecondType>(
            v_j(col),
            n,
            0,
            |&x| x != NJ,
        ),
        KZ => {
            // datetime (f64 days since 2000) is WRITE-ONLY: emitted as
            // Timestamp[ns] so it reads back as KP — same one-way
            // mapping arrow_interop documents.
            let s = v_f(col);
            let a: TimestampNanosecondArray = (0..n)
                .map(|i| {
                    // NaN is the KZ null; values whose ns form falls
                    // outside i64 (pre-1677 / post-2262) are
                    // unrepresentable — null, never a wrapped value.
                    let f = *s.add(i) * 86_400e9;
                    if f.is_nan() || f < i64::MIN as f64 {
                        None
                    } else {
                        (f as i64).checked_add(NS2000)
                    }
                })
                .collect();
            Arc::new(a)
        }
        0 => return Err("nyi: list column".into()),
        _ => return Err(format!("nyi: column type {t}")),
    })
}

/// base64 (standard alphabet, padded) — the ARROW:schema key-value
/// encoding.  Hand-rolled to keep the dependency set at arrow+parquet.
fn b64(data: &[u8]) -> String {
    const A: &[u8; 64] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for ch in data.chunks(3) {
        let b = [ch[0], *ch.get(1).unwrap_or(&0), *ch.get(2).unwrap_or(&0)];
        let w = u32::from_be_bytes([0, b[0], b[1], b[2]]);
        for k in 0..4 {
            out.push(if k <= ch.len() {
                A[(w >> (18 - 6 * k) & 63) as usize] as char
            } else {
                '='
            });
        }
    }
    out
}

/// The ARROW:schema footer metadata ArrowWriter would have embedded:
/// length-prefixed legacy-IPC schema message, base64'd.  Readers use it
/// to restore Arrow-level types Parquet cannot express (Duration → KN).
fn arrow_schema_meta(schema: &Schema) -> KeyValue {
    let gen = IpcDataGenerator::default();
    let mut track = DictionaryTracker::new(true);
    let msg = gen
        .schema_to_bytes_with_dictionary_tracker(
            schema,
            &mut track,
            &IpcWriteOptions::default(),
        )
        .ipc_message;
    let mut buf = Vec::with_capacity(msg.len() + 8);
    buf.extend_from_slice(&[255, 255, 255, 255]);
    buf.extend_from_slice(&(msg.len() as u32).to_le_bytes());
    buf.extend_from_slice(&msg);
    KeyValue::new(ARROW_SCHEMA_META_KEY.to_string(), b64(&buf))
}

/// Write L table `tbl` → Parquet at `path`.  Row groups are written in
/// order; inside each, columns encode + compress in parallel.
pub fn write_table(tbl: K, path: &str) -> Result<(), String> {
    unsafe {
        if kt(tbl) != XT {
            return Err("pq_write: not a table".into());
        }
        // XT payload[0] is the column dict; dict payload is [keys;vals].
        let dict = *v_k(tbl);
        let names = *v_k(dict);
        let colsl = *v_k(dict).add(1);
        let nc = vn(names) as usize;
        let nrows = if nc > 0 { kn(*v_k(colsl)) as usize } else { 0 };
        let mut fields = Vec::with_capacity(nc);
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(nc);
        for c in 0..nc {
            let nm = CStr::from_ptr(*v_s(names).add(c))
                .to_str()
                .map_err(|_| "pq_write: column name utf8")?;
            let arr = col_to_arrow(*v_k(colsl).add(c), nrows)?;
            // nullable=true unconditionally: L cannot promise absence
            // of sentinels, and readers treat it as "may contain".
            fields.push(Field::new(nm, arr.data_type().clone(), true));
            arrays.push(arr);
        }
        let schema = Arc::new(Schema::new(fields));
        let props = Arc::new(
            WriterProperties::builder()
                .set_compression(Compression::ZSTD(ZstdLevel::default()))
                .set_dictionary_enabled(true)
                .set_max_row_group_size(ROW_GROUP)
                .set_key_value_metadata(Some(vec![arrow_schema_meta(
                    &schema,
                )]))
                .build(),
        );
        let pq_schema =
            ArrowSchemaConverter::new().convert(&schema).ctx("pq_write")?;
        let file =
            File::create(path).ctx(&format!("pq_write: {path}"))?;
        let mut fw = SerializedFileWriter::new(
            file,
            pq_schema.root_schema_ptr(),
            props.clone(),
        )
        .ctx("pq_write")?;
        let mut off = 0usize;
        while off < nrows {
            let len = ROW_GROUP.min(nrows - off);
            let writers = get_column_writers(&pq_schema, &props, &schema)
                .ctx("pq_write")?;
            // One thread per column: encode + compress this row group's
            // chunks concurrently, then append them in schema order.
            let done: Vec<Result<ArrowColumnChunk, String>> =
                std::thread::scope(|s| {
                    let hs: Vec<_> = writers
                        .into_iter()
                        .zip(schema.fields())
                        .zip(&arrays)
                        .map(|((mut w, f), a)| {
                            let sl = a.slice(off, len);
                            std::thread::Builder::new()
                                .stack_size(crate::WORKER_STACK)
                                .spawn_scoped(s, move || {
                                    let who = "pq_write";
                                    for leaf in
                                        compute_leaves(f, &sl).ctx(who)?
                                    {
                                        w.write(&leaf).ctx(who)?;
                                    }
                                    w.close().ctx(who)
                                })
                                .expect("pq_write: spawn")
                        })
                        .collect();
                    hs.into_iter()
                        .map(|h| {
                            h.join().unwrap_or_else(|_| {
                                Err("pq_write: worker panic".into())
                            })
                        })
                        .collect()
                });
            let mut rg = fw.next_row_group().ctx("pq_write")?;
            for chunk in done {
                chunk?.append_to_row_group(&mut rg).ctx("pq_write")?;
            }
            rg.close().ctx("pq_write")?;
            off += len;
        }
        fw.close().ctx("pq_write")?;
    }
    Ok(())
}
