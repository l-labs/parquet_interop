//! read — Parquet row groups → L table columns.
//!
//! arrow-rs decodes the format (all physical encodings, all supported
//! compressions, dictionary pages); this module converts Arrow arrays
//! into freshly allocated L vectors and schedules the work.  Flat
//! schemas only: a column with no L type raises 'nyi — at open time if
//! it is projected, never otherwise.
//!
//! Performance model, in the order the costs actually rank:
//!
//!   * PROJECTION.  `ProjectionMask::roots` + `with_projection` means
//!     unrequested column chunks are never read off disk, never
//!     decompressed and never allocated.  Two columns out of eight is
//!     a quarter of the bytes, not a filter applied afterwards.
//!   * SYMBOLS.  meta.rs asks the reader for `Dictionary(Int32, Utf8)`,
//!     so a symbol column arrives as (dictionary page, index vector).
//!     `fill_sym_dict` interns each DISTINCT value once and gathers
//!     pointers through the keys — O(cardinality) calls into the host
//!     intern table instead of O(rows), and no per-row hashing at all.
//!     The PLAIN fallback goes through `SymCache`, a direct-mapped
//!     pointer cache, for the same reason.
//!   * BULK COPY.  A primitive column is ONE memcpy of the Arrow values
//!     buffer into the K payload (the layouts are identical); nulls are
//!     then patched by visiting only the ZERO bits of the null bitmap,
//!     and epoch shifts are a vectorized in-place add on the copy.
//!   * SCHEDULING.  ONE work pool over (file × row group).  Columns are
//!     pre-sized from every footer first, so each task owns a disjoint
//!     row range of shared vectors: no locks, no merge, no per-file
//!     thread pool that would leave 47 cores idle on the last file.
//!
//! Null policy: Int32→0Ni, Int64→0Nj, Float64→0n (NaN), Float32→NaN,
//! Int16→0Nh, Boolean/UInt8→0, Utf8→empty symbol.  Timestamps of any
//! unit normalize to ns and shift epoch 1970→2000 (KP); Date32 shifts
//! by 10957 days (KD); Duration keeps its raw ns magnitude (KN).

use crate::ffi::*;
use crate::meta::{Set, Src};
use crate::pool::nthreads;
use crate::Ctx;
use arrow::array::*;
use arrow::buffer::NullBuffer;
use arrow::compute::cast;
use arrow::datatypes::*;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use parquet::basic::Type as PhysType;
use parquet::data_type::{
    DataType as PqType, DoubleType as PqF64, FloatType as PqF32,
    Int32Type as PqI32, Int64Type as PqI64,
};
use parquet::file::properties::{ReaderProperties, ReaderPropertiesPtr};
use parquet::file::reader::RowGroupReader;
use parquet::column::page::{Page, PageReader};
use parquet::file::serialized_reader::{
    SerializedPageReader, SerializedRowGroupReader,
};
use std::fs::File;
use std::mem::ManuallyDrop;
use std::sync::OnceLock;
use std::os::raw::c_char;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed};
use std::sync::{Arc, Mutex};

/// Rows decoded per Arrow batch.  MEASURED, not guessed: at 1M rows a
/// batch's per-column buffers are ~8 MB, which glibc serves straight
/// from mmap and hands back with munmap — 48 workers churning that pay
/// the kernel's mmap lock and the page zeroing a fresh mapping brings,
/// which is what `clear_page_erms` was doing in the profile.  Smaller
/// buffers are recycled from the arena instead.  100M rows on the
/// 48-core box, min-of-5, fresh process, interleaved:
///
///   batch     8 columns zstd     2-column projection
///   1M            0.544 s              0.115 s
///   256K          0.329 s              0.075 s
///   64K           0.292 s              0.061 s
///   16K           0.244 s              0.078 s
///
/// 16K is where the curve TURNS: the wide read keeps gaining, but the
/// projected one loses 28% to per-batch overhead it no longer has the
/// rows to amortize.  64K is the value that wins both, and it caps
/// `pq_stream`'s transient DRAM as a second win.
pub const BATCH_ROWS: usize = 1 << 16;

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

// ── Symbol interning cache ──────────────────────────────────────────

/// Cache slots: 4096 × 16 B = 64 KiB, L2-resident on every target and
/// wide enough for the cardinalities Parquet dictionaries carry.
const SLOTS: usize = 1 << 12;

/// Strings at least this long bypass the cache: verifying a hit would
/// cost more than the intern it saves, and `Slot::len` is a u32.
const LONG: usize = 1 << 12;

#[derive(Clone, Copy)]
struct Slot {
    tag: u32,                                                                   // high hash bits: fast reject
    len: u32,                                                                   // byte length of the symbol
    sym: *mut c_char,                                                           // interned pointer; null = empty slot
}

/// Direct-mapped string → interned-symbol cache.
///
/// The host's `sn` is a global intern table; calling it once per ROW is
/// what made symbol columns 30% of a read.  This answers repeats
/// without touching it.  A slot VERIFIES against the INTERNED bytes —
/// permanent, NUL-terminated host memory — not against the Arrow batch
/// the string came from, so one cache safely outlives every batch and
/// every row group a worker decodes: no clearing, and no pointer into
/// a freed decode buffer can ever be dereferenced.
pub struct SymCache {
    slots: Box<[Slot]>,
    /// Dictionary pages this worker has already resolved, MRU first.
    /// A row group's dictionary is ONE Arc shared by all of its 64K-row
    /// batches, so without this the whole dictionary is re-probed per
    /// batch — 16 rebuilds of a 2000-entry table per 1M-row row group,
    /// and a 200K-entry dictionary costs more probes than the file has
    /// rows.  The Arc is kept ALIVE in the entry: identity is a pointer
    /// compare, and a freed dictionary could otherwise be replaced by a
    /// different one at the same address.  Four entries covers a table
    /// with four symbol columns interleaved batch by batch.
    dicts: Vec<(ArrayRef, Vec<*mut c_char>)>,
    /// The same memo for the CODES path: dictionary → union ids.
    luts: Vec<(ArrayRef, Option<Vec<u32>>)>,
}

/// Dictionaries memoized per worker (see `SymCache::dicts`).
const DICTS: usize = 4;

impl SymCache {
    pub fn new() -> Self {
        let empty = Slot { tag: 0, len: 0, sym: std::ptr::null_mut() };
        Self {
            slots: vec![empty; SLOTS].into_boxed_slice(),
            dicts: Vec::with_capacity(DICTS),
            luts: Vec::with_capacity(DICTS),
        }
    }

    /// This dictionary's values as UNION ids, built once per dictionary.
    /// None when a value is not in the union at all — the caller then
    /// abandons the pair for that column.
    unsafe fn lut(
        &mut self,
        vals: &ArrayRef,
        cd: &Codes,
    ) -> Result<Option<&[u32]>, String> {
        if let Some(i) =
            self.luts.iter().position(|(v, _)| Arc::ptr_eq(v, vals))
        {
            self.luts[..=i].rotate_right(1);
            return Ok(self.luts[0].1.as_deref());
        }
        let sa = utf8_of(vals)?;
        let mut t = Vec::with_capacity(sa.len());
        let mut ok = true;
        for k in 0..sa.len() {
            let b: &[u8] =
                if sa.is_null(k) { b"" } else { sa.value(k).as_bytes() };
            match cd.ix.get(b) {
                Some(&id) => t.push(id),
                None => {
                    ok = false;
                    break;
                }
            }
        }
        self.luts.truncate(DICTS - 1);
        self.luts.insert(0, (vals.clone(), ok.then_some(t)));
        Ok(self.luts[0].1.as_deref())
    }

    /// The dictionary value → interned symbol table for `vals`, built
    /// once per dictionary rather than once per batch.
    unsafe fn dict_table(
        &mut self,
        vals: &ArrayRef,
        empty: *mut c_char,
    ) -> Result<&[*mut c_char], String> {
        if let Some(i) =
            self.dicts.iter().position(|(v, _)| Arc::ptr_eq(v, vals))
        {
            self.dicts[..=i].rotate_right(1);                                   // MRU first
            return Ok(&self.dicts[0].1);
        }
        let sa = utf8_of(vals)?;
        let t: Vec<*mut c_char> = (0..sa.len())
            .map(|k| {
                if sa.is_null(k) {
                    empty
                } else {
                    self.get(sa.value(k).as_bytes())
                }
            })
            .collect();
        self.dicts.truncate(DICTS - 1);
        self.dicts.insert(0, (vals.clone(), t));
        Ok(&self.dicts[0].1)
    }

    /// Interned symbol for `b`, from the cache when it is already there.
    #[inline]
    pub unsafe fn get(&mut self, b: &[u8]) -> *mut c_char {
        if b.len() >= LONG {
            return intern(b);
        }
        let h = fxh(b);
        let s = &mut self.slots[(h as usize) & (SLOTS - 1)];
        let tag = (h >> 32) as u32;
        if !s.sym.is_null()
            && s.tag == tag
            && s.len as usize == b.len()
            && std::slice::from_raw_parts(s.sym as *const u8, b.len()) == b
        {
            return s.sym;
        }
        let p = intern(b);
        *s = Slot { tag, len: b.len() as u32, sym: p };
        p
    }
}

/// FxHash-style multiplicative hash: one multiply per 8 bytes, against
/// the SipHash a std `HashMap` would run over every row.
#[inline]
fn fxh(b: &[u8]) -> u64 {
    const M: u64 = 0x517c_c1b7_2722_0a95;
    let mut h = b.len() as u64;
    let mut c = b;
    while c.len() >= 8 {
        let w = u64::from_le_bytes(c[..8].try_into().unwrap());
        h = (h ^ w).wrapping_mul(M);
        c = &c[8..];
    }
    if !c.is_empty() {
        let mut t = [0u8; 8];
        t[..c.len()].copy_from_slice(c);
        h = (h ^ u64::from_le_bytes(t)).wrapping_mul(M);
    }
    h ^ (h >> 29)
}

// ── Arrow batch → L column ──────────────────────────────────────────

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
#[cfg_attr(feature = "hotpath", hotpath::measure)]
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
        // wrapped-around wrong value.  Spelled as compare-and-select
        // rather than `sub_checked(..).unwrap_or(..)`, because an
        // Option per element does not vectorize and this does: `nullv`
        // IS the type's minimum for every column that reaches here
        // (0Ni/0Nj), so `nullv + sh` is exactly the floor below which
        // the subtraction would wrap, and `sh` is always positive.
        let lo = nullv.add_wrapping(sh);
        for v in std::slice::from_raw_parts_mut(d, n) {
            let w = v.sub_wrapping(sh);
            *v = if v.is_lt(lo) { nullv } else { w };
        }
    }
    if let Some(nb) = a.nulls() {
        each_null(nb, |i| unsafe { *d.add(i) = nullv });
    }
    Ok(())
}

/// Borrow `arr` as a `StringArray`: one Arrow cast when it is not Utf8
/// already, then the downcast.  The clone is an Arc bump — a
/// `StringArray` is two buffers, a length and an offset.
fn utf8_of(arr: &ArrayRef) -> Result<StringArray, String> {
    to_type(arr, DataType::Utf8, "pq_read: string")?
        .as_any()
        .downcast_ref::<StringArray>()
        .cloned()
        .ok_or_else(|| "pq_read: string layout".into())
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
/// exactly once (through the cache, so a dictionary repeated across
/// row groups costs a pointer compare rather than an intern), then
/// gather interned pointers through the key array.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
unsafe fn fill_sym_dict<T: ArrowDictionaryKeyType>(
    a: &DictionaryArray<T>,
    col: K,
    off: usize,
    sy: &mut SymCache,
) -> Result<(), String> {
    let empty = sy.get(b"");
    let tbl = sy.dict_table(a.values(), empty)?;
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

/// Plain strings → symbols, one `SymCache` probe per row.  This is the
/// fallback path: it runs only when the dictionary hint was rejected
/// or the column was genuinely written without dictionary pages.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
unsafe fn fill_sym_plain(
    arr: &ArrayRef,
    col: K,
    off: usize,
    sy: &mut SymCache,
) -> Result<(), String> {
    let a = utf8_of(arr)?;
    let d = v_s(col);
    let empty = sy.get(b"");
    for i in 0..a.len() {
        *d.add(off + i) = if a.is_null(i) {
            empty
        } else {
            sy.get(a.value(i).as_bytes())
        };
    }
    Ok(())
}

/// Copy one Arrow batch column into the pre-allocated L column `col`
/// starting at row `off`.  `lt` is the already-validated L target type.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub(crate) unsafe fn fill_col(
    lt: i16,
    col: K,
    off: usize,
    arr: &ArrayRef,
    sy: &mut SymCache,
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
            DataType::Int8 => {
                fill_prim::<Int8Type>(arr, v_g(col) as *mut i8, off, 0, 0)?
            }
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
                arr => fill_sym_dict(arr, col, off, sy)?,
                _ => return Err("pq_read: dict layout".into())
            ),
            _ => fill_sym_plain(arr, col, off, sy)?,
        },
        _ => return Err("pq_read: internal type dispatch".into()),
    }
    Ok(())
}

// ── Zero-copy columns ───────────────────────────────────────────────

/// A column whose Parquet PHYSICAL type and L vector type are the same
/// machine type, so the page decoder can write straight into the K
/// payload — no Arrow array, no allocation, no copy.  The value is the
/// width the decoder uses; `I64` also carries the NANOSECONDS PER UNIT
/// a temporal column needs (1 for everything else).  The epoch shift
/// and the null sentinel come from the L type separately.
#[derive(Clone, Copy, PartialEq)]
enum Raw {
    I32,
    I64(i64),
    F32,
    F64,
}

/// Nanoseconds per time unit — the normalization the Arrow path gets
/// out of `cast`, and which `patch` folds into its own pass.
pub fn ns_per(u: &TimeUnit) -> i64 {
    match u {
        TimeUnit::Second => 1_000_000_000,
        TimeUnit::Millisecond => 1_000_000,
        TimeUnit::Microsecond => 1_000,
        TimeUnit::Nanosecond => 1,
    }
}

/// Is this column decodable straight into K?  BOTH types have to line
/// up: an INT64 column of any timestamp unit is the 8 bytes L wants
/// once each value is multiplied out to ns, which `patch` does in the
/// pass it was already making; the same column stored as INT96 is not.
/// Anything this returns None for keeps the Arrow path, which is still
/// the only path for symbols, booleans, bytes and shorts.
///
/// The unit is part of the ANSWER, not just a filter: two files of one
/// read agree when they agree on the L type, so Timestamp(us) can meet
/// Timestamp(ns) in one `pq_read`, and `select` compares whole `Raw`s
/// precisely so a scale is never taken from the wrong file.
fn raw_of(dt: &DataType, pt: PhysType) -> Option<Raw> {
    Some(match (dt, pt) {
        (DataType::Int32, PhysType::INT32)
        | (DataType::Date32, PhysType::INT32)
        | (DataType::Time32(TimeUnit::Millisecond), PhysType::INT32) => {
            Raw::I32
        }
        (DataType::Int64, PhysType::INT64) => Raw::I64(1),
        (DataType::Timestamp(u, _), PhysType::INT64)
        | (DataType::Duration(u), PhysType::INT64) => Raw::I64(ns_per(u)),
        (DataType::Float32, PhysType::FLOAT) => Raw::F32,
        (DataType::Float64, PhysType::DOUBLE) => Raw::F64,
        _ => return None,
    })
}

/// Reader properties for the raw path — no bloom filters, nothing to
/// configure; built once for the process.
fn rprops() -> &'static ReaderPropertiesPtr {
    static P: OnceLock<ReaderPropertiesPtr> = OnceLock::new();
    P.get_or_init(|| Arc::new(ReaderProperties::builder().build()))
}

/// Decode column chunk `leaf` of row group `g` DIRECTLY into the K
/// payload at `[off, off+rows)`.  The values buffer is a `Vec` built
/// over the K range, and what makes that sound is an INTERNAL invariant
/// of parquet 55.2 — stated in full once, at the `ManuallyDrop` below,
/// and nowhere else.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
#[allow(clippy::too_many_arguments)]
unsafe fn raw_col(
    src: &Src,
    file: &Arc<File>,
    g: usize,
    leaf: usize,
    rt: Raw,
    lt: i16,
    col: K,
    off: usize,
    rows: usize,
    defs: &mut Vec<i16>,
    who: &str,
) -> Result<usize, String> {
    let md = src.md.metadata();
    let rgr = SerializedRowGroupReader::new(
        file.clone(),
        md.row_group(g),
        None,
        rprops().clone(),
    )
    .ctx(who)?;
    let cr = rgr.get_column_reader(leaf).ctx(who)?;
    let maxdef = md.file_metadata().schema_descr().column(leaf).max_def_level();
    let base = va(col);
    // Decoded in batches, like the Arrow path and for the same measured
    // reason: a whole row group's definition levels and the spread pass
    // over them would leave L2, while 64K rows stay in it from the
    // decode to the patch that follows it.
    macro_rules! run {
        ($pq:ty, $n:ty, $nullv:expr, $sh:expr, $mul:expr) => {{
            let mut rd = <$pq as PqType>::get_column_reader(cr)
                .ok_or_else(|| format!("{who}: column reader type"))?;
            let mut done = 0usize;
            loop {
                let want = BATCH_ROWS.min(rows - done);
                if want == 0 {
                    break;
                }
                let p = (base as *mut $n).add(off + done);
                // ManuallyDrop: the memory belongs to the host vector,
                // and nothing here may ever free or reallocate it.
                //
                // THE INVARIANT THIS RESTS ON (parquet 55.2, pinned in
                // Cargo.toml for exactly this reason): with no
                // repetition levels `read_records` reads
                // `min(remaining_records, remaining_levels)` levels per
                // page and at most that many values, and
                // `ColumnValueDecoderImpl::read` resizes the buffer by
                // exactly the count it was asked for and decodes into
                // that slice.  `want` is therefore a hard ceiling, so
                // the Vec never grows past the capacity it was born
                // with, never reallocates, and never frees host memory
                // — whatever a forged page header claims.  Re-audit
                // this function before moving off 55.2.
                let mut b =
                    ManuallyDrop::new(Vec::from_raw_parts(p, 0, want));
                defs.clear();
                let (recs, vals, _) = rd
                    .read_records(want, Some(defs), None, &mut b)
                    .ctx(who)?;
                if b.as_ptr() != p || b.len() > want {
                    // Unreachable by the argument above; if parquet-rs
                    // ever changes it, fail loudly, not silently.
                    return Err(format!("{who}: values buffer moved"));
                }
                if maxdef > 0 && vals < recs && defs.len() < recs {
                    // One definition level per RECORD is the whole
                    // basis of the spread below; fewer and `patch`
                    // would leave the values packed at the front and
                    // the tail unwritten, which is the silent-wrong
                    // -data case this path exists to avoid.
                    return Err(format!("{who}: definition levels short"));
                }
                patch(
                    p,
                    recs,
                    vals,
                    $nullv,
                    $sh,
                    $mul,
                    (maxdef > 0).then(|| &defs[..]),
                    maxdef,
                );
                done += recs;
                if recs == 0 {
                    break;
                }
            }
            done
        }};
    }
    let recs = match rt {
        Raw::I32 => {
            run!(PqI32, i32, NI, if lt == KD { DAY2000 } else { 0 }, 1)
        }
        Raw::I64(m) => {
            run!(PqI64, i64, NJ, if lt == KP { NS2000 } else { 0 }, m)
        }
        Raw::F32 => run!(PqF32, f32, f32::NAN, 0.0, 1.0),
        Raw::F64 => run!(PqF64, f64, f64::NAN, 0.0, 1.0),
    };
    // One pass over the rows just written: nulls to the L sentinel and,
    // for the two epoch-shifted types, 1970 -> 2000.  `defs` is empty
    // when the column cannot be null, and the shift is zero for every
    // type but KD and KP, so the common column pays one branch.
    Ok(recs)
}

/// Spread, unit-scale, null-fill and epoch-shift one decoded column IN
/// PLACE.
///
/// `read_records` writes only the NON-NULL values, packed at the front:
/// the arrow layer is what normally spreads them, and this path has no
/// arrow layer.  So the pass runs BACKWARDS — the last value belongs at
/// the last non-null level, and every source index is at or below its
/// destination — filling nulls as it goes, and applying the epoch shift
/// to the value it just placed.  `vals == n` means nothing was null and
/// nothing has to move.
///
/// `mul` is the unit scale (1 = none): the multiply must be CHECKED and
/// must answer the L null on overflow, because that is exactly what the
/// Arrow path's `cast(safe = true)` does — it nulls a value whose ns
/// form leaves i64 — and the two paths have to agree byte for byte.
///
/// The floor test is spelled only where a shift exists.  It must not be
/// applied to a float column: `ArrowNativeTypeOp`'s comparisons are
/// TOTAL, so every finite value is "less than" a NaN null and a plain
/// `is_lt(nullv)` would blank the whole column.
#[allow(clippy::too_many_arguments)]
#[cfg_attr(feature = "hotpath", hotpath::measure)]
unsafe fn patch<T: ArrowNativeTypeOp>(
    d: *mut T,
    n: usize,
    vals: usize,
    nullv: T,
    sh: T,
    mul: T,
    defs: Option<&[i16]>,
    maxdef: i16,
) {
    let lo = nullv.add_wrapping(sh);
    let one = mul.is_eq(T::ONE);
    let shift = |w: T| {
        let w = if one {
            w
        } else {
            match w.mul_checked(mul) {
                Ok(v) => v,
                Err(_) => return nullv,                                         // as arrow's safe cast nulls it
            }
        };
        if sh.is_zero() {
            w
        } else if w.is_lt(lo) {
            nullv                                                               // the shift would wrap below the L epoch
        } else {
            w.sub_wrapping(sh)
        }
    };
    match defs {
        Some(dl) if vals < n && dl.len() >= n => {
            let mut vp = vals;
            for i in (0..n).rev() {
                *d.add(i) = if dl[i] == maxdef {
                    vp -= 1;
                    shift(*d.add(vp))
                } else {
                    nullv
                };
            }
        }
        _ if !sh.is_zero() || !one => {
            for v in std::slice::from_raw_parts_mut(d, n) {
                *v = shift(*v);
            }
        }
        _ => {}
    }
}

// ── Symbol columns as (dictionary; codes) ───────────────────────────

/// A symbol column the caller asked for as a PAIR: the union of every
/// row group's dictionary, and one index per row into it.
///
/// Why it exists: a dictionary-encoded Parquet column already IS
/// (dictionary; codes).  Handing L 100M interned POINTERS spends 8
/// bytes and a gather per row, and the storage layer then rebuilds the
/// domain it was just given.  The pair keeps the shape the file had.
pub struct Codes {
    /// Union dictionary, interned host symbols, FIRST-SEEN order: row
    /// groups in window order, each dictionary in its own order.  An
    /// entry no row references stays (Parquet dictionaries may carry
    /// unused entries and this never scans the codes to find out).
    d: Vec<*mut c_char>,
    /// Union value bytes → id, for turning a row group's own
    /// dictionary into a lookup table at decode time.
    ix: HashMap<Vec<u8>, u32>,
    /// The id of the empty symbol, which is what BOTH a Parquet null
    /// and an empty string read as.
    empty: u32,
    /// L type of the codes vector: KG (<=256 entries), KH (<=32768) or
    /// KI.  The bound is what q can INDEX with, not what the bits hold:
    /// `D[C]` has to stay a legal expression, so KH stops at its
    /// positive range.
    lt: i16,
}

// The dictionary pointers are only ever read, and the host's symbols
// are permanent; the plan is shared by every decode worker.
unsafe impl Send for Codes {}
unsafe impl Sync for Codes {}

impl Codes {
    /// The narrowest L type that can index `n` entries.
    fn width(n: usize) -> i16 {
        match n {
            0..=256 => KG,
            257..=32768 => KH,
            _ => KI,
        }
    }
}

/// One row group of a read window: (file, row group in that file, the
/// result row its rows start at, how many rows).  Laid out before a
/// page is touched, which is what lets the workers write into shared
/// vectors with no synchronization at all.
type Rg = (usize, usize, usize, usize);

/// Every row group of `rgs` carries a dictionary page for column `c`.
fn all_dict(set: &Set, rgs: &[Rg], c: usize) -> bool {
    rgs.iter().all(|&(f, g, _, _)| set.srcs[f].chunk_dict(g, c))
}

/// One dictionary page: a hash of its entry SET, the page bytes, and a
/// span per entry into them.  One allocation for the page instead of
/// one per entry, and the hash is taken HERE, in the parallel phase, so
/// the serial merge that follows skips a repeat with one u64 compare.
///
/// The hash is over the set, not the bytes: row groups of one file
/// carry the same values in a DIFFERENT order often enough that a
/// byte-wise key deduplicates nothing (measured — it cost 2.5 ms of a
/// 19 ms read).  A collision would merge nothing and leave a value out
/// of the union, which the decode then notices and answers by reading
/// the column as plain symbols; it cannot produce a wrong code.
type DictBuf = (u64, Vec<u8>, Vec<(u32, u32)>);

/// The entries of one column chunk's dictionary page.  Only the PAGE is
/// read — no data page, no values — so this costs one small read per
/// row group, which is what lets the union be known before a single
/// code is written.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn dict_page(
    src: &Src,
    g: usize,
    c: usize,
    who: &str,
) -> Result<DictBuf, String> {
    let md = src.md.metadata();
    let f = File::open(&src.path).ctx(&format!("{who}: {}", src.path))?;
    let rg = md.row_group(g);
    let empty = || (0, Vec::new(), Vec::new());
    // `chunk` resolves the ROOT index `c` to this file's leaf; a column
    // that has no single leaf simply has no pair path.
    let Some(cc) = src.chunk(g, c) else {
        return Ok(empty());
    };
    let mut pr =
        SerializedPageReader::new(Arc::new(f), cc, rg.num_rows() as usize, None)
            .ctx(who)?;
    let Some(p) = pr.get_next_page().ctx(who)? else {
        return Ok(empty());
    };
    let Page::DictionaryPage { buf, num_values, .. } = p else {
        return Ok(empty());                                                     // no dictionary after all
    };
    // A byte-array dictionary page is PLAIN: each entry is a 4-byte
    // little-endian length followed by its bytes.  The entries stay in
    // the page BUFFER and come back as spans — a dictionary of 2000
    // values across 100 row groups would otherwise be 200_000 little
    // allocations to make and, worse, to drop.
    let buf = buf.to_vec();
    let mut out = Vec::with_capacity(num_values as usize);
    let mut i = 0usize;
    for _ in 0..num_values {
        let n = buf
            .get(i..i + 4)
            .map(|b| u32::from_le_bytes(b.try_into().unwrap()) as usize)
            .filter(|n| i + 4 + n <= buf.len())
            .ok_or_else(|| {
                format!("{who}: {}: short dictionary", src.path)
            })?;
        out.push(((i + 4) as u32, n as u32));
        i += 4 + n;
    }
    let h = out.iter().fold(
        fxh(&(out.len() as u64).to_le_bytes()),
        |h, &(o, n)| {
            h ^ fxh(&buf[o as usize..(o + n) as usize])
                .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        },
    );
    Ok((h, buf, out))
}

/// Build the union dictionary for column `c` over the window, or None
/// when the column cannot take the pair path.  The dictionary PAGES are
/// read in parallel; the union is merged in row-group ORDER, which is
/// what makes "first seen" a definition rather than a race.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn codes_plan(
    set: &Set,
    rgs: &[Rg],
    c: usize,
    who: &str,
) -> Result<Option<Codes>, String> {
    if rgs.is_empty() || !all_dict(set, rgs, c) {
        return Ok(None);
    }
    let out = crate::pool::par_map(rgs.len(), |i| {
        let (f, g, _, _) = rgs[i];
        dict_page(&set.srcs[f], g, c, who)
    })
    .ok_or_else(|| format!("{who}: dictionary panic"))?;
    let mut d: Vec<*mut c_char> = Vec::new();
    let mut ix: HashMap<Vec<u8>, u32> = HashMap::new();
    let mut nulls = false;
    let mut seen: HashSet<u64> = HashSet::new();
    for (i, r) in out.into_iter().enumerate() {
        let (h, buf, spans) = r?;
        if spans.is_empty() {
            return Ok(None);                                                    // not the dictionary shape
        }
        // Row groups of one file carry the same SET of values far more
        // often than not, and a set already in the union contributes
        // nothing: the hash was taken with the page, in PARALLEL, so a
        // repeat costs one compare here.  Hashing the entries in this
        // loop instead was 200_000 hashes on the serial path — 2.3 ms
        // of a 19 ms read, and the whole gap to the plain path.
        if seen.insert(h) {
            for &(o, n) in &spans {
                let v = &buf[o as usize..(o + n) as usize];
                if !ix.contains_key(v) {
                    ix.insert(v.to_vec(), d.len() as u32);
                    d.push(unsafe { intern(v) });
                }
            }
        }
        let (f, g, _, _) = rgs[i];
        // Statistics are the only cheap answer to "does this chunk hold
        // nulls"; without them, assume it might.
        nulls |= set.srcs[f].chunk(g, c).is_none_or(|cc| {
            cc.statistics()
                .is_none_or(|s| s.null_count_opt().is_none_or(|k| k > 0))
        });
    }
    let empty = match ix.get(&Vec::new()) {
        Some(&i) => i,
        None => {
            // The empty symbol is what a null reads as, so the union
            // needs it once the window can produce one.
            let id = d.len() as u32;
            if nulls {
                d.push(unsafe { intern(b"") });
                ix.insert(Vec::new(), id);
            }
            id
        }
    };
    let lt = Codes::width(d.len().max(empty as usize + 1));
    Ok(Some(Codes { d, ix, empty, lt }))
}

/// Fill one batch of a symbol column as CODES.  The row group's own
/// dictionary is turned into a lookup table ONCE per dictionary (the
/// same Arc every batch of a row group shares), and the per-row work is
/// one table lookup and one narrow store — no string is touched, and
/// no pointer is written.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
unsafe fn fill_codes<T: ArrowDictionaryKeyType>(
    a: &DictionaryArray<T>,
    cd: &Codes,
    col: K,
    off: usize,
    sy: &mut SymCache,
    give: &AtomicBool,
) -> Result<(), String> {
    let lut = match sy.lut(a.values(), cd)? {
        Some(l) => l,
        None => {
            // A page fell back to PLAIN and brought a value the union
            // never saw: this column cannot be a pair, and the caller
            // re-reads it as symbols.
            give.store(true, Relaxed);
            return Ok(());
        }
    };
    let keys = a.keys();
    let e = cd.empty;
    macro_rules! store {
        ($n:ty, $v:expr) => {{
            let d = $v.add(off);
            for (i, k) in keys.values().iter().enumerate() {
                *d.add(i) =
                    *lut.get(k.as_usize()).unwrap_or(&e) as $n;
            }
            if let Some(nb) = keys.nulls() {
                each_null(nb, |i| *d.add(i) = e as $n);
            }
        }};
    }
    match cd.lt {
        KG => store!(u8, v_g(col)),
        KH => store!(i16, v_h(col)),
        _ => store!(i32, v_i(col)),
    }
    Ok(())
}

/// Dispatch a batch of a symbol column to `fill_codes` on whatever
/// integer width the file chose for its dictionary keys.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
unsafe fn sym_codes(
    arr: &ArrayRef,
    cd: &Codes,
    col: K,
    off: usize,
    sy: &mut SymCache,
    give: &AtomicBool,
) -> Result<(), String> {
    // One arm per key width `ArrowDictionaryKeyType` covers.  A width
    // outside the table is NOT an error: this module's contract is that
    // a column which cannot take the pair path is read as plain symbols
    // instead, which is the same answer a PLAIN chunk gets below.
    macro_rules! keyed {
        ($k:expr; $($d:ident $t:ty),*) => {
            match $k {
                $(DataType::$d => {
                    fill_codes(as_dict::<$t>(arr)?, cd, col, off, sy, give)
                })*
                _ => {
                    give.store(true, Relaxed);
                    Ok(())
                }
            }
        };
    }
    match arr.data_type() {
        DataType::Dictionary(k, _) => keyed!(**k;
            Int8 Int8Type, Int16 Int16Type, Int32 Int32Type,
            Int64 Int64Type, UInt8 UInt8Type, UInt16 UInt16Type,
            UInt32 UInt32Type, UInt64 UInt64Type),
        // The hint was refused (a PLAIN chunk): no pair for this column.
        _ => {
            give.store(true, Relaxed);
            Ok(())
        }
    }
}

/// Borrow a batch column as a dictionary array of key type `T`.
fn as_dict<T: ArrowDictionaryKeyType>(
    arr: &ArrayRef,
) -> Result<&DictionaryArray<T>, String> {
    arr.as_any()
        .downcast_ref::<DictionaryArray<T>>()
        .ok_or_else(|| "pq_read: dictionary layout".into())
}

// ── Projection ──────────────────────────────────────────────────────

/// A resolved column projection: which file columns to decode, which L
/// types they land in, and where each RESULT column finds its source
/// inside a projected Arrow batch.
pub struct Sel {
    /// Result position → column index in the file schema.
    pub out: Vec<usize>,
    /// Result position → L vector type.
    pub lts: Vec<i16>,
    /// The selection as the FILE orders it: sorted, unique, file column
    /// indices.  A projected batch holds exactly these, in this order.
    uniq: Vec<usize>,
    /// Result position → Some(width) when the column decodes straight
    /// into K, None when it goes through Arrow.  Decided once, and only
    /// when EVERY file of the set agrees.
    raw: Vec<Option<Raw>>,
}

/// One column PART of a row group: the result positions it fills, where
/// each of them sits in ITS OWN projected batch, and the projection
/// that reads exactly those columns.  A whole read is one part; a
/// window too small to fill the machine is split into several, so the
/// columns of a single row group decode in parallel (`split`).
struct Part {
    /// Result positions this part decodes STRAIGHT into K.
    raw: Vec<usize>,
    /// Result positions this part decodes through an Arrow batch.
    outs: Vec<usize>,
    /// Batch column index of each of `outs` within this part's batch.
    bat: Vec<usize>,
    /// Per FILE, because leaf numbering is a property of that file's
    /// parquet schema — two files may agree on every L column and
    /// still differ in the leaves of an unprojected nested one.
    masks: Vec<ProjectionMask>,
}

/// Cut `sel` into parts.  A part is EITHER a set of Arrow columns (one
/// reader, one projection) OR a single zero-copy column, never both:
/// they read the same row group through different readers, so sharing a
/// task would make one wait for the other and walk the row group twice.
/// The Arrow columns are split `k` ways, which is what fills the
/// machine when a window has too few row groups of its own.
fn split(set: &Set, sel: &Sel, k: usize) -> Vec<Part> {
    let mask = |want: &[usize]| {
        set.srcs
            .iter()
            .map(|s| {
                ProjectionMask::roots(
                    s.md.parquet_schema(),
                    want.iter().copied(),
                )
            })
            .collect::<Vec<_>>()
    };
    let mut parts: Vec<Part> = (0..sel.out.len())
        .filter(|&p| sel.raw[p].is_some())
        .map(|p| Part {
            raw: vec![p],
            outs: Vec::new(),
            bat: Vec::new(),
            masks: Vec::new(),
        })
        .collect();
    let arw: Vec<usize> =
        (0..sel.out.len()).filter(|&p| sel.raw[p].is_none()).collect();
    let n = arw.len();
    if n == 0 {
        return parts;
    }
    let k = k.clamp(1, n);
    for j in 0..k {
        // Contiguous in FILE order, so each part's projection is the
        // slice of the batch its columns land in.
        let outs: Vec<usize> = arw[j * n / k..(j + 1) * n / k].to_vec();
        if outs.is_empty() {
            continue;
        }
        let want: Vec<usize> = outs.iter().map(|&p| sel.out[p]).collect();
        let mut srt = want.clone();
        srt.sort_unstable();
        let bat = want.iter().map(|c| srt.binary_search(c).unwrap()).collect();
        parts.push(Part {
            raw: Vec::new(),
            outs,
            bat,
            masks: mask(&srt),
        });
    }
    parts
}

/// Resolve `want` (None = every column, in file order) against `set`.
pub fn select(
    set: &Set,
    want: Option<&[String]>,
    who: &str,
) -> Result<Sel, String> {
    let out: Vec<usize> = match want {
        None => (0..set.names.len()).collect(),
        Some(w) => w
            .iter()
            .map(|n| {
                set.names
                    .iter()
                    .position(|x| x == n)
                    .ok_or_else(|| format!("{who}: no column {n}"))
            })
            .collect::<Result<_, _>>()?,
    };
    let mut lts = Vec::with_capacity(out.len());
    for &c in &out {
        // Only a PROJECTED column has to have an L type: reading two
        // clean columns out of a file that also holds a list column is
        // exactly what projection is for.
        lts.push(set.lts[c].clone()?);
    }
    let mut uniq = out.clone();
    uniq.sort_unstable();
    uniq.dedup();
    if uniq.len() != out.len() {
        // Two result columns of the same name would build a table whose
        // key dict has a duplicate key — refuse rather than hand the
        // host an object whose lookups answer only the first one.
        return Err(format!("{who}: duplicate column"));
    }
    // A column is zero-copy only if every file stores it the same way
    // and stores it flat: a repeated column needs repetition levels,
    // which the raw path does not read (and parquet-rs refuses without),
    // and a NESTED one is not even at index `c` — `column(i)` is leaf
    // indexed, so a Struct ahead of this column would hand the decoder
    // one of the struct's own leaves and never say so.
    let raw: Vec<Option<Raw>> = out
        .iter()
        .map(|&c| {
            let mut r = None;
            for (i, s) in set.srcs.iter().enumerate() {
                let l = s.leaf.get(c).copied().flatten()?;
                let d = s.md.parquet_schema().column(l);
                // The WHOLE Raw has to match across the set, unit scale
                // included: files agree when they agree on the L type,
                // so a Timestamp(us) file can sit beside a
                // Timestamp(ns) one and their raw i64s do not mean the
                // same thing.  Comparing physical types alone let the
                // second file's microseconds be read as nanoseconds.
                let ri = raw_of(
                    s.md.schema().field(c).data_type(),
                    d.physical_type(),
                );
                r = if i == 0 || ri == r { ri } else { None };
                if d.max_rep_level() != 0 {
                    r = None;
                }
            }
            r
        })
        .collect();
    Ok(Sel { out, lts, uniq, raw })
}

// ── Decode pool ─────────────────────────────────────────────────────

/// One unit of decode work: the columns of part `p` of row group `g` of
/// file `f`, whose `rows` rows land at row `off` of every result column
/// the part owns.  Destinations are byte-disjoint across tasks — a
/// different row range, a different column, or both — which is what
/// lets the workers write into shared vectors with no synchronization.
struct Task {
    f: usize,
    g: usize,
    off: usize,
    rows: usize,
    p: usize,
}

/// Read global row groups `[g0, g1)` of `set`, columns `sel`, into a
/// fresh L table.  The window is in the same global numbering `pq_meta`
/// reports: files in argument order, row groups in file order.
pub fn read(
    set: &Set,
    sel: &Sel,
    g0: usize,
    g1: usize,
    codes: bool,
    who: &str,
) -> Result<K, String> {
    // Lay the window out first: every task's destination row is known
    // before a single page is touched, which is what lets the workers
    // write into shared vectors with no synchronization at all.
    let mut rgs: Vec<Rg> = Vec::new();
    let mut total = 0usize;
    let mut g = 0usize;
    for (f, s) in set.srcs.iter().enumerate() {
        for (i, &rows) in s.rg_rows.iter().enumerate() {
            if g >= g0 && g < g1 && rows > 0 {
                let rows = rows as usize;
                rgs.push((f, i, total, rows));
                total += rows;
            }
            g += 1;
        }
    }
    // A window of one or two row groups would otherwise decode on one
    // or two threads whatever the machine has — the small-batch case a
    // streaming caller lives in.  Split its COLUMNS instead, enough of
    // them to give every worker something, never more parts than there
    // are columns to read.
    let k = if rgs.is_empty() {
        1
    } else {
        (2 * nthreads()).div_ceil(rgs.len()).clamp(1, sel.uniq.len().max(1))
    };
    let parts = split(set, sel, k);
    let tasks: Vec<Task> = rgs
        .iter()
        .flat_map(|&(f, g, off, rows)| {
            (0..parts.len()).map(move |p| Task { f, g, off, rows, p })
        })
        .collect();
    if total > i32::MAX as usize {
        // One L vector is 2^31-bounded — refuse rather than truncate.
        return Err(format!("{who}: >2^31 rows"));
    }
    // The pair path is decided per COLUMN and before anything is
    // allocated: the width of a codes vector is a property of the union
    // dictionary, which the dictionary PAGES answer without reading a
    // single value.
    let mut cds: Vec<Option<Arc<Codes>>> = vec![None; sel.out.len()];
    if codes {
        for (p, c) in cds.iter_mut().enumerate() {
            if sel.lts[p] == KS {
                *c = codes_plan(set, &rgs, sel.out[p], who)?.map(Arc::new);
            }
        }
    }
    let nc = sel.out.len();
    unsafe {
        let names = ktn(KS as i32, nc as i64);
        let cols = ktn(0, nc as i64);
        let mut colv = Vec::with_capacity(nc);
        for (p, cd) in cds.iter().enumerate() {
            *v_s(names).add(p) = intern(set.names[sel.out[p]].as_bytes());
            // Insert each column into the list IMMEDIATELY so a later
            // error can free everything with one r0 of each container.
            let lt = match cd {
                Some(c) => c.lt,
                None => sel.lts[p],
            };
            let col = ktn(lt as i32, total as i64);
            *v_k(cols).add(p) = col;
            colv.push(col);
        }
        let gives: Vec<AtomicBool> =
            (0..nc).map(|_| AtomicBool::new(false)).collect();
        if let Err(e) =
            decode(set, sel, &parts, &colv, &tasks, &cds, &gives, who)
        {
            r0(names);
            r0(cols);
            return Err(e);
        }
        // A column that met a value its union never saw (a page that
        // fell back to PLAIN) is read AGAIN, as plain symbols.  Rare by
        // construction — and correct rather than clever.
        for p in 0..nc {
            if cds[p].is_some() && gives[p].load(Relaxed) {
                let one = Sel {
                    out: vec![sel.out[p]],
                    lts: vec![sel.lts[p]],
                    uniq: vec![sel.out[p]],
                    raw: vec![None],
                };
                let ks = ktn(KS as i32, total as i64);
                let ps = split(set, &one, 1);
                let ts: Vec<Task> = rgs
                    .iter()
                    .map(|&(f, g, off, rows)| Task { f, g, off, rows, p: 0 })
                    .collect();
                let no = [None];
                let ng = [AtomicBool::new(false)];
                let r = decode(
                    set, &one, &ps, &[ks], &ts, &no, &ng, who,
                );
                if let Err(e) = r {
                    r0(ks);
                    r0(names);
                    r0(cols);
                    return Err(e);
                }
                r0(colv[p]);
                *v_k(cols).add(p) = ks;
                colv[p] = ks;
                cds[p] = None;
            }
        }
        if !codes {
            return Ok(xT(xD(names, cols)));
        }
        // With `codes` on the answer is the column DICT, not a table: a
        // table cannot hold a column that is a 2-list, and the caller
        // asked for exactly that.  `flip` it when nothing paired.
        for p in 0..nc {
            if let Some(c) = &cds[p] {
                let d = ktn(KS as i32, c.d.len() as i64);
                for (i, &sym) in c.d.iter().enumerate() {
                    *v_s(d).add(i) = sym;
                }
                *v_k(cols).add(p) = klist(&[d, colv[p]]);
            }
        }
        Ok(xD(names, cols))
    }
}

/// Run `tasks` over one pool of workers pulling from a shared cursor.
/// Dynamic claiming, not a static split: row groups differ in rows and
/// in compressibility, and a static split would end at the slowest one.
#[allow(clippy::too_many_arguments)]
#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn decode(
    set: &Set,
    sel: &Sel,
    parts: &[Part],
    cols: &[K],
    tasks: &[Task],
    cds: &[Option<Arc<Codes>>],
    gives: &[AtomicBool],
    who: &str,
) -> Result<(), String> {
    if tasks.is_empty() {
        return Ok(());
    }
    let nthr = nthreads().min(tasks.len());
    let next = AtomicUsize::new(0);
    let stop = AtomicBool::new(false);
    let err: Mutex<Option<String>> = Mutex::new(None);
    // Every worker runs the SAME body and claims from one cursor: row
    // groups differ in rows and in compressibility, and a static split
    // would end at the slowest one.  The calling thread is one of them.
    let body = || {
        let mut sy = SymCache::new();
        let mut defs: Vec<i16> = Vec::new();
        while !stop.load(Relaxed) {
            let i = next.fetch_add(1, Relaxed);
            if i >= tasks.len() {
                break;
            }
            let t = &tasks[i];
            if let Err(e) = one(
                set, sel, &parts[t.p], cols, t, &mut sy, &mut defs, cds,
                gives, who,
            ) {
                stop.store(true, Relaxed);
                let mut g = err.lock().unwrap_or_else(|p| p.into_inner());
                g.get_or_insert(e);
                return;
            }
        }
    };
    let (_, panicked) = crate::pool::run(nthr - 1, &body, body);
    if let Some(e) = err.lock().unwrap_or_else(|p| p.into_inner()).take() {
        return Err(e);
    }
    if panicked {
        return Err(format!("{who}: worker panic"));
    }
    Ok(())
}

/// Decode one part of one row group into its row range of the shared
/// columns.
#[allow(clippy::too_many_arguments)]
#[cfg_attr(feature = "hotpath", hotpath::measure)]
fn one(
    set: &Set,
    sel: &Sel,
    part: &Part,
    cols: &[K],
    t: &Task,
    sy: &mut SymCache,
    defs: &mut Vec<i16>,
    cds: &[Option<Arc<Codes>>],
    gives: &[AtomicBool],
    who: &str,
) -> Result<(), String> {
    let src = &set.srcs[t.f];
    let f = File::open(&src.path).ctx(&format!("{who}: {}", src.path))?;
    // Columns whose bytes are already what L wants are decoded by the
    // page reader straight into the K payload — no Arrow array, no
    // allocation, no copy.
    if !part.raw.is_empty() {
        let fa = Arc::new(
            f.try_clone().ctx(&format!("{who}: {}", src.path))?,
        );
        for &p in &part.raw {
            // Leaf, not root: `select` proved every file has exactly
            // one leaf for this column, but not that it is at `out[p]`.
            let Some(leaf) = src.leaf.get(sel.out[p]).copied().flatten()
            else {
                return Err(format!("{who}: {}: column leaf", src.path));
            };
            let n = unsafe {
                raw_col(
                    src,
                    &fa,
                    t.g,
                    leaf,
                    sel.raw[p].expect("raw part"),
                    sel.lts[p],
                    cols[p],
                    t.off,
                    t.rows,
                    defs,
                    who,
                )?
            };
            if n != t.rows {
                return Err(format!(
                    "{who}: {}: row group {} delivered {} rows, footer \
                     says {}",
                    src.path, t.g, n, t.rows
                ));
            }
        }
        if part.outs.is_empty() {
            return Ok(());
        }
    }
    // new_with_metadata reuses the footer parsed at open time: a 1000
    // row-group read parses one footer per FILE, not per row group.
    let rdr = ParquetRecordBatchReaderBuilder::new_with_metadata(
        f,
        src.md.clone(),
    )
    .with_projection(part.masks[t.f].clone())
    .with_row_groups(vec![t.g])
    .with_batch_size(BATCH_ROWS)
    .build()
    .ctx(who)?;
    let mut off = t.off;
    for b in rdr {
        let b = b.ctx(who)?;
        for (i, &p) in part.outs.iter().enumerate() {
            let arr = b.column(part.bat[i]);
            unsafe {
                match &cds[p] {
                    Some(c) => sym_codes(arr, c, cols[p], off, sy, &gives[p])?,
                    None => fill_col(sel.lts[p], cols[p], off, arr, sy)?,
                }
            };
        }
        off += b.num_rows();
    }
    if off - t.off != t.rows {
        // arrow-rs CAPS the read at the footer's row count; it does not
        // guarantee it.  The whole window was laid out from those
        // counts, so a row group that delivers a different number has
        // either left this task's tail unwritten (a forged, inflated
        // count) or silently dropped rows (a deflated one).  Neither is
        // a table anyone should be handed: fail, and let the pool's
        // abort flag free the columns.  Nothing has to pre-fill the
        // unwritten tail first: read() r0's every column on this path
        // and the host frees a SYMBOL vector as bytes — it never
        // dereferences the elements — so the slots the decode never
        // reached are never read by anyone (asserted by test_w1.q's
        // forged-row-group cases, symbol column included).
        return Err(format!(
            "{who}: {}: row group {} delivered {} rows, footer says {}",
            src.path,
            t.g,
            off - t.off,
            t.rows
        ));
    }
    Ok(())
}
